#!/usr/bin/env node
/**
 * Production‑Ready Matching & Embedding Webhook Server
 *
 * FIXES (this pass):
 *   - ✅ FIX: Cache stampede — concurrent requests no longer trigger N parallel
 *            full-table job/company fetches. A single in-flight promise is
 *            shared and reused by all callers.
 *   - ✅ FIX: warmupCache() now fails fast — if the cache cannot be warmed
 *            after retries, the process exits instead of silently serving
 *            traffic against an empty cache (which was the real cause of
 *            the "statement timeout" / "Matching timeout" cascade).
 *   - ✅ FIX: set_config warning now logs once, not on every cache refresh.
 *   - ✅ FIX: Added env var validation (SUPABASE_URL) at startup.
 *
 * (carried over from previous pass)
 *   - apply_url uses job.apply_url ONLY
 *   - top_k parameter is respected
 *   - career_page_url returned separately
 *   - Improved logging for missing geolocation
 *   - Cache TTL and concurrency configurable via .env
 *   - Uses abortSignal for request timeouts
 *   - Sets statement_timeout via SQL before queries (requires the
 *     public.set_config() wrapper function — see migration SQL)
 */

const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const axios = require('axios');
const fs = require('fs');
const https = require('https');
const { v4: uuidv4 } = require('uuid');
require('dotenv').config();

// ─── Configuration ──────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
const HOST = process.env.HOST || '0.0.0.0';
const MAX_CONCURRENT_MATCHES = parseInt(process.env.MAX_CONCURRENT_MATCHES || '5', 10);
const JOB_CACHE_TTL_MS = parseInt(process.env.JOB_CACHE_TTL_MS || '600000', 10);
const TIMEOUT_MS = parseInt(process.env.TIMEOUT_MS || '120000', 10); // 120 seconds

// ─── Express app ──────────────────────────────────────────────────────
const app = express();
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

// ─── Supabase ────────────────────────────────────────────────────────
const serviceKey = process.env.SUPABASE_SERVICE_KEY;
const supabaseUrl = process.env.SUPABASE_URL;

if (!supabaseUrl) {
    console.error('❌ SUPABASE_URL is not set in .env');
    process.exit(1);
}
if (!serviceKey) {
    console.error('❌ SUPABASE_SERVICE_KEY is not set in .env');
    process.exit(1);
}
console.log(`🔑 Supabase service key loaded (starts with ${serviceKey.slice(0, 20)}...)`);

const supabase = createClient(
    supabaseUrl,
    serviceKey,
    { realtime: { transport: ws } }
);

// ─── Voyage AI ──────────────────────────────────────────────────────
const VOYAGE_API_KEY = process.env.VOYAGE_API_KEY;
const VOYAGE_URL = 'https://api.voyageai.com/v1/embeddings';
const VOYAGE_MODEL = process.env.VOYAGE_MODEL || 'voyage-3-large';

// ─── Custom Semaphore ──────────────────────────────────────────────
class Semaphore {
    constructor(limit) {
        this.limit = limit;
        this.running = 0;
        this.queue = [];
    }
    async acquire() {
        if (this.running < this.limit) {
            this.running++;
            return;
        }
        return new Promise((resolve) => {
            this.queue.push(resolve);
        });
    }
    release() {
        if (this.queue.length > 0) {
            const resolve = this.queue.shift();
            resolve();
        } else {
            this.running--;
        }
    }
    async run(fn) {
        await this.acquire();
        try {
            return await fn();
        } finally {
            this.release();
        }
    }
}

const matchSemaphore = new Semaphore(MAX_CONCURRENT_MATCHES);

// ─── Helper: Set statement_timeout via SQL ──────────────────────────
// NOTE: requires a public.set_config(text, text, boolean) wrapper function
// in Supabase (Postgres does not expose the built-in set_config via RPC by
// default). See migration SQL. We only warn about this ONCE, not on every
// cache refresh, so logs don't get spammed.
let statementTimeoutWarned = false;
async function setStatementTimeout(seconds = 180) {
    try {
        const { error } = await supabase.rpc('set_config', {
            setting_name: 'statement_timeout',
            setting_value: `${seconds}s`,
            is_local: true
        });
        if (error) {
            if (!statementTimeoutWarned) {
                console.warn(`⚠️ Could not set statement_timeout: ${error.message}`);
                console.warn(`   Run the public.set_config() migration SQL to fix this permanently.`);
                statementTimeoutWarned = true;
            }
        } else {
            console.log(`✅ Statement timeout set to ${seconds}s`);
        }
    } catch (e) {
        if (!statementTimeoutWarned) {
            console.warn(`⚠️ set_config RPC not available: ${e.message}`);
            statementTimeoutWarned = true;
        }
    }
}

// ─── Job Cache ────────────────────────────────────────────────────────
let cachedJobs = null;
let jobsCacheTimestamp = 0;
let jobsFetchInFlight = null; // ✅ FIX: dedupe concurrent fetches

async function fetchJobsFromSupabase() {
    const start = Date.now();
    console.log(`⏳ Fetching jobs from Supabase...`);

    // NOTE: this only sets statement_timeout for *this* RPC call's own
    // transaction — PostgREST gives every REST call its own connection, so
    // it does NOT carry over to the .select() calls below. Real fix is
    // `alter role service_role set statement_timeout = '300s';` in the
    // Supabase SQL editor. We still call this for defense-in-depth.
    await setStatementTimeout(180);

    const jobs = [];
    const PAGE_SIZE = 1000;
    let lastId = null; // ✅ FIX: keyset pagination instead of OFFSET/range

    while (true) {
        let query = supabase
            .from('jobs')
            .select(
                'id, title, company_id, company_name, apply_url, location, location_lat, location_lng, remote_type, seniority_level, structured_skills, skill_embedding'
            )
            .not('skill_embedding', 'is', null)
            .order('id', { ascending: true })
            .limit(PAGE_SIZE)
            .abortSignal(AbortSignal.timeout(120000));

        // ✅ FIX: seek from last seen id instead of OFFSET — avoids Postgres
        // having to walk/skip all previous rows on every page, which is
        // what was causing later pages (offset 5000+) to time out.
        if (lastId !== null) {
            query = query.gt('id', lastId);
        }

        const { data, error } = await query;

        if (error) {
            console.error(`❌ Supabase query error (after id ${lastId}):`, error);
            throw error;
        }
        if (!data || data.length === 0) break;
        jobs.push(...data);
        lastId = data[data.length - 1].id;
        if (data.length < PAGE_SIZE) break;
    }

    console.log(`✅ Loaded ${jobs.length} jobs in ${Date.now() - start}ms`);
    return jobs;
}

async function getCachedJobs() {
    const now = Date.now();
    if (cachedJobs && (now - jobsCacheTimestamp) < JOB_CACHE_TTL_MS) {
        return cachedJobs;
    }

    // ✅ FIX: if a fetch is already running, await it instead of starting
    // another parallel full-table scan (this was causing the statement
    // timeouts under concurrent requests).
    if (jobsFetchInFlight) {
        return jobsFetchInFlight;
    }

    jobsFetchInFlight = (async () => {
        try {
            const jobs = await fetchJobsFromSupabase();
            cachedJobs = jobs;
            jobsCacheTimestamp = Date.now();
            return jobs;
        } finally {
            jobsFetchInFlight = null;
        }
    })();

    return jobsFetchInFlight;
}

// ─── Companies Cache ──────────────────────────────────────────────────
let companiesMap = null;
let companiesCacheTimestamp = 0;
let companiesFetchInFlight = null; // ✅ FIX: dedupe concurrent fetches

async function fetchCompaniesFromSupabase() {
    const start = Date.now();
    console.log(`⏳ Fetching companies...`);

    const { data, error } = await supabase
        .from('companies')
        .select('"Id", "Name", "detected_career_url", "last_crawled_at", "crawl_status"')
        .abortSignal(AbortSignal.timeout(60000));

    if (error) {
        console.error('❌ Companies fetch error:', error);
        throw error;
    }

    const byId = new Map();
    const byName = new Map();
    data.forEach(c => {
        byId.set(c.Id, {
            Name: c.Name,
            career_page_url: c.detected_career_url || null,
            last_crawled_at: c.last_crawled_at || null,
            crawl_status: c.crawl_status || null
        });
        const key = c.Name.toLowerCase().trim();
        if (!byName.has(key)) byName.set(key, c);
    });

    console.log(`✅ Loaded ${data.length} companies in ${Date.now() - start}ms`);
    return { byId, byName };
}

async function getCompaniesMap() {
    const now = Date.now();
    if (companiesMap && (now - companiesCacheTimestamp) < JOB_CACHE_TTL_MS) {
        return companiesMap;
    }

    if (companiesFetchInFlight) {
        return companiesFetchInFlight;
    }

    companiesFetchInFlight = (async () => {
        try {
            const map = await fetchCompaniesFromSupabase();
            companiesMap = map;
            companiesCacheTimestamp = Date.now();
            return map;
        } finally {
            companiesFetchInFlight = null;
        }
    })();

    return companiesFetchInFlight;
}

// ─── Warm‑up function ──────────────────────────────────────────────────
// ✅ FIX: now returns a boolean success flag. startServer() will refuse to
// accept traffic if warm-up never succeeds — previously it started the
// HTTPS server regardless, which meant every incoming request had to do
// its own cold full-table fetch (the real cause of the timeout cascade).
async function warmupCache(retries = 3) {
    console.log('🔥 Warming up cache...');
    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            await getCachedJobs();
            await getCompaniesMap();
            console.log('✅ Cache warm‑up complete.');
            return true;
        } catch (err) {
            console.error(`❌ Cache warm‑up attempt ${attempt} failed:`, err.message);
            if (attempt < retries) {
                console.log(`⏳ Retrying in 5s...`);
                await new Promise(r => setTimeout(r, 5000));
            }
        }
    }
    return false;
}

// ─── Helpers ──────────────────────────────────────────────────────────
function parseEmbedding(embedding) {
    if (!embedding) return null;
    if (Array.isArray(embedding)) return embedding;
    if (typeof embedding === 'string') {
        try { return JSON.parse(embedding); } catch { return null; }
    }
    return null;
}

function cosineSimilarity(vecA, vecB) {
    const a = parseEmbedding(vecA);
    const b = parseEmbedding(vecB);
    if (!a || !b || a.length !== b.length) return 0;
    let dot = 0, magA = 0, magB = 0;
    for (let i = 0; i < a.length; i++) {
        dot += a[i] * b[i];
        magA += a[i] * a[i];
        magB += b[i] * b[i];
    }
    if (magA === 0 || magB === 0) return 0;
    const sim = dot / (Math.sqrt(magA) * Math.sqrt(magB));
    return isNaN(sim) ? 0 : Math.max(0, Math.min(sim, 1));
}

function haversine(lat1, lon1, lat2, lon2) {
    if (!lat1 || !lon1 || !lat2 || !lon2) return Infinity;
    const R = 6371;
    const toRad = deg => deg * Math.PI / 180;
    const dLat = toRad(lat2 - lat1);
    const dLon = toRad(lon2 - lon1);
    const a = Math.sin(dLat/2) ** 2 +
              Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
              Math.sin(dLon/2) ** 2;
    return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function citiesMatch(cityA, cityB) {
    if (!cityA || !cityB) return false;
    const a = cityA.toLowerCase().trim();
    const b = cityB.toLowerCase().trim();
    if (a === b) return true;
    return a.includes(b) || b.includes(a);
}

function candidateToText(candidate) {
    const parts = [];
    if (candidate.name) parts.push(`Role: ${candidate.name}`);
    if (candidate.skill_scores && typeof candidate.skill_scores === 'object') {
        const entries = Object.entries(candidate.skill_scores);
        if (entries.length > 0) {
            const skillsText = entries.map(([skill, score]) => `${skill} (${score}/5)`).join(', ');
            parts.push(`Skills: ${skillsText}`);
        }
    }
    if (candidate.seniority_level) parts.push(`Seniority: ${candidate.seniority_level}`);
    if (candidate.remote_preference) parts.push(`Remote: ${candidate.remote_preference}`);
    if (candidate.location) parts.push(`Location: ${candidate.location}`);
    return parts.join('. ') || 'No data available';
}

async function embedCandidate(candidate) {
    const text = candidateToText(candidate);
    if (!text || text.length < 5) {
        throw new Error('No valid text to embed');
    }
    try {
        const response = await axios.post(VOYAGE_URL, {
            model: VOYAGE_MODEL,
            input: [text]
        }, {
            headers: {
                'Authorization': `Bearer ${VOYAGE_API_KEY}`,
                'Content-Type': 'application/json'
            },
            timeout: 60000
        });
        return response.data.data[0].embedding;
    } catch (err) {
        throw new Error(`Voyage AI error: ${err.message}`);
    }
}

function extractCompanyNameFromTitle(title) {
    if (!title) return null;
    const patterns = [
        /[—–\-]\s*(.+?)(?:\s*\(|$)/,
        /\|\s*(.+?)(?:\s*\(|$)/,
        /–\s*(.+?)(?:\s*\(|$)/,
        /bei\s+(.+?)(?:\s*\(|$)/i,
        /(?:für|an|mit)\s+(.+?)(?:\s*\(|$)/i,
    ];
    for (const pattern of patterns) {
        const match = title.match(pattern);
        if (match && match[1]) {
            let name = match[1].trim();
            if (name.length > 2 && name.length < 60) return name;
        }
    }
    const companyWords = ['GmbH', 'AG', 'KG', 'SE', 'e.V.', 'UG', 'GbR', 'OHG'];
    const parts = title.split(/[—–\-|\/]/);
    for (const part of parts.reverse()) {
        const trimmed = part.trim();
        if (companyWords.some(w => trimmed.includes(w))) {
            return trimmed;
        }
    }
    return null;
}

// ─── Matching function ────────────────────────────────────────────────
async function matchCandidate(candidateData, radius, topK = 10) {
    const { salesforce_contact_id, name, skill_scores, seniority_level, remote_preference, location, location_lat, location_lng } = candidateData;

    // 1. Check/update candidate in Supabase
    let { data: existingCandidate, error: fetchError } = await supabase
        .from('candidates')
        .select('id, skill_embedding')
        .eq('salesforce_contact_id', salesforce_contact_id)
        .single()
        .abortSignal(AbortSignal.timeout(30000));

    let candidateId;
    if (fetchError && fetchError.code !== 'PGRST116') {
        throw new Error(`Supabase fetch error: ${fetchError.message}`);
    }

    if (existingCandidate) {
        candidateId = existingCandidate.id;
        await supabase
            .from('candidates')
            .update({
                name,
                skill_scores,
                seniority_level,
                remote_preference,
                location,
                location_lat: location_lat || null,
                location_lng: location_lng || null,
                updated_at: new Date().toISOString()
            })
            .eq('id', candidateId)
            .abortSignal(AbortSignal.timeout(30000));
    } else {
        const { data: newCandidate, error: insertError } = await supabase
            .from('candidates')
            .insert({
                salesforce_contact_id,
                name,
                skill_scores,
                seniority_level,
                remote_preference,
                location,
                location_lat: location_lat || null,
                location_lng: location_lng || null,
                created_at: new Date().toISOString(),
                updated_at: new Date().toISOString()
            })
            .select('id')
            .single()
            .abortSignal(AbortSignal.timeout(30000));
        if (insertError) throw new Error(`Supabase insert error: ${insertError.message}`);
        candidateId = newCandidate.id;
    }

    // 2. Embed candidate if needed
    let skillEmbedding = existingCandidate?.skill_embedding;
    if (!skillEmbedding) {
        const candidateObj = { name, skill_scores, seniority_level, remote_preference, location };
        skillEmbedding = await embedCandidate(candidateObj);
        await supabase
            .from('candidates')
            .update({ skill_embedding: skillEmbedding, updated_at: new Date().toISOString() })
            .eq('id', candidateId)
            .abortSignal(AbortSignal.timeout(30000));
    } else {
        skillEmbedding = parseEmbedding(skillEmbedding);
    }
    if (!skillEmbedding) throw new Error('Invalid candidate embedding');

    // 3. Fetch jobs and companies from cache (now stampede-safe)
    const jobs = await getCachedJobs();
    const companies = await getCompaniesMap();

    // 4. Match against jobs
    const startMatch = Date.now();
    const matches = [];
    let missingGeolocationCount = 0;

    for (const job of jobs) {
        const sim = cosineSimilarity(skillEmbedding, job.skill_embedding);
        if (sim < 0.01) continue;

        const isRemote = job.remote_type && job.remote_type.toLowerCase() === 'remote';
        let include = false;
        let distance = null;

        if (location_lat && location_lng && job.location_lat && job.location_lng) {
            distance = haversine(location_lat, location_lng, job.location_lat, job.location_lng);
        } else {
            missingGeolocationCount++;
            if (missingGeolocationCount <= 5) {
                console.warn(`⚠️ Missing location data for job "${job.title}" (id: ${job.id}) or candidate. Distance will be NULL.`);
            }
        }

        if (isRemote) {
            include = true;
        } else if (distance !== null && distance <= radius) {
            include = true;
        } else if (distance === null && location && job.location) {
            if (citiesMatch(location, job.location)) include = true;
        } else if (distance === null && !location && !job.location) {
            include = true;
        }

        if (!include) continue;

        let companyName = job.company_name || null;
        let careerPageUrl = null;
        let lastCrawledAt = null;
        let crawlStatus = null;

        if (job.company_id) {
            const companyInfo = companies.byId.get(job.company_id);
            if (companyInfo) {
                if (!companyName) companyName = companyInfo.Name || null;
                careerPageUrl = companyInfo.career_page_url || null;
                lastCrawledAt = companyInfo.last_crawled_at || null;
                crawlStatus = companyInfo.crawl_status || null;
            }
        }

        if (!companyName) {
            const extracted = extractCompanyNameFromTitle(job.title);
            if (extracted) {
                const key = extracted.toLowerCase().trim();
                let matched = companies.byName.get(key);
                if (!matched) {
                    const cleanKey = key.replace(/\s*(gmbh|ag|kg|se|e\.v\.|ug|gbr|ohg)\s*$/, '').trim();
                    if (cleanKey !== key) matched = companies.byName.get(cleanKey);
                }
                if (matched) {
                    companyName = matched.Name;
                    careerPageUrl = careerPageUrl || matched.detected_career_url || null;
                    lastCrawledAt = lastCrawledAt || matched.last_crawled_at || null;
                    crawlStatus = crawlStatus || matched.crawl_status || null;
                    console.log(`   🔍 Fallback name match: "${companyName}" for job "${job.title}"`);
                }
            }
        }

        const applyUrl = job.apply_url || null;
        const topSkills = (job.structured_skills || []).slice(0, 5).join(', ');

        matches.push({
            job_id: job.id,
            job_title: job.title || 'Untitled',
            company_id: job.company_id,
            company_name: companyName,
            apply_url: applyUrl,
            career_page_url: careerPageUrl,
            last_crawled_at: lastCrawledAt,
            crawl_status: crawlStatus,
            location: job.location,
            remote_type: job.remote_type,
            seniority_level: job.seniority_level,
            top_skills: topSkills,
            similarity_score: sim,
            location_distance_km: distance,
            final_score: Math.round(sim * 10000) / 100
        });
    }

    if (missingGeolocationCount > 0) {
        console.warn(`⚠️ ${missingGeolocationCount} jobs had missing location coordinates. Distance filter may be inaccurate.`);
        console.warn(`   Please run geocoding scripts: node scripts/geocode-jobs.js and node scripts/geocode-candidates.js`);
    }

    matches.sort((a, b) => b.similarity_score - a.similarity_score);
    const topMatches = matches.slice(0, topK);
    console.log(`⏱️ Matching loop took ${Date.now() - startMatch}ms for ${jobs.length} jobs`);

    if (topMatches.length > 0) {
        await supabase
            .from('matches')
            .delete()
            .eq('candidate_id', candidateId)
            .abortSignal(AbortSignal.timeout(30000));

        const rows = topMatches.map(m => ({
            candidate_id: candidateId,
            job_id: m.job_id,
            similarity_score: Math.round(m.similarity_score * 10000) / 100,
            location_distance_km: m.location_distance_km,
            final_score: m.final_score,
            company_name: m.company_name,
            apply_url: m.apply_url || null,
            career_page_url: m.career_page_url || null,
            job_title: m.job_title,
            job_location: m.location,
            remote_type: m.remote_type,
            seniority_level: m.seniority_level,
            top_skills: m.top_skills,
            created_at: new Date().toISOString()
        }));
        await supabase.from('matches').insert(rows);
    }

    return {
        candidate_id: candidateId,
        salesforce_contact_id,
        candidate_name: name,
        matches_count: topMatches.length,
        matches: topMatches
    };
}

// ─── Webhook handler ──────────────────────────────────────────────────
app.post('/webhook/match-candidate', async (req, res) => {
    const startTime = Date.now();
    const requestId = uuidv4();
    console.log(`[${requestId}] Received request`);

    try {
        const {
            salesforce_contact_id,
            name,
            skill_scores,
            seniority_level,
            remote_preference,
            location,
            location_lat,
            location_lng,
            radius = 50,
            top_k = 10
        } = req.body;

        if (!salesforce_contact_id) {
            throw new Error('Missing required field: salesforce_contact_id');
        }

        if (!skill_scores || typeof skill_scores !== 'object' || Object.keys(skill_scores).length === 0) {
            console.warn(`[${requestId}] Warning: skill_scores empty for ${salesforce_contact_id}`);
        }

        const matchPromise = matchSemaphore.run(() => matchCandidate({
            salesforce_contact_id,
            name,
            skill_scores,
            seniority_level,
            remote_preference,
            location,
            location_lat,
            location_lng
        }, radius, top_k));

        const result = await Promise.race([
            matchPromise,
            new Promise((_, reject) => setTimeout(() => reject(new Error('Matching timeout')), TIMEOUT_MS))
        ]);

        const elapsed = Date.now() - startTime;
        console.log(`[${requestId}] Completed in ${elapsed}ms, found ${result.matches_count} matches`);

        res.status(200).json({
            success: true,
            ...result,
            message: `Successfully matched candidate with ${result.matches_count} job(s)`,
            timestamp: new Date().toISOString()
        });

    } catch (err) {
        const elapsed = Date.now() - startTime;
        console.error(`[${requestId}] Error after ${elapsed}ms: ${err.message}`);
        res.status(500).json({
            success: false,
            error: err.message,
            timestamp: new Date().toISOString()
        });
    }
});

// ─── Health check ─────────────────────────────────────────────────────
app.get('/health', (req, res) => {
    res.status(200).json({
        status: 'ok',
        cache_warm: !!cachedJobs,
        jobs_cached: cachedJobs ? cachedJobs.length : 0,
        timestamp: new Date().toISOString()
    });
});

// ─── Start HTTPS server ──────────────────────────────────────────────
const SSL_KEY_PATH = process.env.SSL_KEY || '/etc/ssl/private/server.key';
const SSL_CERT_PATH = process.env.SSL_CERT || '/etc/ssl/certs/server.crt';

let useHttps = false;
try {
    if (fs.existsSync(SSL_KEY_PATH) && fs.existsSync(SSL_CERT_PATH)) {
        useHttps = true;
    }
} catch (e) { /* ignore */ }

async function startServer() {
    // ✅ FIX: fail fast instead of starting the server against a cold/empty
    // cache — that was the actual trigger for the statement-timeout cascade.
    const warmed = await warmupCache();
    if (!warmed) {
        console.error('❌ Cache warm-up failed after all retries. Exiting so PM2 can restart cleanly.');
        console.error('   Check SUPABASE_URL / SUPABASE_SERVICE_KEY in .env and RLS grants on jobs/candidates.');
        process.exit(1);
    }

    if (useHttps) {
        const options = {
            key: fs.readFileSync(SSL_KEY_PATH),
            cert: fs.readFileSync(SSL_CERT_PATH)
        };
        https.createServer(options, app).listen(PORT, HOST, () => {
            console.log(`🔒 HTTPS server running on https://${HOST}:${PORT}`);
            console.log(`📍 POST to https://${HOST}:${PORT}/webhook/match-candidate`);
            console.log(`💚 Health check: https://${HOST}:${PORT}/health`);
            console.log(`⚡ Concurrency limit: ${MAX_CONCURRENT_MATCHES}`);
            console.log(`⏳ Job cache TTL: ${JOB_CACHE_TTL_MS/1000}s`);
            console.log(`⏱️ Request timeout: ${TIMEOUT_MS/1000}s`);
        });
    } else {
        app.listen(PORT, HOST, () => {
            console.log(`🚀 HTTP server running on http://${HOST}:${PORT}`);
            console.log(`📍 POST to http://${HOST}:${PORT}/webhook/match-candidate`);
            console.log(`💚 Health check: http://${HOST}:${PORT}/health`);
            console.log(`⚡ Concurrency limit: ${MAX_CONCURRENT_MATCHES}`);
            console.log(`⏳ Job cache TTL: ${JOB_CACHE_TTL_MS/1000}s`);
            console.log(`⏱️ Request timeout: ${TIMEOUT_MS/1000}s`);
        });
    }
}

startServer().catch(err => {
    console.error('Fatal startup error:', err);
    process.exit(1);
});
