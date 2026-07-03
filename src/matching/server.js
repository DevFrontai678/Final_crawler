#!/usr/bin/env node
/**
 * Production‑Ready Matching & Embedding Webhook Server
 * 
 * Features:
 *   - Cache pre‑warming on startup (jobs and companies)
 *   - Concurrent request handling with custom semaphore
 *   - 60‑second timeout per request (adjustable via TIMEOUT_MS)
 *   - Detailed logging of each step
 *   - Returns career_page_url, last_crawled_at, crawl_status for each match
 *   - Improved company lookup by name (fallback when company_id missing)
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
const JOB_CACHE_TTL_MS = parseInt(process.env.JOB_CACHE_TTL_MS || '300000', 10);
const TIMEOUT_MS = parseInt(process.env.TIMEOUT_MS || '60000', 10);

// ─── Express app ──────────────────────────────────────────────────────
const app = express();
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

// ─── Supabase ────────────────────────────────────────────────────────
const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

// ─── Voyage AI ──────────────────────────────────────────────────────
const VOYAGE_API_KEY = process.env.VOYAGE_API_KEY;
const VOYAGE_URL = 'https://api.voyageai.com/v1/embeddings';
const VOYAGE_MODEL = 'voyage-3-large';

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

// ─── Job Cache ────────────────────────────────────────────────────────
let cachedJobs = null;
let jobsCacheTimestamp = 0;

async function getCachedJobs() {
    const now = Date.now();
    if (cachedJobs && (now - jobsCacheTimestamp) < JOB_CACHE_TTL_MS) {
        return cachedJobs;
    }
    const start = Date.now();
    console.log(`⏳ Fetching jobs from Supabase...`);
    const jobs = [];
    let page = 0;
    const PAGE_SIZE = 1000;
    let hasMore = true;
    while (hasMore) {
        const startRow = page * PAGE_SIZE;
        const endRow = startRow + PAGE_SIZE - 1;
        const { data, error } = await supabase
            .from('jobs')
            .select('id, title, company_id, location, location_lat, location_lng, remote_type, seniority_level, structured_skills, skill_embedding')
            .not('skill_embedding', 'is', null)
            .order('id', { ascending: true })
            .range(startRow, endRow);
        if (error) throw error;
        if (!data || data.length === 0) break;
        jobs.push(...data);
        page++;
        if (data.length < PAGE_SIZE) break;
    }
    cachedJobs = jobs;
    jobsCacheTimestamp = now;
    console.log(`✅ Loaded ${jobs.length} jobs in ${Date.now() - start}ms`);
    return jobs;
}

// ─── Companies Cache ──────────────────────────────────────────────────
let companiesMap = null;
let companiesCacheTimestamp = 0;

async function getCompaniesMap() {
    const now = Date.now();
    if (companiesMap && (now - companiesCacheTimestamp) < JOB_CACHE_TTL_MS) {
        return companiesMap;
    }
    const start = Date.now();
    console.log(`⏳ Fetching companies...`);
    const { data, error } = await supabase
        .from('companies')
        .select('"Id", "Name", "detected_career_url", "last_crawled_at", "crawl_status"');
    if (error) throw error;
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
    companiesMap = { byId, byName };
    companiesCacheTimestamp = now;
    console.log(`✅ Loaded ${data.length} companies in ${Date.now() - start}ms`);
    return companiesMap;
}

// ─── Warm‑up function ──────────────────────────────────────────────────
async function warmupCache() {
    console.log('🔥 Warming up cache...');
    try {
        await getCachedJobs();
        await getCompaniesMap();
        console.log('✅ Cache warm‑up complete.');
    } catch (err) {
        console.error('❌ Cache warm‑up failed:', err.message);
    }
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
async function matchCandidate(candidateData, radius) {
    const { salesforce_contact_id, name, skill_scores, seniority_level, remote_preference, location, location_lat, location_lng } = candidateData;

    // 1. Check/update candidate in Supabase
    let { data: existingCandidate, error: fetchError } = await supabase
        .from('candidates')
        .select('id, skill_embedding')
        .eq('salesforce_contact_id', salesforce_contact_id)
        .single();

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
            .eq('id', candidateId);
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
            .single();
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
            .eq('id', candidateId);
    } else {
        skillEmbedding = parseEmbedding(skillEmbedding);
    }
    if (!skillEmbedding) throw new Error('Invalid candidate embedding');

    // 3. Fetch jobs and companies from cache (will be fast after warm‑up)
    const jobs = await getCachedJobs();
    const companies = await getCompaniesMap();

    // 4. Match against jobs
    const startMatch = Date.now();
    const matches = [];
    for (const job of jobs) {
        const sim = cosineSimilarity(skillEmbedding, job.skill_embedding);
        if (sim < 0.01) continue;

        const isRemote = job.remote_type && job.remote_type.toLowerCase() === 'remote';
        let include = false;
        let distance = null;

        let candidateLat = location_lat;
        let candidateLng = location_lng;
        let jobLat = job.location_lat;
        let jobLng = job.location_lng;

        if (candidateLat && candidateLng && jobLat && jobLng) {
            distance = haversine(candidateLat, candidateLng, jobLat, jobLng);
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

        // ─── Get company details (improved) ────────────────────────
        let companyInfo = companies.byId.get(job.company_id);
        let companyName = companyInfo?.Name || null;
        let careerPageUrl = companyInfo?.career_page_url || null;
        let lastCrawledAt = companyInfo?.last_crawled_at || null;
        let crawlStatus = companyInfo?.crawl_status || null;

        // If not found by ID, try by extracted name
        if (!companyName) {
            const extracted = extractCompanyNameFromTitle(job.title);
            if (extracted) {
                const key = extracted.toLowerCase().trim();
                let matched = companies.byName.get(key);
                if (matched) {
                    companyName = matched.Name;
                    careerPageUrl = matched.detected_career_url || null;
                    lastCrawledAt = matched.last_crawled_at || null;
                    crawlStatus = matched.crawl_status || null;
                    console.log(`   🔍 Found company by name: "${companyName}" for job "${job.title}"`);
                } else {
                    // Try partial match (remove GmbH etc.)
                    const cleanKey = key.replace(/\s*(gmbh|ag|kg|se|e\.v\.|ug|gbr|ohg)\s*$/, '').trim();
                    if (cleanKey !== key) {
                        const partialMatch = companies.byName.get(cleanKey);
                        if (partialMatch) {
                            companyName = partialMatch.Name;
                            careerPageUrl = partialMatch.detected_career_url || null;
                            lastCrawledAt = partialMatch.last_crawled_at || null;
                            crawlStatus = partialMatch.crawl_status || null;
                            console.log(`   🔍 Found company by partial match: "${companyName}" for job "${job.title}"`);
                        }
                    }
                }
            }
        }

        const topSkills = (job.structured_skills || []).slice(0, 5).join(', ');
        matches.push({
            job_id: job.id,
            job_title: job.title || 'Untitled',
            company_id: job.company_id,
            company_name: companyName,
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

    matches.sort((a, b) => b.similarity_score - a.similarity_score);
    const topMatches = matches.slice(0, 10);
    console.log(`⏱️ Matching loop took ${Date.now() - startMatch}ms for ${jobs.length} jobs`);

    // 5. Store matches (optional, but keep)
    if (topMatches.length > 0) {
        await supabase
            .from('matches')
            .delete()
            .eq('candidate_id', candidateId);

        const rows = topMatches.map(m => ({
            candidate_id: candidateId,
            job_id: m.job_id,
            similarity_score: Math.round(m.similarity_score * 10000) / 100,
            location_distance_km: m.location_distance_km,
            final_score: m.final_score,
            company_name: m.company_name,
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
        }, radius));

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
    res.status(200).json({ status: 'ok', timestamp: new Date().toISOString() });
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
    await warmupCache();

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
