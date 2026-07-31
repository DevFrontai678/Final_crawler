#!/usr/bin/env node
/**
 * Production‑Ready Matching & Embedding Webhook Server
 *
 * FIXES:
 *   - PAGE_SIZE = 2000 (faster per‑query, avoids Cloudflare 522 timeout)
 *   - Retry logic (3 attempts per page) with 2s delay
 *   - Uses Set for candidate skills (O(1) lookup)
 *   - Early skip for missing company / garbage titles
 *   - Converts top_skills to array for Supabase insert
 *   - Default top_k = 30 matches
 *   - Only uses columns that exist in matches table
 *   - Continues even if cache warm‑up fails
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
const TIMEOUT_MS = parseInt(process.env.TIMEOUT_MS || '60000', 10);

const MIN_SIMILARITY = 0.25;
const REMOTE_MIN_SIMILARITY = 0.30;
const TITLE_BONUS = 0.10;
const SKILL_WEIGHT = 0.35;
const MIN_SKILL_OVERLAP = 0.50;

// ─── Garbage title patterns ────────────────────────────────────────────
const GARBAGE_TITLE_PATTERNS = [
    /karriere/i, /career/i, /great to have you here/i, /super, dass du hier bist/i,
    /wir suchen dich/i, /initiativbewerbung/i, /are you looking for new challenges/i,
    /jobs at/i, /dein traumjob/i, /willkommen in ihrer zukunft/i,
    /bewerbungsprozess/i, /neustart/i, /karriere -/i, /career -/i,
    /stellenangebote/i, /job offers/i, /join/i, /career opportunities/i,
    /work with us/i, /come join us/i, /offene stellen/i, /jobs/i,
    /stellenangebot/i, /karriere bei/i, /join us/i, /open positions/i,
    /join our team/i, /careers/i
];

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
const VOYAGE_MODEL = process.env.VOYAGE_MODEL || 'voyage-3-large';

// ─── Semaphore ──────────────────────────────────────────────────────
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
        return new Promise((resolve) => this.queue.push(resolve));
    }
    release() {
        if (this.queue.length > 0) {
            this.queue.shift()();
        } else {
            this.running--;
        }
    }
    async run(fn) {
        await this.acquire();
        try { return await fn(); } finally { this.release(); }
    }
}
const matchSemaphore = new Semaphore(MAX_CONCURRENT_MATCHES);

// ─── Job Cache (with retry & smaller PAGE_SIZE) ────────────────────
let cachedJobs = null;
let jobsCacheTimestamp = 0;

async function getCachedJobs() {
    const now = Date.now();
    if (cachedJobs && (now - jobsCacheTimestamp) < JOB_CACHE_TTL_MS) {
        return cachedJobs;
    }
    const start = Date.now();
    console.log(`⏳ Fetching jobs (PAGE_SIZE=2000, with retries)...`);
    const jobs = [];
    let page = 0;
    const PAGE_SIZE = 2000; // ⬇️ Smaller = faster per-query
    const MAX_RETRIES = 3;
    const RETRY_DELAY = 2000; // 2 seconds

    while (true) {
        const startRow = page * PAGE_SIZE;
        const endRow = startRow + PAGE_SIZE - 1;
        let success = false;
        let data = null;
        let error = null;

        for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                console.log(`   📄 Page ${page + 1} (rows ${startRow}-${endRow}) attempt ${attempt}...`);
                const result = await supabase
                    .from('jobs')
                    .select(
                        'id, title, company_id, company_name, apply_url, location, location_lat, location_lng, remote_type, seniority_level, structured_skills, skill_embedding'
                    )
                    .not('skill_embedding', 'is', null)
                    .order('id', { ascending: true })
                    .range(startRow, endRow);

                if (result.error) {
                    error = result.error;
                    console.warn(`   ⚠️ Attempt ${attempt} failed: ${result.error.message}`);
                    if (attempt < MAX_RETRIES) {
                        console.log(`   ⏳ Waiting ${RETRY_DELAY}ms before retry...`);
                        await new Promise(r => setTimeout(r, RETRY_DELAY));
                    }
                    continue;
                }

                data = result.data;
                success = true;
                break;
            } catch (err) {
                error = err;
                console.warn(`   ⚠️ Attempt ${attempt} error: ${err.message}`);
                if (attempt < MAX_RETRIES) {
                    console.log(`   ⏳ Waiting ${RETRY_DELAY}ms before retry...`);
                    await new Promise(r => setTimeout(r, RETRY_DELAY));
                }
            }
        }

        if (!success) {
            console.error(`❌ Failed to fetch page ${page + 1} after ${MAX_RETRIES} attempts:`, error?.message || 'Unknown error');
            // Instead of throwing, break and use what we have
            break;
        }

        if (!data || data.length === 0) break;
        jobs.push(...data);
        page++;
        if (data.length < PAGE_SIZE) break;
    }

    cachedJobs = jobs;
    jobsCacheTimestamp = now;
    console.log(`✅ Loaded ${cachedJobs.length} jobs in ${Date.now() - start}ms`);
    return cachedJobs;
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

async function warmupCache() {
    console.log('🔥 Warming up cache...');
    try {
        await getCachedJobs();
        await getCompaniesMap();
        console.log('✅ Cache warm‑up complete.');
    } catch (err) {
        console.error('❌ Cache warm‑up failed:', err.message);
        console.log('⚠️ Server will load jobs on first request instead.');
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
    return a === b || a.includes(b) || b.includes(a);
}

function candidateToText(candidate) {
    const parts = [];
    if (candidate.name) parts.push(`Role: ${candidate.name}`);
    if (candidate.skill_scores && typeof candidate.skill_scores === 'object') {
        const entries = Object.entries(candidate.skill_scores);
        if (entries.length > 0) {
            parts.push(`Skills: ${entries.map(([s, score]) => `${s} (${score}/5)`).join(', ')}`);
        }
    }
    if (candidate.seniority_level) parts.push(`Seniority: ${candidate.seniority_level}`);
    if (candidate.remote_preference) parts.push(`Remote: ${candidate.remote_preference}`);
    if (candidate.location) parts.push(`Location: ${candidate.location}`);
    return parts.join('. ') || 'No data available';
}

async function embedCandidate(candidate) {
    const text = candidateToText(candidate);
    if (!text || text.length < 5) throw new Error('No valid text to embed');
    try {
        const response = await axios.post(VOYAGE_URL, {
            model: VOYAGE_MODEL,
            input: [text]
        }, {
            headers: { 'Authorization': `Bearer ${VOYAGE_API_KEY}`, 'Content-Type': 'application/json' },
            timeout: 60000
        });
        return response.data.data[0].embedding;
    } catch (err) {
        throw new Error(`Voyage AI error: ${err.message}`);
    }
}

function isGarbageTitle(title) {
    if (!title) return true;
    return GARBAGE_TITLE_PATTERNS.some(p => p.test(title));
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
    for (const p of patterns) {
        const m = title.match(p);
        if (m && m[1]) {
            const name = m[1].trim();
            if (name.length > 2 && name.length < 60) return name;
        }
    }
    const companyWords = ['GmbH', 'AG', 'KG', 'SE', 'e.V.', 'UG', 'GbR', 'OHG'];
    const parts = title.split(/[—–\-|\/]/);
    for (const part of parts.reverse()) {
        const trimmed = part.trim();
        if (companyWords.some(w => trimmed.includes(w))) return trimmed;
    }
    return null;
}

// ─── Matching function ────────────────────────────────────────────────
async function matchCandidate(candidateData, radius, topK = 30) {
    const { salesforce_contact_id, name, skill_scores, seniority_level, remote_preference, location, location_lat, location_lng } = candidateData;

    // 1. Candidate upsert
    let { data: existingCandidate, error: fetchError } = await supabase
        .from('candidates')
        .select('id, skill_embedding')
        .eq('salesforce_contact_id', salesforce_contact_id)
        .single();

    let candidateId;
    if (fetchError && fetchError.code !== 'PGRST116') throw new Error(`Supabase fetch error: ${fetchError.message}`);
    if (existingCandidate) {
        candidateId = existingCandidate.id;
        await supabase
            .from('candidates')
            .update({ name, skill_scores, seniority_level, remote_preference, location, location_lat, location_lng, updated_at: new Date().toISOString() })
            .eq('id', candidateId);
    } else {
        const { data: newCandidate, error: insertError } = await supabase
            .from('candidates')
            .insert({ salesforce_contact_id, name, skill_scores, seniority_level, remote_preference, location, location_lat, location_lng, created_at: new Date().toISOString(), updated_at: new Date().toISOString() })
            .select('id')
            .single();
        if (insertError) throw new Error(`Supabase insert error: ${insertError.message}`);
        candidateId = newCandidate.id;
    }

    // 2. Embed candidate
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

    // 3. Fetch data
    const [jobs, companies] = await Promise.all([getCachedJobs(), getCompaniesMap()]);

    // 4. Candidate skills as Set for O(1) lookup
    const candidateSkillSet = new Set(Object.keys(skill_scores || {}).map(s => s.toLowerCase().trim()));
    const candidateTitleWords = (name || '').toLowerCase().split(/\s+/);

    // 5. Matching loop
    const startMatch = Date.now();
    const matches = [];
    const seenJobIds = new Set();
    const seenJobKey = new Set();
    let skippedNoCompany = 0, skippedGarbage = 0, skippedNoSkill = 0, skippedLowSim = 0;

    for (const job of jobs) {
        // Skip if no company name
        if (!job.company_name || job.company_name.trim() === '') { skippedNoCompany++; continue; }
        // Skip garbage titles
        if (isGarbageTitle(job.title)) { skippedGarbage++; continue; }
        // Deduplicate
        if (seenJobIds.has(job.id)) continue;
        seenJobIds.add(job.id);
        const key = `${job.company_id}|${job.title?.trim().toLowerCase()}|${job.location?.trim().toLowerCase() || ''}`;
        if (seenJobKey.has(key)) continue;
        seenJobKey.add(key);

        // Skill presence check (only if candidate has skills)
        let hasSkillMatch = true;
        if (candidateSkillSet.size > 0) {
            const jobSkills = job.structured_skills || [];
            const jobSkillSet = new Set(jobSkills.map(s => s.toLowerCase().trim()));
            let found = false;
            for (const skill of candidateSkillSet) {
                if (jobSkillSet.has(skill)) { found = true; break; }
            }
            if (!found) {
                skippedNoSkill++;
                continue;
            }
            hasSkillMatch = found;
        }

        // Embedding similarity
        let sim = cosineSimilarity(skillEmbedding, job.skill_embedding);
        if (sim < MIN_SIMILARITY) { skippedLowSim++; continue; }

        // Title bonus
        if (job.title) {
            const titleLower = job.title.toLowerCase();
            const bonus = candidateTitleWords.some(w => titleLower.includes(w) && w.length > 3) ? TITLE_BONUS : 0;
            sim = Math.min(sim + bonus, 1);
        }

        // Skill overlap ratio
        let overlapRatio = 0;
        if (candidateSkillSet.size > 0 && job.structured_skills?.length > 0) {
            const jobSkillSet = new Set(job.structured_skills.map(s => s.toLowerCase().trim()));
            let common = 0;
            for (const skill of candidateSkillSet) {
                if (jobSkillSet.has(skill)) common++;
            }
            overlapRatio = common / Math.max(job.structured_skills.length, candidateSkillSet.size);
            if (overlapRatio < MIN_SKILL_OVERLAP) {
                sim = sim * 0.3;
            } else {
                sim = (sim * (1 - SKILL_WEIGHT)) + (overlapRatio * SKILL_WEIGHT);
            }
        } else if (candidateSkillSet.size > 0 && job.structured_skills?.length === 0) {
            sim = sim * 0.5;
        }

        if (sim < MIN_SIMILARITY) { skippedLowSim++; continue; }

        // Distance
        let distance = null;
        if (location_lat && location_lng && job.location_lat && job.location_lng) {
            distance = haversine(location_lat, location_lng, job.location_lat, job.location_lng);
        }

        const isRemote = job.remote_type && job.remote_type.toLowerCase() === 'remote';
        let include = false;
        if (isRemote) {
            include = sim >= REMOTE_MIN_SIMILARITY;
        } else if (distance !== null && distance <= radius) {
            include = true;
        } else if (distance === null && location && job.location) {
            include = citiesMatch(location, job.location);
        } else if (distance === null && !location && !job.location) {
            include = true;
        }
        if (!include) continue;

        // Company info
        let companyName = job.company_name || null;
        let careerPageUrl = null, lastCrawledAt = null, crawlStatus = null;
        if (job.company_id) {
            const info = companies.byId.get(job.company_id);
            if (info) {
                companyName = companyName || info.Name || null;
                careerPageUrl = info.career_page_url || null;
                lastCrawledAt = info.last_crawled_at || null;
                crawlStatus = info.crawl_status || null;
            }
        }
        if (!companyName) {
            const extracted = extractCompanyNameFromTitle(job.title);
            if (extracted) {
                const key = extracted.toLowerCase().trim();
                let matched = companies.byName.get(key);
                if (!matched) {
                    const clean = key.replace(/\s*(gmbh|ag|kg|se|e\.v\.|ug|gbr|ohg)\s*$/, '').trim();
                    if (clean !== key) matched = companies.byName.get(clean);
                }
                if (matched) {
                    companyName = matched.Name;
                    careerPageUrl = careerPageUrl || matched.detected_career_url || null;
                    lastCrawledAt = lastCrawledAt || matched.last_crawled_at || null;
                    crawlStatus = crawlStatus || matched.crawl_status || null;
                }
            }
        }

        const topSkills = (job.structured_skills || []).slice(0, 5).join(', ');

        matches.push({
            job_id: job.id,
            job_title: job.title || 'Untitled',
            company_id: job.company_id,
            company_name: companyName,
            apply_url: job.apply_url || null,
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

    console.log(`🔍 Skipped: ${skippedNoCompany} no company, ${skippedGarbage} garbage, ${skippedNoSkill} no skill, ${skippedLowSim} low sim`);

    matches.sort((a, b) => b.similarity_score - a.similarity_score);
    const topMatches = matches.slice(0, topK);
    console.log(`⏱️ Matching loop took ${Date.now() - startMatch}ms for ${jobs.length} jobs → ${topMatches.length} matches`);

    // 6. Store matches (using only columns that exist)
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
            top_skills: m.top_skills ? m.top_skills.split(',').map(s => s.trim()).filter(Boolean) : [],
            created_at: new Date().toISOString()
        }));
        const { error: insertError } = await supabase.from('matches').insert(rows);
        if (insertError) console.error('❌ Match insert error:', insertError.message);
    }

    return {
        candidate_id: candidateId,
        salesforce_contact_id,
        candidate_name: name,
        matches_count: topMatches.length,
        matches: topMatches
    };
}

// ─── Webhook ──────────────────────────────────────────────────────────
app.post('/webhook/match-candidate', async (req, res) => {
    const requestId = uuidv4();
    const startTime = Date.now();
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
            top_k = 30
        } = req.body;

        if (!salesforce_contact_id) {
            throw new Error('Missing required field: salesforce_contact_id');
        }

        if (!skill_scores || typeof skill_scores !== 'object' || Object.keys(skill_scores).length === 0) {
            console.warn(`[${requestId}] Warning: skill_scores empty for ${salesforce_contact_id}`);
        }

        const result = await matchSemaphore.run(() => matchCandidate({
            salesforce_contact_id,
            name,
            skill_scores,
            seniority_level,
            remote_preference,
            location,
            location_lat,
            location_lng
        }, radius, top_k));

        const elapsed = Date.now() - startTime;
        console.log(`[${requestId}] Completed in ${elapsed}ms, found ${result.matches_count} matches`);
        res.status(200).json({ success: true, ...result, message: `Successfully matched candidate with ${result.matches_count} job(s)`, timestamp: new Date().toISOString() });

    } catch (err) {
        const elapsed = Date.now() - startTime;
        console.error(`[${requestId}] Error after ${elapsed}ms: ${err.message}`);
        res.status(500).json({ success: false, error: err.message, timestamp: new Date().toISOString() });
    }
});

app.get('/health', (req, res) => {
    res.status(200).json({ status: 'ok', timestamp: new Date().toISOString() });
});

// ─── Start server ──────────────────────────────────────────────────────
const SSL_KEY_PATH = process.env.SSL_KEY || '/etc/ssl/private/server.key';
const SSL_CERT_PATH = process.env.SSL_CERT || '/etc/ssl/certs/server.crt';

let useHttps = false;
try {
    if (fs.existsSync(SSL_KEY_PATH) && fs.existsSync(SSL_CERT_PATH)) useHttps = true;
} catch (e) {}

async function startServer() {
    await warmupCache();
    const server = useHttps
        ? https.createServer({ key: fs.readFileSync(SSL_KEY_PATH), cert: fs.readFileSync(SSL_CERT_PATH) }, app)
        : app;
    server.listen(PORT, HOST, () => {
        console.log(`${useHttps ? '🔒 HTTPS' : '🚀 HTTP'} server running on ${useHttps ? 'https' : 'http'}://${HOST}:${PORT}`);
        console.log(`📍 POST to /webhook/match-candidate`);
        console.log(`💚 Health check: /health`);
        console.log(`⚡ Concurrency: ${MAX_CONCURRENT_MATCHES}, Cache TTL: ${JOB_CACHE_TTL_MS/1000}s, Timeout: ${TIMEOUT_MS/1000}s`);
        console.log(`\n⚠️ To speed up Supabase queries, run this SQL once:`);
        console.log(`   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_jobs_skill_embedding_not_null ON jobs (id) WHERE skill_embedding IS NOT NULL;\n`);
    });
}

startServer().catch(err => {
    console.error('Fatal startup error:', err);
    process.exit(1);
});
