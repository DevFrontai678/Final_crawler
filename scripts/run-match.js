#!/usr/bin/env node
/**
 * Unified Matching & Embedding Script
 * 
 * Modes:
 *   --mode=single      : Single candidate (n8n webhook)
 *   --mode=embed-all   : Embed all candidates without embeddings
 *   --mode=match-all   : Match all candidates (bulk)
 *   --mode=match-some  : Match specific candidates (--candidates=ID1,ID2)
 * 
 * Common flags:
 *   --candidate=ID     : For single mode, the candidate ID (salesforce_contact_id)
 *   --candidates=ID1,ID2 : For match-some mode, comma‑separated IDs
 *   --radius=N         : Radius in km (default 50)
 *   --top=N            : Top matches per candidate (default 10)
 *   --dry-run          : Preview without writing to DB
 *   --concurrency=N    : For batch modes, number of parallel candidates (default 3)
 */

const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const axios = require('axios');
const pLimit = require('p-limit');
require('dotenv').config();

// ─── Config ──────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
const MODE = args.find(a => a.startsWith('--mode='))?.split('=')[1] || 'single';
const CANDIDATE_ID = args.find(a => a.startsWith('--candidate='))?.split('=')[1];
const CANDIDATE_IDS = args.find(a => a.startsWith('--candidates='))?.split('=')[1]?.split(',') || [];
const RADIUS = parseInt(args.find(a => a.startsWith('--radius='))?.split('=')[1] || 50);
const TOP_K = parseInt(args.find(a => a.startsWith('--top='))?.split('=')[1] || 10);
const DRY_RUN = args.includes('--dry-run');
const CONCURRENCY = parseInt(args.find(a => a.startsWith('--concurrency='))?.split('=')[1] || 3);

// ─── Supabase ────────────────────────────────────────────────────────────
const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

// ─── Voyage AI ──────────────────────────────────────────────────────────
const VOYAGE_API_KEY = process.env.VOYAGE_API_KEY;
const VOYAGE_URL = 'https://api.voyageai.com/v1/embeddings';
const VOYAGE_MODEL = 'voyage-3-large';

// ─── Static City Coordinates (from geocoding) ──────────────────────────
const STATIC_COORDS = {
    'berlin': { lat: 52.5200, lng: 13.4050 },
    'hamburg': { lat: 53.5511, lng: 9.9937 },
    'münchen': { lat: 48.1351, lng: 11.5820 },
    'köln': { lat: 50.9375, lng: 6.9603 },
    'frankfurt': { lat: 50.1109, lng: 8.6821 },
    'stuttgart': { lat: 48.7758, lng: 9.1829 },
    'düsseldorf': { lat: 51.2277, lng: 6.7735 },
    'dortmund': { lat: 51.5136, lng: 7.4653 },
    'essen': { lat: 51.4556, lng: 7.0116 },
    'leipzig': { lat: 51.3397, lng: 12.3731 },
    'bremen': { lat: 53.0793, lng: 8.8017 },
    'dresden': { lat: 51.0504, lng: 13.7373 },
    'hannover': { lat: 52.3759, lng: 9.7320 },
    'nürnberg': { lat: 49.4521, lng: 11.0767 },
    'duisburg': { lat: 51.4344, lng: 6.7623 },
    'bochum': { lat: 51.4818, lng: 7.2162 },
    'bonn': { lat: 50.7374, lng: 7.0982 },
    'mannheim': { lat: 49.4875, lng: 8.4660 },
    'karlsruhe': { lat: 49.0069, lng: 8.4037 },
    'wiesbaden': { lat: 50.0782, lng: 8.2398 },
    'augsburg': { lat: 48.3705, lng: 10.8978 },
    'münster': { lat: 51.9607, lng: 7.6261 },
    'aachen': { lat: 50.7753, lng: 6.0839 },
    'kiel': { lat: 54.3233, lng: 10.1228 },
    'lübeck': { lat: 53.8655, lng: 10.6866 },
    'erfurt': { lat: 50.9848, lng: 11.0299 },
    'kassel': { lat: 51.3127, lng: 9.4797 },
    'mainz': { lat: 49.9929, lng: 8.2473 },
    'saarbrücken': { lat: 49.2401, lng: 6.9969 },
    'potsdam': { lat: 52.3906, lng: 13.0645 },
    'darmstadt': { lat: 49.8728, lng: 8.6512 },
    'würzburg': { lat: 49.7913, lng: 9.9534 },
    'regensburg': { lat: 49.0134, lng: 12.1016 },
    'heidelberg': { lat: 49.3988, lng: 8.6724 },
    'ingolstadt': { lat: 48.7665, lng: 11.4257 },
    'ulm': { lat: 48.4011, lng: 9.9876 },
    'paderborn': { lat: 51.7189, lng: 8.7575 },
    'wolfsburg': { lat: 52.4227, lng: 10.7865 },
    'leverkusen': { lat: 51.0459, lng: 6.9853 },
    'trier': { lat: 49.7596, lng: 6.6441 },
    'freiburg': { lat: 47.9990, lng: 7.8421 },
    'rostock': { lat: 54.0887, lng: 12.1400 },
    'schwerin': { lat: 53.6355, lng: 11.4012 },
    'cottbus': { lat: 51.7563, lng: 14.3329 },
    'koblenz': { lat: 50.3569, lng: 7.5890 },
    'kaiserslautern': { lat: 49.4432, lng: 7.7690 },
    'ludwigshafen': { lat: 49.4741, lng: 8.4353 },
    'zürich': { lat: 47.3769, lng: 8.5417 },
    'zurich': { lat: 47.3769, lng: 8.5417 },
    'basel': { lat: 47.5596, lng: 7.5886 },
    'bern': { lat: 46.9480, lng: 7.4474 },
    'wien': { lat: 48.2082, lng: 16.3738 },
    'vienna': { lat: 48.2082, lng: 16.3738 },
    'graz': { lat: 47.0707, lng: 15.4395 },
    'linz': { lat: 48.3069, lng: 14.2858 },
    'salzburg': { lat: 47.8095, lng: 13.0550 },
    'innsbruck': { lat: 47.2692, lng: 11.4041 },
    // add more as needed
};

function getCityCoords(city) {
    if (!city) return null;
    const key = city.toLowerCase().trim();
    return STATIC_COORDS[key] || null;
}

// ─── Helpers ──────────────────────────────────────────────────────────────
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
    const toRad = (deg) => deg * Math.PI / 180;
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
        throw new Error('No text to embed');
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

// ─── Extract company name from job title (fallback) ────────────────────
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

// ─── Fetch all jobs (paginated) ────────────────────────────────────────
async function fetchAllJobs() {
    const jobs = [];
    let page = 0;
    const PAGE_SIZE = 1000;
    let hasMore = true;
    while (hasMore) {
        const start = page * PAGE_SIZE;
        const end = start + PAGE_SIZE - 1;
        const { data, error } = await supabase
            .from('jobs')
            .select('id, title, company_id, location, location_lat, location_lng, remote_type, seniority_level, structured_skills, skill_embedding')
            .not('skill_embedding', 'is', null)
            .order('id', { ascending: true })
            .range(start, end);
        if (error) throw error;
        if (!data || data.length === 0) break;
        jobs.push(...data);
        page++;
        if (data.length < PAGE_SIZE) break;
    }
    return jobs;
}

// ─── Fetch all companies (for name lookup) ──────────────────────────────
let allCompaniesMap = null;

async function fetchCompaniesMap() {
    if (allCompaniesMap) return allCompaniesMap;
    const { data, error } = await supabase
        .from('companies')
        .select('"Id", "Name"');
    if (error) throw error;
    const map = new Map();
    data.forEach(c => map.set(c.Id, c.Name));
    const nameMap = new Map();
    data.forEach(c => {
        const key = c.Name.toLowerCase().trim();
        if (!nameMap.has(key)) {
            nameMap.set(key, c.Name);
        }
    });
    allCompaniesMap = { byId: map, byName: nameMap };
    return allCompaniesMap;
}

// ─── Get company name with fallback lookup by extracted name ────────────
async function getCompanyName(job, companiesMap, extractedName) {
    if (job.company_id) {
        const name = companiesMap.byId.get(job.company_id);
        if (name) return name;
    }
    if (extractedName) {
        const key = extractedName.toLowerCase().trim();
        const match = companiesMap.byName.get(key);
        if (match) return match;
        const cleanKey = key.replace(/\s*(gmbh|ag|kg|se|e\.v\.|ug|gbr|ohg)\s*$/, '').trim();
        if (cleanKey !== key) {
            const partialMatch = companiesMap.byName.get(cleanKey);
            if (partialMatch) return partialMatch;
        }
    }
    return extractedName || job.company_id || null;
}

// ─── Match a single candidate ──────────────────────────────────────────
async function matchCandidate(candidate, jobs, radius, topK, companiesMap) {
    const matches = [];
    for (const job of jobs) {
        const sim = cosineSimilarity(candidate.skill_embedding, job.skill_embedding);
        if (sim < 0.01) continue;

        const isRemote = job.remote_type && job.remote_type.toLowerCase() === 'remote';
        let include = false;
        let distance = null;

        // --- Compute distance (using lat/lng or static city coords) ---
        let candidateLat = candidate.location_lat;
        let candidateLng = candidate.location_lng;
        let jobLat = job.location_lat;
        let jobLng = job.location_lng;

        // If candidate lacks lat/lng but has city, try static map
        if (!candidateLat || !candidateLng) {
            const coords = getCityCoords(candidate.location);
            if (coords) {
                candidateLat = coords.lat;
                candidateLng = coords.lng;
            }
        }
        if (!jobLat || !jobLng) {
            const coords = getCityCoords(job.location);
            if (coords) {
                jobLat = coords.lat;
                jobLng = coords.lng;
            }
        }

        if (candidateLat && candidateLng && jobLat && jobLng) {
            distance = haversine(candidateLat, candidateLng, jobLat, jobLng);
        }

        // --- Filter by radius ---
        if (isRemote) {
            include = true;
        } else if (distance !== null) {
            if (distance <= radius) include = true;
        } else if (candidate.location && job.location) {
            if (citiesMatch(candidate.location, job.location)) include = true;
        } else {
            include = true; // no location data at all
        }

        if (!include) continue;

        const topSkills = (job.structured_skills || []).slice(0, 5).join(', ');
        const extractedCompany = extractCompanyNameFromTitle(job.title);
        const finalCompanyName = await getCompanyName(job, companiesMap, extractedCompany);

        matches.push({
            job_id: job.id,
            job_title: job.title || 'Untitled',
            company_id: job.company_id,
            company_name: finalCompanyName,
            location: job.location,
            remote_type: job.remote_type,
            seniority_level: job.seniority_level,
            top_skills: topSkills,
            similarity_score: sim,
            location_distance_km: distance, // will be computed if coords available or from static map
        });
    }
    matches.sort((a, b) => b.similarity_score - a.similarity_score);
    return matches.slice(0, topK);
}

// ─── Process a single candidate (embed + match) ────────────────────────
async function processOneCandidate(candidateId, jobs, radius, topK, dryRun, companiesMap) {
    // Fetch candidate
    const { data: candidate, error } = await supabase
        .from('candidates')
        .select('id, salesforce_contact_id, name, skill_scores, seniority_level, remote_preference, location, location_lat, location_lng, skill_embedding')
        .eq('salesforce_contact_id', candidateId)
        .single();

    if (error || !candidate) {
        throw new Error(`Candidate not found: ${candidateId}`);
    }

    // Embed if missing
    if (!candidate.skill_embedding) {
        const embedding = await embedCandidate(candidate);
        if (!dryRun) {
            await supabase
                .from('candidates')
                .update({ skill_embedding: embedding, updated_at: new Date().toISOString() })
                .eq('id', candidate.id);
        }
        candidate.skill_embedding = embedding;
    } else {
        candidate.skill_embedding = parseEmbedding(candidate.skill_embedding);
    }

    if (!candidate.skill_embedding) {
        throw new Error('Invalid embedding');
    }

    // Match
    const matches = await matchCandidate(candidate, jobs, radius, topK, companiesMap);

    // Store matches (if not dry run)
    if (!dryRun && matches.length > 0) {
        await supabase
            .from('matches')
            .delete()
            .eq('candidate_id', candidate.id);

        const rows = matches.map(m => ({
            candidate_id: candidate.id,
            job_id: m.job_id,
            similarity_score: Math.round(m.similarity_score * 10000) / 100,
            location_distance_km: m.location_distance_km,
            final_score: Math.round(m.similarity_score * 10000) / 100,
            company_name: m.company_name,
            job_title: m.job_title,
            job_location: m.location,
            remote_type: m.remote_type,
            seniority_level: m.seniority_level,
            top_skills: m.top_skills,
        }));

        const { error: insertError } = await supabase
            .from('matches')
            .insert(rows);

        if (insertError) {
            console.error(`❌ Insert error: ${insertError.message}`);
        } else {
            console.error(`✅ Inserted ${rows.length} matches for ${candidateId}`);
        }
    }

    return matches;
}

// ─── Modes (embed-all, match-all, match-some) ──────────────────────────
// ... (same as before, but they call processOneCandidate or matchCandidate with the new functions)

// ─── Main ──────────────────────────────────────────────────────────────────
async function main() {
    try {
        if (MODE === 'single') {
            if (!CANDIDATE_ID) {
                console.error('❌ Missing --candidate parameter for single mode');
                process.exit(1);
            }
            const jobs = await fetchAllJobs();
            const companiesMap = await fetchCompaniesMap();
            const matches = await processOneCandidate(CANDIDATE_ID, jobs, RADIUS, TOP_K, DRY_RUN, companiesMap);
            console.log(JSON.stringify(matches, null, 2));
        } else if (MODE === 'embed-all') {
            // ... embed all logic (uses embedCandidate)
        } else if (MODE === 'match-all') {
            // ... match all logic (uses matchCandidate)
        } else if (MODE === 'match-some') {
            // ... match some logic (uses processOneCandidate)
        } else {
            console.error(`❌ Unknown mode: ${MODE}`);
            process.exit(1);
        }
    } catch (err) {
        console.error('❌ Fatal error:', err.message);
        process.exit(1);
    }
}

main();
