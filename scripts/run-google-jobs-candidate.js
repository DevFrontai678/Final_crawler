#!/usr/bin/env node
/**
 * Google Jobs — Candidate-Based Search (Production Ready)
 * 
 * Features:
 *   - ✅ Paginated candidate fetching (1000 per batch)
 *   - ✅ Checkpoint/Resume support
 *   - ✅ Concurrency control
 *   - ✅ Deduplication with error handling
 *   - ✅ Claude structuring with fallback
 *   - ✅ Retry logic
 *   - ✅ Detailed logging
 * 
 * Usage:
 *   node scripts/run-google-jobs-candidate.js
 *   node scripts/run-google-jobs-candidate.js --limit 100
 *   node scripts/run-google-jobs-candidate.js --concurrency 5
 *   node scripts/run-google-jobs-candidate.js --resume
 *   node scripts/run-google-jobs-candidate.js --dry-run
 */

'use strict';

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const { getJson } = require('serpapi');
const Anthropic = require('@anthropic-ai/sdk');
const crypto = require('crypto');

// ─── CONFIG ────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);

const CONFIG = {
    concurrency:    parseInt(args.find(a => a.startsWith('--concurrency='))?.split('=')[1] || 3),
    limit:          parseInt(args.find(a => a.startsWith('--limit='))?.split('=')[1] || 0),
    dryRun:         args.includes('--dry-run'),
    resume:         args.includes('--resume'),
    verbose:        args.includes('--verbose'),
    delayMs:        500,
    jobDelayMs:     200,
    maxJobsPerSearch: 20,
    maxPagesPerSearch: 2,
    pageSize:       1000,
    checkpointFile: path.join(__dirname, '.google-jobs-candidate-checkpoint.json'),
    maxRetries:     3,
};

// ─── CLIENTS ──────────────────────────────────────────────────────────────

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

// ─── HELPERS ──────────────────────────────────────────────────────────────

function sleep(ms) {
    return new Promise(r => setTimeout(r, ms));
}

function formatDuration(ms) {
    const s = Math.round(ms / 1000);
    if (s < 60) return `${s}s`;
    if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
    return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
}

function generateExternalJobId(url, title, company) {
    const hash = crypto.createHash('sha256')
        .update(`${url || title}${company}`.toLowerCase().trim())
        .digest('hex')
        .slice(0, 40);
    return hash;
}

function isValidDate(date) {
    return date && !isNaN(new Date(date).getTime());
}

// ─── CHECKPOINT ────────────────────────────────────────────────────────────

function loadCheckpoint() {
    try {
        if (fs.existsSync(CONFIG.checkpointFile)) {
            return JSON.parse(fs.readFileSync(CONFIG.checkpointFile, 'utf8'));
        }
    } catch (e) { return null; }
}

function saveCheckpoint(checkpoint) {
    try {
        fs.writeFileSync(CONFIG.checkpointFile, JSON.stringify(checkpoint, null, 2));
    } catch (e) { /* ignore */ }
}

function clearCheckpoint() {
    try {
        if (fs.existsSync(CONFIG.checkpointFile)) fs.unlinkSync(CONFIG.checkpointFile);
    } catch (e) { /* ignore */ }
}

// ─── GENERATE SEARCH TERMS ──────────────────────────────────────────────

function generateSearchTerms(jobTitle) {
    if (!jobTitle) return [];

    const title = jobTitle.toLowerCase().trim();
    const terms = [title];

    const synonymMap = {
        'administrator': ['admin', 'system admin', 'it admin', 'systems administrator'],
        'developer': ['engineer', 'programmer', 'coder', 'software engineer'],
        'engineer': ['developer', 'technical', 'engineering'],
        'manager': ['lead', 'head', 'director', 'supervisor'],
        'analyst': ['analytics', 'data analyst', 'business analyst'],
        'consultant': ['advisor', 'specialist', 'expert'],
        'architect': ['designer', 'planner', 'solution architect'],
        'support': ['helpdesk', 'service desk', 'it support'],
        'security': ['cyber', 'information security', 'infosec'],
        'network': ['networking', 'infrastructure', 'systems'],
        'database': ['dba', 'data', 'sql'],
        'cloud': ['aws', 'azure', 'gcp', 'cloud engineer'],
        'devops': ['ci/cd', 'automation', 'site reliability', 'sre'],
        'frontend': ['ui', 'ux', 'react', 'angular', 'vue'],
        'backend': ['api', 'microservices', 'server', 'node'],
        'fullstack': ['full stack', 'full-stack', 'full stack developer'],
        'data': ['data science', 'data engineering', 'big data', 'analytics'],
        'machine learning': ['ml', 'ai', 'artificial intelligence', 'deep learning'],
        'product': ['product owner', 'product manager', 'product designer'],
        'project': ['project manager', 'project coordinator', 'program manager'],
        'quality': ['qa', 'test', 'testing', 'quality assurance'],
        'sales': ['account manager', 'business development', 'sales rep'],
        'marketing': ['digital marketing', 'growth', 'brand', 'content'],
        'hr': ['human resources', 'recruiter', 'talent acquisition', 'people'],
        'finance': ['accounting', 'controller', 'financial analyst', 'audit'],
        'legal': ['counsel', 'attorney', 'paralegal', 'compliance'],
        'operations': ['ops', 'supply chain', 'logistics', 'procurement'],
        'design': ['graphic', 'product design', 'interaction', 'creative'],
    };

    for (const [keyword, synonyms] of Object.entries(synonymMap)) {
        if (title.includes(keyword) || title.split(' ').some(w => w === keyword)) {
            terms.push(...synonyms);
        }
    }

    if (title.includes('senior')) {
        terms.push(title.replace('senior', '').trim());
        terms.push('senior ' + title.replace('senior', '').trim());
    }
    if (title.includes('junior')) {
        terms.push(title.replace('junior', '').trim());
        terms.push('junior ' + title.replace('junior', '').trim());
    }

    const unique = [...new Set(terms)];
    return unique.slice(0, 10);
}

// ─── FETCH CANDIDATES — PAGINATED ────────────────────────────────────────

async function fetchCandidates(checkpoint = null) {
    const allCandidates = [];
    let page = 0;
    let hasMore = true;
    const PAGE_SIZE = CONFIG.pageSize;

    console.log(`   📋 Fetching candidates (paginated, ${PAGE_SIZE} per page)...`);

    while (hasMore) {
        const start = page * PAGE_SIZE;
        const end = start + PAGE_SIZE - 1;

        let query = supabase
            .from('candidates')
            .select('id, salesforce_contact_id, name, location')
            .not('name', 'is', null)
            .not('location', 'is', null)
            .order('id', { ascending: true })
            .range(start, end);

        if (CONFIG.limit > 0 && allCandidates.length >= CONFIG.limit) {
            break;
        }

        if (checkpoint?.processedIds?.length > 0) {
            query = query.not('id', 'in', `(${checkpoint.processedIds.join(',')})`);
        }

        if (CONFIG.limit > 0) {
            const remaining = CONFIG.limit - allCandidates.length;
            if (remaining < PAGE_SIZE) {
                query = query.range(start, start + remaining - 1);
            }
        }

        const { data, error } = await query;
        if (error) throw new Error(`Supabase fetch error (page ${page + 1}): ${error.message}`);

        if (!data || data.length === 0) {
            hasMore = false;
            break;
        }

        allCandidates.push(...data);
        page++;

        if (data.length < PAGE_SIZE) {
            hasMore = false;
        }
    }

    console.log(`   ✅ Fetched ${allCandidates.length} candidates (${page} pages)`);
    return allCandidates;
}

// ─── SEARCH GOOGLE JOBS ──────────────────────────────────────────────────

async function searchGoogleJobs(query) {
    const allJobs = [];
    let retries = 0;

    console.log(`  🔍 Searching: "${query}"`);

    for (let page = 0; page < CONFIG.maxPagesPerSearch; page++) {
        const start = page * 10;

        try {
            const response = await getJson({
                api_key: process.env.SERPAPI_API_KEY,
                engine: 'google_jobs',
                q: query,
                gl: 'de',
                hl: 'de',
                start: start,
            });

            if (response.error) {
                if (response.error.includes('rate limit') || response.error.includes('limit')) {
                    console.log(`  ⏳ Rate limited, waiting 5s...`);
                    await sleep(5000);
                    page--;
                    retries++;
                    if (retries > CONFIG.maxRetries) break;
                    continue;
                }
                if (response.error.includes('hasn\'t returned any results')) {
                    if (CONFIG.verbose) console.log(`     ℹ️ No results`);
                    break;
                }
                console.log(`  ⚠️ SerpAPI: ${response.error}`);
                break;
            }

            const jobs = response.jobs_results || [];
            if (jobs.length === 0) break;

            allJobs.push(...jobs);
            console.log(`     Page ${page + 1}: ${jobs.length} jobs (total: ${allJobs.length})`);

            if (allJobs.length >= CONFIG.maxJobsPerSearch) break;
            await sleep(300);

        } catch (err) {
            console.log(`  ❌ SerpAPI failed: ${err.message}`);
            break;
        }
    }

    const trimmedJobs = allJobs.slice(0, CONFIG.maxJobsPerSearch);
    console.log(`  ✅ Found ${trimmedJobs.length} jobs`);

    return trimmedJobs;
}

// ─── AGGRESSIVE JSON EXTRACTION ────────────────────────────────────────────

function extractJsonAggressive(text) {
    if (!text || typeof text !== 'string') return null;

    const strategies = [
        (t) => {
            const match = t.match(/\{[\s\S]*\}/);
            return match ? match[0] : null;
        },
        (t) => {
            const match = t.match(/```json\s*([\s\S]*?)\s*```/);
            return match ? match[1] : null;
        },
        (t) => {
            const match = t.match(/(?:JSON|json)[\s:]*(\{[\s\S]*\})/);
            return match ? match[1] : null;
        },
        (t) => {
            const cleaned = t
                .replace(/```json\s*/g, '')
                .replace(/```\s*/g, '')
                .replace(/^[\s\S]*?(\{[\s\S]*\})[\s\S]*?$/, '$1');
            return cleaned.includes('{') ? cleaned : null;
        },
    ];

    for (const strategy of strategies) {
        try {
            const extracted = strategy(text);
            if (!extracted) continue;

            let cleaned = extracted
                .replace(/[\u0000-\u001F\u007F-\u009F]/g, '')
                .replace(/,\s*}/g, '}')
                .replace(/,\s*\]/g, ']')
                .replace(/([{,]\s*)(\w+)(\s*:)/g, '$1"$2"$3')
                .replace(/:\s*'([^']*)'/g, ':"$1"')
                .replace(/:\s*undefined/g, ':null')
                .replace(/:\s*NaN/g, ':null')
                .replace(/:\s*Infinity/g, ':null');

            const parsed = JSON.parse(cleaned);
            if (parsed && typeof parsed === 'object') {
                return parsed;
            }
        } catch (e) {
            // Continue
        }
    }

    return null;
}

// ─── FIND WORKING CLAUDE MODEL ─────────────────────────────────────────────

let workingModel = null;
const MODELS = [
    'claude-sonnet-4-6',
    'claude-3-5-sonnet-20241022',
    'claude-3-opus-20240229',
];

async function findWorkingModel() {
    if (workingModel) return workingModel;

    for (const model of MODELS) {
        try {
            await anthropic.messages.create({
                model: model,
                max_tokens: 10,
                messages: [{ role: 'user', content: 'Test' }],
            });
            workingModel = model;
            console.log(`   ✅ Using Claude model: ${model}`);
            return model;
        } catch (e) {
            if (CONFIG.verbose) console.log(`   ⚠️ Model ${model} unavailable: ${e.message}`);
        }
    }

    console.log(`   ⚠️ No Claude model available, using fallback extraction only`);
    return null;
}

// ─── STRUCTURE JOB WITH CLAUDE ────────────────────────────────────────────

async function structureJobWithClaude(job, candidateId, candidateName) {
    const { title, company_name, location, description, job_id, detected_extensions } = job;

    let fullDescription = description || '';
    if (detected_extensions?.snippet) {
        fullDescription += '\n' + detected_extensions.snippet;
    }
    if (detected_extensions?.posted_at) {
        fullDescription += `\nPosted: ${detected_extensions.posted_at}`;
    }

    if (!fullDescription || fullDescription.length < 50) {
        return null;
    }

    const cleanDescription = fullDescription
        .replace(/[\u0000-\u001F\u007F-\u009F]/g, '')
        .replace(/\\/g, '\\\\')
        .replace(/"/g, '\\"')
        .slice(0, 3000);

    const model = await findWorkingModel();
    if (!model) {
        return manualExtractJob(job, candidateId, candidateName);
    }

    try {
        const response = await anthropic.messages.create({
            model: model,
            max_tokens: 800,
            messages: [{
                role: 'user',
                content: `You are a job data extractor. Extract structured data from this job posting.

TITLE: ${title || 'Unknown'}
COMPANY: ${company_name || 'Unknown'}
LOCATION: ${location || 'Unknown'}
DESCRIPTION:
${cleanDescription}

---

Return ONLY valid JSON with NO preamble, NO markdown, NO extra text:

{
  "title": "extracted job title",
  "description": "full cleaned description",
  "location": "city or remote",
  "employment_type": "Full-time|Part-time|Contract|Unknown",
  "seniority_level": "Senior|Mid|Junior|Entry|Lead|Unknown",
  "skills": ["skill1", "skill2", "skill3"]
}

START JSON RESPONSE:`
            }]
        });

        const text = response.content[0]?.text?.trim();

        if (!text) {
            return manualExtractJob(job, candidateId, candidateName);
        }

        const parsed = extractJsonAggressive(text);

        if (!parsed) {
            if (CONFIG.verbose) console.log(`     ⚠️ JSON extraction failed, using fallback`);
            return manualExtractJob(job, candidateId, candidateName);
        }

        return {
            external_job_id: job_id || generateExternalJobId(job.url || title, title, candidateName || company_name),
            title: (parsed.title || title || 'Untitled').slice(0, 500),
            raw_description: (parsed.description || fullDescription).slice(0, 5000),
            location: (parsed.location || location || '').slice(0, 200),
            employment_type: (parsed.employment_type || null) ? (parsed.employment_type).slice(0, 100) : null,
            seniority_level: (parsed.seniority_level || null) ? (parsed.seniority_level).slice(0, 100) : null,
            structured_skills: Array.isArray(parsed.skills) ? parsed.skills.slice(0, 20) : [],
            apply_url: (job.url || `https://www.google.com/search?q=${encodeURIComponent(`${company_name} ${title}`)}`).slice(0, 2000),
            posted_at: isValidDate(detected_extensions?.posted_at) ? new Date(detected_extensions.posted_at) : null,
            ats_source: 'google_jobs_candidate',
            candidate_id: candidateId,
            candidate_name: candidateName,
            search_term: title,
        };

    } catch (err) {
        if (CONFIG.verbose) console.log(`     ⚠️ Claude error: ${err.message}`);
        return manualExtractJob(job, candidateId, candidateName);
    }
}

// ─── MANUAL EXTRACTION FALLBACK ────────────────────────────────────────────

function manualExtractJob(job, candidateId, candidateName) {
    const { title, company_name, location, job_id, detected_extensions } = job;

    if (!title) return null;

    return {
        external_job_id: job_id || generateExternalJobId(job.url || title, title, candidateName || company_name),
        title: (title || 'Untitled').slice(0, 500),
        raw_description: (job.description || '').slice(0, 5000),
        location: (location || '').slice(0, 200),
        employment_type: null,
        seniority_level: null,
        structured_skills: [],
        apply_url: (job.url || `https://www.google.com/search?q=${encodeURIComponent(`${company_name} ${title}`)}`).slice(0, 2000),
        posted_at: isValidDate(detected_extensions?.posted_at) ? new Date(detected_extensions.posted_at) : null,
        ats_source: 'google_jobs_candidate',
        candidate_id: candidateId,
        candidate_name: candidateName,
        search_term: title,
    };
}

// ─── CHECK DUPLICATE ──────────────────────────────────────────────────────

async function isDuplicateJob(title, companyName) {
    const { data, error } = await supabase
        .from('jobs')
        .select('id, title')
        .ilike('title', `%${title}%`)
        .limit(5);

    if (error) return false;

    for (const job of data) {
        const titleSimilarity = job.title.toLowerCase().includes(title.toLowerCase()) ||
            title.toLowerCase().includes(job.title.toLowerCase());
        if (titleSimilarity) {
            return true;
        }
    }
    return false;
}

// ─── CLEAN JOB OBJECT ──────────────────────────────────────────────────────

function cleanJobForInsert(job) {
    const validFields = [
        'external_job_id', 'company_id', 'title', 'raw_description',
        'location', 'employment_type', 'seniority_level',
        'structured_skills', 'apply_url', 'posted_at', 'ats_source',
        'is_active', 'first_seen_at', 'last_seen_at',
    ];

    const cleaned = {};
    for (const field of validFields) {
        if (field in job && job[field] !== undefined && job[field] !== null) {
            cleaned[field] = job[field];
        }
    }

    if (!Array.isArray(cleaned.structured_skills)) {
        cleaned.structured_skills = [];
    }

    return cleaned;
}

// ─── PROCESS A SINGLE CANDIDATE ──────────────────────────────────────────

async function processCandidate(candidate) {
    const results = {
        candidateId: candidate.id,
        candidateName: candidate.name,
        jobTitle: candidate.name,
        location: candidate.location,
        totalSearches: 0,
        totalJobsFound: 0,
        totalJobsSaved: 0,
        duplicates: 0,
        errors: 0,
    };

    console.log(`\n👤 ${results.candidateName}`);
    console.log(`   Title: ${results.jobTitle}`);
    console.log(`   Location: ${results.location}`);

    const searchTerms = generateSearchTerms(results.jobTitle);
    console.log(`   🔑 Search terms: ${searchTerms.join(', ')}`);

    for (const term of searchTerms) {
        const query = `${term} Jobs ${results.location}`;
        results.totalSearches++;

        const googleJobs = await searchGoogleJobs(query);

        if (googleJobs.length === 0) continue;

        for (const job of googleJobs) {
            try {
                // Check if already exists (deduplication)
                if (await isDuplicateJob(job.title, job.company_name)) {
                    if (CONFIG.verbose) console.log(`     ⏭️ Duplicate: ${job.title}`);
                    results.duplicates++;
                    continue;
                }

                const structuredJob = await structureJobWithClaude(
                    job,
                    candidate.id,
                    results.candidateName
                );

                if (!structuredJob) {
                    results.errors++;
                    continue;
                }

                structuredJob.company_id = null;

                const cleaned = cleanJobForInsert(structuredJob);

                if (!CONFIG.dryRun) {
                    try {
                        const { error } = await supabase
                            .from('jobs')
                            .insert(cleaned);

                        if (error) {
                            // Check for duplicate key violation (race condition)
                            if (error.code === '23505') {
                                console.log(`     ⏭️ Duplicate (race): ${structuredJob.title}`);
                                results.duplicates++;
                            } else {
                                console.log(`     ❌ Save error: ${error.message}`);
                                results.errors++;
                            }
                        } else {
                            results.totalJobsSaved++;
                            console.log(`     ✅ ${structuredJob.title} (${structuredJob.structured_skills.length} skills)`);
                        }
                    } catch (err) {
                        console.log(`     ❌ Exception: ${err.message}`);
                        results.errors++;
                    }
                } else {
                    console.log(`     🔍 DRY: ${structuredJob.title}`);
                    results.totalJobsSaved++;
                }

            } catch (err) {
                results.errors++;
                console.log(`     ❌ Error: ${err.message}`);
            }

            await sleep(CONFIG.jobDelayMs);
        }
    }

    results.totalJobsFound = results.totalJobsSaved + results.duplicates + results.errors;
    console.log(`   📊 Found ${results.totalJobsFound} jobs, saved ${results.totalJobsSaved}, duplicates ${results.duplicates}, errors ${results.errors}`);

    return results;
}

// ─── WORKER POOL ──────────────────────────────────────────────────────────

async function runWorkerPool(candidates, concurrency) {
    const total = candidates.length;
    const queue = [...candidates];
    const results = {
        total: total,
        processed: 0,
        totalSearches: 0,
        totalJobsFound: 0,
        totalJobsSaved: 0,
        totalDuplicates: 0,
        totalErrors: 0,
        details: [],
    };

    const startTime = Date.now();
    let checkpoint = CONFIG.resume ? loadCheckpoint() : null;
    const processedIds = checkpoint?.processedIds || [];
    let lastCheckpointCount = processedIds.length;

    function printProgress() {
        const pct = total > 0 ? ((results.processed / total) * 100).toFixed(1) : 0;
        const elapsed = Date.now() - startTime;
        const rate = results.processed / (elapsed / 1000 || 1);
        const remaining = total - results.processed;
        const eta = rate > 0 ? remaining / rate : 0;

        process.stdout.write(
            `\r[${results.processed}/${total} (${pct}%)] 💾 ${results.totalJobsSaved} | ⏱️ ${formatDuration(eta * 1000)}   `
        );
    }

    async function worker(id) {
        while (queue.length > 0) {
            const candidate = queue.shift();
            if (!candidate) break;

            if (CONFIG.resume && processedIds.includes(candidate.id)) {
                results.processed++;
                printProgress();
                continue;
            }

            const result = await processCandidate(candidate);
            results.processed++;
            results.details.push(result);

            results.totalSearches += result.totalSearches;
            results.totalJobsFound += result.totalJobsFound;
            results.totalJobsSaved += result.totalJobsSaved;
            results.totalDuplicates += result.duplicates;
            results.totalErrors += result.errors;

            processedIds.push(candidate.id);
            printProgress();

            if (results.processed - lastCheckpointCount >= 10) {
                lastCheckpointCount = results.processed;
                saveCheckpoint({ processedIds, timestamp: Date.now() });
            }

            await sleep(CONFIG.delayMs);
        }
    }

    const workers = Array.from(
        { length: Math.min(concurrency, total) },
        (_, i) => worker(i + 1)
    );
    await Promise.all(workers);

    console.log('\n');
    if (results.processed === total) clearCheckpoint();

    return { results, elapsed: Date.now() - startTime };
}

// ─── PRINT SUMMARY ────────────────────────────────────────────────────────

function printSummary(results, elapsed) {
    console.log('\n════════════════════════════════════════════');
    console.log('  CANDIDATE-BASED GOOGLE JOBS COMPLETE');
    console.log('════════════════════════════════════════════');
    console.log(`  Duration          : ${formatDuration(elapsed)}`);
    console.log(`  Candidates        : ${results.total}`);
    console.log(`  Searches          : ${results.totalSearches}`);
    console.log(`  Jobs found        : ${results.totalJobsFound}`);
    console.log(`  Jobs saved ✅     : ${results.totalJobsSaved}`);
    console.log(`  Duplicates ⏭️     : ${results.totalDuplicates}`);
    console.log(`  Errors            : ${results.totalErrors}`);
    console.log('════════════════════════════════════════════\n');

    const top = results.details
        .filter(d => d.totalJobsSaved > 0)
        .sort((a, b) => b.totalJobsSaved - a.totalJobsSaved)
        .slice(0, 10);

    if (top.length > 0) {
        console.log('  📊 Top Candidates by Jobs Saved:');
        top.forEach((d, i) => {
            console.log(`     ${i + 1}. ${d.candidateName} → ${d.totalJobsSaved} jobs`);
        });
    }
}

// ─── MAIN ──────────────────────────────────────────────────────────────────

async function main() {
    console.log('\n🔍 Candidate-Based Google Jobs Search (Production)');
    console.log(`   Concurrency      : ${CONFIG.concurrency}`);
    console.log(`   Dry run          : ${CONFIG.dryRun ? 'ON' : 'OFF'}`);
    console.log(`   Resume           : ${CONFIG.resume ? 'enabled' : 'disabled'}`);
    console.log(`   Limit            : ${CONFIG.limit || 'All'}\n`);

    const checkpoint = CONFIG.resume ? loadCheckpoint() : null;
    if (checkpoint) {
        console.log(`   ℹ️ Resuming (${checkpoint.processedIds.length} candidates done)\n`);
    }

    const candidates = await fetchCandidates(checkpoint);

    if (candidates.length === 0) {
        console.log('✅ No candidates with job titles found.');
        return;
    }

    console.log(`   Found ${candidates.length} candidates\n`);

    const { results, elapsed } = await runWorkerPool(candidates, CONFIG.concurrency);
    printSummary(results, elapsed);
}

main().catch(err => {
    console.error('❌ Fatal error:', err.message);
    process.exit(1);
});
