#!/usr/bin/env node
/**
 * Google Jobs / SerpAPI Integration — FINAL (Using Existing Columns)
 * 
 * Fixes:
 *   - ✅ Only uses columns that exist in jobs table
 *   - ✅ Identifies Google Jobs via ats_source = 'google_jobs'
 *   - ✅ No extra columns needed
 *   - ✅ Proper upsert with onConflict
 *   - ✅ Better error handling
 * 
 * Usage:
 *   node scripts/run-google-jobs.js --concurrency 3 --resume
 *   node scripts/run-google-jobs.js --dry-run
 *   node scripts/run-google-jobs.js --limit 5 --verbose
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
    companyId:      args.find(a => a.startsWith('--company-id='))?.split('=')[1] || null,
    maxJobsPerCompany: parseInt(args.find(a => a.startsWith('--max-jobs='))?.split('=')[1] || 20),
    maxPages:       parseInt(args.find(a => a.startsWith('--pages='))?.split('=')[1] || 3),
    dryRun:         args.includes('--dry-run'),
    resume:         args.includes('--resume'),
    verbose:        args.includes('--verbose'),
    delayMs:        500,
    jobDelayMs:     200,
    pageSize:       1000,
    checkpointFile: path.join(__dirname, '.google-jobs-checkpoint.json'),
    maxRetries:     3,
};

// ─── CLIENTS ──────────────────────────────────────────────────────────────

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

// ─── MODEL CONFIG ──────────────────────────────────────────────────────────

const MODELS = [
    'claude-sonnet-4-6',
    'claude-3-5-sonnet-20241022',
    'claude-3-opus-20240229',
];

let workingModel = null;

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

// ─── FETCH COMPANIES ──────────────────────────────────────────────────────

async function fetchCompanies(checkpoint = null) {
    const allCompanies = [];
    let page = 0;
    let hasMore = true;

    console.log(`   📋 Fetching companies (paginated, ${CONFIG.pageSize} per page)...`);

    while (hasMore) {
        let query = supabase
            .from('companies')
            .select('Id, Name, Website, detected_career_url, ats_type, crawl_status')
            .not('Name', 'is', null)
            .order('Id', { ascending: true })
            .range(page * CONFIG.pageSize, (page + 1) * CONFIG.pageSize - 1);

        if (CONFIG.companyId) {
            query = query.eq('Id', CONFIG.companyId);
            const { data } = await query;
            return data || [];
        }

        query = query
            .in('crawl_status', ['failed', 'error'])
            .or('ats_type.eq.error,ats_type.eq.unknown');

        if (checkpoint?.processedIds?.length > 0) {
            query = query.not('Id', 'in', `(${checkpoint.processedIds.join(',')})`);
        }

        if (CONFIG.limit > 0 && allCompanies.length >= CONFIG.limit) {
            break;
        }

        const { data, error } = await query;
        if (error) throw new Error(`Supabase fetch error (page ${page + 1}): ${error.message}`);

        if (!data || data.length === 0) {
            hasMore = false;
            break;
        }

        allCompanies.push(...data);
        page++;

        if (data.length < CONFIG.pageSize) {
            hasMore = false;
        }
    }

    console.log(`   ✅ Fetched ${allCompanies.length} companies (${page} pages)`);
    return allCompanies;
}

// ─── SEARCH GOOGLE JOBS WITH RETRY ────────────────────────────────────────

async function searchGoogleJobs(companyName) {
    const allJobs = [];
    const searchQuery = `${companyName} jobs`;
    let retries = 0;

    console.log(`  🔍 Searching: "${searchQuery}" (max ${CONFIG.maxPages} pages)`);

    for (let page = 0; page < CONFIG.maxPages; page++) {
        const start = page * 10;

        try {
            const response = await getJson({
                api_key: process.env.SERPAPI_API_KEY,
                engine: 'google_jobs',
                q: searchQuery,
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
                    if (retries > CONFIG.maxRetries) {
                        console.log(`  ❌ Max retries exceeded`);
                        break;
                    }
                    continue;
                }
                if (response.error.includes('hasn\'t returned any results')) {
                    if (CONFIG.verbose) console.log(`     ℹ️ No results for this query`);
                    break;
                }
                console.log(`  ⚠️ SerpAPI: ${response.error}`);
                break;
            }

            const jobs = response.jobs_results || [];
            if (jobs.length === 0) {
                if (CONFIG.verbose) console.log(`     Page ${page + 1}: no more jobs`);
                break;
            }

            allJobs.push(...jobs);
            console.log(`     Page ${page + 1}: ${jobs.length} jobs (total: ${allJobs.length})`);

            if (allJobs.length >= CONFIG.maxJobsPerCompany) break;
            await sleep(300);

        } catch (err) {
            console.log(`  ❌ SerpAPI failed (page ${page + 1}): ${err.message}`);
            break;
        }
    }

    const trimmedJobs = allJobs.slice(0, CONFIG.maxJobsPerCompany);
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

async function structureJobWithClaude(job, companyName) {
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
        return manualExtractJob(job, companyName);
    }

    try {
        const response = await anthropic.messages.create({
            model: model,
            max_tokens: 800,
            messages: [{
                role: 'user',
                content: `You are a job data extractor. Extract structured data from this job posting.

TITLE: ${title || 'Unknown'}
COMPANY: ${company_name || companyName}
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
            return manualExtractJob(job, companyName);
        }

        if (CONFIG.verbose) {
            console.log(`     Claude response (first 200 chars): ${text.slice(0, 200)}`);
        }

        const parsed = extractJsonAggressive(text);

        if (!parsed) {
            if (CONFIG.verbose) console.log(`     ⚠️ JSON extraction failed, using fallback`);
            return manualExtractJob(job, companyName);
        }

        return {
            external_job_id: job_id || generateExternalJobId(job.url || title, title, companyName),
            title: (parsed.title || title || 'Untitled').slice(0, 500),
            raw_description: (parsed.description || fullDescription).slice(0, 5000),
            location: (parsed.location || location || '').slice(0, 200),
            employment_type: (parsed.employment_type || null) ? (parsed.employment_type).slice(0, 100) : null,
            seniority_level: (parsed.seniority_level || null) ? (parsed.seniority_level).slice(0, 100) : null,
            structured_skills: Array.isArray(parsed.skills) ? parsed.skills.slice(0, 20) : [],
            apply_url: (job.url || `https://www.google.com/search?q=${encodeURIComponent(`${companyName} ${title}`)}`).slice(0, 2000),
            posted_at: isValidDate(detected_extensions?.posted_at) ? new Date(detected_extensions.posted_at) : null,
        };

    } catch (err) {
        if (CONFIG.verbose) console.log(`     ⚠️ Claude error: ${err.message}`);
        return manualExtractJob(job, companyName);
    }
}

// ─── MANUAL EXTRACTION FALLBACK ────────────────────────────────────────────

function manualExtractJob(job, companyName) {
    const { title, company_name, location, job_id, detected_extensions } = job;

    if (!title) return null;

    return {
        external_job_id: job_id || generateExternalJobId(job.url || title, title, companyName),
        title: (title || 'Untitled').slice(0, 500),
        raw_description: (job.description || '').slice(0, 5000),
        location: (location || '').slice(0, 200),
        employment_type: null,
        seniority_level: null,
        structured_skills: [],
        apply_url: (job.url || `https://www.google.com/search?q=${encodeURIComponent(`${companyName} ${title}`)}`).slice(0, 2000),
        posted_at: isValidDate(detected_extensions?.posted_at) ? new Date(detected_extensions.posted_at) : null,
    };
}

// ─── CHECK DUPLICATE ──────────────────────────────────────────────────────

async function isDuplicateJob(title, companyId) {
    try {
        const { data, error } = await supabase
            .from('jobs')
            .select('id, title')
            .eq('company_id', companyId)
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
    } catch (err) {
        return false;
    }
}

// ─── CLEAN JOB OBJECT — ONLY USES EXISTING COLUMNS ──────────────────────

function cleanJobForUpsert(job) {
    // 🔥 Only use columns that exist in the jobs table
    const cleaned = {
        company_id: job.company_id,
        external_job_id: job.external_job_id,
        title: job.title,
        raw_description: job.raw_description || null,
        location: job.location || null,
        employment_type: job.employment_type || null,
        seniority_level: job.seniority_level || null,
        structured_skills: Array.isArray(job.structured_skills) ? job.structured_skills : [],
        apply_url: job.apply_url || null,
        posted_at: job.posted_at || null,
        ats_source: 'google_jobs',   // ✅ Identify as Google Jobs
        is_active: true,
        first_seen_at: new Date().toISOString(),
        last_seen_at: new Date().toISOString(),
    };

    // Remove any undefined values
    Object.keys(cleaned).forEach(key => {
        if (cleaned[key] === undefined) {
            delete cleaned[key];
        }
    });

    return cleaned;
}

// ─── PROCESS A SINGLE COMPANY ─────────────────────────────────────────────

async function processCompany(company) {
    const startTime = Date.now();
    const results = {
        companyId: company.Id,
        companyName: company.Name,
        found: 0,
        structured: 0,
        saved: 0,
        duplicates: 0,
        errors: 0,
        jobs: [],
    };

    console.log(`\n🏢 ${company.Name}`);

    try {
        const googleJobs = await searchGoogleJobs(company.Name);

        if (googleJobs.length === 0) {
            console.log(`  ⏭️ No jobs found`);
            return results;
        }

        results.found = googleJobs.length;

        for (const job of googleJobs) {
            try {
                const jobTitle = job.title || 'Untitled';

                if (await isDuplicateJob(jobTitle, company.Id)) {
                    results.duplicates++;
                    console.log(`  ⏭️ Dup: ${jobTitle}`);
                    continue;
                }

                const structuredJob = await structureJobWithClaude(job, company.Name);

                if (!structuredJob) {
                    results.errors++;
                    console.log(`  ❌ Could not structure: ${jobTitle}`);
                    continue;
                }

                results.structured++;
                structuredJob.company_id = company.Id;
                structuredJob.ats_source = 'google_jobs';
                structuredJob.is_active = true;
                structuredJob.first_seen_at = new Date().toISOString();
                structuredJob.last_seen_at = new Date().toISOString();

                results.jobs.push(structuredJob);
                console.log(`  ✅ ${structuredJob.title} (${structuredJob.structured_skills.length} skills)`);

            } catch (err) {
                results.errors++;
                console.log(`  ❌ Error: ${err.message}`);
            }

            await sleep(CONFIG.jobDelayMs);
        }

        // ─── SAVE JOBS ──────────────────────────────────────────────────────
        console.log(`  🔍 DEBUG: jobs.length = ${results.jobs.length}, dryRun = ${CONFIG.dryRun}`);

        if (results.jobs.length > 0 && !CONFIG.dryRun) {
            console.log(`  💾 Attempting to save ${results.jobs.length} jobs...`);

            try {
                const cleanedJobs = results.jobs.map(cleanJobForUpsert);

                if (CONFIG.verbose && cleanedJobs.length > 0) {
                    const sample = { ...cleanedJobs[0] };
                    sample.raw_description = sample.raw_description ? sample.raw_description.slice(0, 100) + '...' : null;
                    console.log(`     Sample job: ${JSON.stringify(sample).slice(0, 500)}...`);
                }

                const { error } = await supabase
                    .from('jobs')
                    .upsert(cleanedJobs, {
                        onConflict: 'company_id,external_job_id',
                    });

                if (error) {
                    console.log(`  ❌ Supabase error: ${error.message}`);
                    if (error.details) console.log(`     Details: ${error.details}`);
                    if (error.hint) console.log(`     Hint: ${error.hint}`);
                    
                    // Fallback: insert one by one
                    console.log(`  🔄 Trying fallback: insert one by one...`);
                    let successCount = 0;
                    for (const job of cleanedJobs) {
                        const { error: singleError } = await supabase
                            .from('jobs')
                            .insert(job, { onConflict: 'company_id,external_job_id' });
                        if (!singleError) {
                            successCount++;
                        } else {
                            console.log(`     ❌ Failed: ${job.title}: ${singleError.message}`);
                        }
                        await sleep(100);
                    }
                    results.saved = successCount;
                    console.log(`  ✅ Saved ${results.saved} jobs (fallback)`);
                } else {
                    results.saved = results.jobs.length;
                    console.log(`  ✅ Saved ${results.saved} jobs`);
                }

            } catch (err) {
                console.log(`  ❌ Exception: ${err.message}`);
                if (CONFIG.verbose) console.log(`     Stack: ${err.stack}`);
            }
        } else if (CONFIG.dryRun) {
            console.log(`  🔍 DRY: Would save ${results.jobs.length} jobs`);
        } else if (results.jobs.length === 0) {
            console.log(`  ⚠️ No jobs to save`);
        }

    } catch (err) {
        console.log(`  ❌ Company error: ${err.message}`);
    }

    const elapsed = Date.now() - startTime;
    console.log(`  ⏱️ ${formatDuration(elapsed)}`);

    return results;
}

// ─── WORKER POOL ──────────────────────────────────────────────────────────

async function runWorkerPool(companies, concurrency) {
    const total = companies.length;
    const queue = [...companies];
    const results = {
        totalCompanies: total,
        processed: 0,
        companiesWithJobs: 0,
        totalJobsFound: 0,
        totalJobsStructured: 0,
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
            const company = queue.shift();
            if (!company) break;

            if (CONFIG.resume && processedIds.includes(company.Id)) {
                results.processed++;
                printProgress();
                continue;
            }

            const result = await processCompany(company);
            results.processed++;
            results.details.push(result);

            results.totalJobsFound += result.found;
            results.totalJobsStructured += result.structured;
            results.totalJobsSaved += result.saved;
            results.totalDuplicates += result.duplicates;
            results.totalErrors += result.errors;
            if (result.saved > 0) results.companiesWithJobs++;

            processedIds.push(company.Id);
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
    console.log('  GOOGLE JOBS COMPLETE');
    console.log('════════════════════════════════════════════');
    console.log(`  Duration              : ${formatDuration(elapsed)}`);
    console.log(`  Companies processed   : ${results.totalCompanies}`);
    console.log(`  Companies with jobs   : ${results.companiesWithJobs}`);
    console.log(`  ─────────────────────────────────────────`);
    console.log(`  Jobs found            : ${results.totalJobsFound}`);
    console.log(`  Jobs structured       : ${results.totalJobsStructured}`);
    console.log(`  Jobs saved ✅         : ${results.totalJobsSaved}`);
    console.log(`  ─────────────────────────────────────────`);
    console.log(`  Duplicates skipped    : ${results.totalDuplicates}`);
    console.log(`  Errors                : ${results.totalErrors}`);
    console.log('════════════════════════════════════════════\n');

    const top = results.details
        .filter(d => d.saved > 0)
        .sort((a, b) => b.saved - a.saved)
        .slice(0, 10);

    if (top.length > 0) {
        console.log('  📊 Top Companies by Jobs Saved:');
        top.forEach((d, i) => {
            console.log(`     ${i + 1}. ${d.companyName} → ${d.saved} saved`);
        });
    }
}

// ─── MAIN ──────────────────────────────────────────────────────────────────

async function main() {
    console.log('\n🔍 Google Jobs / SerpAPI Integration (Final)');
    console.log(`   Concurrency      : ${CONFIG.concurrency}`);
    console.log(`   Max jobs/co      : ${CONFIG.maxJobsPerCompany}`);
    console.log(`   Mode             : ${CONFIG.dryRun ? '🔍 DRY-RUN' : '💾 LIVE (saving)'}`);
    console.log(`   Resume           : ${CONFIG.resume ? '✅' : '❌'}`);
    console.log(`   Verbose          : ${CONFIG.verbose ? '✅' : '❌'}`);
    console.log(`   Limit            : ${CONFIG.limit || 'All'}\n`);

    const checkpoint = CONFIG.resume ? loadCheckpoint() : null;
    if (checkpoint) {
        console.log(`   ℹ️ Resuming (${checkpoint.processedIds.length} done)\n`);
    }

    const companies = await fetchCompanies(checkpoint);

    if (companies.length === 0) {
        console.log('✅ No companies to process.');
        return;
    }

    console.log(`   Processing ${companies.length} companies...\n`);

    const { results, elapsed } = await runWorkerPool(companies, CONFIG.concurrency);
    printSummary(results, elapsed);
}

main().catch(err => {
    console.error('❌ Fatal error:', err.message);
    process.exit(1);
});
