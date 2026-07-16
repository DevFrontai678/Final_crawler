/**
 * Job Structuring — WITH PAGINATION (Production Ready)
 * 
 * Features:
 *   - ✅ Paginated fetching (1000 per batch)
 *   - ✅ Uses claude-sonnet-4-6 (balanced model)
 *   - ✅ Handles NULL and empty array structured_skills
 *   - ✅ Progress tracking with ETA
 *   - ✅ Better error handling
 *   - ✅ Memory efficient
 * 
 * Usage:
 *   node scripts/run-job-structuring.js
 *   node scripts/run-job-structuring.js --limit 100
 *   node scripts/run-job-structuring.js --dry-run
 */

'use strict';

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const Anthropic = require('@anthropic-ai/sdk');
const ws = require('ws');

// ─── CONFIG ──────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);
const LIMIT = parseInt(args.find(a => a.startsWith('--limit='))?.split('=')[1] || 0);
const DRY_RUN = args.includes('--dry-run');

// ─── CLIENTS ──────────────────────────────────────────────────────────────────

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

// ─── MODEL ──────────────────────────────────────────────────────────────────────

// 🔥 Middle model — Claude Sonnet 4.6 (balanced quality/cost)
const PRIMARY_MODEL = 'claude-sonnet-4-6';
const FALLBACK_MODEL = 'claude-sonnet-4-5-20250929';

// ─── HELPERS ──────────────────────────────────────────────────────────────────

function sleep(ms) {
    return new Promise(r => setTimeout(r, ms));
}

function formatDuration(ms) {
    const s = Math.round(ms / 1000);
    if (s < 60) return `${s}s`;
    if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
    return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
}

// ─── FETCH JOBS — PAGINATED ──────────────────────────────────────────────────

async function fetchJobsToStructure() {
    const allJobs = [];
    let page = 0;
    const PAGE_SIZE = 1000;
    let hasMore = true;

    console.log(`   📋 Fetching jobs in batches of ${PAGE_SIZE}...`);

    while (hasMore) {
        const start = page * PAGE_SIZE;
        const end = start + PAGE_SIZE - 1;

        let query = supabase
            .from('jobs')
            .select('id, title, raw_description')
            .not('raw_description', 'is', null)
            .or('structured_skills.is.null,structured_skills.eq.{}')
            .order('id', { ascending: true })
            .range(start, end);

        if (LIMIT > 0 && allJobs.length >= LIMIT) {
            break;
        }

        if (LIMIT > 0) {
            const remaining = LIMIT - allJobs.length;
            if (remaining < PAGE_SIZE) {
                query = query.range(start, start + remaining - 1);
            }
        }

        const { data, error } = await query;
        if (error) throw new Error(`Supabase fetch error: ${error.message}`);

        if (!data || data.length === 0) {
            hasMore = false;
            break;
        }

        // Filter jobs with description length > 100 characters
        const filtered = data.filter(job => job.raw_description && job.raw_description.length > 100);
        allJobs.push(...filtered);

        console.log(`   ✅ Page ${page + 1}: fetched ${data.length} jobs (${filtered.length} valid)`);

        page++;

        if (data.length < PAGE_SIZE) {
            hasMore = false;
        }

        if (LIMIT > 0 && allJobs.length >= LIMIT) {
            break;
        }
    }

    console.log(`   ✅ Total jobs to structure: ${allJobs.length}`);
    return allJobs;
}

// ─── STRUCTURE JOB WITH CLAUDE ──────────────────────────────────────────────

async function structureJobWithClaude(title, description) {
    try {
        const response = await anthropic.messages.create({
            model: PRIMARY_MODEL,
            max_tokens: 500,
            messages: [{
                role: 'user',
                content: `Extract skills from this job description. Return ONLY a JSON array of skill strings in English.

Job Title: ${title}
Description: ${description.slice(0, 3000)}

JSON array:`
            }]
        });

        const text = response.content[0].text.trim().replace(/```json|```/g, '');
        return JSON.parse(text);
    } catch (err) {
        if (err.status === 404 || err.message.includes('model')) {
            console.log(`  ⚠️ Primary model failed, trying fallback...`);
            try {
                const response = await anthropic.messages.create({
                    model: FALLBACK_MODEL,
                    max_tokens: 500,
                    messages: [{
                        role: 'user',
                        content: `Extract skills from this job description. Return ONLY a JSON array of skill strings in English.

Job Title: ${title}
Description: ${description.slice(0, 3000)}

JSON array:`
                    }]
                });
                const text = response.content[0].text.trim().replace(/```json|```/g, '');
                return JSON.parse(text);
            } catch (fallbackErr) {
                console.log(`  ⚠️ Fallback also failed: ${fallbackErr.message}`);
                return [];
            }
        }
        console.log(`  ⚠️ Claude error: ${err.message}`);
        return [];
    }
}

// ─── PROCESS JOBS ──────────────────────────────────────────────────────────────

async function processJobs(jobs) {
    const total = jobs.length;
    let processed = 0;
    let structured = 0;
    let failed = 0;
    const startTime = Date.now();

    console.log(`\n📋 Total jobs to structure: ${total}\n`);

    for (const job of jobs) {
        processed++;

        if (DRY_RUN) {
            console.log(`  🔍 [DRY] ${processed}/${total}: ${job.title}`);
            structured++;
            continue;
        }

        try {
            const skills = await structureJobWithClaude(job.title, job.raw_description);

            const { error } = await supabase
                .from('jobs')
                .update({
                    structured_skills: skills || [],
                })
                .eq('id', job.id);

            if (error) {
                console.error(`  ❌ ${processed}/${total}: ${job.title} — DB error: ${error.message}`);
                failed++;
            } else {
                const skillCount = skills ? skills.length : 0;
                if (skillCount > 0) {
                    console.log(`  ✅ ${processed}/${total}: ${job.title} — ${skillCount} skills`);
                } else {
                    console.log(`  ⏭️ ${processed}/${total}: ${job.title} — 0 skills`);
                }
                structured++;
            }

        } catch (err) {
            console.log(`  ❌ ${processed}/${total}: ${job.title} — ${err.message}`);
            failed++;
        }

        // Progress
        const elapsed = Date.now() - startTime;
        const rate = processed / (elapsed / 1000);
        const remaining = total - processed;
        const eta = rate > 0 ? remaining / rate : 0;

        process.stdout.write(`\r  Progress: ${processed}/${total} (${((processed/total)*100).toFixed(1)}%) | ✅ ${structured} ❌ ${failed} | ETA: ${formatDuration(eta * 1000)}    `);

        await sleep(400);
    }

    console.log('\n');
    return { processed, structured, failed };
}

// ─── MAIN ──────────────────────────────────────────────────────────────────────

async function main() {
    console.log('\n🧠 Job Structuring — WITH PAGINATION');
    console.log(`   Model   : ${PRIMARY_MODEL} (balanced)`);
    console.log(`   Limit   : ${LIMIT || 'All'}`);
    console.log(`   Dry run : ${DRY_RUN ? 'ON' : 'OFF'}\n`);

    const jobs = await fetchJobsToStructure();

    if (jobs.length === 0) {
        console.log('✅ No jobs need structuring.');
        return;
    }

    const result = await processJobs(jobs);

    console.log('══════════════════════════════════════════');
    console.log('  ✅ JOB STRUCTURING COMPLETE');
    console.log('══════════════════════════════════════════');
    console.log(`  Processed : ${result.processed}`);
    console.log(`  Structured: ${result.structured}`);
    console.log(`  Failed    : ${result.failed}`);
    console.log(`  Model used: ${PRIMARY_MODEL}`);
    console.log('══════════════════════════════════════════\n');
}

main().catch(err => {
    console.error('❌ Fatal error:', err.message);
    process.exit(1);
});
