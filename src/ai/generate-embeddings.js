/**
 * Generate Embeddings — Voyage AI (FIXED with WebSocket)
 * 
 * Features:
 *   - ✅ WebSocket transport fix for Node 20+
 *   - ✅ Batch processing
 *   - ✅ Progress tracking
 *   - ✅ Error handling
 * 
 * Usage:
 *   node scripts/run-embeddings.js
 *   node scripts/run-embeddings.js --limit 100
 *   node scripts/run-embeddings.js --dry-run
 */

const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');   // 🔥 FIX: WebSocket support
const axios = require('axios');
require('dotenv').config();

// ─── CONFIG ──────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);
const LIMIT = parseInt(args.find(a => a.startsWith('--limit='))?.split('=')[1] || 0);
const DRY_RUN = args.includes('--dry-run');

// ─── SUPABASE ──────────────────────────────────────────────────────────────

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }   // 🔥 FIX: WebSocket transport
);

// ─── VOYAGE AI ──────────────────────────────────────────────────────────────

const VOYAGE_API_KEY = process.env.VOYAGE_API_KEY;
const VOYAGE_URL = 'https://api.voyageai.com/v1/embeddings';
const VOYAGE_MODEL = 'voyage-3-large';

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

function jobToText(job) {
    const parts = [];
    if (job.title) parts.push(`Title: ${job.title}`);
    if (job.structured_skills && Array.isArray(job.structured_skills)) {
        parts.push(`Skills: ${job.structured_skills.join(', ')}`);
    }
    if (job.seniority_level) parts.push(`Level: ${job.seniority_level}`);
    if (job.location) parts.push(`Location: ${job.location}`);
    return parts.join('. ') || job.title || '';
}

// ─── FETCH JOBS ──────────────────────────────────────────────────────────────

async function fetchJobsToEmbed() {
    let query = supabase
        .from('jobs')
        .select('id, title, structured_skills, seniority_level, location')
        .not('structured_skills', 'is', null)
        .gt('structured_skills_length', 0)
        .is('skill_embedding', null);

    if (LIMIT > 0) {
        query = query.limit(LIMIT);
    }

    const { data, error } = await query;
    if (error) throw new Error(`Supabase fetch error: ${error.message}`);
    return data || [];
}

// ─── EMBED A BATCH ──────────────────────────────────────────────────────────

async function embedBatch(texts) {
    try {
        const response = await axios.post(VOYAGE_URL, {
            model: VOYAGE_MODEL,
            input: texts
        }, {
            headers: {
                'Authorization': `Bearer ${VOYAGE_API_KEY}`,
                'Content-Type': 'application/json'
            },
            timeout: 60000
        });
        return response.data.data.map(item => item.embedding);
    } catch (err) {
        if (err.response?.status === 429) {
            const retryAfter = parseInt(err.response?.headers?.['retry-after']) || 30;
            return { rateLimit: true, retryAfter };
        }
        throw err;
    }
}

// ─── PROCESS JOBS ──────────────────────────────────────────────────────────────

async function processJobs(jobs) {
    const total = jobs.length;
    let processed = 0;
    let embedded = 0;
    let failed = 0;
    const startTime = Date.now();

    console.log(`\n📋 Total jobs to embed: ${total}\n`);

    // Process in batches of 10
    const BATCH_SIZE = 10;

    for (let i = 0; i < jobs.length; i += BATCH_SIZE) {
        const batch = jobs.slice(i, i + BATCH_SIZE);
        const texts = batch.map(job => jobToText(job));

        if (DRY_RUN) {
            processed += batch.length;
            embedded += batch.length;
            console.log(`  🔍 [DRY] ${processed}/${total}: ${batch.length} jobs`);
            continue;
        }

        try {
            const result = await embedBatch(texts);

            if (result.rateLimit) {
                console.log(`  ⏳ Rate limited, waiting ${result.retryAfter}s...`);
                await sleep(result.retryAfter * 1000);
                i -= BATCH_SIZE; // retry this batch
                continue;
            }

            // Save embeddings
            const embeddings = result;
            const updates = batch.map((job, idx) => ({
                id: job.id,
                skill_embedding: embeddings[idx]
            }));

            for (const update of updates) {
                const { error } = await supabase
                    .from('jobs')
                    .update({ skill_embedding: update.skill_embedding })
                    .eq('id', update.id);

                if (error) {
                    console.error(`  ❌ Update error for ${update.id}: ${error.message}`);
                    failed++;
                } else {
                    embedded++;
                }
            }

            processed += batch.length;

            // Progress
            const elapsed = Date.now() - startTime;
            const rate = processed / (elapsed / 1000);
            const remaining = total - processed;
            const eta = rate > 0 ? remaining / rate : 0;

            process.stdout.write(`\r  Progress: ${processed}/${total} (${((processed/total)*100).toFixed(1)}%) | ✅ ${embedded} ❌ ${failed} | ETA: ${formatDuration(eta * 1000)}    `);

            // Rate limit between batches
            await sleep(100);

        } catch (err) {
            console.log(`  ❌ Batch error: ${err.message}`);
            failed += batch.length;
            processed += batch.length;
        }
    }

    console.log('\n');
    return { processed, embedded, failed };
}

// ─── MAIN ──────────────────────────────────────────────────────────────────────

async function main() {
    console.log('\n🧠 Job Embeddings (FIXED with WebSocket)');
    console.log(`   Limit  : ${LIMIT || 'All'}`);
    console.log(`   Dry run: ${DRY_RUN ? 'ON' : 'OFF'}\n`);

    const jobs = await fetchJobsToEmbed();

    if (jobs.length === 0) {
        console.log('✅ No jobs need embedding.');
        return;
    }

    console.log(`   Found ${jobs.length} jobs to embed\n`);

    const result = await processJobs(jobs);

    console.log('══════════════════════════════════════════');
    console.log('  ✅ JOB EMBEDDINGS COMPLETE');
    console.log('══════════════════════════════════════════');
    console.log(`  Processed : ${result.processed}`);
    console.log(`  Embedded  : ${result.embedded}`);
    console.log(`  Failed    : ${result.failed}`);
    console.log('══════════════════════════════════════════\n');
}

main().catch(err => {
    console.error('❌ Fatal error:', err.message);
    process.exit(1);
});
