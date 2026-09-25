/**
 * scripts/run-job-structuring-worker.js
 * WORKER: BullMQ queue se jobs uthata hai, GPT-4o-mini se structure karta hai,
 * aur Supabase mein title + structured_skills + seniority save karta hai.
 *
 * FIXED: saves `title` from GPT's cleaned_title when original title is null.
 */

// 🔥 CRITICAL: Load environment variables FIRST
require('dotenv').config();

const { Worker } = require('bullmq');
const Redis = require('ioredis');
const { createClient } = require('@supabase/supabase-js');
const { structureJob } = require('../src/ai/gpt-structurer');
const { resolveRemoteType } = require('../src/utils/job-enrichment');
const ws = require('ws');

// ─── CONFIG ─────────────────────────────────────────────────────────────
const QUEUE_NAME = 'job-structuring';
const CONCURRENCY = Number(
    process.env.WORKER_CONCURRENCY ||
    process.env.JOB_STRUCTURING_CONCURRENCY
) || 2;

// ─── CLIENTS ────────────────────────────────────────────────────────────
const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

const redis = new Redis({
    host: process.env.REDIS_HOST || 'localhost',
    port: Number(process.env.REDIS_PORT) || 6379,
    maxRetriesPerRequest: null
});

// ─── STATS ──────────────────────────────────────────────────────────────
const stats = { success: 0, failed: 0, skipped: 0, startTime: Date.now() };

function printStats() {
    const elapsed = ((Date.now() - stats.startTime) / 1000 / 60).toFixed(1);
    const total = stats.success + stats.failed + stats.skipped;
    const rate = total > 0 ? (total / ((Date.now() - stats.startTime) / 1000)).toFixed(1) : 0;
    console.log(
        `\n📊 Stats — Elapsed: ${elapsed}m | ✅ ${stats.success} | ❌ ${stats.failed} | ⏭️ ${stats.skipped} | Rate: ${rate} jobs/s\n`
    );
}

// ─── WORKER ─────────────────────────────────────────────────────────────
const worker = new Worker(
    QUEUE_NAME,
    async (job) => {
        const { jobId, title, raw_description } = job.data;

        try {
            // 1. Structure the job with GPT-4o-mini
            const result = await structureJob({
                title,
                raw_description
            });

            if (!result) {
                stats.skipped++;
                console.log(`⏭️ Skipped job ${jobId} (no result)`);
                return { jobId, status: 'skipped' };
            }

            // Preserve an existing centralized value. GPT's remote_type is intentionally ignored.
            const { data: existingJob, error: existingJobError } = await supabase
                .from('jobs')
                .select('remote_type')
                .eq('id', jobId)
                .maybeSingle();

            if (existingJobError) {
                throw new Error(`Supabase remote_type fetch failed: ${existingJobError.message}`);
            }

            const existingRemoteType = existingJob?.remote_type;
            const normalizedRemoteType = existingRemoteType == null
                ? ''
                : String(existingRemoteType).trim().toLowerCase();
            const validRemoteTypes = new Set(['remote', 'hybrid', 'onsite']);
            const resolvedRemoteType = validRemoteTypes.has(normalizedRemoteType)
                ? existingRemoteType
                : resolveRemoteType(raw_description);

            // 2. Build update object
            const updates = {
                structured_skills: result.skills && result.skills.length > 0 ? result.skills : null,
                seniority_level: result.seniority_level,
                remote_type: resolvedRemoteType,
                employment_type: result.employment_type
            };

            // 3. 🔥 CRITICAL FIX: Save title if original is null/empty
            const originalTitle = title ? String(title).trim() : '';
            const hasValidTitle = originalTitle && originalTitle !== 'null' && originalTitle.length >= 3;

            if (!hasValidTitle && result.cleaned_title && result.cleaned_title.length >= 3) {
                updates.title = result.cleaned_title;
                console.log(`🏷️  Title extracted: "${result.cleaned_title}"`);
            }

            // 4. Save to Supabase
            const { error } = await supabase
                .from('jobs')
                .update(updates)
                .eq('id', jobId);

            if (error) {
                throw new Error(`Supabase update failed: ${error.message}`);
            }

            stats.success++;
            console.log(`✅ Job ${jobId} — ${result.skills.length} skills | ${result.seniority_level}`);

            if (stats.success % 25 === 0) printStats();

            return { jobId, status: 'success', skills: result.skills.length };
        } catch (err) {
            stats.failed++;
            console.error(`❌ Job ${jobId} failed: ${err.message}`);
            throw err;
        }
    },
    {
        connection: redis,
        concurrency: CONCURRENCY
    }
);

// ─── EVENT HANDLERS ─────────────────────────────────────────────────────
worker.on('ready', () => {
    console.log(`🚀 Worker ready | Concurrency: ${CONCURRENCY} | Queue: "${QUEUE_NAME}"`);
    console.log(`   Model: gpt-4o-mini\n`);
});

worker.on('failed', (job, err) => {
    console.error(`❌ Job ${job?.id} failed after ${job?.attemptsMade} attempts: ${err.message}`);
});

worker.on('error', (err) => {
    console.error(`❌ Worker error: ${err.message}`);
});

// ─── GRACEFUL SHUTDOWN ──────────────────────────────────────────────────
async function shutdown() {
    console.log('\n⏹️  Shutting down gracefully...');
    printStats();
    try { await worker.close(); } catch (e) {}
    try { await redis.quit(); } catch (e) {}
    process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

process.on('unhandledRejection', (reason) => {
    console.error('❌ Unhandled rejection:', reason);
});

process.on('uncaughtException', (err) => {
    console.error('❌ Uncaught exception:', err);
    process.exit(1);
});

console.log(`🚀 Worker started | Concurrency: ${CONCURRENCY} | Queue: "${QUEUE_NAME}"`);
