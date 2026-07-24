/**
 * run-job-structuring-worker.js
 * Uses GPT-4.1 Mini – no fallback.
 */

// 🔥 CRITICAL: Load environment variables FIRST
require('dotenv').config();

const { Worker, MetricsTime } = require('bullmq');
const Redis = require('ioredis');
const { createClient } = require('@supabase/supabase-js');
const { structureJob } = require('../src/ai/gpt-structurer');
const ws = require('ws');

const QUEUE_NAME    = 'job-structuring';
const CONCURRENCY   = Number(process.env.WORKER_CONCURRENCY) || 10;

// ─── Clients ───────────────────────────────────────────────────────────────
const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

const redis = new Redis({
    host: process.env.REDIS_HOST || 'localhost',
    port: Number(process.env.REDIS_PORT) || 6379,
    maxRetriesPerRequest: null,
});

// ─── Stats tracker ─────────────────────────────────────────────────────────
const stats = { success: 0, failed: 0, startTime: Date.now() };

function printStats() {
    const elapsed    = ((Date.now() - stats.startTime) / 1000 / 60).toFixed(1);
    const total      = stats.success + stats.failed;
    const rate       = total > 0 ? (total / ((Date.now() - stats.startTime) / 1000)).toFixed(1) : 0;
    console.log(
        `\n📊  Stats — Elapsed: ${elapsed}m | ✅ ${stats.success} | ❌ ${stats.failed} | Rate: ${rate} jobs/s\n`
    );
}

setInterval(printStats, 30_000);

// ─── Worker ────────────────────────────────────────────────────────────────
const worker = new Worker(
    QUEUE_NAME,
    async (job) => {
        const { jobId, title, raw_description } = job.data;

        const structured = await structureJob({ id: jobId, title, raw_description });

        if (!structured) {
            console.warn(`⚠️  GPT returned null for: "${title}" – job will be skipped.`);
            return;
        }

        const { error: updateError } = await supabase
            .from('jobs')
            .update({
                structured_skills: structured.skills          || [],
                seniority_level:   structured.seniority_level || null,
                remote_type:       structured.remote_type     || null,
                employment_type:   structured.employment_type || null,
                location:          structured.location_city   || null,
                last_seen_at:      new Date().toISOString(),
            })
            .eq('id', jobId);

        if (updateError) {
            throw new Error(`Supabase update failed: ${updateError.message}`);
        }

        const skillsCount = structured.skills?.length || 0;
        console.log(`✅  "${title}" — ${skillsCount} skills | ${structured.seniority_level || '?'} | ${structured.remote_type || '?'}`);
    },
    {
        connection:  redis,
        concurrency: CONCURRENCY,
        metrics: { maxDataPoints: MetricsTime.ONE_WEEK },
    }
);

// ─── Event handlers ────────────────────────────────────────────────────────
worker.on('completed', () => { stats.success++; });

worker.on('failed', (job, err) => {
    stats.failed++;
    console.error(`❌  FAILED: "${job?.data?.title}" — Attempt ${job?.attemptsMade}/${job?.opts?.attempts} — ${err.message}`);
});

worker.on('error', (err) => {
    console.error('🔥  Worker error:', err);
});

// ─── Startup ───────────────────────────────────────────────────────────────
console.log(`\n🏃  Worker started (GPT-4.1 Mini only) | Concurrency: ${CONCURRENCY} | Queue: "${QUEUE_NAME}"\n`);
console.log('  Press Ctrl+C to close\n');

// ─── Graceful shutdown ─────────────────────────────────────────────────────
async function shutdown(signal) {
    console.log(`\n⚠️  ${signal} received — shutting down…`);
    printStats();
    await worker.close();
    await redis.quit();
    console.log('👋  Worker stopped.\n');
    process.exit(0);
}

process.on('SIGINT',  () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
