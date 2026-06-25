/**
 * run-job-structuring-worker.js
 * CONSUMER: Queue se jobs uthata hai aur Claude se structure karta hai.
 * Alag terminal mein chalao: node run-job-structuring-worker.js
 * Multiple workers chalana chahte ho? WORKER_COUNT env set karo.
 */

const { Worker, MetricsTime } = require('bullmq');
const Redis = require('ioredis');
const { createClient } = require('@supabase/supabase-js');
const { structureJob } = require('../src/ai/job-structurer');
const ws = require('ws');
require('dotenv').config();

// ─── Config ────────────────────────────────────────────────────────────────
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

// Print stats print on every 30 sec 
setInterval(printStats, 30_000);

// ─── Worker ────────────────────────────────────────────────────────────────
const worker = new Worker(
  QUEUE_NAME,
  async (job) => {
    const { jobId, title, raw_description } = job.data;

    const structured = await structureJob({ id: jobId, title, raw_description });

    if (!structured) {
      console.warn(`⚠️  No structured data returned for: "${title}"`);
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
      // Error throw karo taake BullMQ retry kare
      throw new Error(`Supabase update failed: ${updateError.message}`);
    }

    console.log(`✅  "${title}" — ${structured.skills?.length || 0} skills | ${structured.seniority_level || '?'} | ${structured.remote_type || '?'}`);
  },
  {
    connection:  redis,
    concurrency: CONCURRENCY,
    // Metrics track karo BullMQ dashboard ke liye (optional)
    metrics: {
      maxDataPoints: MetricsTime.ONE_WEEK,
    },
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
console.log(`\n🏃  Worker started | Concurrency: ${CONCURRENCY} | Queue: "${QUEUE_NAME}"\n`);
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
