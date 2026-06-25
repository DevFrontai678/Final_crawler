/**
 * run-job-structuring-queue.js
 * PRODUCER: Supabase se jobs fetch karke BullMQ queue mein daalta hai.
 * Alag chalao: node run-job-structuring-queue.js
 */

const { Queue } = require('bullmq');
const Redis = require('ioredis');
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
require('dotenv').config();

// ─── Config ────────────────────────────────────────────────────────────────
const QUEUE_NAME      = 'job-structuring';
const BATCH_SIZE      = 1000;   // Supabase se ek baar mein kitne fetch karein
const TOTAL_LIMIT     = 25000;  // Maximum jobs to enqueue in one run

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

const queue = new Queue(QUEUE_NAME, { connection: redis });

// ─── Main ──────────────────────────────────────────────────────────────────
async function main() {
  console.log('\n📥  Job Structuring — PRODUCER\n');

  let totalEnqueued = 0;
  let offset        = 0;
  let hasMore       = true;

  while (hasMore && totalEnqueued < TOTAL_LIMIT) {
    const fetchLimit = Math.min(BATCH_SIZE, TOTAL_LIMIT - totalEnqueued);

    // DB-level filter: sirf woh jobs jo abhi tak structure nahi huin
    const { data: jobs, error } = await supabase
      .from('jobs')
      .select('id, title, raw_description, company_id')
      .or('structured_skills.is.null,structured_skills.eq.{}')
      .range(offset, offset + fetchLimit - 1);

    if (error) {
      console.error('❌ Supabase fetch error:', error.message);
      break;
    }

    if (!jobs || jobs.length === 0) {
      hasMore = false;
      break;
    }

    // Bulk enqueue — addBulk ek hi network round-trip mein saari jobs daal deta hai
    const bulkJobs = jobs.map(job => ({
      name: 'structure-job',
      data: {
        jobId:           job.id,
        title:           job.title,
        raw_description: job.raw_description,
        companyId:       job.company_id,
      },
      opts: {
        attempts:    3,
        backoff:     { type: 'exponential', delay: 5000 },
        removeOnComplete: { count: 1000 },  // queue bhari nahi rahegi
        removeOnFail:     { count: 500  },
      },
    }));

    await queue.addBulk(bulkJobs);

    totalEnqueued += jobs.length;
    offset        += jobs.length;
    hasMore        = jobs.length === fetchLimit;

    console.log(`   ✅  Enqueued ${totalEnqueued} jobs so far…`);
  }

  const queueCount = await queue.count();
  console.log(`\n🚀  Done! Total enqueued: ${totalEnqueued}`);
  console.log(`📊  Queue depth right now: ${queueCount}\n`);

  await redis.quit();
}

main().catch(err => {
  console.error('❌ Fatal error:', err);
  process.exit(1);
});
