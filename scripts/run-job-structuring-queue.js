const { Queue, Worker } = require('bullmq');
const Redis = require('ioredis');
const { createClient } = require('@supabase/supabase-js');
const { structureJob } = require('../src/ai/job-structurer');
const ws = require('ws');
require('dotenv').config();

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

const redisConnection = new Redis({
    host: 'localhost',
    port: 6379,
    maxRetriesPerRequest: null
});

const QUEUE_NAME = 'job-structuring';
const queue = new Queue(QUEUE_NAME, { connection: redisConnection });

// ─── Add all jobs that need structuring ──────────────────────────────────
async function addJobsToQueue(limit = 5000) {
    const { data: jobs, error } = await supabase
        .from('jobs')
        .select('id, title, raw_description, company_id')
        .is('structured_skills', null)
        .limit(limit);

    if (error) {
        console.error('❌ Error fetching jobs:', error.message);
        return;
    }

    console.log(`📋 Adding ${jobs.length} jobs to queue...`);

    for (const job of jobs) {
        await queue.add('structure-job', {
            jobId: job.id,
            title: job.title,
            raw_description: job.raw_description,
            companyId: job.company_id
        }, {
            attempts: 3,
            backoff: { type: 'exponential', delay: 5000 }
        });
    }
    console.log(`✅ Added ${jobs.length} jobs to queue`);
}

// ─── Worker: process one job ──────────────────────────────────────────────
const worker = new Worker(QUEUE_NAME, async job => {
    const { jobId, title, raw_description } = job.data;
    console.log(`🔄 Structuring: ${title}`);

    try {
        const structured = await structureJob({ id: jobId, title, raw_description });

        if (!structured) {
            console.log(`⚠️ No structured data for ${title}`);
            return;
        }

        const { error: updateError } = await supabase
            .from('jobs')
            .update({
                structured_skills: structured.skills || [],
                seniority_level: structured.seniority_level || null,
                remote_type: structured.remote_type || null,
                employment_type: structured.employment_type || null,
                location: structured.location_city || null,
                last_seen_at: new Date().toISOString()
            })
            .eq('id', jobId);

        if (updateError) {
            console.error(`❌ Save error for ${title}:`, updateError.message);
            throw updateError; // triggers retry
        }

        console.log(`✅ Saved: ${title} | ${structured.skills?.length || 0} skills`);
    } catch (err) {
        console.error(`❌ Failed: ${title}`, err.message);
        throw err;
    }
}, {
    connection: redisConnection,
    concurrency: 5  // 5 parallel workers
});

worker.on('completed', job => console.log(`✅ Job ${job.id} completed`));
worker.on('failed', (job, err) => console.error(`❌ Job ${job.id} failed:`, err));

// ─── Start ──────────────────────────────────────────────────────────────────
(async () => {
    await addJobsToQueue(5000); // Process 5000 jobs per run (adjust later)
    console.log(`\n🚀 Queue has ${await queue.count()} jobs. Workers running...\n`);
})();

process.on('SIGINT', async () => {
    await worker.close();
    await queue.close();
    await redisConnection.quit();
    process.exit(0);
});
