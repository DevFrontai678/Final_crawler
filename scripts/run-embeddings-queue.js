const { Queue, Worker } = require('bullmq');
const Redis = require('ioredis');
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const axios = require('axios');
require('dotenv').config();

// ─── CONFIG ──────────────────────────────────────────────────────────────────
const BATCH_SIZE = 10;
const CONCURRENCY = 5;
const MAX_RETRIES = 3;
const DRY_RUN = process.argv.includes('--dry-run');

// ─── SUPABASE ──────────────────────────────────────────────────────────────
const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

// ─── REDIS ──────────────────────────────────────────────────────────────────
const redisConnection = new Redis({
    host: 'localhost',
    port: 6379,
    maxRetriesPerRequest: null
});

const QUEUE_NAME = 'embedding-queue';
const embeddingQueue = new Queue(QUEUE_NAME, { connection: redisConnection });

// ─── VOYAGE AI ──────────────────────────────────────────────────────────────
const VOYAGE_API_KEY = process.env.VOYAGE_API_KEY;
const VOYAGE_URL = 'https://api.voyageai.com/v1/embeddings';
const VOYAGE_MODEL = 'voyage-3-large';

// ─── FETCH JOBS TO EMBED (PAGINATED) ─────────────────────────────────────
async function fetchJobsToEmbed() {
    let allJobs = [];
    let page = 0;
    const limit = 1000;
    let hasMore = true;

    while (hasMore) {
        const { data: jobs, error } = await supabase
            .from('jobs')
            .select('id, title, structured_skills, seniority_level, remote_type, location')
            .is('skill_embedding', null)
            .not('structured_skills', 'is', null)
            .order('id', { ascending: true })
            .range(page * limit, (page + 1) * limit - 1);

        if (error) throw new Error(`Supabase fetch error: ${error.message}`);
        if (!jobs || jobs.length === 0) {
            hasMore = false;
        } else {
            allJobs = allJobs.concat(jobs);
            page++;
        }
    }

    // Filter out jobs with empty structured_skills
    return allJobs.filter(job =>
        Array.isArray(job.structured_skills) && job.structured_skills.length > 0
    );
}

// ─── ADD JOBS TO QUEUE ─────────────────────────────────────────────────────
async function addJobsToQueue() {
    const jobs = await fetchJobsToEmbed();
    if (jobs.length === 0) {
        console.log('✅ No jobs need embeddings.');
        return;
    }

    console.log(`📋 Adding ${jobs.length} jobs to the queue...`);
    for (const job of jobs) {
        await embeddingQueue.add('embed-job', {
            jobId: job.id,
            jobData: job
        }, {
            attempts: MAX_RETRIES,
            backoff: { type: 'exponential', delay: 5000 }
        });
    }
    console.log(`✅ Added ${jobs.length} jobs.`);
}

// ─── CONVERT JOB TO TEXT ─────────────────────────────────────────────────
function jobToText(job) {
    const skills = Array.isArray(job.structured_skills)
        ? job.structured_skills.join(', ')
        : (job.structured_skills || '');
    const level = job.seniority_level || '';
    const remote = job.remote_type || '';
    const location = job.location || '';
    return `${job.title}. Skills: ${skills}. Level: ${level}. Remote: ${remote}. Location: ${location}`;
}

// ─── EMBED A BATCH ────────────────────────────────────────────────────────
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
            const retryAfter = err.response?.headers?.['retry-after'] || 30;
            return { rateLimit: true, retryAfter: parseInt(retryAfter) };
        }
        throw err;
    }
}

// ─── WORKER ──────────────────────────────────────────────────────────────────
const worker = new Worker(QUEUE_NAME, async job => {
    const { jobId, jobData } = job.data;

    if (DRY_RUN) {
        console.log(`🔍 [DRY] Would embed job ${jobId}`);
        return;
    }

    const text = jobToText(jobData);
    console.log(`🔄 Processing job ${jobId} (${jobData.structured_skills?.length || 0} skills)`);

    const result = await embedBatch([text]);

    if (result.rateLimit) {
        console.log(`⏳ Rate limit hit, waiting ${result.retryAfter}s...`);
        await new Promise(r => setTimeout(r, result.retryAfter * 1000));
        throw new Error('Rate limit retry');
    }

    const embedding = result[0];
    const { error: updateError } = await supabase
        .from('jobs')
        .update({ skill_embedding: embedding })
        .eq('id', jobId);

    if (updateError) {
        console.error(`❌ Save error for job ${jobId}:`, updateError.message);
        throw updateError;
    }
    console.log(`✅ Embedded job ${jobId}`);
}, {
    connection: redisConnection,
    concurrency: CONCURRENCY
});

worker.on('completed', job => console.log(`✅ Job ${job.id} completed`));
worker.on('failed', (job, err) => console.error(`❌ Job ${job.id} failed:`, err));

// ─── START ──────────────────────────────────────────────────────────────────
(async () => {
    await addJobsToQueue();
    const count = await embeddingQueue.count();
    console.log(`\n🚀 Queue ready with ${count} jobs. Workers running (concurrency: ${CONCURRENCY})...\n`);
})();

// ─── GRACEFUL SHUTDOWN ─────────────────────────────────────────────────────
process.on('SIGINT', async () => {
    console.log('\n⏹️ Shutting down gracefully...');
    await worker.close();
    await embeddingQueue.close();
    await redisConnection.quit();
    process.exit(0);
});
