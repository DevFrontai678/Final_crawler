const { Queue, Worker } = require('bullmq');
const Redis = require('ioredis');
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
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

const QUEUE_NAME = 'embedding-queue';
const queue = new Queue(QUEUE_NAME, { connection: redisConnection });

async function getVoyageEmbedding(text) {
    const response = await axios.post(
        'https://api.voyageai.com/v1/embeddings',
        {
            input: [text],
            model: 'voyage-3-large'
        },
        {
            headers: {
                'Authorization': `Bearer ${process.env.VOYAGE_API_KEY}`,
                'Content-Type': 'application/json'
            },
            timeout: 30000
        }
    );
    return response.data.data[0].embedding;
}

async function addJobsToQueue(limit = 5000) {
    // ─── Fetch ALL jobs with skills, then filter in JS ──────────────────
    const { data: jobs, error } = await supabase
        .from('jobs')
        .select('id, structured_skills')
        .is('skill_embedding', null)
        .not('structured_skills', 'is', null)
        .limit(limit);

    if (error) {
        console.error('❌ Error fetching jobs:', error.message);
        return;
    }

    // Filter out jobs with empty array
    const filteredJobs = jobs.filter(job => 
        Array.isArray(job.structured_skills) && job.structured_skills.length > 0
    );

    console.log(`📋 Found ${filteredJobs.length} jobs with skills (out of ${jobs.length} total)`);

    for (const job of filteredJobs) {
        await queue.add('embed-job', {
            jobId: job.id,
            skills: job.structured_skills
        }, {
            attempts: 3,
            backoff: { type: 'exponential', delay: 5000 }
        });
    }
    console.log(`✅ Added ${filteredJobs.length} jobs to queue`);
}

const worker = new Worker(QUEUE_NAME, async job => {
    const { jobId, skills } = job.data;
    console.log(`🔄 Generating embedding for job ${jobId} (${skills?.length || 0} skills)`);

    if (!skills || skills.length === 0) {
        console.log(`⚠️ No skills for job ${jobId}, skipping`);
        return;
    }

    try {
        const inputText = skills.join(', ');
        const embedding = await getVoyageEmbedding(inputText);

        const { error: updateError } = await supabase
            .from('jobs')
            .update({ skill_embedding: embedding })
            .eq('id', jobId);

        if (updateError) {
            console.error(`❌ Save error for job ${jobId}:`, updateError.message);
            throw updateError;
        }
        console.log(`✅ Saved embedding for job ${jobId}`);
        await new Promise(r => setTimeout(r, 20000)); // 20 sec delay for rate limit

    } catch (err) {
        if (err.response?.status === 429) {
            console.log(`⏳ Rate limit hit, waiting 60s...`);
            await new Promise(r => setTimeout(r, 60000));
            throw new Error('Rate limit retry');
        }
        console.error(`❌ Failed for job ${jobId}:`, err.message);
        throw err;
    }
}, {
    connection: redisConnection,
    concurrency: 1  // Slow but safe for free tier
});

worker.on('completed', job => console.log(`✅ Job ${job.id} completed`));
worker.on('failed', (job, err) => console.error(`❌ Job ${job.id} failed:`, err));

(async () => {
    await addJobsToQueue(5000);
    console.log(`\n🚀 Queue has ${await queue.count()} jobs. Workers running...\n`);
})();

process.on('SIGINT', async () => {
    await worker.close();
    await queue.close();
    await redisConnection.quit();
    process.exit(0);
});
