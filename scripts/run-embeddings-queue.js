const { Queue, Worker } = require('bullmq');
const Redis = require('ioredis');
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
require('dotenv').config();

const voyage = require('voyageai');
const voyageClient = new voyage.Client({ apiKey: process.env.VOYAGE_API_KEY });

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

async function addJobsToQueue(limit = 5000) {
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

    console.log(`📋 Adding ${jobs.length} jobs to queue...`);
    for (const job of jobs) {
        await queue.add('embed-job', {
            jobId: job.id,
            skills: job.structured_skills
        }, {
            attempts: 3,
            backoff: { type: 'exponential', delay: 5000 }
        });
    }
    console.log(`✅ Added ${jobs.length} jobs to queue`);
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
        const response = await voyageClient.embed({
            input: [inputText],
            model: 'voyage-3-large'
        });
        const embedding = response.embeddings[0];

        const { error: updateError } = await supabase
            .from('jobs')
            .update({ skill_embedding: embedding })
            .eq('id', jobId);

        if (updateError) {
            console.error(`❌ Save error for job ${jobId}:`, updateError.message);
            throw updateError;
        }
        console.log(`✅ Saved embedding for job ${jobId}`);
    } catch (err) {
        console.error(`❌ Failed for job ${jobId}:`, err.message);
        throw err;
    }
}, {
    connection: redisConnection,
    concurrency: 5
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
