const { Queue, Worker } = require('bullmq');
const Redis = require('ioredis');
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const { processSoftgardenCompany, closeSoftgardenBrowser } = require('../ats-adapters/softgarden-adapter');
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

const QUEUE_NAME = 'softgarden-crawl';
const softgardenQueue = new Queue(QUEUE_NAME, { connection: redisConnection });

// ─── ADD COMPANIES TO QUEUE WITH PAGINATION ───────────────────────────────
async function addSoftgardenCompaniesToQueue() {
    const pageSize = 1000;
    let page = 0;
    let totalAdded = 0;
    let hasMore = true;

    console.log('📋 Fetching Softgarden companies with pagination...');

    while (hasMore) {
        const start = page * pageSize;
        const end = start + pageSize - 1;

        const { data: companies, error } = await supabase
            .from('companies')
            .select('"Id", detected_career_url, "Name"')
            .eq('ats_type', 'softgarden')
            .eq('crawl_status', 'pending')
            .not('detected_career_url', 'is', null)
            .order('Id', { ascending: true })
            .range(start, end);

        if (error) {
            console.error('Error fetching companies:', error.message);
            break;
        }

        if (!companies || companies.length === 0) {
            hasMore = false;
            break;
        }

        console.log(`📋 Page ${page + 1}: Adding ${companies.length} companies...`);

        for (const company of companies) {
            await softgardenQueue.add('crawl-softgarden-company', {
                companyId: company.Id,
                companyName: company.Name,
                careerUrl: company.detected_career_url
            }, {
                attempts: 3,
                backoff: { type: 'exponential', delay: 5000 }
            });
            totalAdded++;
            console.log(`Added: ${company.Name}`);
        }

        if (companies.length < pageSize) {
            hasMore = false;
        }
        page++;
    }

    console.log(`✅ Total companies added: ${totalAdded}`);
}

// ─── WORKER ──────────────────────────────────────────────────────────────────
const worker = new Worker(QUEUE_NAME, async job => {
    const { companyId, companyName, careerUrl } = job.data;
    console.log(`\n🕸️ Processing: ${companyName}`);

    try {
        const company = { Id: companyId, Name: companyName, detected_career_url: careerUrl };
        const result = await processSoftgardenCompany(company);

        // IDs nahi milay aur fallback bhi 0 jobs — retry se faida nahi, fail mark karo
        if (result.error) {
            await supabase.from('crawl_logs').insert({
                company_id: companyId,
                status: 'failed',
                error_message: result.error,
                created_at: new Date()
            });
            await supabase.from('companies')
                .update({ crawl_status: 'failed' })
                .eq('Id', companyId);
            console.log(`   ⚠️ No IDs found, marked as failed (no retry)`);
            return;
        }

        const jobsToSave = result.jobs.map(j => ({
            company_id: companyId,
            external_job_id: j.external_job_id,
            title: j.title,
            raw_description: j.raw_description ? j.raw_description.slice(0, 5000) : null,
            apply_url: j.apply_url,
            ats_source: j.ats_source || 'softgarden',
            is_active: true,
            first_seen_at: new Date(),
            last_seen_at: new Date()
        }));

        if (jobsToSave.length > 0) {
            const { error: saveError } = await supabase
                .from('jobs')
                .upsert(jobsToSave, {
                    onConflict: 'company_id,external_job_id',
                    ignoreDuplicates: true
                });

            if (saveError) throw new Error(`Supabase save failed: ${saveError.message}`);
            console.log(`   💾 Saved ${jobsToSave.length} jobs (source: ${result.usedFallback ? 'custom_fallback' : 'softgarden'})`);
        } else {
            console.log(`   ⚠️ IDs found but 0 jobs currently posted`);
        }

        await supabase.from('crawl_logs').insert({
            company_id: companyId,
            status: 'success',
            jobs_found: jobsToSave.length,
            created_at: new Date()
        });

        await supabase.from('companies')
            .update({ crawl_status: 'ats_detected' })
            .eq('Id', companyId);

    } catch (err) {
        // Asli exception (network/timeout/Supabase down) — yahan throw karo taake BullMQ retry kare
        await supabase.from('crawl_logs').insert({
            company_id: companyId,
            status: 'error',
            error_message: err.message,
            created_at: new Date()
        }).catch(() => {});
        console.error(`   ❌ Error: ${err.message}`);
        throw err;
    }
}, {
    connection: redisConnection,
    concurrency: 3
});

worker.on('completed', job => console.log(`✅ Job ${job.id} completed`));
worker.on('failed', (job, err) => console.error(`❌ Job ${job?.id} failed: ${err.message}`));

(async () => {
    await addSoftgardenCompaniesToQueue();
    const count = await softgardenQueue.count();
    console.log(`\n🚀 Queue ready with ${count} companies. Workers running (concurrency: 3)...\n`);
})();

process.on('SIGINT', async () => {
    console.log('\n⏹️ Shutting down gracefully...');
    await closeSoftgardenBrowser();
    await worker.close();
    await softgardenQueue.close();
    await redisConnection.quit();
    process.exit(0);
});
