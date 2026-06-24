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

// ─── ADD COMPANIES TO QUEUE ────────────────────────────────────────────────
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
                careerUrl: company.detected_career_url,
                attempt: 1  // 🔥 Track attempt number
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
    const { companyId, companyName, careerUrl, attempt = 1 } = job.data;
    console.log(`\n🕸️ Processing: ${companyName} (Attempt ${attempt}/3)`);

    try {
        const company = { Id: companyId, Name: companyName, detected_career_url: careerUrl };
        
        // 🔥 isRetry = true if attempt > 1
        const result = await processSoftgardenCompany(company, attempt > 1);

        // 🔥 Agar shouldRetry flag hai aur attempt < 3 → retry
        if (result.shouldRetry && attempt < 3) {
            console.log(`   🔄 Retrying ${companyName} (Attempt ${attempt + 1}/3)...`);
            await softgardenQueue.add('crawl-softgarden-company', {
                companyId: companyId,
                companyName: companyName,
                careerUrl: careerUrl,
                attempt: attempt + 1
            }, {
                attempts: 3,
                backoff: { type: 'exponential', delay: 5000 }
            });
            
            await supabase.from('crawl_logs').insert({
                company_id: companyId,
                status: 'retry',
                error_message: `Retry scheduled (attempt ${attempt})`,
                created_at: new Date()
            });
            console.log(`   ✅ Retry scheduled`);
            return;
        }

        // 🔥 Agar error hai aur attempt 3 hai → custom crawler fallback already used
        if (result.error) {
            // Check if this was a custom fallback attempt
            if (result.usedFallback) {
                console.log(`   ⚠️ Custom fallback also failed, marking as failed`);
            } else {
                console.log(`   ❌ Failed after ${attempt} attempts`);
            }
            
            await supabase.from('crawl_logs').insert({
                company_id: companyId,
                status: 'failed',
                error_message: result.error || 'Unknown error',
                created_at: new Date()
            });
            await supabase.from('companies')
                .update({ crawl_status: 'failed' })
                .eq('Id', companyId);
            return;
        }

        // 🔥 Success — save jobs
        const jobsToSave = result.jobs.map((j, index) => {
            const externalId = j.external_job_id || `fallback_${Date.now()}_${index}`;
            return {
                company_id: companyId,
                external_job_id: externalId,
                title: j.title || 'Untitled',
                raw_description: j.raw_description ? j.raw_description.slice(0, 5000) : null,
                apply_url: j.apply_url || null,
                ats_source: j.ats_source || 'softgarden',
                is_active: true,
                first_seen_at: new Date(),
                last_seen_at: new Date()
            };
        });

        if (jobsToSave.length > 0) {
            const { error: saveError } = await supabase
                .from('jobs')
                .upsert(jobsToSave, {
                    onConflict: 'company_id,external_job_id',
                    ignoreDuplicates: true
                });

            if (saveError) {
                console.error(`   ❌ Supabase save error: ${saveError.message}`);
                throw new Error(`Supabase save failed: ${saveError.message}`);
            }
            
            const sourceLabel = result.usedFallback ? 'custom_fallback' : 'softgarden';
            console.log(`   ✅ Saved ${jobsToSave.length} jobs (source: ${sourceLabel}, attempt: ${attempt})`);
            
            // Check jobs without description
            const withoutDesc = jobsToSave.filter(j => !j.raw_description || j.raw_description.length < 100);
            if (withoutDesc.length > 0) {
                console.log(`   ⚠️ ${withoutDesc.length} jobs saved without description`);
            }
        } else {
            console.log(`   ⚠️ No jobs to save`);
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
