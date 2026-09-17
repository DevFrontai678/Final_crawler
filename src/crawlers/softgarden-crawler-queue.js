const { Queue, Worker } = require('bullmq');
const Redis = require('ioredis');
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const { processSoftgardenCompany, closeSoftgardenBrowser } = require('../ats-adapters/softgarden-adapter');
const cheerio = require('cheerio');
const axios = require('axios');
const { CRAWLER_TIMEOUTS } = require('../utils/crawler-timeouts');
const { enrichJobRows } = require('../utils/job-enrichment');
require('dotenv').config();

// ─── BACKFILL HELPERS ──────────────────────────────────────────────────────
function extractDescriptionGeneric(html) {
    const $ = cheerio.load(html);
    const selectors = [
        '.job-description', '.job-details', '.description', '.content',
        '#job-description', '.job-content', '[class*="job-description"]',
        '[class*="job-detail"]', '[class*="description"]', 'article',
        '.main-content', '#content', '.text-content', '.post-content',
        '.entry-content', '.job__description', '.job-listing__description',
        '[itemprop="description"]', '[itemprop="jobDescription"]',
        '[class*="stellenanzeige"]', '[class*="stelle"]', '[class*="anzeige"]',
        '[class*="aufgaben"]', '[class*="profil"]', '[class*="anforderung"]'
    ];
    for (const selector of selectors) {
        const text = $(selector).text().trim();
        if (text && text.length > 200) return text;
    }
    let text = $('body').text();
    text = text.split('\n')
        .map(l => l.trim())
        .filter(l => l.length > 20)
        .filter(l => !/impressum|datenschutz|agb|cookie|footer|menu|navigation|copyright|©/.test(l))
        .join('\n');
    return text.length > 200 ? text : '';
}

function extractLocationFromHTML(html) {
    // reuse same logic from adapter (or duplicate)
    // For brevity, we'll copy from adapter
    // In production, you can import these functions from a shared utils file.
    // I'll just use a simplified version.
    return null; // placeholder
}

function extractCompanyNameFromHTML(html, fallbackName) {
    return null; // placeholder
}

async function fetchHtmlForBackfill(url) {
    try {
        const response = await axios.get(url, { timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS, headers: { 'User-Agent': 'Mozilla/5.0' }, maxRedirects: 5 });
        return response.data;
    } catch (err) {
        return null;
    }
}

async function backfillMissingJobFields(companyId) {
    const { data: jobsToFix, error } = await supabase
        .from('jobs')
        .select('id, apply_url, title, location, company_name, raw_description')
        .eq('company_id', companyId)
        .or('location.is.null,company_name.is.null,raw_description.is.null');

    if (error) {
        console.error(`   Backfill fetch error: ${error.message}`);
        return;
    }
    if (!jobsToFix || jobsToFix.length === 0) return;

    console.log(`   Backfilling ${jobsToFix.length} existing job(s) missing fields...`);
    let updated = 0;

    for (const job of jobsToFix) {
        if (!job.apply_url) continue;
        if (job.location && job.company_name && job.raw_description && job.raw_description.length > 100) continue;

        try {
            const jobHtml = await fetchHtmlForBackfill(job.apply_url);
            if (!jobHtml) continue;

            const updates = {};
            if (!job.location) {
                const loc = extractLocationFromHTML(jobHtml);
                if (loc) updates.location = loc;
            }
            if (!job.company_name) {
                const comp = extractCompanyNameFromHTML(jobHtml, null);
                if (comp) updates.company_name = comp;
            }
            if (!job.raw_description || job.raw_description.length < 100) {
                const desc = extractDescriptionGeneric(jobHtml);
                if (desc && desc.length > 100) updates.raw_description = desc.slice(0, 5000);
            }

            if (Object.keys(updates).length === 0) continue;

            const { error: updateError } = await supabase
                .from('jobs')
                .update(updates)
                .eq('id', job.id);

            if (updateError) {
                console.error(`      Update error for job ${job.id}: ${updateError.message}`);
            } else {
                updated++;
                console.log(`      Updated job ${job.id}: ${Object.keys(updates).join(', ')}`);
            }
        } catch (err) {
            console.error(`      Backfill error for job ${job.id}: ${err.message}`);
        }
    }

    console.log(`   Backfill complete: ${updated} job(s) updated.`);
}

// ─── SUPABASE ──────────────────────────────────────────────────────────────
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

// ─── ADD COMPANIES TO QUEUE ──────────────────────────────────────────────
async function addSoftgardenCompaniesToQueue() {
    const pageSize = 1000;
    let page = 0;
    let totalAdded = 0;
    let hasMore = true;

    // 🔥 Recrawl interval (default 48 hours)
    const recrawlHours = parseInt(process.env.RECRAWL_INTERVAL_HOURS || '48', 10);
    const cutoffIso = new Date(Date.now() - recrawlHours * 60 * 60 * 1000).toISOString();

    console.log(`📋 Fetching Softgarden companies (pending or last crawled before ${cutoffIso})...`);

    while (hasMore) {
        const start = page * pageSize;
        const end = start + pageSize - 1;

        const { data: companies, error } = await supabase
            .from('companies')
            .select('"Id", detected_career_url, "Name", crawl_status, last_crawled_at')
            .eq('ats_type', 'softgarden')
            .neq('crawl_status', 'in_progress')
            .or(`crawl_status.eq.pending,last_crawled_at.lt.${cutoffIso}`)
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
                backoff: { type: 'exponential', delay: 5000 },
                removeOnComplete: 1000,
                removeOnFail: 5000
            });
            totalAdded++;
            console.log(`Added: ${company.Name}`);
        }

        if (companies.length < pageSize) hasMore = false;
        page++;
    }

    console.log(`✅ Total companies queued: ${totalAdded}`);
    return totalAdded;
}

// ─── WORKER ────────────────────────────────────────────────────────────────
let processedCount = 0, totalQueued = 0;
const stats = { processed: 0, failed_fetch: 0, no_jobs: 0, with_jobs: 0, jobs_saved: 0, existing_refreshed: 0, errors: 0 };

function printProgress() {
    const pct = totalQueued > 0 ? ((processedCount / totalQueued) * 100).toFixed(1) : 0;
    const remaining = totalQueued - processedCount;
    console.log(`\n📊 Progress: ${processedCount}/${totalQueued} companies (${pct}%) | Remaining: ${remaining}`);
    console.log(`   ✅ with jobs: ${stats.with_jobs} | ❌ no jobs: ${stats.no_jobs} | 🚫 failed: ${stats.failed_fetch} | 💾 jobs saved: ${stats.jobs_saved}`);
}

const worker = new Worker(QUEUE_NAME, async job => {
    const { companyId, companyName, careerUrl } = job.data;
    const attempt = job.attemptsMade + 1;
    console.log(`\n🕸️ Processing: ${companyName} (Attempt ${attempt}/3)`);

    try {
        const company = { Id: companyId, Name: companyName, detected_career_url: careerUrl };
        const result = await processSoftgardenCompany(company);

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
            console.log(`   ❌ Failed`);
            stats.failed_fetch++;
            return;
        }

        const jobsToSave = await enrichJobRows(result.jobs.map((j, index) => ({
            company_id: companyId,
            external_job_id: j.external_job_id || `fallback_${Date.now()}_${index}`,
            title: j.title || 'Untitled',
            raw_description: j.raw_description ? j.raw_description.slice(0, 5000) : null,
            apply_url: j.apply_url || null,
            ats_source: j.ats_source || 'softgarden',
            location: j.location || null,
            company_name: j.company_name || null,
            is_active: true,
            first_seen_at: new Date(),
            last_seen_at: new Date()
        })), { companyId, companyName: company.Name, atsSource: 'softgarden' });

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
            stats.jobs_saved += jobsToSave.length;
            stats.with_jobs++;
        } else {
            console.log(`   ⚠️ No jobs to save`);
            stats.no_jobs++;
        }

        // Backfill existing jobs (missing fields)
        await backfillMissingJobFields(companyId);

        // Update company status with timestamp
        await supabase.from('companies')
            .update({
                crawl_status: 'ats_detected',
                last_crawled_at: new Date().toISOString()
            })
            .eq('Id', companyId);

        await supabase.from('crawl_logs').insert({
            company_id: companyId,
            status: 'success',
            jobs_found: jobsToSave.length,
            created_at: new Date()
        });

    } catch (err) {
        await supabase.from('crawl_logs').insert({
            company_id: companyId,
            status: 'error',
            error_message: err.message,
            created_at: new Date()
        }).catch(() => {});
        console.error(`   ❌ Error: ${err.message}`);
        stats.errors++;
        throw err;
    }
}, {
    connection: redisConnection,
    concurrency: 3
});

worker.on('completed', job => {
    processedCount++;
    if (processedCount % 5 === 0 || processedCount === totalQueued) printProgress();
    console.log(`✅ Job ${job.id} completed`);
});

worker.on('failed', (job, err) => {
    console.error(`❌ Job ${job?.id} failed: ${err.message}`);
});

// ─── START ──────────────────────────────────────────────────────────────────
(async () => {
    totalQueued = await addSoftgardenCompaniesToQueue();
    if (totalQueued === 0) {
        console.log('No Softgarden companies to process. Exiting.');
        process.exit(0);
    }
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
