const { Queue, Worker } = require('bullmq');
const Redis = require('ioredis');
const { chromium } = require('playwright');
const { createClient } = require('@supabase/supabase-js');
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

const QUEUE_NAME = 'custom-crawl';
const customCrawlQueue = new Queue(QUEUE_NAME, { connection: redisConnection });

async function addCustomCompaniesToQueue(limit = 200) {
    const { data: companies, error } = await supabase
        .from('companies')
        .select('"Id", detected_career_url, "Name"')
        .eq('ats_type', 'custom')
        .not('detected_career_url', 'is', null)
        .limit(limit);

    if (error) {
        console.error('Error fetching companies:', error.message);
        return;
    }

    console.log(`Adding ${companies.length} custom companies to queue...`);
    for (const company of companies) {
        await customCrawlQueue.add('crawl-company', {
            companyId: company.Id,
            companyName: company.Name,
            careerUrl: company.detected_career_url
        }, {
            attempts: 3,
            backoff: { type: 'exponential', delay: 5000 }
        });
        console.log(`Added: ${company.Name}`);
    }
}

const worker = new Worker(QUEUE_NAME, async job => {
    const { companyId, companyName, careerUrl } = job.data;
    console.log(`\n🕸️ Crawling: ${companyName} (${careerUrl})`);

    const browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();

    try {
        await page.goto(careerUrl, { waitUntil: 'networkidle', timeout: 30000 });

        const jobLinks = await page.$$eval(
            'a[href*="job"], a[href*="stelle"], a[href*="karriere"], a[href*="bewerbung"], a[href*="vakanz"], a[href*="offene"], a[href*="jobs"]',
            links => links
                .map(a => a.href)
                .filter(href => 
                    href && 
                    !href.includes('#') && 
                    !href.includes('mailto:') &&
                    href !== window.location.href
                )
        );

        const uniqueLinks = [...new Set(jobLinks)].slice(0, 20);
        console.log(`Found ${uniqueLinks.length} potential job links`);

        const jobs = [];

        for (const link of uniqueLinks) {
            try {
                await page.goto(link, { waitUntil: 'domcontentloaded', timeout: 15000 });
                const title = await page.title();
                const bodyText = await page.$eval('body', el => el.innerText).catch(() => '');
                if (bodyText.length < 50) continue;

                const externalId = Buffer.from(link).toString('base64').slice(0, 50);
                jobs.push({
                    company_id: companyId,
                    external_job_id: externalId,
                    title: title || 'Untitled Job',
                    raw_description: bodyText.slice(0, 5000),
                    apply_url: link,
                    is_active: true,
                    first_seen_at: new Date(),
                    last_seen_at: new Date()
                });
                console.log(`  ✅ Extracted: ${title.substring(0, 60)}`);
            } catch (err) {
                if (err.message.includes('mailto:')) {
                    // skip silently
                } else {
                    console.error(`  ❌ Error on link: ${err.message}`);
                }
            }
        }

        // Deduplicate batch
        const seen = new Set();
        const uniqueJobs = [];
        for (const job of jobs) {
            const key = `${job.company_id}-${job.external_job_id}`;
            if (!seen.has(key)) {
                seen.add(key);
                uniqueJobs.push(job);
            }
        }

        if (uniqueJobs.length) {
            const { error } = await supabase
                .from('jobs')
                .upsert(uniqueJobs, {
                    onConflict: 'company_id, external_job_id',
                    ignoreDuplicates: true
                });
            if (error) console.error('Supabase error:', error);
            else console.log(`💾 Saved ${uniqueJobs.length} jobs for ${companyName}`);
        } else {
            console.log(`⚠️ No jobs found for ${companyName}`);
        }
    } catch (err) {
        console.error(`❌ Crawl failed for ${companyName}: ${err.message}`);
    } finally {
        await browser.close();
    }
}, {
    connection: redisConnection,
    concurrency: 3
});

worker.on('completed', job => console.log(`✅ Job ${job.id} completed`));
worker.on('failed', (job, err) => console.error(`❌ Job ${job.id} failed:`, err));

(async () => {
    await addCustomCompaniesToQueue(200);
    console.log(`\n🚀 Queue has ${await customCrawlQueue.count()} jobs. Workers running...\n`);
})();

process.on('SIGINT', async () => {
    await worker.close();
    await customCrawlQueue.close();
    await redisConnection.quit();
    process.exit(0);
});
