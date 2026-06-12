const { Queue, Worker } = require('bullmq');
const Redis = require('ioredis');
const { chromium } = require('playwright');
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

// Supabase client
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

// Redis connection (default localhost:6379)
const redisConnection = new Redis({ host: 'localhost', port: 6379 });

// Queue name
const QUEUE_NAME = 'custom-crawl';

// Create queue
const customCrawlQueue = new Queue(QUEUE_NAME, { connection: redisConnection });

// Add all custom companies to queue
async function addCustomCompaniesToQueue(limit = 10) {
  // Fetch companies with ats_type = 'custom' and a detected career URL
  const { data: companies, error } = await supabase
    .from('companies')
    .select('"Id", detected_career_url, "Name"')
    .eq('ats_type', 'custom')
    .not('detected_career_url', 'is', null)
    .limit(limit);  // test ke liye limit 10, baad mein hata dena

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

// Worker to process each company
const worker = new Worker(QUEUE_NAME, async job => {
  const { companyId, companyName, careerUrl } = job.data;
  console.log(`\n🕸️ Crawling: ${companyName} (${careerUrl})`);

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();

  try {
    // Navigate to career page
    await page.goto(careerUrl, { waitUntil: 'networkidle', timeout: 30000 });

    // Find job links using common German patterns
    const jobLinks = await page.$$eval('a[href*="job"], a[href*="stelle"], a[href*="karriere"], a[href*="bewerbung"]',
      links => links.map(a => a.href).filter(href =>
        href && !href.includes('#') && href !== window.location.href
      )
    );

    const uniqueLinks = [...new Set(jobLinks)].slice(0, 20); // max 20 jobs per company
    console.log(`Found ${uniqueLinks.length} potential job links`);

    const jobs = [];

    for (const link of uniqueLinks) {
      try {
        await page.goto(link, { waitUntil: 'domcontentloaded', timeout: 15000 });
        const title = await page.title();
        const bodyText = await page.$eval('body', el => el.innerText).catch(() => '');

        if (bodyText.length < 50) continue; // skip pages without enough text

        jobs.push({
          company_id: companyId,
          external_job_id: Buffer.from(link).toString('base64').slice(0, 50),
          title: title,
          raw_description: bodyText.slice(0, 5000),
          apply_url: link,
          is_active: true,
          first_seen_at: new Date(),
          last_seen_at: new Date()
        });
        console.log(`  ✅ Extracted: ${title.substring(0, 60)}`);
      } catch (err) {
        console.error(`  ❌ Error on link ${link}:`, err.message);
      }
    }

    // Save jobs to Supabase
    if (jobs.length > 0) {
      const { error } = await supabase
        .from('jobs')
        .upsert(jobs, { onConflict: 'company_id, external_job_id' });
      if (error) {
        console.error('Supabase upsert error:', error);
      } else {
        console.log(`💾 Saved ${jobs.length} jobs for ${companyName}`);
      }
    } else {
      console.log(`⚠️ No jobs found for ${companyName}`);
    }

  } catch (err) {
    console.error(`❌ Crawl failed for ${companyName}:`, err.message);
  } finally {
    await browser.close();
  }
}, {
  connection: redisConnection,
  concurrency: 3  // process 3 companies in parallel
});

// Worker event listeners
worker.on('completed', job => {
  console.log(`✅ Job ${job.id} completed`);
});

worker.on('failed', (job, err) => {
  console.error(`❌ Job ${job.id} failed:`, err);
});

// Graceful shutdown
process.on('SIGINT', async () => {
  await worker.close();
  await customCrawlQueue.close();
  await redisConnection.quit();
  process.exit(0);
});

// Start the process
(async () => {
  // First, add companies to queue (test with limit 10; change to 200 later)
  await addCustomCompaniesToQueue(10);
  console.log(`\n🚀 Queue has ${await customCrawlQueue.count()} jobs. Workers are running...\n`);
})();