const { Queue, Worker } = require('bullmq');
const Redis = require('ioredis');
const { chromium } = require('playwright');
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const Anthropic = require('@anthropic-ai/sdk');
require('dotenv').config();

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

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

// ─── CHECK IF URL IS A REAL JOB PAGE ────────────────────────────────────────
function isLikelyJobPage(url, title) {
    // Skip these bad URLs
    const skipPatterns = [
        /linkedin\.com/i, /xing\.com/i, /facebook\.com/i, /instagram\.com/i,
        /twitter\.com/i, /youtube\.com/i, /google\.com/i,
        /404/i, /not-found/i, /datenschutz/i, /impressum/i,
        /agb/i, /kontakt/i, /login/i, /signin/i
    ];
    for (const pattern of skipPatterns) {
        if (pattern.test(url) || pattern.test(title || '')) return false;
    }

    // Must look like a job page
    const jobPatterns = [
        /job/i, /stelle/i, /career/i, /karriere/i, /vakanz/i,
        /position/i, /bewerbung/i, /offene/i, /work/i
    ];
    return jobPatterns.some(p => p.test(url) || p.test(title || ''));
}

// ─── SMART EXTRACTION FUNCTION ──────────────────────────────────────────────
async function extractJobDescription(page) {
    const selectors = [
        '.job-description', '.job-details', '.description', '.content',
        '#job-description', '.job-content', '[class*="job-description"]',
        '[class*="job-detail"]', '[class*="description"]', 'article',
        '.main-content', '#content', '.text-content', '.post-content',
        '.entry-content', '.job__description', '.job-listing__description'
    ];

    for (const selector of selectors) {
        try {
            const text = await page.$eval(selector, el => el.innerText).catch(() => null);
            if (text && text.length > 200) return text;
        } catch (e) { /* try next */ }
    }

    const bodyText = await page.$eval('body', el => el.innerText).catch(() => '');
    if (bodyText) {
        const lines = bodyText.split('\n')
            .map(l => l.trim())
            .filter(l => l.length > 20)
            .filter(l => !/impressum|datenschutz|agb|cookie|footer|menu|navigation/i.test(l));
        return lines.join('\n').slice(0, 5000);
    }
    return '';
}

// ─── CLAUDE: EXTRACT SKILLS FROM JOB DESCRIPTION ───────────────────────────
async function extractSkillsWithClaude(title, description) {
    if (!description || description.length < 50) return [];

    try {
        const response = await anthropic.messages.create({
            model: 'claude-opus-4-5',
            max_tokens: 300,
            messages: [{
                role: 'user',
                content: `Extract technical skills from this job posting. Return ONLY a JSON array of skill strings, nothing else.
Example: ["Python", "Docker", "AWS", "React"]

Job Title: ${title}
Job Description: ${description.slice(0, 2000)}

Return only the JSON array:`
            }]
        });

        const text = response.content[0].text.trim();
        // Clean and parse
        const clean = text.replace(/```json|```/g, '').trim();
        const skills = JSON.parse(clean);
        return Array.isArray(skills) ? skills.slice(0, 20) : [];
    } catch (err) {
        console.error('  Claude skill extraction error:', err.message);
        return [];
    }
}

// ─── WORKER ─────────────────────────────────────────────────────────────────
const worker = new Worker(QUEUE_NAME, async job => {
    const { companyId, companyName, careerUrl } = job.data;
    console.log(`\n🕸️  Crawling: ${companyName} (${careerUrl})`);

    const browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();

    try {
        await page.goto(careerUrl, { waitUntil: 'networkidle', timeout: 30000 });

        // Get all job links
        const jobLinks = await page.$$eval(
            'a[href*="job"], a[href*="stelle"], a[href*="karriere"], a[href*="bewerbung"], a[href*="vakanz"], a[href*="offene"], a[href*="position"]',
            links => links
                .map(a => ({ href: a.href, text: a.innerText?.trim() }))
                .filter(l =>
                    l.href &&
                    !l.href.includes('#') &&
                    !l.href.includes('mailto:') &&
                    l.href !== window.location.href
                )
        );

        const uniqueLinks = [...new Map(jobLinks.map(l => [l.href, l])).values()].slice(0, 15);
        console.log(`  Found ${uniqueLinks.length} potential job links`);

        const jobs = [];

        for (const link of uniqueLinks) {
            try {
                await page.goto(link.href, { waitUntil: 'domcontentloaded', timeout: 15000 });
                const title = await page.title();

                // Skip bad pages
                if (!isLikelyJobPage(link.href, title)) {
                    console.log(`  ⏭️  Skipped (not a job page): ${title?.substring(0, 50)}`);
                    continue;
                }

                const description = await extractJobDescription(page);
                if (!description || description.length < 100) continue;

                // Extract skills with Claude
                const skills = await extractSkillsWithClaude(title, description);
                console.log(`  ✅ ${title?.substring(0, 50)} | Skills: ${skills.length > 0 ? skills.slice(0,5).join(', ') : 'none found'}`);

                const externalId = Buffer.from(link.href).toString('base64').slice(0, 50);
                jobs.push({
                    company_id: companyId,
                    external_job_id: externalId,
                    title: title || 'Untitled Job',
                    raw_description: description.slice(0, 5000),
                    structured_skills: skills,
                    apply_url: link.href,
                    is_active: true,
                    first_seen_at: new Date(),
                    last_seen_at: new Date()
                });

            } catch (err) {
                console.error(`  ❌ Error on link: ${err.message?.substring(0, 80)}`);
            }
        }

        // Deduplicate
        const seen = new Set();
        const uniqueJobs = jobs.filter(job => {
            const key = `${job.company_id}-${job.external_job_id}`;
            if (seen.has(key)) return false;
            seen.add(key);
            return true;
        });

        if (uniqueJobs.length) {
            const { error } = await supabase
                .from('jobs')
                .upsert(uniqueJobs, {
                    onConflict: 'company_id,external_job_id',
                    ignoreDuplicates: true
                });
            if (error) console.error('  Supabase error:', error.message);
            else console.log(`  💾 Saved ${uniqueJobs.length} jobs for ${companyName}`);
        } else {
            console.log(`  ⚠️  No valid jobs found for ${companyName}`);
        }

    } catch (err) {
        console.error(`❌ Crawl failed for ${companyName}: ${err.message}`);
        throw err; // BullMQ retry ke liye
    } finally {
        await browser.close();
    }
}, {
    connection: redisConnection,
    concurrency: 3
});

worker.on('completed', job => console.log(`✅ Job ${job.id} completed`));
worker.on('failed', (job, err) => console.error(`❌ Job ${job?.id} failed: ${err.message}`));

(async () => {
    await addCustomCompaniesToQueue(200);
    console.log(`\n🚀 Queue ready. Workers running with concurrency 3...\n`);
})();

process.on('SIGINT', async () => {
    console.log('\nShutting down gracefully...');
    await worker.close();
    await customCrawlQueue.close();
    await redisConnection.quit();
    process.exit(0);
});