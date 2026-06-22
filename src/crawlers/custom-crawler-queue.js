const { Queue, Worker } = require('bullmq');
const Redis = require('ioredis');
const { chromium } = require('playwright');
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const Anthropic = require('@anthropic-ai/sdk');
const scrapingbee = require('scrapingbee');
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

// ─── SCRAPINGBEE CLIENT ─────────────────────────────────────────────────────
const scrapeClient = new scrapingbee.ScrapingBeeClient({
    apiKey: process.env.SCRAPINGBEE_API_KEY
});

// ─── FETCH PAGE: PLAYWRIGHT FIRST, SCRAPINGBEE FALLBACK ──────────────────
async function fetchPageWithFallback(url) {
    // 1. Try Playwright first
    try {
        const browser = await chromium.launch({ headless: true });
        const page = await browser.newPage();
        await page.setExtraHTTPHeaders({
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8'
        });
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
        const html = await page.content();
        await browser.close();
        console.log(`  ✅ Playwright success`);
        return html;
    } catch (playwrightError) {
        console.log(`  ⏳ Playwright failed: ${playwrightError.message}`);
        console.log(`  🔄 Trying ScrapingBee fallback...`);
    }

    // 2. Fallback to ScrapingBee
    try {
        const response = await scrapeClient.get({
            url: url,
            params: {
                render_js: true,
                wait_for: 5000
            }
        });
        console.log(`  ✅ ScrapingBee success`);
        return response.data;
    } catch (scrapingBeeError) {
        console.log(`  ❌ ScrapingBee also failed: ${scrapingBeeError.message}`);
        return null;
    }
}

// ─── ADD COMPANIES TO QUEUE WITH TRUE PAGINATION ──────────────────────────
async function addCustomCompaniesToQueue() {
    const pageSize = 1000;
    let page = 0;
    let totalAdded = 0;
    let hasMore = true;

    console.log('📋 Fetching custom companies with pagination...');

    while (hasMore) {
        const start = page * pageSize;
        const end = start + pageSize - 1;

        const { data: companies, error } = await supabase
            .from('companies')
            .select('"Id", detected_career_url, "Name"')
            .eq('ats_type', 'custom')
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
            await customCrawlQueue.add('crawl-company', {
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

// ─── EXTRACT JOB LINKS – IMPROVED (checks href + link text) ──────────────
function extractJobLinks(html, baseUrl) {
    const cheerio = require('cheerio');
    const $ = cheerio.load(html);
    const links = [];

    // Keywords to look for in href OR link text
    const jobKeywords = [
        'job', 'jobs', 'karriere', 'karrier', 'career', 'careers',
        'stelle', 'stellen', 'stellenangebote', 'offene-stellen',
        'vakanz', 'position', 'positionen', 'ausbildung',
        'praktikum', 'bewerbung', 'vacancies', 'vacancy'
    ];

    $('a').each((_, el) => {
        const href = $(el).attr('href');
        const text = $(el).text().toLowerCase().trim();

        if (!href || href.includes('#') || href.includes('mailto:') || href.includes('tel:')) {
            return;
        }

        // Check if href OR link text contains any job keyword
        const hrefMatches = jobKeywords.some(keyword => href.toLowerCase().includes(keyword));
        const textMatches = jobKeywords.some(keyword => text.includes(keyword));

        if (hrefMatches || textMatches) {
            // Build absolute URL
            let fullUrl = href;
            if (!href.startsWith('http')) {
                try {
                    fullUrl = new URL(href, baseUrl).href;
                } catch (e) {
                    return;
                }
            }
            links.push(fullUrl);
        }
    });

    // Filter out listing pages (keep only detail pages)
    const filtered = links.filter(href =>
        !/karriere|jobs|stellenangebote|offene-stellen|jobboerse|careers|career|bewerbung|bewerben|vacancies/i.test(href) ||
        /\/job\//i.test(href) ||
        /\/stelle\//i.test(href) ||
        /\/position\//i.test(href) ||
        /\/vakanz\//i.test(href) ||
        /\/ausschreibung\//i.test(href) ||
        /\/detail\?/i.test(href) ||
        /\/job-\d+/i.test(href) ||
        /\/vacancy/i.test(href)
    );

    return [...new Set(filtered)].slice(0, 20);
}

// ─── EXTRACT DESCRIPTION ────────────────────────────────────────────────────
function extractDescriptionFromHTML(html) {
    const cheerio = require('cheerio');
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

// ─── CLAUDE: EXTRACT SKILLS ─────────────────────────────────────────────────
async function extractSkillsWithClaude(title, description) {
    if (!description || description.length < 100) return [];

    try {
        const response = await anthropic.messages.create({
            model: 'claude-opus-4-5',
            max_tokens: 400,
            messages: [{
                role: 'user',
                content: `Extract skills from job. Return ONLY a JSON array of skill strings in English.
Job Title: ${title}
Description: ${description.slice(0, 3000)}
JSON array:`
            }]
        });
        const text = response.content[0].text.trim().replace(/```json|```/g, '');
        const skills = JSON.parse(text);
        return Array.isArray(skills) ? skills.slice(0, 25) : [];
    } catch (err) {
        return [];
    }
}

// ─── WORKER ──────────────────────────────────────────────────────────────────
const worker = new Worker(QUEUE_NAME, async job => {
    const { companyId, companyName, careerUrl } = job.data;
    console.log(`\n🕸️ Crawling: ${companyName}`);
    console.log(`   URL: ${careerUrl}`);

    try {
        const html = await fetchPageWithFallback(careerUrl);
        if (!html) {
            console.log(`   ❌ Failed to fetch page with both methods`);
            // Mark as failed so it doesn't stay pending forever
            await supabase
                .from('companies')
                .update({ crawl_status: 'failed' })
                .eq('Id', companyId);
            return;
        }

        const jobLinks = extractJobLinks(html, careerUrl);
        console.log(`   Found ${jobLinks.length} job detail links`);

        const jobs = [];

        for (const link of jobLinks) {
            try {
                const jobHtml = await fetchPageWithFallback(link);
                if (!jobHtml) continue;

                const cheerio = require('cheerio');
                const $ = cheerio.load(jobHtml);
                const title = $('title').text().trim() || 'Untitled Job';

                const description = extractDescriptionFromHTML(jobHtml);
                if (!description || description.length < 100) {
                    console.log(`   ⏭️ Skipped: ${title.substring(0, 50)} (description too short)`);
                    continue;
                }

                const skills = await extractSkillsWithClaude(title, description);

                const skillsDisplay = skills.length > 0
                    ? skills.slice(0, 5).join(', ') + (skills.length > 5 ? ` +${skills.length - 5} more` : '')
                    : 'none';

                console.log(`   ✅ ${title.substring(0, 50)}`);
                console.log(`      Skills (${skills.length}): ${skillsDisplay}`);

                const externalId = Buffer.from(link).toString('base64').slice(0, 50);
                jobs.push({
                    company_id: companyId,
                    external_job_id: externalId,
                    title: title,
                    raw_description: description.slice(0, 5000),
                    structured_skills: skills,
                    apply_url: link,
                    is_active: true,
                    first_seen_at: new Date(),
                    last_seen_at: new Date()
                });

            } catch (err) {
                console.error(`   ❌ Link error: ${err.message?.substring(0, 80)}`);
            }
        }

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
            if (error) console.error('   Supabase error:', error.message);
            else console.log(`   💾 Saved ${uniqueJobs.length} jobs for ${companyName}`);
        } else {
            console.log(`   ⚠️ No valid job pages found for ${companyName}`);
        }

        // Mark company as processed (even if 0 jobs)
        await supabase
            .from('companies')
            .update({ crawl_status: 'ats_detected' })
            .eq('Id', companyId);

    } catch (err) {
        console.error(`❌ Crawl failed: ${err.message}`);
        // Mark as failed so it doesn't stay pending forever
        await supabase
            .from('companies')
            .update({ crawl_status: 'failed' })
            .eq('Id', companyId);
        throw err;
    }
}, {
    connection: redisConnection,
    concurrency: 3
});

worker.on('completed', job => console.log(`✅ Job ${job.id} completed`));
worker.on('failed', (job, err) => console.error(`❌ Job ${job?.id} failed: ${err.message}`));

(async () => {
    await addCustomCompaniesToQueue(0);
    const count = await customCrawlQueue.count();
    console.log(`\n🚀 Queue ready with ${count} companies. Workers running (concurrency: 3)...\n`);
})();

process.on('SIGINT', async () => {
    console.log('\n⏹️ Shutting down gracefully...');
    await worker.close();
    await customCrawlQueue.close();
    await redisConnection.quit();
    process.exit(0);
});