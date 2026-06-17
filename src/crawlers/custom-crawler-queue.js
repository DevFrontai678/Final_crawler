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

// ─── CHECK IF URL IS A REAL JOB DETAIL PAGE ──────────────────────────────
function isJobDetailPage(url, title, pageText) {
    // Skip social media / non-job domains
    const skipDomains = [
        'linkedin.com', 'xing.com', 'facebook.com', 'instagram.com',
        'twitter.com', 'youtube.com', 'google.com', 'glassdoor.com',
        'indeed.com', 'stepstone.de', 'monster.de'
    ];
    for (const domain of skipDomains) {
        if (url.includes(domain)) return false;
    }

    // Skip legal/info pages
    const skipPatterns = [
        /impressum/i, /datenschutz/i, /agb/i, /cookie/i, /privacy/i,
        /kontakt/i, /about-us/i, /ueber-uns/i, /404/i, /not-found/i,
        /login/i, /signin/i, /sign-in/i, /register/i,
        /download/i, /pdf/i, /bilder/i, /images/i
    ];
    for (const pattern of skipPatterns) {
        if (pattern.test(url) || pattern.test(title || '')) return false;
    }

    // Check if page has job description keywords
    const text = (pageText || '').toLowerCase();
    const jobKeywords = [
        'aufgaben', 'anforderungen', 'ihr profil', 'wir bieten',
        'verantwortlichkeiten', 'qualifikation', 'erfahrung',
        'responsibilities', 'requirements', 'qualifications',
        'stellen-id', 'job-id', 'job id', 'referenznummer',
        'ihre aufgaben', 'ihr profil', 'das bringen sie mit',
        'was sie erwartet', 'was sie mitbringen'
    ];
    const hasJobContent = jobKeywords.some(kw => text.includes(kw));

    // Check URL patterns for job detail pages
    const detailPatterns = [
        /\/job\//i, /\/stelle\//i, /\/position\//i,
        /\/vakanz\//i, /\/ausschreibung\//i,
        /\/detail\?/i, /\/apply\?/i, /\/job-details/i,
        /\/job-posting/i, /\/stellenanzeige/i,
        /\/offene-stelle/i, /\/career-detail/i,
        /\/job-offer/i, /\/joblisting/i,
        /\/job-\d+/i, /\/stelle-\d+/i
    ];
    const hasDetailPattern = detailPatterns.some(p => p.test(url));

    // Must have either detail pattern OR job content
    return hasDetailPattern || hasJobContent;
}

// ─── EXTRACT JOB DESCRIPTION ────────────────────────────────────────────────
async function extractJobDescription(page) {
    const selectors = [
        // English selectors
        '.job-description', '.job-details', '.description', '.content',
        '#job-description', '.job-content', '[class*="job-description"]',
        '[class*="job-detail"]', '[class*="description"]', 'article',
        '.main-content', '#content', '.text-content', '.post-content',
        '.entry-content', '.job__description', '.job-listing__description',
        '[itemprop="description"]', '[itemprop="jobDescription"]',
        // German selectors
        '[class*="stellenanzeige"]', '[class*="stelle"]', '[class*="anzeige"]',
        '[class*="job-detail"]', '[class*="detail"]',
        '.job-text', '.job__text', '.description-text',
        // Common WordPress/Shopify patterns
        '.entry-content', '.post-content', '.page-content',
        // More German specific
        '[class*="aufgaben"]', '[class*="profil"]', '[class*="anforderung"]'
    ];

    for (const selector of selectors) {
        try {
            const text = await page.$eval(selector, el => el.innerText).catch(() => null);
            if (text && text.length > 200) return text;
        } catch (e) { /* try next */ }
    }

    // Fallback: filter body text to remove noise
    const bodyText = await page.$eval('body', el => el.innerText).catch(() => '');
    if (bodyText) {
        const lines = bodyText.split('\n')
            .map(l => l.trim())
            .filter(l => l.length > 20)
            .filter(l => !/impressum|datenschutz|agb|cookie|footer|menu|navigation|copyright|©|^$/.test(l));
        const result = lines.join('\n');
        if (result.length > 200) return result;
    }
    return '';
}

// ─── CLAUDE: EXTRACT SKILLS ────────────────────────────────────────────────
async function extractSkillsWithClaude(title, description) {
    if (!description || description.length < 100) return [];

    try {
        const response = await anthropic.messages.create({
            model: 'claude-opus-4-5',
            max_tokens: 400,
            messages: [{
                role: 'user',
                content: `Extract technical and professional skills from this job posting. The text may be in German or English.

Include these types of skills:
- Programming languages (Python, Java, C++, etc.)
- Frameworks and libraries (React, Spring, Angular, etc.)
- Tools and software (Docker, Kubernetes, SAP, AutoCAD, etc.)
- Cloud platforms (AWS, Azure, GCP, etc.)
- Databases (MySQL, PostgreSQL, MongoDB, etc.)
- Methodologies (Agile, Scrum, ITIL, etc.)
- Certifications (CISSP, PMP, etc.)
- German-specific tools (DATEV, SAP, Lexware, etc.)

Return ONLY a JSON array of skill strings in English. No explanation, no markdown, just the array.
Example output: ["Python", "Docker", "AWS", "SAP", "Agile", "PostgreSQL"]

Job Title: ${title}
Job Description: ${description.slice(0, 3000)}

JSON array:`
            }]
        });

        const text = response.content[0].text.trim();
        const clean = text.replace(/```json|```/g, '').trim();
        const skills = JSON.parse(clean);
        return Array.isArray(skills) ? skills.slice(0, 25) : [];
    } catch (err) {
        if (err.message && err.message.includes('rate_limit')) {
            console.log(`  ⏳ Rate limit hit, waiting 60s...`);
            await new Promise(r => setTimeout(r, 60000));
            // Retry once
            try {
                const retryResponse = await anthropic.messages.create({
                    model: 'claude-opus-4-5',
                    max_tokens: 400,
                    messages: [{
                        role: 'user',
                        content: `Extract skills from job: ${title}\n${description.slice(0, 2500)}\n\nReturn JSON array only:`
                    }]
                });
                const text2 = retryResponse.content[0].text.trim().replace(/```json|```/g, '');
                const skills2 = JSON.parse(text2);
                return Array.isArray(skills2) ? skills2.slice(0, 25) : [];
            } catch (e) { return []; }
        }
        console.error(`  Claude error: ${err.message}`);
        return [];
    }
}

// ─── CHECK IF PAGE HAS JOB DESCRIPTION ────────────────────────────────────
async function pageHasJobDescription(page) {
    const text = await page.$eval('body', el => el.innerText).catch(() => '');
    if (!text || text.length < 100) return false;

    const keywords = [
        'aufgaben', 'anforderungen', 'ihr profil', 'wir bieten',
        'verantwortlichkeiten', 'qualifikation', 'erfahrung',
        'responsibilities', 'requirements', 'qualifications',
        'stellen-id', 'job-id', 'referenznummer',
        'ihre aufgaben', 'ihr profil', 'das bringen sie mit'
    ];
    const found = keywords.some(kw => text.toLowerCase().includes(kw));
    return found;
}

// ─── CHECK IF PAGE LOADS SUCCESSFULLY ──────────────────────────────────────
async function safeGoto(page, url, timeout = 60000) {
    try {
        await page.goto(url, { 
            waitUntil: 'domcontentloaded', 
            timeout: timeout 
        });
        return true;
    } catch (err) {
        // If timeout, try one more time with longer timeout
        if (err.message.includes('Timeout')) {
            console.log(`  ⏳ Retry loading: ${url}`);
            try {
                await page.goto(url, { 
                    waitUntil: 'domcontentloaded', 
                    timeout: 90000 
                });
                return true;
            } catch (e) {
                return false;
            }
        }
        return false;
    }
}

// ─── WORKER ──────────────────────────────────────────────────────────────────
const worker = new Worker(QUEUE_NAME, async job => {
    const { companyId, companyName, careerUrl } = job.data;
    console.log(`\n🕸️  Crawling: ${companyName}`);
    console.log(`   URL: ${careerUrl}`);

    const browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();

    await page.setExtraHTTPHeaders({
        'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8'
    });

    try {
        // ─── Load career page with retry ────────────────────────────────
        const loaded = await safeGoto(page, careerUrl, 60000);
        if (!loaded) {
            console.log(`   ❌ Failed to load career page after retry`);
            await browser.close();
            return;
        }

        // ─── Get job links with improved filtering ──────────────────────
        const jobLinks = await page.$$eval(
            'a[href*="job"], a[href*="stelle"], a[href*="karriere"], a[href*="bewerbung"], a[href*="vakanz"], a[href*="offene"], a[href*="position"], a[href*="ausbildung"], a[href*="praktikum"]',
            links => links
                .map(a => ({ href: a.href, text: a.innerText?.trim() }))
                .filter(l =>
                    l.href &&
                    !l.href.includes('#') &&
                    !l.href.includes('mailto:') &&
                    !l.href.includes('tel:') &&
                    l.href !== window.location.href &&
                    // Only job detail URLs, not listing pages
                    !/karriere|jobs|stellenangebote|offene-stellen|jobboerse|careers|career|bewerbung|bewerben/i.test(l.href) ||
                    /\/job\//i.test(l.href) ||
                    /\/stelle\//i.test(l.href) ||
                    /\/position\//i.test(l.href) ||
                    /\/vakanz\//i.test(l.href) ||
                    /\/ausschreibung\//i.test(l.href) ||
                    /\/detail\?/i.test(l.href) ||
                    /\/job-\d+/i.test(l.href)
                )
        );

        // Deduplicate links
        const uniqueLinks = [...new Map(jobLinks.map(l => [l.href, l])).values()].slice(0, 20);
        console.log(`   Found ${uniqueLinks.length} job detail links`);

        const jobs = [];

        for (const link of uniqueLinks) {
            try {
                // ─── Load job page ──────────────────────────────────────
                const jobLoaded = await safeGoto(page, link.href, 30000);
                if (!jobLoaded) {
                    console.log(`   ⏭️  Skipped: ${link.href.substring(0, 50)} (page load failed)`);
                    continue;
                }

                const title = await page.title();

                // Get page text for validation
                const pageText = await page.$eval('body', el => el.innerText).catch(() => '');

                // ─── SKIP: Not a real job page ──────────────────────────
                if (!isJobDetailPage(link.href, title, pageText)) {
                    console.log(`   ⏭️  Skipped: ${title?.substring(0, 50)} (not a job detail page)`);
                    continue;
                }

                // ─── SKIP: No job description content ────────────────────
                if (!await pageHasJobDescription(page)) {
                    console.log(`   ⏭️  Skipped: ${title?.substring(0, 50)} (no description found)`);
                    continue;
                }

                // ─── Extract description ──────────────────────────────────
                const description = await extractJobDescription(page);
                if (!description || description.length < 100) {
                    console.log(`   ⏭️  Skipped: ${title?.substring(0, 50)} (description too short)`);
                    continue;
                }

                // ─── Extract skills with Claude ──────────────────────────
                const skills = await extractSkillsWithClaude(title, description);

                const skillsDisplay = skills.length > 0
                    ? skills.slice(0, 5).join(', ') + (skills.length > 5 ? ` +${skills.length - 5} more` : '')
                    : 'none';

                console.log(`   ✅ ${title?.substring(0, 50)}`);
                console.log(`      Skills (${skills.length}): ${skillsDisplay}`);

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
                console.error(`   ❌ Link error: ${err.message?.substring(0, 80)}`);
            }
        }

        // Deduplicate jobs
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
            console.log(`   ⚠️  No valid job pages found for ${companyName}`);
        }

    } catch (err) {
        console.error(`❌ Crawl failed for ${companyName}: ${err.message}`);
        throw err;
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
    const count = await customCrawlQueue.count();
    console.log(`\n🚀 Queue ready with ${count} companies. Workers running (concurrency: 3)...\n`);
})();

process.on('SIGINT', async () => {
    console.log('\n⏹️  Shutting down gracefully...');
    await worker.close();
    await customCrawlQueue.close();
    await redisConnection.quit();
    process.exit(0);
});
