/**
 * ============================================================================
 * Custom Crawler Queue — WITH LIVE PROGRESS + BACKFILL
 * ============================================================================
 * Features:
 *   - Live progress counter (every 5 companies)
 *   - Backfill: updates existing jobs missing location, company_name, raw_description
 *   - Playwright → ScraperAPI fallback
 *   - Impressum detection + /karriere fallback
 *   - Deduplication, freshness filter, Claude skill extraction
 *   - Summary report at the end
 * ============================================================================
 */

const { Queue, Worker } = require('bullmq');
const Redis = require('ioredis');
const { chromium } = require('playwright');
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const Anthropic = require('@anthropic-ai/sdk');
const cheerio = require('cheerio');
const crypto = require('crypto');
require('dotenv').config();

const { fetchWithScraperAPI } = require('../utils/scraperapi-config');

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
const CONFIG = {
    CONCURRENCY: parseInt(process.env.CRAWLER_CONCURRENCY || '3', 10),
    PAGE_SIZE: 1000,
    MAX_JOB_LINKS_PER_COMPANY: parseInt(process.env.MAX_JOB_LINKS_PER_COMPANY || '20', 10),
    RECRAWL_INTERVAL_HOURS: parseInt(process.env.RECRAWL_INTERVAL_HOURS || '48', 10),
    JOB_FRESHNESS_DAYS: parseInt(process.env.JOB_FRESHNESS_DAYS || '30', 10),
    INCLUDE_UNDATED_JOBS: process.env.INCLUDE_UNDATED_JOBS !== 'false',
    COMPANY_TIMEOUT_MS: parseInt(process.env.COMPANY_TIMEOUT_MS || '120000', 10),
    PLAYWRIGHT_TIMEOUT_MS: parseInt(process.env.PLAYWRIGHT_TIMEOUT_MS || '60000', 10),
    BROWSER_RESTART_THRESHOLD: parseInt(process.env.BROWSER_RESTART_THRESHOLD || '150', 10),
    QUEUE_POLL_INTERVAL_MS: 5000,
    RATE_LIMIT_MAX: parseInt(process.env.CRAWLER_RATE_LIMIT_MAX || '5', 10),
    RATE_LIMIT_DURATION_MS: parseInt(process.env.CRAWLER_RATE_LIMIT_DURATION_MS || '1000', 10),
    SKILL_EXTRACTION_MODEL: process.env.SKILL_EXTRACTION_MODEL || 'claude-haiku-4-5-20251001'
};

const QUEUE_NAME = 'custom-crawl';

// ---------------------------------------------------------------------------
// Clients
// ---------------------------------------------------------------------------
const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

const redisConnection = new Redis({
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT || '6379', 10),
    maxRetriesPerRequest: null
});

const customCrawlQueue = new Queue(QUEUE_NAME, { connection: redisConnection });

// ---------------------------------------------------------------------------
// Shared Playwright browser (with recycling)
// ---------------------------------------------------------------------------
let sharedBrowser = null;
let requestsSinceRestart = 0;

async function getSharedBrowser() {
    if (!sharedBrowser) {
        sharedBrowser = await chromium.launch({ headless: true });
        console.log('Playwright browser launched (shared instance).');
    }
    return sharedBrowser;
}

async function recycleBrowserIfNeeded() {
    requestsSinceRestart++;
    if (requestsSinceRestart >= CONFIG.BROWSER_RESTART_THRESHOLD) {
        console.log(`Recycling Playwright browser after ${requestsSinceRestart} requests.`);
        const previousBrowser = sharedBrowser;
        sharedBrowser = await chromium.launch({ headless: true });
        requestsSinceRestart = 0;
        if (previousBrowser) await previousBrowser.close().catch(() => {});
    }
}

async function fetchWithPlaywright(url) {
    const browser = await getSharedBrowser();
    const context = await browser.newContext({
        extraHTTPHeaders: { 'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8' }
    });
    try {
        const page = await context.newPage();
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: CONFIG.PLAYWRIGHT_TIMEOUT_MS });
        return await page.content();
    } finally {
        await context.close().catch(() => {});
        await recycleBrowserIfNeeded();
    }
}

// ---------------------------------------------------------------------------
// Page fetching: Playwright → ScraperAPI fallback
// ---------------------------------------------------------------------------
async function fetchPageWithFallback(url) {
    try {
        const html = await fetchWithPlaywright(url);
        console.log('   Fetched via Playwright.');
        return html;
    } catch (playwrightError) {
        console.log(`   Playwright failed: ${playwrightError.message}`);
        console.log('   Falling back to ScraperAPI with improved settings...');
    }

    try {
        const html = await fetchWithScraperAPI(url, {
            renderJs: true,
            waitFor: 5000,
            premium: true,
            waitForSelector: 'body'
        });
        console.log('   Fetched via ScraperAPI.');
        return html;
    } catch (scraperApiError) {
        console.log(`   ScraperAPI also failed: ${scraperApiError.message}`);
        return null;
    }
}

// ---------------------------------------------------------------------------
// Impressum detection
// ---------------------------------------------------------------------------
function isImpressumPage(url, html) {
    const urlLower = url.toLowerCase();
    if (urlLower.includes('impressum')) return true;
    if (!html) return false;
    const lower = html.toLowerCase();
    return lower.includes('impressum') ||
           lower.includes('legal notice') ||
           lower.includes('site notice') ||
           lower.includes('rechtliche hinweise');
}

// ---------------------------------------------------------------------------
// Extract job links (enhanced)
// ---------------------------------------------------------------------------
function extractJobLinks(html, baseUrl) {
    const $ = cheerio.load(html);
    const links = [];

    const jobKeywords = [
        'job', 'jobs', 'karriere', 'karrier', 'career', 'careers',
        'stelle', 'stellen', 'stellenangebote', 'offene-stellen',
        'vakanz', 'position', 'positionen', 'ausbildung',
        'praktikum', 'bewerbung', 'vacancies', 'vacancy'
    ];

    // 1. All links
    $('a').each((_, el) => {
        const href = $(el).attr('href');
        const text = $(el).text().toLowerCase().trim();
        if (!href || href.includes('#') || href.includes('mailto:') || href.includes('tel:')) return;
        const hrefMatches = jobKeywords.some(keyword => href.toLowerCase().includes(keyword));
        const textMatches = jobKeywords.some(keyword => text.includes(keyword));
        if (hrefMatches || textMatches) {
            let fullUrl = href;
            if (!href.startsWith('http')) {
                try { fullUrl = new URL(href, baseUrl).href; } catch { return; }
            }
            links.push(fullUrl);
        }
    });

    // 2. Navigation menus
    $('nav a, ul.nav a, .menu a, .navigation a, .main-nav a').each((_, el) => {
        const href = $(el).attr('href');
        const text = $(el).text().toLowerCase().trim();
        if (!href) return;
        if (jobKeywords.some(k => href.toLowerCase().includes(k) || text.includes(k))) {
            let fullUrl = href;
            if (!href.startsWith('http')) {
                try { fullUrl = new URL(href, baseUrl).href; } catch { return; }
            }
            links.push(fullUrl);
        }
    });

    // 3. Filter and deduplicate
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

    return [...new Set(filtered)].slice(0, CONFIG.MAX_JOB_LINKS_PER_COMPANY);
}

// ---------------------------------------------------------------------------
// Description extraction
// ---------------------------------------------------------------------------
function extractDescriptionFromHTML(html) {
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

// ---------------------------------------------------------------------------
// Date extraction & freshness
// ---------------------------------------------------------------------------
function extractPostingDate(html) {
    const $ = cheerio.load(html);
    const jsonLdScripts = $('script[type="application/ld+json"]');
    for (let i = 0; i < jsonLdScripts.length; i++) {
        try {
            const raw = $(jsonLdScripts[i]).html();
            const parsed = JSON.parse(raw);
            const candidates = Array.isArray(parsed) ? parsed : [parsed];
            for (const item of candidates) {
                if (item && item['@type'] === 'JobPosting' && item.datePosted) {
                    const date = new Date(item.datePosted);
                    if (!isNaN(date.getTime())) return date;
                }
            }
        } catch (e) {}
    }
    const metaDate = $('meta[property="article:published_time"]').attr('content') ||
                     $('meta[name="date"]').attr('content');
    if (metaDate) {
        const date = new Date(metaDate);
        if (!isNaN(date.getTime())) return date;
    }
    const bodyText = $('body').text();
    const keywordWindow = bodyText.match(/(veröffentlicht|eingestellt|online seit|posted)[^\d]{0,20}(\d{1,2}[.\/]\d{1,2}[.\/]\d{4})/i);
    if (keywordWindow && keywordWindow[2]) {
        const dateMatch = keywordWindow[2].match(/(\d{1,2})[.\/](\d{1,2})[.\/](\d{4})/);
        if (dateMatch) {
            const [, day, month, year] = dateMatch;
            const date = new Date(`${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`);
            if (!isNaN(date.getTime())) return date;
        }
    }
    return null;
}

function isJobWithinFreshnessWindow(postedDate) {
    if (!postedDate) return CONFIG.INCLUDE_UNDATED_JOBS;
    const ageDays = (Date.now() - postedDate.getTime()) / (1000 * 60 * 60 * 24);
    return ageDays <= CONFIG.JOB_FRESHNESS_DAYS;
}

// ---------------------------------------------------------------------------
// Skill extraction
// ---------------------------------------------------------------------------
async function extractSkillsWithClaude(title, description) {
    if (!description || description.length < 100) return [];
    try {
        const response = await anthropic.messages.create({
            model: CONFIG.SKILL_EXTRACTION_MODEL,
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
        console.log(`   Skill extraction failed: ${err.message}`);
        return [];
    }
}

// ---------------------------------------------------------------------------
// Location & Company name extraction (from HTML)
// ---------------------------------------------------------------------------
function extractLocationFromHTML(html) {
    const $ = cheerio.load(html);
    // 1. JSON-LD
    const jsonLdScripts = $('script[type="application/ld+json"]');
    for (let i = 0; i < jsonLdScripts.length; i++) {
        try {
            const raw = $(jsonLdScripts[i]).html();
            const parsed = JSON.parse(raw);
            const candidates = Array.isArray(parsed) ? parsed : [parsed];
            for (const item of candidates) {
                if (item && item['@type'] === 'JobPosting' && item.jobLocation) {
                    const loc = item.jobLocation;
                    if (typeof loc === 'string') return loc;
                    if (loc.address) {
                        return loc.address.addressLocality || loc.address.streetAddress || loc.address.addressRegion || null;
                    }
                }
            }
        } catch (e) {}
    }
    // 2. Meta tags
    const metaLocation = $('meta[property="og:location"]').attr('content') ||
                         $('meta[name="geo.placename"]').attr('content') ||
                         $('meta[name="location"]').attr('content');
    if (metaLocation) return metaLocation.trim();
    // 3. Selectors
    const selectors = [
        '.job-location', '.location', '.office', '.workplace',
        '[class*="location"]', '[class*="office"]', '[itemprop="jobLocation"]',
        '[class*="ort"]', '[class*="stadt"]'
    ];
    for (const selector of selectors) {
        const text = $(selector).first().text().trim();
        if (text) return text;
    }
    // 4. Fallback text search
    const bodyText = $('body').text();
    const locationMatch = bodyText.match(/(?:Ort|Standort|Arbeitsort|Joblocation)[:\s]+([^\n,]{3,60})/i);
    if (locationMatch) return locationMatch[1].trim();
    return null;
}

function extractCompanyNameFromHTML(html, fallbackName) {
    const $ = cheerio.load(html);
    // 1. JSON-LD
    const jsonLdScripts = $('script[type="application/ld+json"]');
    for (let i = 0; i < jsonLdScripts.length; i++) {
        try {
            const raw = $(jsonLdScripts[i]).html();
            const parsed = JSON.parse(raw);
            const candidates = Array.isArray(parsed) ? parsed : [parsed];
            for (const item of candidates) {
                if (item && item['@type'] === 'JobPosting' && item.hiringOrganization) {
                    const org = item.hiringOrganization;
                    if (typeof org === 'string') return org;
                    if (org.name) return org.name;
                }
            }
        } catch (e) {}
    }
    // 2. Meta tags
    const metaCompany = $('meta[property="og:site_name"]').attr('content') ||
                        $('meta[name="application-name"]').attr('content') ||
                        $('meta[name="company"]').attr('content');
    if (metaCompany) return metaCompany.trim();
    // 3. Selectors
    const selectors = [
        '.company-name', '.employer', '.hiring-company',
        '[itemprop="hiringOrganization"]', '[class*="company"]',
        '.job-company', '.organization-name'
    ];
    for (const selector of selectors) {
        const text = $(selector).first().text().trim();
        if (text && text.length > 1) return text;
    }
    // 4. Fallback text search
    const bodyText = $('body').text();
    const companyMatch = bodyText.match(/(?:Arbeitgeber|Unternehmen|Firma|Company)[:\s]+([^\n,]{2,60})/i);
    if (companyMatch) return companyMatch[1].trim();
    return fallbackName || null;
}

// ---------------------------------------------------------------------------
// Deduplication helpers
// ---------------------------------------------------------------------------
function generateExternalJobId(url) {
    return crypto.createHash('sha256').update(url.trim().toLowerCase()).digest('hex').slice(0, 40);
}

async function splitNewAndExistingJobs(companyId, candidateJobs) {
    if (candidateJobs.length === 0) return { newJobs: [], existingIds: [] };
    const externalIds = candidateJobs.map(j => j.external_job_id);
    const { data: existingRows, error } = await supabase
        .from('jobs')
        .select('external_job_id')
        .eq('company_id', companyId)
        .in('external_job_id', externalIds);
    if (error) {
        console.error(`   Error checking existing jobs: ${error.message}`);
        return { newJobs: candidateJobs, existingIds: [] };
    }
    const existingIds = new Set((existingRows || []).map(r => r.external_job_id));
    const newJobs = candidateJobs.filter(j => !existingIds.has(j.external_job_id));
    return { newJobs, existingIds: Array.from(existingIds) };
}

async function refreshExistingJobs(companyId, existingIds) {
    if (existingIds.length === 0) return;
    const { error } = await supabase
        .from('jobs')
        .update({ last_seen_at: new Date().toISOString(), is_active: true })
        .eq('company_id', companyId)
        .in('external_job_id', existingIds);
    if (error) console.error(`   Error refreshing existing jobs: ${error.message}`);
}

// ---------------------------------------------------------------------------
// Company status helper
// ---------------------------------------------------------------------------
async function markCompanyStatus(companyId, status, { touchTimestamp = true } = {}) {
    const updates = { crawl_status: status };
    if (touchTimestamp) updates.last_crawled_at = new Date().toISOString();
    const { error } = await supabase.from('companies').update(updates).eq('Id', companyId);
    if (error) console.error(`Failed to update status: ${error.message}`);
}

// ---------------------------------------------------------------------------
// BACKFILL: Update existing jobs missing location, company_name, or raw_description
// ---------------------------------------------------------------------------
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
        // Skip if all fields present and description is long enough
        if (job.location && job.company_name && job.raw_description && job.raw_description.length > 100) continue;

        try {
            const jobHtml = await fetchPageWithFallback(job.apply_url);
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
                const desc = extractDescriptionFromHTML(jobHtml);
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

// ---------------------------------------------------------------------------
// GLOBAL STATS & PROGRESS TRACKING
// ---------------------------------------------------------------------------
const stats = {
    processed: 0,
    failed_fetch: 0,
    no_jobs: 0,
    with_jobs: 0,
    jobs_saved: 0,
    existing_refreshed: 0,
    errors: 0,
};

let processedCount = 0;
let totalQueued = 0;

function printProgress() {
    const pct = totalQueued > 0 ? ((processedCount / totalQueued) * 100).toFixed(1) : 0;
    const remaining = totalQueued - processedCount;
    console.log(`\n📊 Progress: ${processedCount}/${totalQueued} companies (${pct}%) | Remaining: ${remaining}`);
    console.log(`   ✅ with jobs: ${stats.with_jobs} | ❌ no jobs: ${stats.no_jobs} | 🚫 failed: ${stats.failed_fetch} | 💾 jobs saved: ${stats.jobs_saved}`);
}

// ---------------------------------------------------------------------------
// Core per-company processing
// ---------------------------------------------------------------------------
async function processCompany(job) {
    const { companyId, companyName: recordCompanyName, careerUrl } = job.data;
    console.log(`\nCrawling: ${recordCompanyName}`);
    console.log(`   URL: ${careerUrl}`);

    await markCompanyStatus(companyId, 'in_progress', { touchTimestamp: false });

    // Try to fetch the given career URL
    let html = await fetchPageWithFallback(careerUrl);
    if (!html) {
        console.log('   Failed to fetch career page.');
        await markCompanyStatus(companyId, 'failed');
        return { status: 'failed_fetch', companyId, companyName: recordCompanyName };
    }

    let jobLinks = extractJobLinks(html, careerUrl);

    // Impressum fallback
    if (isImpressumPage(careerUrl, html) && jobLinks.length === 0) {
        console.log('   Detected impressum page with no job links — trying /karriere fallback.');
        const baseUrl = new URL(careerUrl).origin;
        const fallbackUrl = baseUrl + '/karriere';
        const fallbackHtml = await fetchPageWithFallback(fallbackUrl);
        if (fallbackHtml) {
            jobLinks = extractJobLinks(fallbackHtml, fallbackUrl);
            console.log(`   Fallback found ${jobLinks.length} job links.`);
            html = fallbackHtml; // use fallback HTML for later extraction
        }
    }

    console.log(`   Found ${jobLinks.length} job detail links.`);

    if (jobLinks.length === 0) {
        // Still backfill existing jobs
        await backfillMissingJobFields(companyId);
        await markCompanyStatus(companyId, 'ats_detected');
        return { status: 'no_jobs', companyId, companyName: recordCompanyName };
    }

    const candidateJobs = [];
    const seenInThisRun = new Set();

    for (const link of jobLinks) {
        try {
            const jobHtml = await fetchPageWithFallback(link);
            if (!jobHtml) continue;

            const $ = cheerio.load(jobHtml);
            const title = $('title').text().trim() || 'Untitled Job';
            const description = extractDescriptionFromHTML(jobHtml);

            // Extract location and company name
            const location = extractLocationFromHTML(jobHtml);
            const companyName = extractCompanyNameFromHTML(jobHtml, recordCompanyName);

            if (!description || description.length < 100) {
                console.log(`   Skipped (description too short): ${title.substring(0, 50)}`);
                continue;
            }

            const postedDate = extractPostingDate(jobHtml);
            if (!isJobWithinFreshnessWindow(postedDate)) {
                console.log(`   Skipped (older than ${CONFIG.JOB_FRESHNESS_DAYS} days): ${title.substring(0, 50)}`);
                continue;
            }

            const externalJobId = generateExternalJobId(link);
            if (seenInThisRun.has(externalJobId)) continue;
            seenInThisRun.add(externalJobId);

            const skills = await extractSkillsWithClaude(title, description);
            const skillsPreview = skills.length > 0
                ? skills.slice(0, 5).join(', ') + (skills.length > 5 ? ` +${skills.length - 5} more` : '')
                : 'none';
            console.log(`   Extracted: ${title.substring(0, 50)}`);
            console.log(`      Skills (${skills.length}): ${skillsPreview}`);
            if (location) console.log(`      Location: ${location}`);
            if (companyName) console.log(`      Company: ${companyName}`);

            candidateJobs.push({
                company_id: companyId,
                external_job_id: externalJobId,
                title,
                raw_description: description.slice(0, 5000),
                structured_skills: skills,
                apply_url: link,
                posted_at: postedDate ? postedDate.toISOString() : null,
                location: location,
                company_name: companyName,
                is_active: true,
                first_seen_at: new Date().toISOString(),
                last_seen_at: new Date().toISOString()
            });
        } catch (err) {
            console.error(`   Link error (${link}): ${err.message ? err.message.substring(0, 120) : err}`);
        }
    }

    const { newJobs, existingIds } = await splitNewAndExistingJobs(companyId, candidateJobs);

    let jobsSaved = 0;
    if (newJobs.length > 0) {
        const { error } = await supabase.from('jobs').insert(newJobs);
        if (error) {
            console.error(`   Supabase insert error: ${error.message}`);
        } else {
            console.log(`   Saved ${newJobs.length} new job(s).`);
            jobsSaved = newJobs.length;
        }
    }

    let refreshed = existingIds.length;
    if (refreshed > 0) {
        await refreshExistingJobs(companyId, existingIds);
        console.log(`   Refreshed ${refreshed} previously known job(s).`);
    }

    // Backfill existing jobs (if any missing fields)
    await backfillMissingJobFields(companyId);

    if (newJobs.length === 0 && refreshed === 0) {
        console.log(`   No valid, fresh, non-duplicate jobs found.`);
    }

    await markCompanyStatus(companyId, 'ats_detected');
    return {
        status: 'success',
        companyId,
        companyName: recordCompanyName,
        jobsSaved,
        refreshed
    };
}

// ---------------------------------------------------------------------------
// Queue and worker setup
// ---------------------------------------------------------------------------
async function resetStuckCompanies() {
    const { data, error } = await supabase
        .from('companies')
        .update({ crawl_status: 'pending' })
        .eq('ats_type', 'custom')
        .eq('crawl_status', 'in_progress')
        .select('Id');
    if (error) {
        console.error(`Error resetting stuck companies: ${error.message}`);
        return;
    }
    if (data && data.length > 0) {
        console.log(`Reset ${data.length} companies stuck in "in_progress".`);
    }
}

async function addCustomCompaniesToQueue() {
    const cutoffIso = new Date(Date.now() - CONFIG.RECRAWL_INTERVAL_HOURS * 60 * 60 * 1000).toISOString();
    let page = 0;
    let totalAdded = 0;
    let hasMore = true;

    console.log(`Fetching custom companies due for crawl...`);

    while (hasMore) {
        const start = page * CONFIG.PAGE_SIZE;
        const end = start + CONFIG.PAGE_SIZE - 1;

        const { data: companies, error } = await supabase
            .from('companies')
            .select('"Id", detected_career_url, "Name", crawl_status, last_crawled_at')
            .eq('ats_type', 'custom')
            .not('detected_career_url', 'is', null)
            .neq('crawl_status', 'in_progress')
            .or(`last_crawled_at.is.null,last_crawled_at.lt.${cutoffIso}`)
            .order('Id', { ascending: true })
            .range(start, end);

        if (error) {
            console.error(`Error fetching companies: ${error.message}`);
            break;
        }

        if (!companies || companies.length === 0) {
            hasMore = false;
            break;
        }

        console.log(`Page ${page + 1}: queueing ${companies.length} companies...`);

        for (const company of companies) {
            await customCrawlQueue.add('crawl-company', {
                companyId: company.Id,
                companyName: company.Name,
                careerUrl: company.detected_career_url
            }, {
                jobId: `company-${company.Id}`,
                attempts: 3,
                backoff: { type: 'exponential', delay: 5000 },
                removeOnComplete: 1000,
                removeOnFail: 5000
            });
            totalAdded++;
        }

        if (companies.length < CONFIG.PAGE_SIZE) hasMore = false;
        page++;
    }

    console.log(`Total companies queued: ${totalAdded}`);
    totalQueued = totalAdded;
    return totalAdded;
}

async function withTimeout(promise, ms, label) {
    let timeoutHandle;
    const timeoutPromise = new Promise((_, reject) => {
        timeoutHandle = setTimeout(() => reject(new Error(`Timed out after ${ms}ms: ${label}`)), ms);
    });
    try {
        return await Promise.race([promise, timeoutPromise]);
    } finally {
        clearTimeout(timeoutHandle);
    }
}

// ─── Worker ────────────────────────────────────────────────────────────────
const worker = new Worker(QUEUE_NAME, async job => {
    const result = await withTimeout(
        processCompany(job),
        CONFIG.COMPANY_TIMEOUT_MS,
        `company ${job.data.companyName}`
    );
    return result;
}, {
    connection: redisConnection,
    concurrency: CONFIG.CONCURRENCY,
    limiter: { max: CONFIG.RATE_LIMIT_MAX, duration: CONFIG.RATE_LIMIT_DURATION_MS }
});

// ─── Accumulate stats & progress ──────────────────────────────────────────
worker.on('completed', (job, result) => {
    // Update stats
    stats.processed++;
    if (result.status === 'failed_fetch') {
        stats.failed_fetch++;
    } else if (result.status === 'no_jobs') {
        stats.no_jobs++;
    } else if (result.status === 'success') {
        stats.with_jobs++;
        stats.jobs_saved += (result.jobsSaved || 0);
        stats.existing_refreshed += (result.refreshed || 0);
    } else {
        stats.errors++;
    }

    // Increment progress counter
    processedCount++;
    if (processedCount % 5 === 0 || processedCount === totalQueued) {
        printProgress();
    }

    console.log(`Completed: ${result.companyName || job.data.companyName}`);
});

worker.on('failed', async (job, err) => {
    console.error(`Permanently failed: ${job?.data?.companyName} — ${err.message}`);
    stats.processed++;
    stats.errors++;
    processedCount++;
    if (processedCount % 5 === 0 || processedCount === totalQueued) {
        printProgress();
    }
    if (job?.data?.companyId) {
        await markCompanyStatus(job.data.companyId, 'failed');
    }
});

// ─── Orchestration ──────────────────────────────────────────────────────────
async function waitForQueueCompletion() {
    return new Promise(resolve => {
        const interval = setInterval(async () => {
            const counts = await customCrawlQueue.getJobCounts('waiting', 'active', 'delayed');
            const remaining = counts.waiting + counts.active + counts.delayed;
            if (remaining === 0) {
                clearInterval(interval);
                resolve();
            }
        }, CONFIG.QUEUE_POLL_INTERVAL_MS);
    });
}

async function shutdown(exitCode = 0) {
    console.log('Shutting down gracefully...');
    try { await worker.close(); } catch (e) {}
    try { await customCrawlQueue.close(); } catch (e) {}
    try { if (sharedBrowser) await sharedBrowser.close(); } catch (e) {}
    try { await redisConnection.quit(); } catch (e) {}
    process.exit(exitCode);
}

function printSummary() {
    console.log('\n' + '═'.repeat(60));
    console.log('📊 CUSTOM CRAWLER SUMMARY');
    console.log('═'.repeat(60));
    console.log(`   Total companies processed    : ${stats.processed}`);
    console.log(`   ✅ Companies with jobs saved : ${stats.with_jobs}`);
    console.log(`   ❌ Companies with no jobs    : ${stats.no_jobs}`);
    console.log(`   🚫 Companies failed to fetch : ${stats.failed_fetch}`);
    console.log(`   ⚠️  Other errors             : ${stats.errors}`);
    console.log(`   💾 Total new jobs saved      : ${stats.jobs_saved}`);
    console.log(`   🔄 Total existing refreshed  : ${stats.existing_refreshed}`);
    console.log('═'.repeat(60) + '\n');
}

async function runCrawler() {
    console.log('Starting Custom Crawler (with Live Progress + Backfill)...');
    await resetStuckCompanies();
    const totalAdded = await addCustomCompaniesToQueue();
    if (totalAdded === 0) {
        console.log('No companies due for crawling. Exiting.');
        await shutdown(0);
        return;
    }
    console.log(`Queued ${totalAdded} companies. Processing with concurrency ${CONFIG.CONCURRENCY}...`);
    await waitForQueueCompletion();
    console.log('All companies processed.');
    printProgress(); // final progress
    printSummary();
    await shutdown(0);
}

// ─── Global crash protection ──────────────────────────────────────────────
process.on('unhandledRejection', (reason) => console.error('Unhandled rejection:', reason));
process.on('uncaughtException', (err) => console.error('Uncaught exception:', err));
process.on('SIGINT', () => { printProgress(); printSummary(); shutdown(0); });
process.on('SIGTERM', () => { printProgress(); printSummary(); shutdown(0); });

runCrawler();
