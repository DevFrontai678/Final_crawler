require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const axios = require('axios');
const cheerio = require('cheerio');

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

// ─── SHARED BROWSER FOR PLAYWRIGHT FALLBACK ──────────────────────────────
let globalBrowser = null;

async function getBrowser() {
    if (!globalBrowser || !globalBrowser.isConnected()) {
        const { chromium } = require('playwright');
        globalBrowser = await chromium.launch({ headless: true });
    }
    return globalBrowser;
}

// ─── FETCH HTML: AXIOS + PLAYWRIGHT FALLBACK ─────────────────────────────
async function fetchHtmlWithFallback(url) {
    try {
        const response = await axios.get(url, {
            timeout: 15000,
            headers: { 'User-Agent': 'Mozilla/5.0' },
            maxRedirects: 5
        });
        return response.data;
    } catch (err) {
        console.log(`      ⚠️ Axios failed: ${err.message} → trying Playwright...`);
        let page = null;
        let context = null;
        try {
            const browser = await getBrowser();
            context = await browser.newContext();
            page = await context.newPage();
            await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
            const html = await page.content();
            return html;
        } catch (pwErr) {
            console.log(`      ❌ Playwright also failed: ${pwErr.message}`);
            return null;
        } finally {
            if (page) await page.close().catch(() => {});
            if (context) await context.close().catch(() => {});
        }
    }
}

// ─── EXTRACT DESCRIPTION FROM HTML ────────────────────────────────────────
function extractDescriptionFromHtml(html) {
    const $ = cheerio.load(html);
    const selectors = [
        '.job-description', '.job-details', '.description', '.content',
        '#job-description', '.job-content', '[class*="job-description"]',
        '[class*="job-detail"]', '[class*="description"]', 'article',
        '.main-content', '#content', '.text-content', '.post-content',
        '.entry-content', '.job__description', '.job-listing__description',
        '[itemprop="description"]', '[itemprop="jobDescription"]',
        '.job__text', '.job-text', '.description-text'
    ];

    for (const selector of selectors) {
        const el = $(selector);
        if (el.length) {
            const text = el.text().trim();
            if (text.length > 100) return text;
        }
    }

    let bodyText = $('body').text();
    bodyText = bodyText.split('\n')
        .map(l => l.trim())
        .filter(l => l.length > 20)
        .filter(l => !/impressum|datenschutz|agb|cookie|footer|menu|navigation|copyright|©/.test(l))
        .join('\n');
    return bodyText.length > 100 ? bodyText : '';
}

// ─── EXTRACT JOB LINKS GENERIC ────────────────────────────────────────────
function extractJobLinksGeneric(html, baseUrl) {
    const $ = cheerio.load(html);
    const links = [];

    $('a[href*="job"], a[href*="stelle"], a[href*="karriere"], a[href*="bewerbung"], a[href*="vakanz"], a[href*="offene"], a[href*="position"], a[href*="ausbildung"], a[href*="praktikum"]').each((_, el) => {
        let href = $(el).attr('href');
        if (href && !href.includes('#') && !href.includes('mailto:') && !href.includes('tel:')) {
            if (!href.startsWith('http')) {
                try {
                    href = new URL(href, baseUrl).href;
                } catch (e) { return; }
            }
            links.push(href);
        }
    });

    const filtered = links.filter(href =>
        !/karriere|jobs|stellenangebote|offene-stellen|jobboerse|careers|career|bewerbung|bewerben/i.test(href) ||
        /\/job\//i.test(href) ||
        /\/stelle\//i.test(href) ||
        /\/position\//i.test(href) ||
        /\/vakanz\//i.test(href) ||
        /\/ausschreibung\//i.test(href) ||
        /\/detail\?/i.test(href) ||
        /\/job-\d+/i.test(href)
    );

    return [...new Set(filtered)].slice(0, 20);
}

// ─── GENERIC FALLBACK CRAWL ──────────────────────────────────────────────
async function genericFallbackCrawl(careerPageUrl) {
    console.log(`   🔄 Generic fallback crawling...`);
    const html = await fetchHtmlWithFallback(careerPageUrl);
    if (!html) {
        console.log(`   ❌ Fallback: could not fetch career page`);
        return [];
    }

    const jobLinks = extractJobLinksGeneric(html, careerPageUrl);
    console.log(`   Fallback found ${jobLinks.length} job links`);

    const jobs = [];
    for (const link of jobLinks) {
        const jobHtml = await fetchHtmlWithFallback(link);
        if (!jobHtml) continue;

        const $ = cheerio.load(jobHtml);
        const title = $('title').text().trim() || 'Untitled Job';
        const description = extractDescriptionFromHtml(jobHtml);

        if (!description || description.length < 100) continue;

        const externalId = Buffer.from(link).toString('base64').slice(0, 50);
        jobs.push({
            external_job_id: externalId,
            title,
            raw_description: description.slice(0, 5000),
            apply_url: link,
            location: null,
            employment_type: null,
            ats_source: 'custom_fallback'
        });
    }
    console.log(`   Fallback extracted ${jobs.length} jobs`);
    return jobs;
}

// ─── EXTRACT SOFTGARDEN IDs ──────────────────────────────────────────────
async function extractSoftgardenIds(careerPageUrl) {
    try {
        const html = await fetchHtmlWithFallback(careerPageUrl);
        if (!html) return null;

        const uMatch = html.match(/"userId"\s*:\s*"([a-f0-9-]{36})"/);
        const pMatch = html.match(/"projectId"\s*:\s*"([a-f0-9-]{36})"/);
        if (uMatch && pMatch) {
            return { userId: uMatch[1], projectId: pMatch[1] };
        }

        const apiKeyMatch = html.match(/apiKey=([a-f0-9-]+)/);
        if (apiKeyMatch) {
            const widgetUrl = `https://pcw-api.softgarden.de/widgets/widget?apiKey=${apiKeyMatch[1]}`;
            const widgetHtml = await fetchHtmlWithFallback(widgetUrl);
            if (widgetHtml) {
                const uM = widgetHtml.match(/"userId":"([a-f0-9-]{36})"/);
                const pM = widgetHtml.match(/"projectId":"([a-f0-9-]{36})"/);
                if (uM && pM) return { userId: uM[1], projectId: pM[1] };
            }
        }
        return null;
    } catch (err) {
        return null;
    }
}

// ─── FETCH SOFTGARDEN JOBS ────────────────────────────────────────────────
async function fetchSoftgardenJobs(userId, projectId) {
    try {
        const response = await axios.post(
            'https://pcw-api.softgarden.de/widgets/job-list/job-ads',
            {
                userId,
                projectId,
                locale: 'de',
                numberOfJobsOnPage: 9999999,
                pageNumber: '1',
                isGetFilters: true,
                isActiveCustomJobPages: true,
                isForCurrentLocale: false,
                isUseLayoutsOfSubsidiaries: false,
                listState: {
                    search: '',
                    disableSearchInDescription: false,
                    location: { osmLocation: '', range: 25, coords: [] }
                },
                filterStatus: {
                    careerLevel: false,
                    category: false,
                    partnership: false,
                    region: false,
                    location: false
                }
            },
            {
                headers: { 'Content-Type': 'application/json', 'User-Agent': 'Mozilla/5.0' },
                timeout: 15000
            }
        );

        const data = response.data;
        const ads = data.jobAds || data.jobs || data.data || [];
        const jobs = [];

        for (const ad of ads) {
            const applyUrl = ad.applyUrl || ad.applicationUrl || `https://pcw-api.softgarden.de/job/${ad.id}`;
            const description = await fetchDescriptionFromJobPage(applyUrl);
            jobs.push({
                external_job_id: String(ad.id || ad.jobAdId || ad.externalId),
                title: ad.jobTitle || ad.title || ad.name || 'Untitled',
                location: ad.location?.city || ad.city || ad.locationName || null,
                employment_type: ad.workTime || ad.employmentType || null,
                raw_description: description,
                apply_url: applyUrl,
                ats_source: 'softgarden'
            });
            await new Promise(r => setTimeout(r, 500)); // rate limit
        }
        return jobs;
    } catch (err) {
        console.log(`   ❌ Softgarden API error: ${err.message}`);
        return [];
    }
}

// ─── FETCH DESCRIPTION FROM JOB PAGE ─────────────────────────────────────
async function fetchDescriptionFromJobPage(jobUrl) {
    const html = await fetchHtmlWithFallback(jobUrl);
    if (!html) return null;
    return extractDescriptionFromHtml(html);
}

// ─── PROCESS COMPANY ──────────────────────────────────────────────────────
async function processSoftgardenCompany(company) {
    console.log(`\n🔍 Processing: ${company.Name}`);
    console.log(`   URL: ${company.detected_career_url}`);

    // 1. Try Softgarden IDs
    const ids = await extractSoftgardenIds(company.detected_career_url);
    if (ids) {
        console.log(`   ✅ Found Softgarden IDs`);
        const jobs = await fetchSoftgardenJobs(ids.userId, ids.projectId);
        if (jobs.length > 0) {
            return { company, jobs, usedFallback: false };
        } else {
            console.log(`   ⚠️ Softgarden API returned 0 jobs → trying fallback`);
            const fallbackJobs = await genericFallbackCrawl(company.detected_career_url);
            return { company, jobs: fallbackJobs, usedFallback: true };
        }
    }

    // 2. If no IDs, use generic fallback
    console.log(`   ❌ No Softgarden IDs found → using generic fallback`);
    const fallbackJobs = await genericFallbackCrawl(company.detected_career_url);
    return { company, jobs: fallbackJobs, usedFallback: true };
}

// ─── RUN ────────────────────────────────────────────────────────────────────
async function run() {
    const { data: companies, error } = await supabase
        .from('companies')
        .select('"Id", "Name", detected_career_url')
        .eq('ats_type', 'softgarden')
        .eq('crawl_status', 'ats_detected');

    if (error) {
        console.error('❌ Supabase error:', error.message);
        return;
    }

    if (!companies || companies.length === 0) {
        console.log('No Softgarden companies to process.');
        return;
    }

    console.log(`📋 Processing ${companies.length} Softgarden companies...\n`);

    let totalJobs = 0;
    for (const company of companies) {
        const result = await processSoftgardenCompany(company);
        if (result.jobs.length === 0) continue;

        for (const job of result.jobs) {
            const { error: insertError } = await supabase
                .from('jobs')
                .upsert({
                    company_id: company.Id,
                    external_job_id: job.external_job_id,
                    title: job.title,
                    location: job.location,
                    employment_type: job.employment_type,
                    raw_description: job.raw_description ? job.raw_description.slice(0, 5000) : null,
                    apply_url: job.apply_url,
                    is_active: true,
                    first_seen_at: new Date(),
                    last_seen_at: new Date(),
                    ats_source: result.usedFallback ? 'softgarden_fallback' : 'softgarden'
                }, { onConflict: 'company_id,external_job_id' });

            if (insertError) {
                console.error(`   ❌ Save error: ${insertError.message}`);
            }
        }
        totalJobs += result.jobs.length;
        console.log(`   💾 Saved ${result.jobs.length} jobs for ${company.Name}`);
        await new Promise(r => setTimeout(r, 500));
    }

    console.log(`\n✅ Done! Total Softgarden jobs saved: ${totalJobs}`);
}

run().catch(console.error);
