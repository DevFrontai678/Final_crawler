require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const { chromium } = require('playwright');
const axios = require('axios');
const cheerio = require('cheerio');
const crypto = require('crypto');
const { CRAWLER_TIMEOUTS } = require('../src/utils/crawler-timeouts');
const { enrichJobForStorage } = require('../src/utils/job-enrichment');
const { runCompaniesInBatches } = require('../src/utils/company-batch-runner');

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

// ─── HELPER: generate external_hash ──────────────────────────────────────
function generateExternalHash(companyId, externalJobId) {
    if (!companyId || !externalJobId) return null;
    return crypto.createHash('sha256')
        .update(`${companyId}:${externalJobId}`)
        .digest('hex')
        .slice(0, 64);
}

// ─── REMOTE TYPE DETECTION ──────────────────────────────────────────────
function detectRemoteType(description) {
    const text = (description || '').toLowerCase();
    if (text.includes('remote') || text.includes('homeoffice') || text.includes('100% remote') || text.includes('full remote')) {
        return 'remote';
    }
    if (text.includes('hybrid') || text.includes('teilweise remote') || text.includes('mobile work') || text.includes('flexibles arbeiten')) {
        return 'hybrid';
    }
    return 'onsite';
}

// ─── SKIP NON-HTML FILES ────────────────────────────────────────────────
const SKIP_EXTENSIONS = ['.pdf', '.docx', '.xlsx', '.zip', '.rar', '.ppt', '.pptx', '.csv', '.png', '.jpg', '.jpeg', '.gif', '.svg'];
function isSkipFile(url) {
    if (!url) return true;
    const lower = url.toLowerCase();
    return SKIP_EXTENSIONS.some(ext => lower.endsWith(ext) || lower.includes(ext + '?'));
}

// ─── SELF‑HEALING BROWSER ──────────────────────────────────────────────
let browserInstance = null;
let browserInitPromise = null;

async function getBrowser() {
    if (browserInstance && browserInstance.isConnected()) {
        return browserInstance;
    }
    if (browserInitPromise) {
        return browserInitPromise;
    }
    browserInitPromise = (async () => {
        try {
            browserInstance = await chromium.launch({ headless: true });
            return browserInstance;
        } finally {
            browserInitPromise = null;
        }
    })();
    return browserInitPromise;
}

// ─── OPTIMIZED FETCH ──────────────────────────────────────────────────
async function customCrawlerFetchPage(url) {
    if (isSkipFile(url)) {
        console.log(`   ⏭️ Skipping non-HTML file: ${url}`);
        return null;
    }

    try {
        const response = await axios.get(url, {
            timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
            headers: { 'User-Agent': 'Mozilla/5.0' }
        });
        const contentType = response.headers['content-type'] || '';
        if (contentType.includes('html') && response.data.length > 1000) {
            console.log(`   ✅ Fetched via Axios (${response.data.length} chars)`);
            return response.data;
        }
        console.log(`   ⚠️ Axios returned empty or non-HTML, trying Playwright...`);
    } catch (axiosErr) {
        console.log(`   ⚠️ Axios failed: ${axiosErr.message}, trying Playwright...`);
    }

    try {
        const browser = await getBrowser();
        const page = await browser.newPage();
        await page.setExtraHTTPHeaders({
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8'
        });
        await page.goto(url, { waitUntil: 'networkidle', timeout: CRAWLER_TIMEOUTS.NAVIGATION_TIMEOUT_MS });
        const html = await page.content();
        await page.close();
        if (html && html.length > 1000) {
            console.log(`   ✅ Fetched via Playwright (${html.length} chars)`);
            return html;
        }
        console.log(`   ⚠️ Playwright returned empty page`);
    } catch (pwErr) {
        console.log(`   ❌ Playwright failed: ${pwErr.message}`);
        if (pwErr.message.includes('closed') || pwErr.message.includes('Target page')) {
            browserInstance = null;
        }
    }
    return null;
}

// ─── EXTRACT JOB LINKS ──────────────────────────────────────────────────
function customCrawlerExtractLinks(html, baseUrl) {
    const $ = cheerio.load(html);
    const links = [];

    const jobKeywords = [
        'job', 'jobs', 'karriere', 'karrier', 'career', 'careers',
        'stelle', 'stellen', 'stellenangebote', 'offene-stellen',
        'vakanz', 'position', 'positionen', 'ausbildung',
        'praktikum', 'bewerbung', 'vacancies', 'vacancy',
        'mitarbeiter', 'fachkraft', 'führungskraft', 'leitung',
        'entwickler', 'engineer', 'manager', 'consultant',
        'concludis', 'concludis-job', 'bewirb', 'join-us'
    ];

    $('a').each((_, el) => {
        const href = $(el).attr('href');
        const text = $(el).text().toLowerCase().trim();
        if (!href || href.includes('#') || href.includes('mailto:') || href.includes('tel:')) return;
        if (isSkipFile(href)) return;
        const hrefLower = href.toLowerCase();
        if (jobKeywords.some(kw => hrefLower.includes(kw) || text.includes(kw))) {
            let fullUrl = href;
            if (!href.startsWith('http')) {
                try {
                    fullUrl = new URL(href, baseUrl).href;
                } catch (e) { return; }
            }
            links.push(fullUrl);
        }
    });

    const unique = [...new Set(links)];
    const filtered = unique.filter(href =>
        !/impressum|datenschutz|agb|cookie|kontakt|about|team|news|blog|unternehmen|über-uns|karriere-übersicht/i.test(href)
    );
    return filtered.length > 0 ? filtered.slice(0, 500) : unique.slice(0, 500);
}

// ─── SCRAPE SINGLE JOB PAGE ────────────────────────────────────────────
async function customCrawlerScrapeJob(url) {
    if (isSkipFile(url)) return null;
    const html = await customCrawlerFetchPage(url);
    if (!html) return null;
    const $ = cheerio.load(html);
    const title = $('title').text().trim() || 'Untitled';

    const selectors = [
        '.job-description', '.job-details', '.description', '.content',
        '#job-description', '.job-content', '[class*="job-description"]',
        '[class*="job-detail"]', '[class*="description"]', 'article',
        '.main-content', '#content', '.text-content', '.post-content',
        '.entry-content', '.job__description', '.job-listing__description',
        '[itemprop="description"]', '[itemprop="jobDescription"]',
        '.concludis-job-description', '.concludis-description',
        '.vacancy-description', '.job-description__text'
    ];
    let description = '';
    for (const selector of selectors) {
        const text = $(selector).text().trim();
        if (text && text.length > 200) {
            description = text;
            break;
        }
    }
    if (!description) {
        description = $('body').text()
            .split('\n')
            .map(l => l.trim())
            .filter(l => l.length > 20)
            .filter(l => !/impressum|datenschutz|agb|cookie|footer|menu|navigation|copyright|©/.test(l))
            .join('\n')
            .slice(0, 5000);
    }

    const locationSelectors = [
        '.location', '.office', '.city', '.job-location',
        '[itemprop="jobLocation"]', '.address', '.place',
        '[class*="location"]', '[class*="office"]', '[class*="city"]',
        '.job-location__text', '.vacancy-location'
    ];
    let location = null;
    for (const sel of locationSelectors) {
        const text = $(sel).text().trim();
        if (text && text.length > 1 && text.length < 100) {
            location = text;
            break;
        }
    }
    if (!location) {
        const bodyText = $('body').text();
        const match = bodyText.match(/(?:Ort|Standort|Location):\s*([^\n\r]+)/i);
        if (match) location = match[1].trim();
    }

    return { title, description, location };
}

// ─── PROCESS ONE CONCLUDIS COMPANY ────────────────────────────────────
async function processConcludisCompany(company) {
    console.log(`\n🔍 Processing: ${company.Name}`);
    console.log(`   URL: ${company.detected_career_url}`);

    let concludisDomain = null;
    let companySlug = null;

    // ─── Detect slug from URL ──────────────────────────────────────────
    if (company.detected_career_url && company.detected_career_url.includes('concludis.de')) {
        let match = company.detected_career_url.match(/https?:\/\/([^.]+)\.concludis\.de/);
        if (match && match[1] !== 'www') {
            companySlug = match[1];
            concludisDomain = `${companySlug}.concludis.de`;
        } else {
            match = company.detected_career_url.match(/concludis\.de\/companies\/([^\/?]+)/);
            if (match) {
                companySlug = match[1];
                concludisDomain = `www.concludis.de/companies/${companySlug}`;
            }
        }
    }

    if (!companySlug) {
        try {
            const response = await axios.get(company.detected_career_url, {
                timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
                headers: { 'User-Agent': 'Mozilla/5.0' }
            });
            const html = response.data;
            const $ = cheerio.load(html);
            let foundUrl = null;
            $('iframe[src*="concludis.de"], a[href*="concludis.de"], script[src*="concludis.de"]').each((_, el) => {
                const src = $(el).attr('src') || $(el).attr('href') || '';
                if (src.includes('concludis.de')) {
                    foundUrl = src;
                }
            });
            if (foundUrl) {
                let match = foundUrl.match(/https?:\/\/([^.]+)\.concludis\.de/);
                if (match && match[1] !== 'www') {
                    companySlug = match[1];
                    concludisDomain = `${companySlug}.concludis.de`;
                } else {
                    match = foundUrl.match(/concludis\.de\/companies\/([^\/?]+)/);
                    if (match) {
                        companySlug = match[1];
                        concludisDomain = `www.concludis.de/companies/${companySlug}`;
                    }
                }
            }
        } catch (err) {
            console.log(`   ⚠️ Could not fetch page: ${err.message}`);
        }
    }

    if (!companySlug) {
        let candidate = company.Name
            .toLowerCase()
            .replace(/[^a-z0-9]/g, '')
            .replace(/gmbh|ag|kg|co|e\.k\./g, '')
            .trim();
        if (candidate.length > 3) {
            companySlug = candidate;
            concludisDomain = `${companySlug}.concludis.de`;
            console.log(`   ⚠️ Using guessed slug: ${companySlug}`);
        } else {
            console.log(`   ❌ Could not find Concludis slug for ${company.Name}`);
            return { company, jobs: [], error: 'No slug found' };
        }
    }

    console.log(`   ✅ Slug: ${companySlug}`);
    console.log(`   ✅ Concludis Domain: ${concludisDomain}`);

    const jobs = [];

    // ─── LAYER 1: API ──────────────────────────────────────────────────
    const apiEndpoints = [
        `https://${companySlug}.concludis.de/api/jobs`,
        `https://${companySlug}.concludis.de/api/v1/jobs`,
        `https://api.concludis.de/v1/companies/${companySlug}/jobs`,
        `https://www.concludis.de/api/companies/${companySlug}/jobs`,
        `https://recruiting.concludis.de/${companySlug}/api/jobs`,
        `https://${companySlug}.concludis.de/xml/jobs`
    ];

    for (const apiUrl of apiEndpoints) {
        try {
            const response = await axios.get(apiUrl, {
                timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
                headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0' }
            });
            const data = response.data;
            let items = data.jobs || data.data || data;
            if (!Array.isArray(items)) {
                if (apiUrl.includes('.xml')) {
                    const $xml = cheerio.load(data, { xmlMode: true });
                    const positions = $xml('job');
                    if (positions.length > 0) {
                        positions.each((_, el) => {
                            const getId = (tag) => {
                                const val = $xml(`${tag}`, el).text().trim();
                                return val || null;
                            };
                            const id = getId('id') || String(Math.random());
                            const title = getId('title') || getId('name') || 'Untitled';
                            const location = getId('location') || getId('office') || null;
                            const description = getId('description') || getId('jobDescription') || '';
                            jobs.push({
                                external_job_id: id,
                                title: title,
                                location: location,
                                employment_type: null,
                                remote_type: detectRemoteType(description),
                                raw_description: description.slice(0, 5000),
                                apply_url: `https://${companySlug}.concludis.de/jobs/${id}`,
                                ats_source: 'concludis'
                            });
                        });
                        console.log(`   ✅ Layer 1 (API): Found ${jobs.length} jobs via XML (${apiUrl})`);
                        break;
                    }
                }
                items = [];
            }
            for (const item of items) {
                const description = (item.description || item.jobDescription || '');
                const location = item.location || item.office || item.city || null;
                jobs.push({
                    external_job_id: String(item.id || item.jobId || Math.random()),
                    title: item.title || item.name || item.jobTitle || 'Untitled',
                    location: location,
                    employment_type: item.employmentType || item.schedule || null,
                    remote_type: detectRemoteType(description),
                    raw_description: description.slice(0, 5000),
                    apply_url: `https://${companySlug}.concludis.de/jobs/${item.id || item.jobId}`,
                    ats_source: 'concludis'
                });
            }
            if (jobs.length > 0) {
                console.log(`   ✅ Layer 1 (API): Found ${jobs.length} jobs from ${apiUrl}`);
                break;
            }
        } catch (err) {
            // ignore and try next endpoint
        }
    }

    // ─── LAYER 2: HTML Scraping on Concludis domain ──────────────────
    if (jobs.length === 0) {
        console.log(`   🔄 Layer 2: HTML scraping (Concludis domain)...`);
        try {
            const pageUrl = concludisDomain.startsWith('http') ? concludisDomain : `https://${concludisDomain}`;
            const response = await axios.get(pageUrl, {
                timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
                headers: { 'User-Agent': 'Mozilla/5.0' }
            });
            const html = response.data;
            const $ = cheerio.load(html);
            const jobSelectors = [
                '.job', '.job-item', '.job-listing', '.job-card',
                '.position', '.position-item', '.vacancy', '.vacancy-item',
                '[data-job-id]', '[data-position-id]', '.job-offer',
                '.concludis-job', '.concludis-position',
                'article.job', 'div.job', 'li.job'
            ];
            const selector = jobSelectors.join(', ');
            const jobElements = $(selector);
            if (jobElements.length > 0) {
                jobElements.each((_, el) => {
                    const title = $(el).find('.title, .job-title, h2, h3, .job-name, .position-title').first().text().trim() || 'Untitled';
                    const link = $(el).find('a').first().attr('href') || '';
                    const location = $(el).find('.location, .office, .city, .job-location').first().text().trim() || null;
                    const description = $(el).find('.description, .job-description, .job-text, .position-description').first().text().trim() || '';
                    const id = $(el).attr('data-job-id') || $(el).attr('data-id') || $(el).attr('data-position-id') || String(Math.random());
                    let fullUrl = link;
                    if (link && !link.startsWith('http')) {
                        try {
                            fullUrl = new URL(link, pageUrl).href;
                        } catch (e) { fullUrl = `https://${companySlug}.concludis.de/jobs/${id}`; }
                    } else if (!link) {
                        fullUrl = `https://${companySlug}.concludis.de/jobs/${id}`;
                    }
                    jobs.push({
                        external_job_id: id,
                        title: title,
                        location: location,
                        employment_type: null,
                        remote_type: detectRemoteType(description),
                        raw_description: description.slice(0, 5000),
                        apply_url: fullUrl,
                        ats_source: 'concludis'
                    });
                });
                console.log(`   ✅ Layer 2: Found ${jobs.length} jobs via HTML scraping`);
            } else {
                console.log(`   ⚠️ Layer 2: No job listings found on Concludis page`);
            }
        } catch (err) {
            console.log(`   ❌ Layer 2 failed: ${err.message}`);
        }
    }

    // ─── LAYER 3: CUSTOM CRAWLER FALLBACK ──────────────────────────────
    if (jobs.length === 0) {
        console.log(`   🔄 Layer 3: Custom crawler fallback (original career page)...`);
        const html = await customCrawlerFetchPage(company.detected_career_url);
        if (html) {
            const links = customCrawlerExtractLinks(html, company.detected_career_url);
            let saved = 0;
            for (const link of links) {
                if (jobs.some(j => j.apply_url === link)) continue;
                const jobData = await customCrawlerScrapeJob(link);
                if (jobData) {
                    const id = Buffer.from(link).toString('base64').slice(0, 50);
                    jobs.push({
                        external_job_id: id,
                        title: jobData.title,
                        location: jobData.location,
                        employment_type: null,
                        remote_type: detectRemoteType(jobData.description),
                        raw_description: jobData.description.slice(0, 5000),
                        apply_url: link,
                        ats_source: 'concludis'
                    });
                    saved++;
                }
            }
            if (saved > 0) {
                console.log(`   ✅ Layer 3: Found ${saved} jobs via custom crawler`);
            } else {
                console.log(`   ⚠️ Layer 3: No jobs found`);
            }
        } else {
            console.log(`   ❌ Layer 3: Failed to fetch page`);
        }
    }

    // ─── Enrich jobs with company_name ────────────────────────────────
    const enrichedJobs = jobs.map(job => ({
        ...job,
        company_name: company.Name,
        external_hash: generateExternalHash(company.Id, job.external_job_id) || job.external_job_id
    }));

    return { company, jobs: enrichedJobs, error: jobs.length === 0 ? 'No jobs found' : null };
}

// ─── MAIN RUNNER ──────────────────────────────────────────────────────────
async function run() {
    const { data: companies, error } = await supabase
        .from('companies')
        .select('"Id", "Name", "Website", detected_career_url')
        .eq('ats_type', 'concludis')
        .eq('crawl_status', 'pending');

    if (error) {
        console.error('❌ Supabase error:', error.message);
        return;
    }

    if (!companies || companies.length === 0) {
        console.log('No Concludis companies found.');
        return;
    }

    console.log(`📋 Processing ${companies.length} Concludis companies...\n`);

    const { jobs: totalJobs } = await runCompaniesInBatches(companies, {
        batchSize: parseInt(process.env.CRAWLER_COMPANY_BATCH_SIZE || '10', 10),
        label: 'CONCLUDIS',
        handler: async (company, meta) => {
            try {
                const result = await processConcludisCompany(company);
                if (result.jobs.length === 0) {
                    console.log(`   ⚠️ [${meta.companyIndex}/${meta.companyTotal}] ${company.Name} | no jobs found`);
                    await supabase.from('companies')
                        .update({ crawl_status: 'failed' })
                        .eq('Id', company.Id);
                    return { status: 'no_jobs', jobs: [] };
                }

                for (const job of result.jobs) {
                    const storageJob = await enrichJobForStorage({
                        company_id: company.Id,
                        company_name: job.company_name || company.Name || null,
                        company_website: company.Website || null,
                        external_job_id: job.external_job_id,
                        external_hash: job.external_hash,
                        title: job.title,
                        location: job.location,
                        employment_type: job.employment_type,
                        remote_type: job.remote_type,
                        raw_description: job.raw_description,
                        apply_url: job.apply_url,
                        ats_source: 'concludis',
                        is_active: true
                    });

                    const { error: insertError } = await supabase
                        .from('jobs')
                        .upsert({
                            ...storageJob,
                            first_seen_at: new Date(),
                            last_seen_at: new Date()
                        }, { onConflict: 'company_id,external_job_id' });

                    if (insertError) {
                        console.error(`   ❌ Save error for job ${job.title}: ${insertError.message}`);
                    }
                }
                console.log(`   💾 [${meta.companyIndex}/${meta.companyTotal}] ${company.Name} | saved=${result.jobs.length}`);
                await supabase.from('companies')
                    .update({ crawl_status: 'completed' })
                    .eq('Id', company.Id);
                return { status: 'completed', jobs: result.jobs };
            } catch (err) {
                console.error(`   ⚠️ [${meta.companyIndex}/${meta.companyTotal}] ${company.Name} failed: ${err.message}`);
                await supabase.from('companies')
                    .update({ crawl_status: 'failed' })
                    .eq('Id', company.Id);
                return { status: 'failed', jobs: [] };
            }
        }
    });

    if (browserInstance && browserInstance.isConnected()) {
        await browserInstance.close();
    }

    console.log(`\n✅ Done! Total Concludis jobs saved: ${totalJobs}`);
}

run().catch(console.error);
