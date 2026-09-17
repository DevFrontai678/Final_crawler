require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const { chromium } = require('playwright');
const axios = require('axios');
const cheerio = require('cheerio');
const crypto = require('crypto');
const { CRAWLER_TIMEOUTS } = require('../src/utils/crawler-timeouts');
const { enrichJobForStorage } = require('../src/utils/job-enrichment');

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

// ─── CONFIG ─────────────────────────────────────────────────────────────────
const CONFIG = {
    PLAYWRIGHT_TIMEOUT: CRAWLER_TIMEOUTS.NAVIGATION_TIMEOUT_MS,
    PLAYWRIGHT_RETRIES: 1,
    AXIOS_TIMEOUT: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
    JOB_SCRAPE_CONCURRENCY: 5,
    JOB_LINK_LIMIT: 60,
    MIN_DESCRIPTION_LENGTH: 50,
    DELAY_BETWEEN_COMPANIES: 500
};

// ─── Remote Type ─────────────────────────────────────────────────────────────
function detectRemoteType(description) {
    const text = (description || '').toLowerCase();
    if (text.includes('remote') || text.includes('homeoffice') || text.includes('100% remote') || text.includes('full remote')) return 'remote';
    if (text.includes('hybrid') || text.includes('teilweise remote') || text.includes('mobile work') || text.includes('flexibles arbeiten')) return 'hybrid';
    return 'onsite';
}

// ─── Skip binary files ───────────────────────────────────────────────────────
const SKIP_EXTENSIONS = ['.pdf','.docx','.xlsx','.zip','.rar','.ppt','.pptx','.csv','.png','.jpg','.jpeg','.gif','.svg'];
function isSkipFile(url) {
    if (!url) return true;
    const lower = url.toLowerCase();
    return SKIP_EXTENSIONS.some(ext => lower.endsWith(ext) || lower.includes(ext + '?'));
}

// ─── Axios fetch ─────────────────────────────────────────────────────────────
async function axiosFetchPage(url) {
    if (isSkipFile(url)) return null;
    try {
        const response = await axios.get(url, {
            timeout: CONFIG.AXIOS_TIMEOUT,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            },
            maxRedirects: 5
        });
        return response.data || null;
    } catch (_) { return null; }
}

// ─── Playwright fetch ─────────────────────────────────────────────────────────
async function playwrightFetchPage(url) {
    if (isSkipFile(url)) return null;
    let browser;
    for (let attempt = 0; attempt <= CONFIG.PLAYWRIGHT_RETRIES; attempt++) {
        try {
            browser = await chromium.launch({ headless: true });
            const page = await browser.newPage();
            await page.setExtraHTTPHeaders({ 'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8' });
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: CONFIG.PLAYWRIGHT_TIMEOUT });
            await page.waitForTimeout(2000);
            const html = await page.content();
            await browser.close();
            return html;
        } catch (err) {
            console.log(`   ⚠️ Playwright attempt ${attempt + 1} failed: ${err.message.split('\n')[0]}`);
            if (browser) { try { await browser.close(); } catch (_) {} }
            if (attempt < CONFIG.PLAYWRIGHT_RETRIES) await new Promise(r => setTimeout(r, 1000));
        }
    }
    return null;
}

// ─── Smart fetch: Axios first, Playwright fallback ──────────────────────────
async function smartFetchPage(url) {
    if (!url || isSkipFile(url)) return null;
    const html = await axiosFetchPage(url);
    if (html && html.length > 300) return html;
    return await playwrightFetchPage(url);
}

// ─── Concurrency helper ──────────────────────────────────────────────────────
async function runWithConcurrency(items, concurrency, fn) {
    const results = new Array(items.length).fill(null);
    let index = 0;
    async function worker() {
        while (true) {
            const i = index++;
            if (i >= items.length) break;
            try { results[i] = await fn(items[i], i); }
            catch (_) { results[i] = null; }
        }
    }
    await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, worker));
    return results;
}

// ─── Extract ALL job-related links from a page ──────────────────────────────
function extractJobLinks(html, baseUrl) {
    const $ = cheerio.load(html);
    const links = new Set();

    const jobKeywords = [
        'job', 'jobs', 'karriere', 'career', 'careers',
        'stelle', 'stellen', 'stellenangebote', 'offene-stellen',
        'stellenmarkt', 'vakanz', 'position', 'positionen',
        'ausbildung', 'praktikum', 'bewerbung', 'vacancies',
        'vacancy', 'entwickler', 'engineer', 'manager',
        'consultant', 'mitarbeiter', 'fachkraft', 'leitung'
    ];

    const jobDetailPatterns = [
        /\/jobs?\//i, /\/karriere\//i, /\/career\//i,
        /\/stelle[n]?\//i, /\/position\//i, /\/vacancy\//i,
        /\/bewerbung\//i, /[?&](job|position|id|vid)=\d/i,
        /\/\d{4,}/, /stellenangebote\//i, /offene-stellen\//i
    ];

    $('a[href]').each((_, el) => {
        const href = $(el).attr('href') || '';
        const text = $(el).text().toLowerCase().trim();

        if (!href || href.startsWith('#') || href.startsWith('javascript')
            || href.includes('mailto:') || href.includes('tel:')) return;
        if (isSkipFile(href)) return;

        let fullUrl;
        try {
            fullUrl = href.startsWith('http') ? href : new URL(href, baseUrl).href;
        } catch (_) { return; }

        fullUrl = fullUrl.split('#')[0];
        if (!fullUrl) return;

        const hrefLower = fullUrl.toLowerCase();
        const matched = jobKeywords.some(kw => hrefLower.includes(kw) || text.includes(kw))
            || jobDetailPatterns.some(p => p.test(fullUrl));

        if (matched) links.add(fullUrl);
    });

    const filtered = [...links].filter(href =>
        !/impressum|datenschutz|agb|cookie|login|register|logout|passwort|password|\/en\/|\/fr\/|sitemap/i.test(href)
    );

    return (filtered.length > 0 ? filtered : [...links]).slice(0, CONFIG.JOB_LINK_LIMIT);
}

// ─── Scrape a single job page ────────────────────────────────────────────────
async function scrapeJobPage(url) {
    if (isSkipFile(url)) return null;

    const html = await axiosFetchPage(url);
    if (!html || html.length < 200) return null;

    const $ = cheerio.load(html);
    $('nav, footer, script, style, .cookie-banner, #cookie, noscript').remove();

    const title = $('h1').first().text().trim()
        || $('title').text().trim().split(/[-|–]/)[0].trim()
        || 'Untitled';

    const descSelectors = [
        '[itemprop="description"]', '[itemprop="jobDescription"]',
        '.job-description', '.job-details', '#job-description',
        '[class*="job-description"]', '[class*="job-detail"]',
        '[class*="stellenanzeige"]', '[class*="stelle"]',
        '[class*="aufgaben"]', '[class*="anforderung"]',
        '[class*="profil"]', '[class*="anzeige"]',
        'article', 'main', '.main-content', '#main-content',
        '#content', '.content', '.text-content',
        '.post-content', '.entry-content', '.description'
    ];

    let description = '';
    for (const sel of descSelectors) {
        const el = $(sel).first();
        if (!el.length) continue;
        const text = el.text().replace(/\s+/g, ' ').trim();
        if (text.length > description.length) description = text;
        if (description.length > 500) break;
    }

    if (description.length < CONFIG.MIN_DESCRIPTION_LENGTH) {
        description = $('body').text()
            .split('\n').map(l => l.trim()).filter(l => l.length > 20)
            .filter(l => !/impressum|datenschutz|cookie|copyright|©|navigation|menu/i.test(l))
            .join('\n').slice(0, 5000);
    }

    if (description.length < CONFIG.MIN_DESCRIPTION_LENGTH) return null;

    const location = $(
        '.location, .office, .city, .job-location, [itemprop="jobLocation"], [class*="location"], [class*="ort"]'
    ).first().text().trim() || null;

    return { title, description: description.slice(0, 5000), location };
}

// ─── Deep crawl: career page + one level of sub-pages ───────────────────────
async function deepCrawlForJobLinks(startUrl) {
    const visited = new Set();
    let allJobLinks = new Set();

    const html = await smartFetchPage(startUrl);
    if (!html) return [];
    visited.add(startUrl);

    const firstLinks = extractJobLinks(html, startUrl);
    firstLinks.forEach(l => allJobLinks.add(l));

    const listingPagePatterns = [
        /\/(karriere|jobs?|careers?|stellen|stellenangebote|offene-stellen|vacancies?)\/?$/i,
        /\/(karriere|jobs?|careers?|stellen)\?/i
    ];

    const $ = cheerio.load(html);
    const subListingPages = new Set();

    $('a[href]').each((_, el) => {
        const href = $(el).attr('href') || '';
        let fullUrl;
        try { fullUrl = href.startsWith('http') ? href : new URL(href, startUrl).href; }
        catch (_) { return; }
        fullUrl = fullUrl.split('#')[0];

        if (visited.has(fullUrl)) return;
        if (isSkipFile(fullUrl)) return;

        try {
            const startDomain = new URL(startUrl).hostname;
            const linkDomain = new URL(fullUrl).hostname;
            if (startDomain !== linkDomain) return;
        } catch (_) { return; }

        if (listingPagePatterns.some(p => p.test(fullUrl))) {
            subListingPages.add(fullUrl);
        }
    });

    for (const subUrl of [...subListingPages].slice(0, 5)) {
        if (visited.has(subUrl)) continue;
        visited.add(subUrl);
        const subHtml = await axiosFetchPage(subUrl);
        if (!subHtml) continue;
        const subLinks = extractJobLinks(subHtml, subUrl);
        subLinks.forEach(l => allJobLinks.add(l));
    }

    return [...allJobLinks].slice(0, CONFIG.JOB_LINK_LIMIT);
}

// ─── Main company processor ──────────────────────────────────────────────────
async function processUmantisCompany(company) {
    console.log(`\n🔍 [${company.Name}]`);
    console.log(`   URL: ${company.detected_career_url}`);

    let companySlug = null;
    let umantisDomain = null;
    const url = company.detected_career_url || '';

    let m;

    m = url.match(/https?:\/\/([^./]+)\.umantis\.com/);
    if (m && !['www','recruiting'].includes(m[1])) {
        companySlug = m[1];
        umantisDomain = `${companySlug}.umantis.com`;
    }

    if (!companySlug) {
        m = url.match(/umantis\.com\/([^\/?#\s]+)/);
        if (m && m[1] !== 'api') {
            companySlug = m[1];
            umantisDomain = `recruiting.umantis.com/${companySlug}`;
        }
    }

    if (!companySlug) {
        m = url.match(/https?:\/\/([^./]+)\.lumesse\.com/);
        if (m && m[1] !== 'www') { companySlug = m[1]; umantisDomain = `${companySlug}.lumesse.com`; }
    }

    if (!companySlug) {
        const html = await axiosFetchPage(url);
        if (html) {
            const $ = cheerio.load(html);
            let found = null;
            $('iframe, a, script, [src], [data-src]').each((_, el) => {
                const src = $(el).attr('src') || $(el).attr('href') || $(el).attr('data-src') || '';
                if ((src.includes('umantis.com') || src.includes('lumesse.com')) && !found) found = src;
            });
            const searchIn = found || (typeof html === 'string' ? html : '');

            m = searchIn.match(/https?:\/\/([^./]+)\.umantis\.com/);
            if (m && !['www','recruiting'].includes(m[1])) { companySlug = m[1]; umantisDomain = `${companySlug}.umantis.com`; }

            if (!companySlug) {
                m = searchIn.match(/umantis\.com\/([^\/?'"#\s]+)/);
                if (m && m[1] !== 'api') { companySlug = m[1]; umantisDomain = `recruiting.umantis.com/${companySlug}`; }
            }
            if (!companySlug) {
                m = searchIn.match(/https?:\/\/([^./]+)\.lumesse\.com/);
                if (m && m[1] !== 'www') { companySlug = m[1]; umantisDomain = `${companySlug}.lumesse.com`; }
            }
        }
    }

    if (!companySlug) {
        const guess = company.Name.toLowerCase()
            .replace(/[äöü]/g, c => ({ ä:'ae', ö:'oe', ü:'ue' }[c] || c))
            .replace(/[^a-z0-9]/g, '')
            .replace(/gmbh|ag|kg|co|ek|inc|ltd/g, '').trim();
        if (guess.length > 3) {
            companySlug = guess;
            umantisDomain = `${companySlug}.umantis.com`;
            console.log(`   ⚠️ Guessed slug: ${companySlug}`);
        } else {
            console.log(`   ❌ Cannot determine slug for ${company.Name}`);
            return { company, jobs: [], error: 'No slug' };
        }
    }

    console.log(`   ✅ Slug: ${companySlug} | Domain: ${umantisDomain}`);

    const jobs = [];

    // ─── LAYER 1: Umantis API ──────────────────────────────────────────────
    const apiUrls = [
        `https://${companySlug}.umantis.com/api/jobs`,
        `https://${companySlug}.umantis.com/api/v1/jobs`,
        `https://${companySlug}.umantis.com/rest/jobs`,
        `https://recruiting.umantis.com/${companySlug}/api/jobs`,
        `https://${companySlug}.lumesse.com/api/jobs`,
        `https://${companySlug}.umantis.com/xml/jobs`
    ];

    for (const apiUrl of apiUrls) {
        try {
            const resp = await axios.get(apiUrl, {
                timeout: CONFIG.AXIOS_TIMEOUT,
                headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0' }
            });
            const data = resp.data;
            let items = data.jobs || data.data || data;

            if (!Array.isArray(items)) {
                if (apiUrl.endsWith('.xml')) {
                    const $x = cheerio.load(data, { xmlMode: true });
                    $x('job').each((_, el) => {
                        const g = tag => $x(tag, el).text().trim() || null;
                        const id = g('id') || String(Math.random());
                        const desc = g('description') || g('jobDescription') || '';
                        jobs.push({
                            external_job_id: id, title: g('title') || g('name') || 'Untitled',
                            location: g('location') || g('office') || null, employment_type: null,
                            remote_type: detectRemoteType(desc), raw_description: desc.slice(0, 5000),
                            apply_url: `https://${companySlug}.umantis.com/jobs/${id}`, ats_source: 'umantis'
                        });
                    });
                    if (jobs.length > 0) { console.log(`   ✅ Layer 1: ${jobs.length} jobs (XML)`); break; }
                }
                items = [];
            }
            for (const item of items) {
                const desc = item.description || item.jobDescription || '';
                jobs.push({
                    external_job_id: String(item.id || item.jobId || Math.random()),
                    title: item.title || item.name || item.jobTitle || 'Untitled',
                    location: item.location || item.office || item.city || null,
                    employment_type: item.employmentType || item.schedule || null,
                    remote_type: detectRemoteType(desc), raw_description: desc.slice(0, 5000),
                    apply_url: `https://${companySlug}.umantis.com/jobs/${item.id || item.jobId}`,
                    ats_source: 'umantis'
                });
            }
            if (jobs.length > 0) { console.log(`   ✅ Layer 1: ${jobs.length} jobs (API)`); break; }
        } catch (_) {}
    }

    // ─── LAYER 2: Umantis domain HTML scraping ──────────────────────────────
    if (jobs.length === 0) {
        const isSubdomain = !umantisDomain.startsWith('recruiting.umantis.com');
        const pageUrl = umantisDomain.startsWith('http') ? umantisDomain : `https://${umantisDomain}`;

        if (isSubdomain) {
            console.log(`   🔄 Layer 2: HTML scraping ${pageUrl}...`);
            let html = await axiosFetchPage(pageUrl);
            if (!html || html.length < 500) {
                console.log(`   🔄 Layer 2: Axios insufficient, trying Playwright...`);
                html = await playwrightFetchPage(pageUrl);
            }

            if (html) {
                const $ = cheerio.load(html);
                const jobEls = $('.job, .position, .job-item, .job-listing, [data-job-id], .job-card, .job-posting, .vacancy, .job-offer');

                if (jobEls.length > 0) {
                    jobEls.each((_, el) => {
                        const title = $(el).find('.title, .job-title, h2, h3, .job-name, .vacancy-title').first().text().trim() || 'Untitled';
                        const link  = $(el).find('a').first().attr('href') || '';
                        const loc   = $(el).find('.location, .office, .city, .job-location').first().text().trim() || null;
                        const desc  = $(el).find('.description, .job-description, .job-text').first().text().trim() || '';
                        const id    = $(el).attr('data-job-id') || $(el).attr('data-id') || String(Math.random());

                        let fullUrl = link;
                        if (link && !link.startsWith('http')) {
                            try { fullUrl = new URL(link, pageUrl).href; }
                            catch (_) { fullUrl = `https://${companySlug}.umantis.com/jobs/${id}`; }
                        } else if (!link) { fullUrl = `https://${companySlug}.umantis.com/jobs/${id}`; }

                        jobs.push({ external_job_id: id, title, location: loc, employment_type: null,
                            remote_type: detectRemoteType(desc), raw_description: desc.slice(0, 5000),
                            apply_url: fullUrl, ats_source: 'umantis' });
                    });
                    console.log(`   ✅ Layer 2: ${jobs.length} jobs (HTML elements)`);
                } else {
                    $('script').each((_, el) => {
                        if (jobs.length > 0) return;
                        const content = $(el).html() || '';
                        if (!content.includes('jobs') || !content.includes('"id"')) return;
                        try {
                            const match = content.match(/\{[\s\S]*?"jobs"[\s\S]*?\}/);
                            if (match) {
                                const parsed = JSON.parse(match[0]);
                                const items = parsed.jobs || parsed.data || [];
                                if (Array.isArray(items)) {
                                    for (const item of items) {
                                        const desc = item.description || '';
                                        jobs.push({
                                            external_job_id: String(item.id || Math.random()),
                                            title: item.title || 'Untitled', location: item.location || null,
                                            employment_type: null, remote_type: detectRemoteType(desc),
                                            raw_description: desc.slice(0, 5000),
                                            apply_url: `https://${companySlug}.umantis.com/jobs/${item.id}`,
                                            ats_source: 'umantis'
                                        });
                                    }
                                }
                            }
                        } catch (_) {}
                    });
                    if (jobs.length > 0) console.log(`   ✅ Layer 2: ${jobs.length} jobs (script JSON)`);
                    else console.log(`   ⚠️ Layer 2: No job listings found`);
                }
            } else {
                console.log(`   ❌ Layer 2: Could not fetch page`);
            }
        } else {
            console.log(`   ⏭️ Layer 2: Skipping recruiting.umantis.com path (unreliable DNS)`);
        }
    }

    // ─── LAYER 3: Deep crawl company career page ──────────────────────────────
    if (jobs.length === 0) {
        console.log(`   🔄 Layer 3: Deep crawl...`);

        const allLinks = await deepCrawlForJobLinks(company.detected_career_url);
        console.log(`   📎 Found ${allLinks.length} candidate links`);

        if (allLinks.length === 0) {
            console.log(`   ⚠️ Layer 3: No job links found`);
            return { company, jobs: [], error: 'No jobs found' };
        }

        const scraped = await runWithConcurrency(allLinks, CONFIG.JOB_SCRAPE_CONCURRENCY, async (link) => {
            const data = await scrapeJobPage(link);
            if (!data) return null;
            return { link, data };
        });

        const seenUrls = new Set();
        for (const result of scraped) {
            if (!result) continue;
            const { link, data } = result;
            if (seenUrls.has(link)) continue;
            seenUrls.add(link);

            const id = Buffer.from(link).toString('base64').slice(0, 50);
            jobs.push({
                external_job_id: id, title: data.title, location: data.location,
                employment_type: null, remote_type: detectRemoteType(data.description),
                raw_description: data.description.slice(0, 5000),
                apply_url: link, ats_source: 'umantis'
            });
        }

        if (jobs.length > 0) console.log(`   ✅ Layer 3: ${jobs.length} jobs scraped`);
        else console.log(`   ⚠️ Layer 3: No jobs found after scraping`);
    }

    // ─── Enrich jobs with company_name and external_hash ────────────────
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
        .select('"Id", "Name", detected_career_url')
        .eq('ats_type', 'umantis')
        .eq('crawl_status', 'pending');

    if (error) {
        console.error('❌ Supabase error:', error.message);
        return;
    }

    if (!companies || companies.length === 0) {
        console.log('No Umantis companies found.');
        return;
    }

    console.log(`📋 Processing ${companies.length} Umantis companies sequentially...\n`);

    let totalJobs = 0;
    for (const company of companies) {
        try {
            const result = await processUmantisCompany(company);
            if (result.jobs.length === 0) {
                console.log(`   ⚠️ No jobs found for ${company.Name}`);
                await supabase.from('companies')
                    .update({ crawl_status: 'failed' })
                    .eq('Id', company.Id);
                continue;
            }

            for (const job of result.jobs) {
                const storageJob = await enrichJobForStorage({
                    company_id: company.Id,
                    company_name: job.company_name || company.Name || null,
                    external_job_id: job.external_job_id,
                    external_hash: job.external_hash,
                    title: job.title,
                    location: job.location,
                    employment_type: job.employment_type,
                    remote_type: job.remote_type,
                    raw_description: job.raw_description,
                    apply_url: job.apply_url,
                    ats_source: 'umantis',
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
            totalJobs += result.jobs.length;
            console.log(`   💾 Saved ${result.jobs.length} jobs for ${company.Name}`);
            await supabase.from('companies')
                .update({ crawl_status: 'completed' })
                .eq('Id', company.Id);
        } catch (err) {
            console.error(`   ⚠️ Skipping ${company.Name}: ${err.message}`);
            await supabase.from('companies')
                .update({ crawl_status: 'failed' })
                .eq('Id', company.Id);
        }
        await new Promise(r => setTimeout(r, CONFIG.DELAY_BETWEEN_COMPANIES));
    }

    console.log(`\n✅ Done! Total Umantis jobs saved: ${totalJobs}`);
}

run().catch(console.error);
