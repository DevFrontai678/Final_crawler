const axios = require('axios');
const cheerio = require('cheerio');
const pdfParse = require('pdf-parse');
require('dotenv').config();
const { CRAWLER_TIMEOUTS } = require('../utils/crawler-timeouts');

// ─── PDF HELPERS ──────────────────────────────────────────────────────────
async function downloadAndParsePDF(pdfUrl) {
    try {
        const response = await axios.get(pdfUrl, {
            responseType: 'arraybuffer',
            timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
            headers: { 'User-Agent': 'Mozilla/5.0' }
        });
        const pdfBuffer = Buffer.from(response.data);
        const data = await pdfParse(pdfBuffer);
        return data.text || '';
    } catch (err) {
        console.log(`   PDF download/parse error: ${err.message}`);
        return null;
    }
}

function isPdfUrl(url) {
    return url.toLowerCase().endsWith('.pdf') || url.includes('.pdf?') || url.includes('.pdf#');
}

// ─── EXTRACT LOCATION / COMPANY / DESCRIPTION (HTML) ────────────────────
function extractLocationFromHTML(html) {
    const $ = cheerio.load(html);
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
    const metaLocation = $('meta[property="og:location"]').attr('content') ||
                         $('meta[name="geo.placename"]').attr('content') ||
                         $('meta[name="location"]').attr('content');
    if (metaLocation) return metaLocation.trim();
    const selectors = [
        '.job-location', '.location', '.office', '.workplace',
        '[class*="location"]', '[class*="office"]', '[itemprop="jobLocation"]',
        '[class*="ort"]', '[class*="stadt"]'
    ];
    for (const selector of selectors) {
        const text = $(selector).first().text().trim();
        if (text) return text;
    }
    const bodyText = $('body').text();
    const locationMatch = bodyText.match(/(?:Ort|Standort|Arbeitsort|Joblocation)[:\s]+([^\n,]{3,60})/i);
    if (locationMatch) return locationMatch[1].trim();
    return null;
}

function extractCompanyNameFromHTML(html, fallbackName) {
    const $ = cheerio.load(html);
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
    const metaCompany = $('meta[property="og:site_name"]').attr('content') ||
                        $('meta[name="application-name"]').attr('content') ||
                        $('meta[name="company"]').attr('content');
    if (metaCompany) return metaCompany.trim();
    const selectors = [
        '.company-name', '.employer', '.hiring-company',
        '[itemprop="hiringOrganization"]', '[class*="company"]',
        '.job-company', '.organization-name'
    ];
    for (const selector of selectors) {
        const text = $(selector).first().text().trim();
        if (text && text.length > 1) return text;
    }
    const bodyText = $('body').text();
    const companyMatch = bodyText.match(/(?:Arbeitgeber|Unternehmen|Firma|Company)[:\s]+([^\n,]{2,60})/i);
    if (companyMatch) return companyMatch[1].trim();
    return fallbackName || null;
}

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

// ─── EXTRACT LOCATION / COMPANY (from job object) ──────────────────────
function extractLocationFromJob(job) {
    const location = job.location || job.jobLocation || job.place || job.city || null;
    if (location) {
        if (typeof location === 'string') return location;
        if (location.address) {
            return location.address.addressLocality ||
                   location.address.streetAddress ||
                   location.address.addressRegion || null;
        }
        return location.name || null;
    }
    if (job.jobLocation && job.jobLocation.address) {
        return job.jobLocation.address.addressLocality ||
               job.jobLocation.address.streetAddress ||
               job.jobLocation.address.addressRegion || null;
    }
    return null;
}

function extractCompanyNameFromJob(job, fallback) {
    const company = job.hiringOrganization || job.company || job.employer || null;
    if (company) {
        if (typeof company === 'string') return company;
        if (company.name) return company.name;
    }
    if (job.companyName) return job.companyName;
    return fallback || null;
}

// ─── AGGRESSIVE CAREER PAGE DISCOVERY ────────────────────────────────────
async function fetchHtmlForDiscovery(url) {
    try {
        const response = await axios.get(url, { timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS, headers: { 'User-Agent': 'Mozilla/5.0' }, maxRedirects: 5 });
        return response.data;
    } catch (err) {
        return null;
    }
}

async function discoverCareerPage(baseUrl) {
    const careerKeywords = ['karriere', 'jobs', 'career', 'stellenangebote', 'offene-stellen', 'vacancies'];
    const base = new URL(baseUrl).origin;

    const commonPaths = [
        '/karriere', '/jobs', '/careers', '/stellenangebote', '/offene-stellen',
        '/en/careers', '/de/karriere', '/about/careers', '/company/careers',
        '/job-angebote', '/vakanz', '/stellen', '/vacancies'
    ];
    for (const path of commonPaths) {
        const testUrl = base + path;
        const html = await fetchHtmlForDiscovery(testUrl);
        if (html && html.length > 500) {
            const $ = cheerio.load(html);
            const hasJobContent = $('a[href*="job"]').length > 0 || $('a[href*="stelle"]').length > 0 ||
                html.includes('stellenangebote') || html.includes('offene stellen');
            if (hasJobContent) {
                console.log(`   [Softgarden] Found career page at: ${testUrl}`);
                return testUrl;
            }
        }
    }

    const html = await fetchHtmlForDiscovery(base);
    if (!html) return null;
    const $ = cheerio.load(html);
    const sections = ['nav', 'footer', 'header', '.navigation', '.main-nav', '.site-nav', '.menu', '.footer-links'];
    const links = [];
    for (const section of sections) {
        $(section + ' a').each((_, el) => {
            const href = $(el).attr('href');
            const text = $(el).text().toLowerCase();
            if (href && careerKeywords.some(k => href.toLowerCase().includes(k) || text.includes(k))) {
                links.push(href);
            }
        });
    }
    if (links.length === 0) {
        $('a').each((_, el) => {
            const href = $(el).attr('href');
            const text = $(el).text().toLowerCase();
            if (href && careerKeywords.some(k => href.toLowerCase().includes(k) || text.includes(k))) {
                links.push(href);
            }
        });
    }
    for (const link of links) {
        let fullUrl = link;
        if (!link.startsWith('http')) fullUrl = new URL(link, base).href;
        const testHtml = await fetchHtmlForDiscovery(fullUrl);
        if (testHtml && testHtml.length > 500) {
            const $test = cheerio.load(testHtml);
            const hasJobs = $test('a[href*="job"]').length > 0 || $test('a[href*="stelle"]').length > 0 ||
                testHtml.includes('stellenangebote') || testHtml.includes('offene stellen');
            if (hasJobs) {
                console.log(`   [Softgarden] Found career page via link: ${fullUrl}`);
                return fullUrl;
            }
        }
    }
    return null;
}

// ─── ORIGINAL FUNCTIONS (KEEP EVERYTHING) ───────────────────────────────

// 1. parseConfigFromHtml
function parseConfigFromHtml(html) {
    const $ = cheerio.load(html);
    const uMatch = html.match(/"userId"\s*:\s*"([a-f0-9-]{36})"/);
    const pMatch = html.match(/"projectId"\s*:\s*"([a-f0-9-]{36})"/);
    const pgMatch = html.match(/"pageId"\s*:\s*"([a-f0-9-]{36})"/);
    if (uMatch && pMatch) {
        console.log(`    ✅ Method 1: JSON pattern`);
        return { userId: uMatch[1], projectId: pMatch[1], pageId: pgMatch ? pgMatch[1] : null };
    }
    const jsU = html.match(/userId['":\s]+['"]([a-f0-9-]{36})['"]/);
    const jsP = html.match(/projectId['":\s]+['"]([a-f0-9-]{36})['"]/);
    const jsPg = html.match(/pageId['":\s]+['"]([a-f0-9-]{36})['"]/);
    if (jsU && jsP) {
        console.log(`    ✅ Method 2: JS variable`);
        return { userId: jsU[1], projectId: jsP[1], pageId: jsPg ? jsPg[1] : null };
    }
    let scriptConfig = null;
    $('script').each((_, el) => {
        const content = $(el).html() || '';
        if (!content.includes('userId')) return;
        const su = content.match(/"userId"\s*:\s*"([a-f0-9-]{36})"/);
        const sp = content.match(/"projectId"\s*:\s*"([a-f0-9-]{36})"/);
        const spg = content.match(/"pageId"\s*:\s*"([a-f0-9-]{36})"/);
        if (su && sp) {
            scriptConfig = { userId: su[1], projectId: sp[1], pageId: spg ? spg[1] : null };
        }
    });
    if (scriptConfig) {
        console.log(`    ✅ Method 3: Script tag`);
        return scriptConfig;
    }
    const cronMatch = html.match(/cron\?apiKey=[^&"']+&userId=([a-f0-9-]{36})(?:[^"']*?)projectId=([a-f0-9-]{36})/);
    if (cronMatch) {
        console.log(`    ✅ Method 4: Cron URL`);
        return { userId: cronMatch[1], projectId: cronMatch[2], pageId: null };
    }
    return null;
}

// 2. Global browser
let globalBrowser = null;

async function getBrowser() {
    if (!globalBrowser || !globalBrowser.isConnected()) {
        const { chromium } = require('playwright');
        console.log(`    🔄 Launching Softgarden browser instance...`);
        globalBrowser = await chromium.launch({ headless: true });
    }
    return globalBrowser;
}

async function closeSoftgardenBrowser() {
    if (globalBrowser) {
        await globalBrowser.close().catch(() => {});
        globalBrowser = null;
    }
}

// 3. extractConfigWithPlaywright
async function extractConfigWithPlaywright(url) {
    let context = null, page = null;
    try {
        console.log(`    Playwright launching...`);
        const browser = await getBrowser();
        context = await browser.newContext({
            userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            locale: 'de-DE',
        });
        page = await context.newPage();
        let capturedConfig = null;
        page.on('request', request => {
            const reqUrl = request.url();
            const postData = request.postData();
            if (reqUrl.includes('job-list/job-ads') && postData) {
                try {
                    const body = JSON.parse(postData);
                    if (body.userId && body.projectId) {
                        console.log(`    ✅ Captured from network!`);
                        capturedConfig = {
                            userId: body.userId,
                            projectId: body.projectId,
                            pageId: body.pageId || null
                        };
                    }
                } catch (e) {}
            }
        });
        await page.goto(url, { waitUntil: 'networkidle', timeout: CRAWLER_TIMEOUTS.NAVIGATION_TIMEOUT_MS });
        // Accept cookies
        try {
            const acceptBtn = await page.locator('button:has-text("Accept"), button:has-text("Zustimmen"), button:has-text("Alle akzeptieren")').first();
                if (await acceptBtn.isVisible({ timeout: CRAWLER_TIMEOUTS.VISIBILITY_TIMEOUT_MS })) {
                await acceptBtn.click();
                console.log('    🍪 Accepted cookies');
            }
        } catch (e) {}
        await page.waitForTimeout(CRAWLER_TIMEOUTS.WAIT_TIMEOUT_MS);
        if (capturedConfig) return capturedConfig;
        const html = await page.content();
        const htmlConfig = parseConfigFromHtml(html);
        if (htmlConfig) {
            console.log(`    ✅ Found in rendered HTML!`);
            return htmlConfig;
        }
        const jsConfig = await page.evaluate(() => {
            const result = {};
            if (window.__NEXT_DATA__) {
                const data = JSON.stringify(window.__NEXT_DATA__);
                const uMatch = data.match(/"userId":"([a-f0-9-]{36})"/);
                const pMatch = data.match(/"projectId":"([a-f0-9-]{36})"/);
                const pgMatch = data.match(/"pageId":"([a-f0-9-]{36})"/);
                if (uMatch) result.userId = uMatch[1];
                if (pMatch) result.projectId = pMatch[1];
                if (pgMatch) result.pageId = pgMatch[1];
            }
            if (window.userId) result.userId = window.userId;
            if (window.projectId) result.projectId = window.projectId;
            if (window.pageId) result.pageId = window.pageId;
            if (window.__store__) {
                const state = JSON.stringify(window.__store__.getState());
                const uMatch = state.match(/"userId":"([a-f0-9-]{36})"/);
                const pMatch = state.match(/"projectId":"([a-f0-9-]{36})"/);
                if (uMatch) result.userId = uMatch[1];
                if (pMatch) result.projectId = pMatch[1];
            }
            return result.userId && result.projectId ? result : null;
        });
        if (jsConfig) {
            console.log(`    ✅ Found in JS window!`);
            return jsConfig;
        }
        console.log(`    ❌ Playwright bhi fail hua`);
        return null;
    } catch (err) {
        console.log(`    Playwright error: ${err.message}`);
        return null;
    } finally {
        if (page) await page.close().catch(() => {});
        if (context) await context.close().catch(() => {});
    }
}

// 4. extractSoftgardenConfig
async function extractSoftgardenConfig(url) {
    try {
        console.log(`    Static fetch: ${url}`);
        const response = await axios.get(url, {
            timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
            headers: { 'User-Agent': 'Mozilla/5.0' },
            maxRedirects: 5
        });
        const config = parseConfigFromHtml(response.data);
        if (config) return config;
    } catch (err) {
        console.log(`    Static failed: ${err.message}`);
    }
    return await extractConfigWithPlaywright(url);
}

// 5. EXCLUDED_SUBDOMAINS
const EXCLUDED_SUBDOMAINS = ['certificate', 'datagroup', 'commerzdirektservice', 'hegemann-gruppe', 'next'];
function isExcludedSlug(slug) {
    return EXCLUDED_SUBDOMAINS.some(ex => slug.toLowerCase().includes(ex));
}

// 6. extractSoftgardenIds (ORIGINAL)
async function extractSoftgardenIds(careerPageUrl) {
    try {
        console.log(`    Extracting from: ${careerPageUrl}`);
        console.log(`    Trying direct page first...`);
        const directConfig = await extractSoftgardenConfig(careerPageUrl);
        if (directConfig) {
            console.log(`    ✅ Found directly on career page!`);
            return directConfig;
        }
        const response = await axios.get(careerPageUrl, {
            timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
            headers: { 'User-Agent': 'Mozilla/5.0' },
            maxRedirects: 5
        });
        const html = response.data;
        const $ = cheerio.load(html);
        const allLinks = [];
        $('a, iframe, script').each((_, el) => {
            const href = $(el).attr('href') || $(el).attr('src') || $(el).attr('data-src') || '';
            if (href) allLinks.push(href);
        });
        for (const link of allLinks) {
            const match = link.match(/https?:\/\/([^.]+)\.career\.softgarden\.de/);
            if (match && !isExcludedSlug(match[1])) {
                console.log(`    Found .de subdomain: ${match[1]}`);
                const config = await extractSoftgardenConfig(`https://${match[1]}.career.softgarden.de`);
                if (config) return config;
            } else if (match) {
                console.log(`    ⏭️ Skipping known junk subdomain: ${match[1]}`);
            }
        }
        for (const link of allLinks) {
            const match = link.match(/https?:\/\/([^./]+)\.softgarden\.io/);
            if (match && !isExcludedSlug(match[1])) {
                console.log(`    Found .io subdomain: ${match[1]}`);
                const config = await extractSoftgardenConfig(`https://${match[1]}.softgarden.io`);
                if (config) return config;
            } else if (match) {
                console.log(`    ⏭️ Skipping known junk subdomain: ${match[1]}`);
            }
        }
        console.log(`    ❌ No valid Softgarden config found`);
        return null;
    } catch (err) {
        console.log(`    Error: ${err.message}`);
        return null;
    }
}

// 7. fetchJobsFeedJson
async function fetchJobsFeedJson(baseUrl) {
    try {
        const feedUrl = `${baseUrl.replace(/\/$/, '')}/jobs.feed.json`;
        const response = await axios.get(feedUrl, {
            timeout: CRAWLER_TIMEOUTS.SELECTOR_TIMEOUT_MS,
            headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json' }
        });
        const data = response.data;
        let items = [];
        if (data.dataFeedElement) items = data.dataFeedElement;
        else if (data.jobs) items = data.jobs;
        else if (data.data) items = data.data;
        const map = new Map();
        for (const entry of items) {
            const job = entry.item || entry;
            if (!job) continue;
            const id = job.identifier?.value || job.id || job.jobId || job.jobPostingId;
            if (!id) continue;
            map.set(String(id), {
                title: job.title || job.jobTitle || null,
                raw_description: job.description || job.jobDescription || null,
                apply_url: job.url || job.applicationUrl || job.applyUrl || null,
                employment_type: job.employmentType || job.workTime || null,
                location: extractLocationFromJob(job),
                company_name: extractCompanyNameFromJob(job, null),
                datePosted: job.datePosted || job.validThrough || job.dateCreated || job.publicationDate || null
            });
        }
        if (map.size > 0) console.log(`    ✅ jobs.feed.json mila: ${map.size} jobs full data ke saath`);
        return map;
    } catch (err) {
        return new Map();
    }
}

// 8. discoverSoftgardenFeedMap
async function discoverSoftgardenFeedMap(careerPageUrl) {
    let feedMap = await fetchJobsFeedJson(careerPageUrl);
    if (feedMap.size > 0) return feedMap;
    try {
        const response = await axios.get(careerPageUrl, { timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS, headers: { 'User-Agent': 'Mozilla/5.0' } });
        const html = response.data;
        const $ = cheerio.load(html);
        const allLinks = [];
        $('a, iframe, script').each((_, el) => {
            const href = $(el).attr('href') || $(el).attr('src') || $(el).attr('data-src') || '';
            if (href) allLinks.push(href);
        });
        for (const link of allLinks) {
            const deMatch = link.match(/https?:\/\/([^.]+)\.career\.softgarden\.de/);
            if (deMatch && !isExcludedSlug(deMatch[1])) {
                const map = await fetchJobsFeedJson(`https://${deMatch[1]}.career.softgarden.de`);
                if (map.size > 0) return map;
            }
            const ioMatch = link.match(/https?:\/\/([^./]+)\.softgarden\.io/);
            if (ioMatch && !isExcludedSlug(ioMatch[1])) {
                const map = await fetchJobsFeedJson(`https://${ioMatch[1]}.softgarden.io`);
                if (map.size > 0) return map;
            }
        }
        return new Map();
    } catch (err) {
        return new Map();
    }
}

// 9. getSoftgardenSubdomain
async function getSoftgardenSubdomain(careerPageUrl) {
    if (!careerPageUrl) return null;
    let match = careerPageUrl.match(/https?:\/\/([^.]+)\.career\.softgarden\.(?:de|io)/);
    if (match && !isExcludedSlug(match[1])) return match[1];
    try {
        const response = await axios.get(careerPageUrl, { timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS, headers: { 'User-Agent': 'Mozilla/5.0' } });
        const html = response.data;
        const $ = cheerio.load(html);
        const links = [];
        $('a, iframe, script, link').each((_, el) => {
            const href = $(el).attr('href') || $(el).attr('src') || '';
            if (href) links.push(href);
        });
        for (const link of links) {
            const m = link.match(/https?:\/\/([^.]+)\.career\.softgarden\.(?:de|io)/);
            if (m && !isExcludedSlug(m[1])) return m[1];
            const m2 = link.match(/https?:\/\/([^.]+)\.softgarden\.io/);
            if (m2 && !isExcludedSlug(m2[1])) return m2[1];
        }
        const domainMatch = careerPageUrl.match(/https?:\/\/(?:www\.)?([^.]+)\./);
        if (domainMatch && domainMatch[1].toLowerCase() === 'she') return 'shejobs';
    } catch (err) { console.log(`   ⚠️ Subdomain scan failed: ${err.message}`); }
    return null;
}

// 10. genericFallbackCrawl (and helpers)
function extractJobLinksGeneric(html, baseUrl) {
    const $ = cheerio.load(html);
    const links = [];
    const jobKeywords = ['job','jobs','karriere','karrier','career','careers','stelle','stellen','stellenangebote','offene-stellen','vakanz','position','positionen','ausbildung','praktikum','bewerbung','vacancies','vacancy'];
    $('a').each((_, el) => {
        const href = $(el).attr('href');
        const text = $(el).text().toLowerCase().trim();
        if (!href || href.includes('#') || href.includes('mailto:') || href.includes('tel:')) return;
        const hrefMatches = jobKeywords.some(k => href.toLowerCase().includes(k));
        const textMatches = jobKeywords.some(k => text.includes(k));
        if (hrefMatches || textMatches) {
            let fullUrl = href;
            if (!href.startsWith('http')) {
                try { fullUrl = new URL(href, baseUrl).href; } catch { return; }
            }
            links.push(fullUrl);
        }
    });
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

async function fetchHtmlForFallback(url) {
    try {
        const response = await axios.get(url, { timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS, headers: { 'User-Agent': 'Mozilla/5.0' }, maxRedirects: 5 });
        return response.data;
    } catch (err) {
        // fallback to Playwright (reuse the browser)
        let context = null, page = null;
        try {
            const browser = await getBrowser();
            context = await browser.newContext();
            page = await context.newPage();
            await page.goto(url, { waitUntil: 'domcontentloaded', timeout: CRAWLER_TIMEOUTS.NAVIGATION_TIMEOUT_MS });
            try {
                const acceptBtn = await page.locator('button:has-text("Accept"), button:has-text("Zustimmen"), button:has-text("Alle akzeptieren")').first();
                if (await acceptBtn.isVisible({ timeout: CRAWLER_TIMEOUTS.VISIBILITY_TIMEOUT_MS })) await acceptBtn.click();
            } catch (e) {}
            await page.waitForTimeout(CRAWLER_TIMEOUTS.WAIT_TIMEOUT_MS);
            return await page.content();
        } catch (playwrightErr) {
            return null;
        } finally {
            if (page) await page.close().catch(() => {});
            if (context) await context.close().catch(() => {});
        }
    }
}

async function genericFallbackCrawl(careerPageUrl) {
    console.log(`    🔄 Last resort: generic job-link scraping...`);
    const html = await fetchHtmlForFallback(careerPageUrl);
    if (!html) {
        console.log(`    ❌ Fallback: could not fetch career page`);
        return [];
    }
    const jobLinks = extractJobLinksGeneric(html, careerPageUrl);
    console.log(`    Fallback found ${jobLinks.length} job detail links`);
    const jobs = [];
    for (const link of jobLinks) {
        const jobHtml = await fetchHtmlForFallback(link);
        if (!jobHtml) continue;
        const $ = cheerio.load(jobHtml);
        const title = $('title').text().trim() || 'Untitled Job';
        const description = extractDescriptionGeneric(jobHtml);
        if (!description || description.length < 100) continue;
        const location = extractLocationFromHTML(jobHtml);
        const companyName = extractCompanyNameFromHTML(jobHtml, null);
        const externalId = Buffer.from(link).toString('base64').slice(0, 50);
        jobs.push({
            external_job_id: externalId,
            title,
            raw_description: description.slice(0, 5000),
            apply_url: link,
            location: location,
            company_name: companyName,
            ats_source: 'custom_fallback'
        });
    }
    console.log(`    Fallback extracted ${jobs.length} jobs`);
    return jobs;
}

// ─── UPDATED fetchSoftgardenJobs ──────────────────────────────────────────
async function fetchSoftgardenJobs(userId, projectId, pageId, feedMap = new Map()) {
    try {
        console.log(`    Fetching jobs...`);
        console.log(`    userId: ${userId}, projectId: ${projectId}, pageId: ${pageId || 'none'}`);
        const payload = {
            userId, projectId, locale: 'de', numberOfJobsOnPage: 9999999, pageNumber: '1',
            isGetFilters: true, isActiveCustomJobPages: true, isForCurrentLocale: false,
            isUseLayoutsOfSubsidiaries: false,
            listState: { search: '', disableSearchInDescription: false, location: { osmLocation: '', range: 25, coords: [] },
                filters: { careerLevel: [], category: [], location: [], company: [], partnership: [], region: [], country: [] }
            },
            filterStatus: { careerLevel: false, category: false, partnership: false, region: false, location: false }
        };
        if (pageId) payload.pageId = pageId;
        const response = await axios.post('https://pcw-api.softgarden.de/widgets/job-list/job-ads', payload, {
            headers: { 'Content-Type': 'application/json', 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json' },
            timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS
        });
        const data = response.data;
        const rawJobs = data?.jobs || data?.jobAds || data?.data || [];
        console.log(`    ✅ ${rawJobs.length} jobs found!`);
        const jobs = rawJobs.map(job => {
            const externalId = String(job.jobPostingId || job.id || job.jobAdId || Math.random());
            const feedEntry = feedMap.get(externalId);
            const location = (feedEntry && feedEntry.location) || extractLocationFromJob(job);
            const companyName = (feedEntry && feedEntry.company_name) || extractCompanyNameFromJob(job, null);
            const rawDescription = (feedEntry && feedEntry.raw_description) || job.jobDescription || job.description || null;
            const applyUrl = (feedEntry && feedEntry.apply_url) || job.applyUrl || job.applicationUrl ||
                `https://pcw-api.softgarden.de/job/${job.jobPostingId}` || null;
            return {
                external_job_id: externalId,
                title: (feedEntry && feedEntry.title) || job.jobTitle || job.title || job.name || 'Unknown',
                location: location,
                employment_type: (feedEntry && feedEntry.employment_type) || job.workTime || job.employmentType || null,
                raw_description: rawDescription,
                apply_url: applyUrl,
                department: job.category || job.department || null,
                company_name: companyName,
                ats_source: 'softgarden'
            };
        });

        // Enrichment: if description missing or too short, fetch detail page or PDF
        console.log(`    📄 Enriching ${jobs.length} jobs with full descriptions...`);
        for (const job of jobs) {
            if (job.raw_description && job.raw_description.length > 100) continue;
            if (!job.apply_url) continue;
            let content = null;
            if (isPdfUrl(job.apply_url)) {
                content = await downloadAndParsePDF(job.apply_url);
            } else {
                const html = await fetchHtmlForFallback(job.apply_url);
                if (html) {
                    content = extractDescriptionGeneric(html);
                    if (!job.location) {
                        const loc = extractLocationFromHTML(html);
                        if (loc) job.location = loc;
                    }
                    if (!job.company_name) {
                        const comp = extractCompanyNameFromHTML(html, null);
                        if (comp) job.company_name = comp;
                    }
                }
            }
            if (content && content.length > 100) {
                job.raw_description = content.slice(0, 5000);
            }
        }

        const withDescription = jobs.filter(j => j.raw_description && j.raw_description.length > 100).length;
        console.log(`    📄 ${withDescription}/${jobs.length} jobs have usable descriptions`);
        return jobs;
    } catch (err) {
        console.log(`    API error: ${err.message}`);
        if (err.response) console.log(`    Status: ${err.response.status}`);
        return [];
    }
}

// ─── UPDATED processSoftgardenCompany ──────────────────────────────────────
async function processSoftgardenCompany(company) {
    console.log(`\n🔍 Processing: ${company.Name}`);
    console.log(`   Career URL: ${company.detected_career_url}`);

    // Try to get subdomain
    const subdomain = await getSoftgardenSubdomain(company.detected_career_url);
    if (subdomain) console.log(`   ✅ Subdomain: ${subdomain}`);
    else console.log(`   ⚠️ No subdomain found`);

    // If no career URL, discover
    let effectiveUrl = company.detected_career_url;
    if (!effectiveUrl || effectiveUrl.trim() === '') {
        console.log('   No career URL provided, attempting discovery...');
        effectiveUrl = await discoverCareerPage('https://' + company.Name.replace(/ /g, '').toLowerCase() + '.com');
        if (!effectiveUrl) {
            console.log('   Could not discover any career page.');
            return { company, jobs: [], error: 'No career URL found' };
        }
        console.log(`   Discovered career page: ${effectiveUrl}`);
    }

    const ids = await extractSoftgardenIds(effectiveUrl);
    const feedMap = await discoverSoftgardenFeedMap(effectiveUrl);

    if (!ids) {
        console.log(`   ⚠️ Softgarden IDs nahi milay — trying generic fallback...`);
        const fallbackJobs = await genericFallbackCrawl(effectiveUrl);
        if (fallbackJobs.length > 0) {
            console.log(`   ✅ Fallback successful: ${fallbackJobs.length} jobs`);
            return { company, jobs: fallbackJobs, error: null, usedFallback: true };
        }
        console.log(`   ❌ Fallback bhi fail`);
        return { company, jobs: [], error: 'No IDs, fallback 0 jobs' };
    }

    console.log(`   ✅ userId: ${ids.userId}, projectId: ${ids.projectId}`);
    const jobs = await fetchSoftgardenJobs(ids.userId, ids.projectId, ids.pageId, feedMap);
    console.log(`   📋 Total jobs: ${jobs.length}`);
    return { company, ids, jobs, error: null };
}

// ─── EXPORT ──────────────────────────────────────────────────────────────
module.exports = {
    processSoftgardenCompany,
    fetchSoftgardenJobs,
    extractSoftgardenIds,
    closeSoftgardenBrowser,
    // plus other exports if needed
};
