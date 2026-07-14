/**
 * ============================================================================
 * PRODUCTION CUSTOM CRAWLER v15 - EXACT CITY EXTRACTION
 * ============================================================================
 * FIXES:
 *   ✅ Location: extracts ONLY city name (e.g., "Worms" from "Lebenshilfe Worms")
 *   ✅ Location: null if no valid city found
 *   ✅ PDF parse import fixed
 *   ✅ Graceful shutdown on Ctrl+C
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
const axios = require('axios');
const { fetchWithScraperAPI } = require('../utils/scraperapi-config');
require('dotenv').config();

// ─── PDF PARSE FIX ─────────────────────────────────────────────────────────
let pdfParse = null;
try {
    pdfParse = require('pdf-parse');
} catch (e) {
    console.log('[PDF] pdf-parse not installed – PDF parsing disabled');
}

// ---------------------------------------------------------------------------
// CONFIGURATION
// ---------------------------------------------------------------------------
const CONFIG = {
    CONCURRENCY: parseInt(process.env.CRAWLER_CONCURRENCY || '3', 10),
    PAGE_SIZE: 1000,
    MAX_JOB_LINKS_PER_COMPANY: parseInt(process.env.MAX_JOB_LINKS_PER_COMPANY || '500', 10),
    RECRAWL_INTERVAL_HOURS: parseInt(process.env.RECRAWL_INTERVAL_HOURS || '48', 10),
    COMPANY_TIMEOUT_MS: parseInt(process.env.COMPANY_TIMEOUT_MS || '180000', 10),
    JOB_TIMEOUT_MS: parseInt(process.env.JOB_TIMEOUT_MS || '30000', 10),
    PLAYWRIGHT_TIMEOUT_MS: parseInt(process.env.PLAYWRIGHT_TIMEOUT_MS || '60000', 10),
    BROWSER_RESTART_THRESHOLD: parseInt(process.env.BROWSER_RESTART_THRESHOLD || '100', 10),
    QUEUE_POLL_INTERVAL_MS: 5000,
    RATE_LIMIT_MAX: parseInt(process.env.CRAWLER_RATE_LIMIT_MAX || '5', 10),
    RATE_LIMIT_DURATION_MS: parseInt(process.env.CRAWLER_RATE_LIMIT_DURATION_MS || '1000', 10),
    SKILL_EXTRACTION_MODEL: process.env.SKILL_EXTRACTION_MODEL || 'claude-haiku-4-5-20251001',
    BATCH_INSERT_SIZE: 50,
    RETRY_EXTRACTION_ATTEMPTS: 3,
    PAGINATION_MAX_PAGES: 50,
    LOAD_MORE_MAX_CLICKS: 100,
    NETWORK_WAIT_MS: 3000
};

const QUEUE_NAME = 'custom-crawl';

// ---------------------------------------------------------------------------
// KNOWN CITIES (extended list)
// ---------------------------------------------------------------------------
const KNOWN_CITIES = new Set([
    // Germany
    'Berlin', 'Hamburg', 'Munich', 'Cologne', 'Frankfurt', 'Stuttgart', 'Düsseldorf',
    'Dortmund', 'Essen', 'Leipzig', 'Dresden', 'Hanover', 'Nuremberg', 'Duisburg',
    'Bochum', 'Wuppertal', 'Bielefeld', 'Bonn', 'Mannheim', 'Karlsruhe', 'Wiesbaden',
    'Mönchengladbach', 'Gelsenkirchen', 'Aachen', 'Kiel', 'Magdeburg', 'Braunschweig',
    'Chemnitz', 'Göttingen', 'Oberhausen', 'Hagen', 'Rostock', 'Kassel', 'Saarbrücken',
    'Augsburg', 'Ulm', 'Oldenburg', 'Potsdam', 'Halle', 'Erfurt', 'Mülheim', 'Jena',
    'Ludwigshafen', 'Trier', 'Recklinghausen', 'Offenbach', 'Freiburg', 'Heidelberg',
    'Rosenheim', 'Koblenz', 'Krefeld', 'Neuss', 'Reutlingen', 'Landshut', 'Passau',
    'Straubing', 'Bamberg', 'Bayreuth', 'Regensburg', 'Ingolstadt', 'Fürth', 'Erlangen',
    'Würzburg', 'Aschaffenburg', 'Darmstadt', 'Mainz', 'Limburg', 'Wetzlar',
    'Gießen', 'Marburg', 'Fulda', 'Kassel', 'Göttingen', 'Hildesheim', 'Salzgitter',
    'Wolfsburg', 'Braunschweig', 'Hanover', 'Celle', 'Lüneburg', 'Uelzen', 'Stade',
    'Buxtehude', 'Pinneberg', 'Elmshorn', 'Norderstedt', 'Ahrensburg', 'Glinde',
    'Reinbek', 'Bad Oldesloe', 'Itzehoe', 'Heide', 'Husum', 'Flensburg', 'Schleswig',
    'Eckernförde', 'Kiel', 'Neumünster', 'Rendsburg', 'Plön', 'Eutin', 'Lübeck',
    'Bad Schwartau', 'Travemünde', 'Rostock', 'Schwerin', 'Wismar', 'Güstrow',
    'Neubrandenburg', 'Stralsund', 'Greifswald', 'Frankfurt Oder', 'Cottbus',
    'Brandenburg', 'Potsdam', 'Eberswalde', 'Oranienburg', 'Bernau', 'Strausberg',
    'Fürstenwalde', 'Frankfurt am Main', 'Offenbach am Main', 'Hanau', 'Darmstadt',
    'Wiesbaden', 'Mainz', 'Koblenz', 'Trier', 'Saarbrücken', 'Kaiserslautern',
    'Ludwigshafen am Rhein', 'Speyer', 'Worms', 'Mannheim', 'Heidelberg', 'Karlsruhe',
    'Baden-Baden', 'Offenburg', 'Freiburg', 'Konstanz', 'Singen', 'Villingen-Schwenningen',
    'Rottweil', 'Tuttlingen', 'Ulm', 'Neu-Ulm', 'Augsburg', 'Kempten', 'Memmingen',
    'Lindau', 'Oberstdorf', 'Garmisch-Partenkirchen', 'Mittenwald', 'Bad Reichenhall',
    'Traunstein', 'Rosenheim', 'Chiemsee', 'Mühldorf', 'Altötting', 'Burghausen',
    'Landshut', 'Straubing', 'Deggendorf', 'Passau', 'Regensburg', 'Neumarkt',
    'Ingolstadt', 'Eichstätt', 'Weißenburg', 'Gunzenhausen', 'Nördlingen', 'Donauwörth',
    'Dillingen', 'Günzburg', 'Kempten', 'Kaufbeuren', 'Marktoberdorf', 'Füssen',
    'Sonthofen', 'Immenstadt', 'Oberstdorf', 'Garmisch-Partenkirchen', 'Murnau',
    'Weilheim', 'Schongau', 'Landsberg', 'Fürstenfeldbruck', 'Dachau', 'Freising',
    'Erding', 'Moosburg', 'Landshut', 'Pfaffenhofen', 'Neuburg', 'Schrobenhausen',
    'Aichach', 'Friedberg', 'Augsburg', 'Gersthofen', 'Neusäß', 'Dinkelscherben',
    'Biberach', 'Wangen', 'Ravensburg', 'Weingarten', 'Friedrichshafen', 'Konstanz',
    'Singen', 'Radolfzell', 'Stockach', 'Überlingen', 'Meersburg', 'Tettnang',
    'Bad Saulgau', 'Riedlingen', 'Ehingen', 'Blaubeuren', 'Laichingen', 'Münsingen',
    'Bad Urach', 'Metzingen', 'Reutlingen', 'Tübingen', 'Rottenburg', 'Herrenberg',
    'Böblingen', 'Sindelfingen', 'Leonberg', 'Stuttgart', 'Ludwigsburg', 'Kornwestheim',
    'Bietigheim', 'Vaihingen', 'Marbach', 'Backnang', 'Waiblingen', 'Schorndorf',
    'Göppingen', 'Eislingen', 'Geislingen', 'Heidenheim', 'Aalen', 'Ellwangen',
    'Crailsheim', 'Schwäbisch Hall', 'Künzelsau', 'Öhringen', 'Neuenstein',
    'Waldenburg', 'Forchtenberg', 'Bad Mergentheim', 'Tauberbischofsheim', 'Wertheim',
    'Miltenberg', 'Obernburg', 'Aschaffenburg', 'Hanau', 'Gelnhausen', 'Bad Soden',
    'Kronberg', 'Königstein', 'Bad Homburg', 'Friedrichsdorf', 'Usingen', 'Weilrod',
    'Schmitten', 'Bad Nauheim', 'Friedberg', 'Butzbach', 'Lich', 'Laubach', 'Hungen',
    'Nidda', 'Schotten', 'Grünberg', 'Alsfeld', 'Lauterbach', 'Schlitz', 'Fulda',
    'Hünfeld', 'Bad Hersfeld', 'Bebra', 'Rotenburg', 'Melsungen', 'Witzenhausen',
    'Eschwege', 'Bad Sooden', 'Wanfried', 'Treffurt', 'Creuzburg', 'Eisenach',
    'Gotha', 'Erfurt', 'Weimar', 'Jena', 'Gera', 'Zeitz', 'Naumburg', 'Weißenfels',
    'Merseburg', 'Halle', 'Eisleben', 'Sangerhausen', 'Nordhausen', 'Sondershausen',
    'Mühlhausen', 'Langensalza', 'Heiligenstadt', 'Duderstadt', 'Göttingen',
    'Northeim', 'Einbeck', 'Osterode', 'Clausthal', 'Goslar', 'Bad Harzburg',
    'Wernigerode', 'Quedlinburg', 'Halberstadt', 'Blankenburg', 'Thale', 'Ballenstedt',
    'Aschersleben', 'Staßfurt', 'Schönebeck', 'Magdeburg', 'Burg', 'Genthin',
    'Rathenow', 'Brandenburg', 'Potsdam', 'Werder', 'Beelitz', 'Luckenwalde',
    'Jüterbog', 'Trebbin', 'Zossen', 'Königs Wusterhausen', 'Mittenwalde',
    'Schönefeld', 'Blankenfelde', 'Teltow', 'Kleinmachnow', 'Stahnsdorf',
    // Austria
    'Vienna', 'Graz', 'Linz', 'Salzburg', 'Innsbruck', 'Klagenfurt', 'Villach',
    'Wels', 'Sankt Pölten', 'Dornbirn', 'Steyr', 'Feldkirch', 'Bregenz', 'Wolfsberg',
    'Baden', 'Mödling', 'Eisenstadt', 'Wiener Neustadt', 'Krems', 'Amstetten',
    // Switzerland
    'Zurich', 'Geneva', 'Basel', 'Bern', 'Lausanne', 'Winterthur', 'Lucerne',
    'St. Gallen', 'Lugano', 'Biel', 'Thun', 'Köniz', 'La Chaux-de-Fonds',
    'Schaffhausen', 'Fribourg', 'Chur', 'Neuchâtel', 'Vernier', 'Uster',
    'Sion', 'Emmen', 'Kriens', 'Zug', 'Rapperswil', 'Wädenswil', 'Dietikon',
    'Baar', 'Kreuzlingen', 'Wil', 'Gossau', 'Bülach', 'Horgen', 'Männedorf',
    'Meilen', 'Adliswil', 'Opfikon', 'Regensdorf', 'Dübendorf', 'Volketswil'
]);

// ─── STOPWORDS (reject these)
const STOPWORDS = new Set([
    'standort', 'anfahrt', 'anzeigen', 'technischer', 'support', 'service',
    'kundenzentren', 'produkte', 'navigation', 'hauptnavigation',
    'bewerbungsgespräch', 'entscheidet', 'unternehmen', 'leistungen',
    'bildungsstandorte', 'träger', 'home', 'start', 'plus', 'spenden', 'solutions',
    'faq', 'jobs', 'karriere', 'careers', 'job', 'career', 'kontakt', 'impressum',
    'datenschutz', 'agb', 'cookie', 'cookies', 'einstellungen', 'settings',
    'preference', 'centre', 'center', 'multiple', 'locations', 'eingeben',
    'gmbh', 'ag', 'kg', 'se', 'e.v.', 'ug', 'gbr', 'ohg', 'instagram', 'facebook',
    'twitter', 'linkedin', 'youtube', 'alle', 'stellenangebote', 'startseite',
    'online', 'suchen', 'search', 'karriereportal', 'menu', 'homeclose',
    'leistungen', 'bewerben', 'verwaltung', 'marketing', 'company', 'bereiche',
    'abteilungen', 'unsere', 'ihre', 'ihr', 'ihnen', 'ihres', 'ihrer', 'abteilung',
    'bereich', 'team', 'mitarbeiter', 'kollegen', 'projekte', 'kunden', 'partner',
    'lieferanten', 'dienstleister', 'software', 'hardware', 'infrastruktur',
    'netzwerk', 'datenbank', 'entwicklung', 'produktion', 'vertrieb', 'einkauf',
    'personal', 'finanzen', 'controlling', 'buchhaltung', 'steuern', 'recht',
    'qualität', 'sicherheit', 'umwelt', 'arbeitsschutz', 'adresse', 'kurfürstenstraße'
]);

// ---------------------------------------------------------------------------
// CLIENTS
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
// BROWSER MANAGEMENT
// ---------------------------------------------------------------------------
let sharedBrowser = null;
let requestsSinceRestart = 0;
let browserContext = null;

async function getSharedBrowser() {
    if (!sharedBrowser) {
        sharedBrowser = await chromium.launch({ headless: true });
        console.log('[BROWSER] Playwright browser launched');
    }
    return sharedBrowser;
}

async function getBrowserContext() {
    if (!browserContext) {
        const browser = await getSharedBrowser();
        browserContext = await browser.newContext({
            extraHTTPHeaders: { 'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8' },
            viewport: { width: 1280, height: 800 }
        });
    }
    return browserContext;
}

async function recycleBrowserIfNeeded() {
    requestsSinceRestart++;
    if (requestsSinceRestart >= CONFIG.BROWSER_RESTART_THRESHOLD) {
        console.log(`[BROWSER] Recycling after ${requestsSinceRestart} requests`);
        const previousBrowser = sharedBrowser;
        const previousContext = browserContext;
        sharedBrowser = await chromium.launch({ headless: true });
        browserContext = await sharedBrowser.newContext({
            extraHTTPHeaders: { 'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8' },
            viewport: { width: 1280, height: 800 }
        });
        requestsSinceRestart = 0;
        if (previousContext) await previousContext.close().catch(() => {});
        if (previousBrowser) await previousBrowser.close().catch(() => {});
    }
}

// ---------------------------------------------------------------------------
// COOKIE ACCEPT
// ---------------------------------------------------------------------------
async function acceptCookies(page) {
    const cookieSelectors = [
        'button[aria-label*="cookie"]',
        'button[aria-label*="Cookie"]',
        'button[id*="cookie"]',
        'button[class*="cookie"]',
        'button:has-text("Accept")',
        'button:has-text("Accept all")',
        'button:has-text("Zustimmen")',
        'button:has-text("Alle akzeptieren")',
        'button:has-text("OK")',
        'a:has-text("Accept")',
        'a:has-text("Zustimmen")',
        '#cookie-consent-accept',
        '.cookie-accept-button',
        '.cookie-consent-accept',
    ];

    for (const selector of cookieSelectors) {
        try {
            const acceptBtn = await page.locator(selector).first();
            if (await acceptBtn.isVisible({ timeout: 1500 })) {
                await acceptBtn.click();
                return true;
            }
        } catch (e) {}
    }
    return false;
}

// ─── FETCH PAGE ────────────────────────────────────────────────────────────
async function fetchWithPlaywright(url, options = {}) {
    const { waitForSelector, timeout = CONFIG.PLAYWRIGHT_TIMEOUT_MS } = options;
    const context = await getBrowserContext();
    const page = await context.newPage();

    try {
        const jobResponses = [];
        page.on('response', async (response) => {
            const respUrl = response.url();
            if (respUrl.includes('job') || respUrl.includes('position') || respUrl.includes('vacancy') || respUrl.includes('api')) {
                try {
                    const data = await response.json().catch(() => null);
                    if (data) jobResponses.push({ url: respUrl, data });
                } catch (e) {}
            }
        });

        await page.goto(url, { waitUntil: 'domcontentloaded', timeout });
        await acceptCookies(page);

        if (waitForSelector) {
            await page.waitForSelector(waitForSelector, { timeout: 10000 }).catch(() => {});
        }

        await page.waitForTimeout(2000);
        const html = await page.content();

        let networkJobs = [];
        for (const resp of jobResponses) {
            const data = resp.data;
            if (data) {
                const jobs = data.jobs || data.data || data.items || data.results || data.positions || data.offers || [];
                if (Array.isArray(jobs) && jobs.length > 0) {
                    networkJobs = networkJobs.concat(jobs);
                }
            }
        }

        await page.close().catch(() => {});
        await recycleBrowserIfNeeded();

        return { html, networkJobs };
    } catch (err) {
        await page.close().catch(() => {});
        await recycleBrowserIfNeeded();
        throw err;
    }
}

async function fetchPageWithFallback(url, options = {}) {
    try {
        const result = await fetchWithPlaywright(url, options);
        console.log('[FETCH] Playwright success');
        return result;
    } catch (playwrightError) {
        console.log(`[FETCH] Playwright failed: ${playwrightError.message}`);
        console.log('[FETCH] Falling back to ScraperAPI...');
    }

    try {
        const html = await fetchWithScraperAPI(url, {
            renderJs: true,
            waitFor: 5000,
            premium: true,
            waitForSelector: 'body'
        });
        console.log('[FETCH] ScraperAPI success');
        return { html, networkJobs: [] };
    } catch (scraperApiError) {
        console.log(`[FETCH] ScraperAPI failed: ${scraperApiError.message}`);
        return null;
    }
}

// ─── PDF HANDLING ──────────────────────────────────────────────────────────
async function downloadAndParsePDF(pdfUrl) {
    if (!pdfParse) {
        console.log('[PDF] pdf-parse not available');
        return null;
    }
    try {
        const response = await axios.get(pdfUrl, {
            responseType: 'arraybuffer',
            timeout: 30000,
            headers: { 'User-Agent': 'Mozilla/5.0' }
        });
        const pdfBuffer = Buffer.from(response.data);
        const data = await pdfParse(pdfBuffer);
        return data.text || '';
    } catch (err) {
        console.log(`[PDF] Parse error: ${err.message}`);
        return null;
    }
}

function isPdfUrl(url) {
    return url.toLowerCase().endsWith('.pdf') || url.includes('.pdf?') || url.includes('.pdf#');
}

// ─── CAREER PAGE DISCOVERY ──────────────────────────────────────────────
async function discoverCareerPage(baseUrl) {
    const careerKeywords = ['karriere', 'jobs', 'career', 'stellenangebote', 'offene-stellen', 'vacancies'];
    if (careerKeywords.some(k => baseUrl.toLowerCase().includes(k))) {
        return baseUrl;
    }

    const base = new URL(baseUrl).origin;
    const commonPaths = [
        '/karriere', '/jobs', '/careers', '/stellenangebote', '/offene-stellen',
        '/en/careers', '/de/karriere', '/about/careers', '/company/careers',
        '/job-angebote', '/vakanz', '/stellen', '/vacancies'
    ];

    for (const path of commonPaths) {
        const testUrl = base + path;
        try {
            const result = await fetchPageWithFallback(testUrl, { waitForSelector: 'body' });
            if (result && result.html && result.html.length > 500) {
                const $ = cheerio.load(result.html);
                const hasJobContent = $('a[href*="job"]').length > 0 || $('a[href*="stelle"]').length > 0 ||
                    result.html.includes('stellenangebote') || result.html.includes('offene stellen');
                if (hasJobContent) {
                    return testUrl;
                }
            }
        } catch (e) {}
    }

    return null;
}

// ─── EXTRACT ALL JOB LINKS ──────────────────────────────────────────────
async function extractAllJobLinks(baseUrl, companyName) {
    const allLinks = new Set();

    try {
        const context = await getBrowserContext();
        const page = await context.newPage();

        await page.goto(baseUrl, { waitUntil: 'domcontentloaded', timeout: CONFIG.PLAYWRIGHT_TIMEOUT_MS });
        await acceptCookies(page);
        await page.waitForTimeout(2000);

        const loadMoreSelectors = [
            'button:has-text("Load more")',
            'button:has-text("Mehr laden")',
            'button:has-text("Weitere anzeigen")',
            'button:has-text("Alle anzeigen")',
            'button:has-text("Mehr anzeigen")',
            'a:has-text("Load more")',
            'a:has-text("Mehr laden")',
            'a:has-text("Weitere anzeigen")',
            '.load-more', '.show-more', '.view-more',
            '#load-more', '[data-load-more]', '[class*="load-more"]'
        ];

        let loadMoreCount = 0;
        let previousHeight = 0;
        while (loadMoreCount < CONFIG.LOAD_MORE_MAX_CLICKS) {
            let clicked = false;
            for (const selector of loadMoreSelectors) {
                try {
                    const button = await page.locator(selector).first();
                    if (await button.isVisible({ timeout: 1000 })) {
                        await button.click();
                        clicked = true;
                        loadMoreCount++;
                        await page.waitForTimeout(2000);
                        const currentHeight = await page.evaluate(() => document.body.scrollHeight);
                        if (currentHeight === previousHeight) break;
                        previousHeight = currentHeight;
                        break;
                    }
                } catch (e) {}
            }
            if (!clicked) break;
        }

        const html = await page.content();
        const $ = cheerio.load(html);

        const jobPatterns = [
            /\/job\//i, /\/job-\d+/i, /\/jobs\//i,
            /\/position\//i, /\/positions\//i, /\/position-\d+/i,
            /\/vacancy\//i, /\/vacancies\//i, /\/vakanz\//i,
            /\/stellenangebot\//i, /\/stellenangebote\//i, /\/stelle\//i, /\/stellen\//i,
            /\/apply\//i, /\/bewerbung\//i, /\/bewerben\//i,
            /\/detail\?/i, /\/details\?/i,
            /[?&]job_id=\d+/i, /[?&]position_id=\d+/i,
            /[?&]id=\d+.*job/i, /[?&]vacancy=\d+/i,
            /\/karriere\//i, /\/career\//i, /\/careers\//i,
            /\/offene-stellen\//i, /\/open-positions\//i,
            /\/job-angebot\//i, /\/job-posting\//i,
            /\/beruf\//i, /\/arbeit\//i,
            /\/ausschreibung\//i, /\/ausbildungsplatz\//i,
            /\/praktikum\//i, /\/trainee\//i,
            /[?&]job_id=\d+/i, /[?&]id_job=\d+/i,
            /\/joboffer\//i, /\/job-offer\//i,
            /\/career-\d+/i, /\/job-\d+/i,
            /\/recruitment\//i, /\/recruiting\//i,
            /\/jobs-\d+/i, /\/offene-stelle\//i
        ];

        $('a').each((_, el) => {
            const href = $(el).attr('href');
            const text = $(el).text().trim().toLowerCase();
            if (!href || href.includes('#') || href.includes('mailto:') || href.includes('tel:')) return;

            let fullUrl = href;
            if (!href.startsWith('http')) {
                try { fullUrl = new URL(href, baseUrl).href; } catch { return; }
            }

            const isJobLink = jobPatterns.some(pattern => pattern.test(fullUrl));
            if (isJobLink) {
                allLinks.add(fullUrl);
                return;
            }

            const jobWords = ['job', 'stelle', 'karriere', 'vacancy', 'position', 'bewerbung', 'vakanz', 'beruf', 'arbeit'];
            if (jobWords.some(w => text.includes(w) || fullUrl.toLowerCase().includes(w))) {
                const isMain = /\/karriere\/?$|\/jobs\/?$|\/careers\/?$/i.test(fullUrl);
                if (!isMain) {
                    allLinks.add(fullUrl);
                }
            }
        });

        const networkData = await page.evaluate(() => {
            const jobs = [];
            if (window.jobs) jobs.push(...(Array.isArray(window.jobs) ? window.jobs : [window.jobs]));
            if (window.Jobs) jobs.push(...(Array.isArray(window.Jobs) ? window.Jobs : [window.Jobs]));
            if (window.jobList) jobs.push(...(Array.isArray(window.jobList) ? window.jobList : [window.jobList]));
            if (window.__NEXT_DATA__ && window.__NEXT_DATA__.props) {
                const props = window.__NEXT_DATA__.props;
                if (props.jobs) jobs.push(...props.jobs);
                if (props.jobList) jobs.push(...props.jobList);
            }
            return jobs;
        });

        for (const job of networkData) {
            const url = job.url || job.link || job.applyUrl || job.href || job.id;
            if (url) {
                const fullUrl = url.toString();
                const isJobLink = jobPatterns.some(pattern => pattern.test(fullUrl));
                if (isJobLink) allLinks.add(fullUrl);
            }
        }

        await page.close().catch(() => {});
        await recycleBrowserIfNeeded();

    } catch (err) {
        console.log(`[EXTRACT] Error with Playwright: ${err.message}`);
        try {
            const result = await fetchPageWithFallback(baseUrl, { waitForSelector: 'body' });
            if (result && result.html) {
                const $ = cheerio.load(result.html);
                const jobPatterns = [/\/job\//i, /\/position\//i, /\/vacancy\//i, /\/stelle\//i, /\/vakanz\//i, /\/karriere\//i];
                $('a').each((_, el) => {
                    const href = $(el).attr('href');
                    if (!href) return;
                    const isJobLink = jobPatterns.some(p => p.test(href));
                    if (isJobLink) {
                        let fullUrl = href;
                        if (!href.startsWith('http')) {
                            try { fullUrl = new URL(href, baseUrl).href; } catch { return; }
                        }
                        allLinks.add(fullUrl);
                    }
                });
            }
        } catch (e) {}
    }

    const uniqueLinks = [...allLinks];
    console.log(`[LINKS] Total unique job detail links found: ${uniqueLinks.length}`);
    return uniqueLinks.slice(0, CONFIG.MAX_JOB_LINKS_PER_COMPANY);
}

// ─── FIND JOB CONTAINER ──────────────────────────────────────────────────
function findJobContainer($) {
    const selectors = [
        '.job-description', '.job-details', '.job-content', '.job-listing',
        '[class*="job-description"]', '[class*="job-detail"]',
        '[class*="job-content"]', '[class*="job-listing"]',
        'article', '.main-content', '#content', '.content-area',
        '.post-content', '.entry-content', '.page-content'
    ];

    for (const selector of selectors) {
        const el = $(selector);
        if (el.length > 0) {
            const text = el.text().trim();
            if (text.length > 200) return el;
        }
    }
    return $('body');
}

// ─── EXTRACT JOB TITLE ──────────────────────────────────────────────────
function extractJobTitleFromContainer(container, $) {
    const titleSelectors = ['h1', 'h2', '.title', '.job-title', '[class*="title"]', '[class*="job-title"]'];
    for (const selector of titleSelectors) {
        const el = container.find(selector).first();
        if (el.length > 0) {
            const text = el.text().trim();
            if (text.length > 5 && text.length < 200) return text;
        }
    }
    const firstP = container.find('p').first();
    if (firstP.length > 0) {
        const text = firstP.text().trim();
        if (text.length > 5 && text.length < 200) return text;
    }
    const pageTitle = $('title').text().trim();
    if (pageTitle) {
        const cleaned = pageTitle.replace(/ - (Karriere|Jobs|Stellenangebote|Career|Careers|Startseite|Homepage|Home|Start)$/i, '').trim();
        if (cleaned.length > 5) return cleaned;
    }
    return null;
}

// ─── 🔥 LOCATION EXTRACTION – EXACT CITY ONLY ──────────────────────────
function extractLocationFromContainer(container, $) {
    const containerText = container.text();

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
                    if (typeof loc === 'string') {
                        const city = extractCityFromText(loc);
                        if (city) return city;
                    }
                    if (loc.address) {
                        const city = loc.address.addressLocality || loc.address.addressRegion;
                        if (city) {
                            const extracted = extractCityFromText(city);
                            if (extracted) return extracted;
                        }
                    }
                }
            }
        } catch (e) {}
    }

    // 2. Pattern: "Ort:", "Standort:", etc.
    const patterns = [
        /(?:Ort|Standort|Arbeitsort|Location|Stadt|City)[:\s]+([^\n,;.!?]{2,150})/i,
        /(?:Work Location|Job Location)[:\s]+([^\n,;.!?]{2,150})/i
    ];

    for (const pattern of patterns) {
        const match = containerText.match(pattern);
        if (match) {
            const candidate = match[1].trim();
            const city = extractCityFromText(candidate);
            if (city) return city;
        }
    }

    // 3. Look for any known city in the container text
    const words = containerText.split(/\s+/);
    for (const word of words) {
        const clean = word.replace(/[,;.!?:]$/, '');
        const city = extractCityFromText(clean);
        if (city) return city;
        // Check for multi-word cities
        const idx = words.indexOf(word);
        if (idx < words.length - 1) {
            const next = words[idx + 1].replace(/[,;.!?:]$/, '');
            const combined = clean + ' ' + next;
            const city2 = extractCityFromText(combined);
            if (city2) return city2;
            if (idx < words.length - 2) {
                const next2 = words[idx + 2].replace(/[,;.!?:]$/, '');
                const combined3 = clean + ' ' + next + ' ' + next2;
                const city3 = extractCityFromText(combined3);
                if (city3) return city3;
            }
        }
    }

    // 4. Meta tags (fallback)
    const metaLocation = $('meta[property="og:location"]').attr('content') ||
                         $('meta[name="geo.placename"]').attr('content');
    if (metaLocation) {
        const city = extractCityFromText(metaLocation);
        if (city) return city;
    }

    return null;
}

// ─── 🔥 EXTRACT CITY NAME FROM TEXT ──────────────────────────────────────
function extractCityFromText(text) {
    if (!text || typeof text !== 'string') return null;

    // Clean the text
    let cleaned = text.replace(/\s+/g, ' ').trim();

    // Remove common noise patterns
    const noise = ['Standort', 'Anfahrt', 'anzeigen', 'technischer', 'support', 'service',
        'Kundenzentren', 'Produkte', 'Navigation', 'Hauptnavigation',
        'Bewerbungsgespräch', 'entscheidet', 'Unternehmen', 'Leistungen',
        'Bildungsstandorte', 'Träger', 'Adresse', 'Kurfürstenstraße'];
    for (const word of noise) {
        cleaned = cleaned.replace(new RegExp('\\b' + word + '\\b', 'gi'), '');
    }
    cleaned = cleaned.trim();

    if (!cleaned || cleaned.length < 2) return null;

    // Split on common separators and check each part
    const separators = /[,;.!?]|\s[-–]\s|\s[|]\s|\s–\s|–|—|\s-\s/;
    const parts = cleaned.split(separators).map(p => p.trim()).filter(p => p.length > 1);

    for (const part of parts) {
        // Check if part is a known city
        if (isKnownCity(part)) return part;
        // Check if part contains a known city (e.g., "Lebenshilfe Worms" -> "Worms")
        const cityMatch = findKnownCityInText(part);
        if (cityMatch) return cityMatch;
    }

    // If no separator, try the whole string
    if (isKnownCity(cleaned)) return cleaned;
    const cityMatch = findKnownCityInText(cleaned);
    if (cityMatch) return cityMatch;

    // Try splitting by spaces and check each word
    const words = cleaned.split(/\s+/);
    for (const word of words) {
        const cleanWord = word.replace(/[,;.!?:]$/, '');
        if (isKnownCity(cleanWord)) return cleanWord;
        // Check if word contains a known city
        const subMatch = findKnownCityInText(cleanWord);
        if (subMatch) return subMatch;
    }

    return null;
}

function isKnownCity(text) {
    if (!text) return false;
    const trimmed = text.trim();
    if (trimmed.length < 2) return false;
    if (trimmed.length > 80) return false;
    if (!/[aeiouyäöü]/i.test(trimmed)) return false;
    if (!/^[A-ZÄÖÜ]/.test(trimmed)) return false;
    if (STOPWORDS.has(trimmed.toLowerCase())) return false;
    if (KNOWN_CITIES.has(trimmed)) return true;
    // Also check if it's a known city without special characters
    const clean = trimmed.replace(/[^a-zA-ZÄÖÜäöüß\s]/g, '');
    if (KNOWN_CITIES.has(clean)) return true;
    return false;
}

function findKnownCityInText(text) {
    if (!text) return null;
    // Check if any known city is a substring
    for (const city of KNOWN_CITIES) {
        if (text.includes(city)) return city;
    }
    return null;
}

// ─── COMPANY NAME ──────────────────────────────────────────────────────────
function getCompanyName(html, recordName) {
    if (recordName && recordName.length > 2) {
        return recordName;
    }

    const $ = cheerio.load(html);
    const container = findJobContainer($);
    if (!container) return recordName || null;

    const containerText = container.text();

    const jsonLdScripts = $('script[type="application/ld+json"]');
    for (let i = 0; i < jsonLdScripts.length; i++) {
        try {
            const raw = $(jsonLdScripts[i]).html();
            const parsed = JSON.parse(raw);
            const candidates = Array.isArray(parsed) ? parsed : [parsed];
            for (const item of candidates) {
                if (item && item['@type'] === 'JobPosting' && item.hiringOrganization) {
                    const org = item.hiringOrganization;
                    if (typeof org === 'string') {
                        const cleaned = cleanCompanyName(org);
                        if (cleaned) return cleaned;
                    }
                    if (org.name) {
                        const cleaned = cleanCompanyName(org.name);
                        if (cleaned) return cleaned;
                    }
                }
            }
        } catch (e) {}
    }

    const patterns = [
        /(?:Arbeitgeber|Unternehmen|Firma|Company|Employer)[:\s]+([^\n,]{2,100})/i,
        /bei\s+([A-Z][a-zäöüß]+(?:\s+[A-Z][a-zäöüß]+)*(?:\s+(?:GmbH|AG|KG|SE|e\.V\.|UG|GbR|OHG))?)/i
    ];

    for (const pattern of patterns) {
        const match = containerText.match(pattern);
        if (match) {
            const candidate = match[1].trim().split(/\s*[,\n]/)[0].trim();
            if (candidate.length > 2) {
                const cleaned = cleanCompanyName(candidate);
                if (cleaned) return cleaned;
            }
        }
    }

    const metaCompany = $('meta[property="og:site_name"]').attr('content') ||
                        $('meta[name="application-name"]').attr('content') ||
                        $('meta[property="og:title"]').attr('content');
    if (metaCompany) {
        const cleaned = cleanCompanyName(metaCompany);
        if (cleaned) return cleaned;
    }

    return recordName || null;
}

function cleanCompanyName(text) {
    if (!text || typeof text !== 'string') return null;
    let cleaned = text.replace(/\s+/g, ' ').trim();
    const noise = ['Unternehmen', 'Leistungen', 'Bildungsstandorte', 'Träger', 'Kundenzentren', 'Produkte', 'Instagram'];
    for (const word of noise) {
        cleaned = cleaned.replace(new RegExp('\\b' + word + '\\b', 'gi'), '');
    }
    cleaned = cleaned.trim();
    if (!cleaned || cleaned.length < 2) return null;
    if (cleaned.length > 150) return null;
    if (!/[A-ZÄÖÜ]/.test(cleaned)) return null;
    return cleaned;
}

// ─── SENIORITY, REMOTE, EMPLOYMENT ──────────────────────────────────────
function extractSeniorityLevel(text) {
    const lower = text.toLowerCase();
    const keywords = {
        'executive': ['executive', 'director', 'c-level', 'vp', 'head of', 'chief'],
        'lead': ['lead', 'architect', 'principal', 'team lead', 'tech lead'],
        'senior': ['senior', 'leitung', 'manager', 'experienced', 'erfahren', 'erfahrene'],
        'mid': ['mid', 'professional', 'specialist', 'mit erfahrung'],
        'junior': ['junior', 'entry', 'einsteiger', 'assistant', 'trainee']
    };
    for (const [level, words] of Object.entries(keywords)) {
        if (words.some(w => lower.includes(w))) return level;
    }
    return 'mid';
}

function extractRemoteType(text) {
    const lower = text.toLowerCase();
    if (lower.includes('remote') || lower.includes('home office') || lower.includes('100% remote')) return 'remote';
    if (lower.includes('hybrid') || lower.includes('teilweise home office')) return 'hybrid';
    if (lower.includes('onsite') || lower.includes('präsenz') || lower.includes('vor ort')) return 'onsite';
    return 'hybrid';
}

function extractEmploymentType(text) {
    const lower = text.toLowerCase();
    if (lower.includes('vollzeit') || lower.includes('full-time') || lower.includes('full time')) return 'full-time';
    if (lower.includes('teilzeit') || lower.includes('part-time') || lower.includes('part time')) return 'part-time';
    if (lower.includes('befristet') || lower.includes('contract')) return 'contract';
    if (lower.includes('ausbildung') || lower.includes('apprentice')) return 'apprenticeship';
    if (lower.includes('praktikum') || lower.includes('internship')) return 'internship';
    return 'full-time';
}

// ─── DESCRIPTION EXTRACTION ──────────────────────────────────────────────
function extractDescriptionFromHTML(html) {
    const $ = cheerio.load(html);
    const selectors = [
        '.job-description', '.job-details', '.description', '.content',
        '#job-description', '.job-content', '[class*="job-description"]',
        '[class*="job-detail"]', '[class*="description"]', 'article',
        '.main-content', '#content', '.text-content', '.post-content',
        '.entry-content', '.job__description', '.job-listing__description',
        '[itemprop="description"]', '[itemprop="jobDescription"]',
        '[class*="stellenanzeige"]', '[class*="aufgaben"]', '[class*="anforderung"]'
    ];
    for (const selector of selectors) {
        const text = $(selector).text().trim();
        if (text && text.length > 300) {
            return cleanDescription(text);
        }
    }
    let text = $('body').text();
    text = text.split('\n')
        .map(l => l.trim())
        .filter(l => l.length > 20)
        .filter(l => !/impressum|datenschutz|agb|cookie|footer|menu|navigation|copyright|©/i.test(l))
        .join('\n');
    return text.length > 300 ? cleanDescription(text) : null;
}

function cleanDescription(text) {
    if (!text) return null;
    text = text.replace(/\s+/g, ' ').trim();
    text = text.replace(/^[\s\-:]+/, '').trim();
    const words = text.split(/\s+/).length;
    if (words < 50) return null;
    if (text.length < 300) return null;
    return text.slice(0, 5000);
}

// ─── SKILL EXTRACTION ──────────────────────────────────────────────────────
async function extractSkillsWithClaude(title, description) {
    if (!description || description.length < 100) return [];
    try {
        const response = await anthropic.messages.create({
            model: CONFIG.SKILL_EXTRACTION_MODEL,
            max_tokens: 400,
            messages: [{
                role: 'user',
                content: `Extract only technical and professional skills from this job posting. Return ONLY a JSON array of skills in English.
Job Title: ${title}
Description: ${description.slice(0, 3000)}
Return ONLY JSON array:`
            }]
        });
        const text = response.content[0].text.trim().replace(/```json|```/g, '');
        const skills = JSON.parse(text);
        return Array.isArray(skills) ? skills.slice(0, 25) : [];
    } catch (err) {
        console.log(`[SKILLS] Extraction failed: ${err.message}`);
        return [];
    }
}

// ─── DEDUPLICATION ──────────────────────────────────────────────────────────
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
        console.error(`[DB] Deduplication error: ${error.message}`);
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
    if (error) console.error(`[DB] Refresh error: ${error.message}`);
}

// ─── DATABASE OPERATIONS ──────────────────────────────────────────────────
async function markCompanyStatus(companyId, status, { touchTimestamp = true } = {}) {
    const updates = { crawl_status: status };
    if (touchTimestamp) updates.last_crawled_at = new Date().toISOString();
    const { error } = await supabase.from('companies').update(updates).eq('Id', companyId);
    if (error) console.error(`[DB] Status update error: ${error.message}`);
}

async function batchInsertJobs(jobs) {
    if (jobs.length === 0) return 0;
    let inserted = 0;
    for (let i = 0; i < jobs.length; i += CONFIG.BATCH_INSERT_SIZE) {
        const batch = jobs.slice(i, i + CONFIG.BATCH_INSERT_SIZE);
        const { error } = await supabase.from('jobs').insert(batch);
        if (error) {
            console.error(`[DB] Batch insert error: ${error.message}`);
        } else {
            inserted += batch.length;
        }
    }
    return inserted;
}

// ─── STATISTICS ────────────────────────────────────────────────────────────
const stats = {
    processed: 0,
    failed_fetch: 0,
    no_jobs: 0,
    with_jobs: 0,
    jobs_saved: 0,
    existing_refreshed: 0,
    errors: 0,
    missing_location: 0,
    missing_company: 0,
    missing_description: 0,
    skipped_validation: 0
};

let processedCount = 0, totalQueued = 0;

function printProgress() {
    const pct = totalQueued > 0 ? ((processedCount / totalQueued) * 100).toFixed(1) : 0;
    console.log(`\n[PROGRESS] ${processedCount}/${totalQueued} (${pct}%)`);
    console.log(`   ✅ With jobs: ${stats.with_jobs} | ❌ No jobs: ${stats.no_jobs} | 🚫 Failed: ${stats.failed_fetch}`);
    console.log(`   💾 Jobs saved: ${stats.jobs_saved} | 🔄 Refreshed: ${stats.existing_refreshed}`);
    console.log(`   ⚠️  Validation failures: ${stats.skipped_validation}`);
}

// ─── CORE PROCESSING ──────────────────────────────────────────────────────
async function processCompany(job) {
    const { companyId, companyName: recordCompanyName, careerUrl } = job.data;
    console.log(`\n[CRAWL] Processing: ${recordCompanyName}`);

    await markCompanyStatus(companyId, 'in_progress', { touchTimestamp: false });

    let effectiveUrl = careerUrl;
    if (!careerUrl || careerUrl.trim() === '') {
        console.log('[CAREER] No URL provided, attempting discovery...');
        effectiveUrl = await discoverCareerPage('https://' + recordCompanyName.replace(/ /g, '').toLowerCase() + '.com');
        if (!effectiveUrl) {
            console.log('[CAREER] Discovery failed');
            await markCompanyStatus(companyId, 'failed');
            return { status: 'failed_fetch', companyId };
        }
    }

    const jobLinks = await extractAllJobLinks(effectiveUrl, recordCompanyName);
    console.log(`[LINKS] Found ${jobLinks.length} job detail links`);

    if (jobLinks.length === 0) {
        await markCompanyStatus(companyId, 'no_jobs');
        return { status: 'no_jobs', companyId };
    }

    const candidateJobs = [];
    const seenInThisRun = new Set();

    for (const link of jobLinks) {
        try {
            const jobData = await withTimeout(
                processJobLink(link, recordCompanyName),
                CONFIG.JOB_TIMEOUT_MS,
                `job ${link.slice(0, 50)}`
            );

            if (!jobData) continue;

            const { title, description, location, seniorityLevel, remoteType, employmentType } = jobData;
            const finalCompanyName = recordCompanyName;

            if (!title || !description || !finalCompanyName) {
                if (!finalCompanyName) stats.missing_company++;
                if (!description) stats.missing_description++;
                stats.skipped_validation++;
                console.log(`[VALIDATION] Skipped job - missing required fields`);
                continue;
            }

            const externalJobId = generateExternalJobId(link);
            if (seenInThisRun.has(externalJobId)) continue;
            seenInThisRun.add(externalJobId);

            const skills = await extractSkillsWithClaude(title, description);

            candidateJobs.push({
                company_id: companyId,
                external_job_id: externalJobId,
                title: title.slice(0, 255),
                raw_description: description,
                structured_skills: skills,
                seniority_level: seniorityLevel,
                location: location ? location.slice(0, 100) : null,
                remote_type: remoteType,
                employment_type: employmentType,
                apply_url: link,
                company_name: finalCompanyName.slice(0, 100),
                is_active: true,
                first_seen_at: new Date().toISOString(),
                last_seen_at: new Date().toISOString()
            });

            console.log(`[JOB] ✅ ${title.slice(0, 60)}`);
            console.log(`      🏢 Company: ${finalCompanyName}`);
            console.log(`      📍 Location: ${location || 'null (not found)'}`);
            console.log(`      📊 Seniority: ${seniorityLevel}`);
            console.log(`      🏠 Remote: ${remoteType}`);
            console.log(`      📋 Employment: ${employmentType}`);
            console.log(`      📝 Description length: ${description.length} chars`);
            console.log(`      🧠 Skills: ${skills.length > 0 ? skills.slice(0, 5).join(', ') + (skills.length > 5 ? ' +' + (skills.length - 5) + ' more' : '') : 'none'}`);

        } catch (err) {
            console.error(`[JOB] Error: ${err.message}`);
        }
    }

    const { newJobs, existingIds } = await splitNewAndExistingJobs(companyId, candidateJobs);

    let jobsSaved = 0;
    if (newJobs.length > 0) {
        jobsSaved = await batchInsertJobs(newJobs);
        console.log(`[DB] Saved ${jobsSaved} new jobs`);
    }

    if (existingIds.length > 0) {
        await refreshExistingJobs(companyId, existingIds);
        console.log(`[DB] Refreshed ${existingIds.length} existing jobs`);
    }

    await markCompanyStatus(companyId, 'completed');
    return {
        status: 'success',
        companyId,
        jobsSaved,
        refreshed: existingIds.length
    };
}

// ─── PROCESS JOB LINK ──────────────────────────────────────────────────────
async function processJobLink(url, fallbackCompanyName) {
    let html = null;
    let jobText = '';

    if (isPdfUrl(url)) {
        const pdfText = await downloadAndParsePDF(url);
        if (pdfText) jobText = pdfText;
    } else {
        const result = await fetchPageWithFallback(url, { waitForSelector: 'body' });
        if (result) html = result.html;
        if (!html) return null;
    }

    if (jobText) {
        const lines = jobText.split('\n').filter(l => l.trim().length > 20);
        const title = lines.length > 0 ? lines[0].trim().slice(0, 255) : 'PDF Job';
        const description = jobText.slice(0, 5000);
        if (!description || description.length < 300) return null;
        return {
            title,
            description,
            location: null,
            seniorityLevel: extractSeniorityLevel(description),
            remoteType: extractRemoteType(description),
            employmentType: extractEmploymentType(description)
        };
    }

    const $ = cheerio.load(html);
    const container = findJobContainer($);
    if (!container || container.text().length < 200) return null;

    const title = extractJobTitleFromContainer(container, $);
    if (!title || title.length < 5) return null;

    const description = extractDescriptionFromHTML(html);
    if (!description || description.length < 300) return null;

    // 🔥 Location extraction: returns null if no city found
    const location = extractLocationFromContainer(container, $);

    const seniorityLevel = extractSeniorityLevel(description);
    const remoteType = extractRemoteType(description);
    const employmentType = extractEmploymentType(description);

    return {
        title,
        description,
        location: location, // may be null
        seniorityLevel,
        remoteType,
        employmentType
    };
}

async function withTimeout(promise, ms, label) {
    let timeoutHandle;
    const timeoutPromise = new Promise((_, reject) => {
        timeoutHandle = setTimeout(() => reject(new Error(`Timeout: ${label}`)), ms);
    });
    try {
        return await Promise.race([promise, timeoutPromise]);
    } finally {
        clearTimeout(timeoutHandle);
    }
}

// ─── QUEUE SETUP ────────────────────────────────────────────────────────────
async function resetStuckCompanies() {
    const { data, error } = await supabase
        .from('companies')
        .update({ crawl_status: 'pending' })
        .eq('ats_type', 'custom')
        .eq('crawl_status', 'in_progress')
        .select('Id');
    if (data && data.length > 0) {
        console.log(`[RESET] Cleared ${data.length} stuck companies`);
    }
}

async function addCustomCompaniesToQueue() {
    const cutoffIso = new Date(Date.now() - CONFIG.RECRAWL_INTERVAL_HOURS * 60 * 60 * 1000).toISOString();
    let page = 0;
    let totalAdded = 0;
    let hasMore = true;

    console.log('[QUEUE] Fetching companies due for crawling...');

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
            console.error(`[QUEUE] Fetch error: ${error.message}`);
            break;
        }

        if (!companies || companies.length === 0) {
            hasMore = false;
            break;
        }

        console.log(`[QUEUE] Page ${page + 1}: Adding ${companies.length} companies...`);

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

    console.log(`[QUEUE] Total queued: ${totalAdded}`);
    totalQueued = totalAdded;
    return totalAdded;
}

// ─── WORKER ──────────────────────────────────────────────────────────────────
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

worker.on('completed', (job, result) => {
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
    processedCount++;
    if (processedCount % 10 === 0 || processedCount === totalQueued) {
        printProgress();
    }
});

worker.on('failed', async (job, err) => {
    console.error(`[FAILED] ${job?.data?.companyName} — ${err.message}`);
    stats.processed++;
    stats.errors++;
    processedCount++;
    if (job?.data?.companyId) {
        await markCompanyStatus(job.data.companyId, 'failed');
    }
    if (processedCount % 10 === 0 || processedCount === totalQueued) {
        printProgress();
    }
});

// ─── ORCHESTRATION ──────────────────────────────────────────────────────────
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
    console.log('[SHUTDOWN] Closing gracefully...');
    try { await worker.close(); } catch (e) {}
    try { await customCrawlQueue.close(); } catch (e) {}
    try { if (sharedBrowser) await sharedBrowser.close(); } catch (e) {}
    try { await redisConnection.quit(); } catch (e) {}
    process.exit(exitCode);
}

function printSummary() {
    console.log('\n' + '═'.repeat(70));
    console.log('PRODUCTION CRAWLER SUMMARY - FINAL REPORT');
    console.log('═'.repeat(70));
    console.log(`Companies processed          : ${stats.processed}`);
    console.log(`Companies with jobs saved    : ${stats.with_jobs}`);
    console.log(`Companies with no jobs       : ${stats.no_jobs}`);
    console.log(`Companies failed to fetch    : ${stats.failed_fetch}`);
    console.log(`Other errors                 : ${stats.errors}`);
    console.log(`Total new jobs saved         : ${stats.jobs_saved}`);
    console.log(`Total existing jobs refreshed: ${stats.existing_refreshed}`);
    console.log(`Validation failures          : ${stats.skipped_validation}`);
    console.log(`  - Missing company name     : ${stats.missing_company}`);
    console.log(`  - Missing description      : ${stats.missing_description}`);
    console.log('═'.repeat(70) + '\n');
}

// ─── MAIN ──────────────────────────────────────────────────────────────────
async function runCrawler() {
    console.log('[START] Production Crawler v15 - EXACT CITY EXTRACTION');
    console.log(`[CONFIG] Concurrency: ${CONFIG.CONCURRENCY} | Job timeout: ${CONFIG.JOB_TIMEOUT_MS}ms`);

    await resetStuckCompanies();
    const totalAdded = await addCustomCompaniesToQueue();

    if (totalAdded === 0) {
        console.log('[QUEUE] No companies queued. Exiting.');
        await shutdown(0);
        return;
    }

    console.log(`[QUEUE] Processing ${totalAdded} companies...`);
    await waitForQueueCompletion();

    console.log('[COMPLETE] All companies processed');
    printProgress();
    printSummary();
    await shutdown(0);
}

// ─── GRACEFUL SHUTDOWN ──────────────────────────────────────────────────
process.on('SIGINT', () => {
    console.log('\n⏹️  Received Ctrl+C. Shutting down gracefully...');
    printProgress();
    printSummary();
    shutdown(0);
});
process.on('SIGTERM', () => {
    console.log('\n⏹️  Received SIGTERM. Shutting down gracefully...');
    printProgress();
    printSummary();
    shutdown(0);
});

process.on('unhandledRejection', (reason) => console.error('[ERROR] Unhandled rejection:', reason));
process.on('uncaughtException', (err) => console.error('[ERROR] Uncaught exception:', err));

runCrawler().catch(err => {
    console.error('[FATAL]', err);
    shutdown(1);
});
