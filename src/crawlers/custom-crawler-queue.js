/**
 * ============================================================================
 * CUSTOM CRAWLER v21 — DEEP CATEGORY CRAWLING + RELEVANCE FILTER
 * ============================================================================
 * For every company, in a single flow:
 *   1) Deep crawl: fetch career page → detect category pages → crawl INTO each
 *      category (max 10) → extract actual JOB detail links (not categories)
 *   2) GPT-4o-mini: validate it's a real job + check relevance to Majori's
 *      3 divisions (IT Consulting, Business/Finance&Legal, Engineering/Construction)
 *      + structure (skills, seniority, employment, remote, cleaned_title)
 *   3) Geocode (city → lat/lng) via Nominatim (cached, rate-limited)
 *   4) Voyage AI embed → skill_embedding vector
 *   5) Save to Supabase
 *
 * Non-jobs and non-relevant jobs are SKIPPED.
 * ============================================================================
 */

const TEST_MODE = process.env.CRAWLER_TEST_MODE === '1';
const NO_RUNTIME = process.env.CRAWLER_NO_RUNTIME === '1';
const ENABLE_RUNTIME = !TEST_MODE && !NO_RUNTIME;
const { Queue, Worker } = ENABLE_RUNTIME ? require('bullmq') : { Queue: null, Worker: null };
const Redis = ENABLE_RUNTIME ? require('ioredis') : null;
const { chromium } = TEST_MODE ? { chromium: null } : require('playwright');
const { createClient } = ENABLE_RUNTIME ? require('@supabase/supabase-js') : { createClient: null };
const OpenAI = ENABLE_RUNTIME ? require('openai') : null;
const ws = ENABLE_RUNTIME ? require('ws') : null;
const cheerio = require('cheerio');
const crypto = require('crypto');
const axios = require('axios');
const { fetchWithScraperAPI } = TEST_MODE ? { fetchWithScraperAPI: null } : require('../utils/scraperapi-config');
const { CRAWLER_TIMEOUTS } = require('../utils/crawler-timeouts');
require('dotenv').config();

function ts() {
    return new Date().toISOString();
}

const _console = {
    log: console.log.bind(console),
    warn: console.warn.bind(console),
    error: console.error.bind(console),
};

const logState = {
    companyName: null,
    companyId: null,
    pageUrl: null,
    jobsSaved: 0,
    jobsFound: 0,
    step: null,
};

function truncateForLog(value, maxLen = 120) {
    const text = String(value || '').trim();
    return text.length > maxLen ? `${text.slice(0, maxLen - 1)}…` : text;
}

function setLogContext(patch = {}) {
    Object.assign(logState, patch);
}

function resetLogContext() {
    setLogContext({
        companyName: null,
        companyId: null,
        pageUrl: null,
        jobsSaved: 0,
        jobsFound: 0,
        step: null,
    });
}

function formatLogPrefix() {
    const parts = [`[${ts()}]`];
    if (logState.companyName || logState.companyId) {
        const companyPart = logState.companyName
            ? `${logState.companyName}${logState.companyId ? `#${logState.companyId}` : ''}`
            : `${logState.companyId}`;
        parts.push(`[company=${truncateForLog(companyPart, 70)}]`);
    }
    if (logState.pageUrl) parts.push(`[page=${truncateForLog(logState.pageUrl, 90)}]`);
    parts.push(`[saved=${logState.jobsSaved ?? 0}]`);
    if (Number.isFinite(logState.jobsFound)) parts.push(`[found=${logState.jobsFound}]`);
    if (logState.step) parts.push(`[${logState.step}]`);
    return parts.join(' ');
}

function logInfo(scope, message) {
    _console.log(`${formatLogPrefix()} [${scope}] ${message}`);
}

function logWarn(scope, message) {
    _console.warn(`${formatLogPrefix()} [${scope}] ${message}`);
}

function logError(scope, message) {
    _console.error(`${formatLogPrefix()} [${scope}] ${message}`);
}

console.log = (...args) => _console.log(formatLogPrefix(), ...args);
console.warn = (...args) => _console.warn(formatLogPrefix(), ...args);
console.error = (...args) => _console.error(formatLogPrefix(), ...args);

// ─── PDF PARSE ────────────────────────────────────────────────────────────
let pdfParse = null;
try { pdfParse = require('pdf-parse'); } catch (e) {
    if (!TEST_MODE) logWarn('PDF', 'pdf-parse not installed');
}

// ─── ENV VALIDATION ───────────────────────────────────────────────────────
const REQUIRED_KEYS = ['SUPABASE_URL', 'SUPABASE_SERVICE_KEY', 'OPENAI_API_KEY', 'VOYAGE_API_KEY'];
for (const key of REQUIRED_KEYS) {
    if (ENABLE_RUNTIME && !process.env[key]) {
        logError('ENV', `Missing env var: ${key}`);
        process.exit(1);
    }
}
if (ENABLE_RUNTIME) logInfo('ENV', 'Environment OK');

// ─── MAJORI'S 3 DIVISIONS (relevance filter) ──────────────────────────────
const DIVISIONS = {
    'IT Consulting': [
        'software', 'developer', 'engineer', 'entwickler', 'programmierer',
        'architect', 'architekt', 'devops', 'cloud', 'data', 'analyst',
        'consultant', 'berater', 'it ', 'systemadmin', 'administrator',
        'network', 'netzwerk', 'security', 'cyber', 'ai ', 'ml ', 'machine learning',
        'fullstack', 'frontend', 'backend', 'javascript', 'python', 'java ',
        '.net', 'php ', 'react', 'angular', 'vue', 'sql', 'database', 'datenbank',
        'scrum master', 'product owner', 'qa ', 'tester', 'automation',
        'sap ', 'salesforce', 'servicenow', 'workday', 'erp ', 'crm ',
        'tech lead', 'cto ', 'ciso ', 'head of it', 'it-manager', 'it manager'
    ],
    'Business (Finance & Legal)': [
        'finance', 'finanzen', 'controlling', 'controller', 'accountant',
        'buchhalter', 'buchhaltung', 'steuer', 'tax ', 'audit', 'legal',
        'jurist', 'lawyer', 'anwalt', 'rechtsanwalt', 'compliance',
        'datenschutzbeauftragter', 'data protection officer', 'dpo ',
        'vertragsmanager', 'contract manager', 'risk', 'risiko',
        'treasury', 'kredit', 'credit', 'investment', 'banking',
        'kaufmännisch', 'kaufmann', 'kauffrau', 'business analyst',
        'business development', 'm&a', 'merger', 'acquisition',
        'payroll', 'lohnbuchhaltung', 'hr payroll'
    ],
    'Engineering (Construction)': [
        'bau', 'construction', 'architekt', 'architect', 'bauingenieur',
        'civil engineer', 'structural', 'tragwerk', 'statik', 'bauleiter',
        'site manager', 'projektleiter bau', 'bauprojektleiter',
        'hochbau', 'tiefbau', 'ingenieurbau', 'brückenbau', 'tunnelbau',
        'bim ', 'bauzeichner', 'cad ', 'konstrukteur', 'konstruktion',
        'gebäudetechnik', 'tga ', 'hkls ', 'heizung', 'sanitär', 'lüftung',
        'elektroplanung', 'vermessung', 'vermesser', 'geomatik',
        'projektsteuerung', 'planung', 'entwurf', 'ausschreibung',
        'verkehrsplanung', 'landschaftsbau', 'landschaftsarchitekt'
    ]
};

// ─── CONFIG ───────────────────────────────────────────────────────────────
const CONFIG = {
    CONCURRENCY: parseInt(process.env.CRAWLER_CONCURRENCY || '10', 10),
    PAGE_SIZE: 1000,
    MAX_JOB_LINKS_PER_COMPANY: parseInt(process.env.MAX_JOB_LINKS_PER_COMPANY || '5000', 10),
    MAX_DISCOVERY_PAGES_PER_COMPANY: parseInt(process.env.MAX_DISCOVERY_PAGES_PER_COMPANY || '1000', 10),
    RECRAWL_INTERVAL_HOURS: parseInt(process.env.RECRAWL_INTERVAL_HOURS || '48', 10),
    COMPANY_TIMEOUT_MS: CRAWLER_TIMEOUTS.COMPANY_TIMEOUT_MS,
    JOB_TIMEOUT_MS: CRAWLER_TIMEOUTS.JOB_TIMEOUT_MS,
    PLAYWRIGHT_TIMEOUT_MS: CRAWLER_TIMEOUTS.PAGE_CONTENT_TIMEOUT_MS,
    BROWSER_RESTART_THRESHOLD: parseInt(process.env.BROWSER_RESTART_THRESHOLD || '100', 10),
    QUEUE_POLL_INTERVAL_MS: 5000,
    RATE_LIMIT_MAX: parseInt(process.env.CRAWLER_RATE_LIMIT_MAX || '5', 10),
    RATE_LIMIT_DURATION_MS: parseInt(process.env.CRAWLER_RATE_LIMIT_DURATION_MS || '1000', 10),
    BATCH_INSERT_SIZE: 50,
    LOAD_MORE_MAX_CLICKS: 50,
    AUTO_SCROLL_MAX_STEPS: 15,
    MAX_CATEGORIES_PER_COMPANY: parseInt(process.env.MAX_CATEGORIES_PER_COMPANY || '1000', 10),
    MAX_SUBDOMAINS_PER_COMPANY: 10,
    MIN_JOB_CONTENT_WORDS: parseInt(process.env.MIN_JOB_CONTENT_WORDS || '80', 10),
    MIN_JOB_PAGE_SCORE: parseInt(process.env.MIN_JOB_PAGE_SCORE || '5', 10),
    PARTIAL_CRAWL_MIN_FETCH_RATIO: parseFloat(process.env.PARTIAL_CRAWL_MIN_FETCH_RATIO || '0.70'),
    PARTIAL_CRAWL_MIN_SAVE_RATIO: parseFloat(process.env.PARTIAL_CRAWL_MIN_SAVE_RATIO || '0.25'),
    GPT_MODEL: 'gpt-4o-mini',
    VOYAGE_MODEL: process.env.VOYAGE_MODEL || 'voyage-3-large',
    NOMINATIM_USER_AGENT: 'customer-matching-crawler/1.0 (contact: support@frontrun.ai)',
};

const QUEUE_NAME = 'custom-crawl';

// ─── NON-JOB URL PATTERNS (skip during crawl) ─────────────────────────────
const NON_JOB_URL_PATTERNS = [
    /datenschutz/i, /datenschutzerklärung/i, /privacy/i, /impressum/i,
    /agb/i, /agbs/i, /cookie/i, /widerruf/i, /barrierefreiheit/i,
    /informationspflicht/i, /nutzungsbedingungen/i, /teilnahmebedingungen/i,
    /\/faq\b/i, /\/kontakt\b/i, /\/contact\b/i, /\/anfahrt\b/i,
    /\/presse\b/i, /\/pressemitteilung/i, /\/news\b/i, /\/blog\b/i,
    /\/galerie\b/i, /\/impressionen\b/i, /\/ueber-uns\b/i, /\/about\b/i,
    /\/team\b/i, /\/management\b/i, /\/historie/i,
    /\/produkte\b/i, /\/produkt\b/i, /\/loesungen\b/i, /\/services\b/i,
    /\/module\b/i, /\/funktionen\b/i, /\/features\b/i,
    /\.jpg$/i, /\.png$/i, /\.zip$/i, /\.docx?$/i,
    /linkedin\.com/i, /facebook\.com/i, /twitter\.com/i, /x\.com/i,
    /instagram\.com/i, /youtube\.com/i, /xing\.com/i,
    /\/login\b/i, /\/register\b/i, /\/signup\b/i
];

function isNonJobUrl(url) {
    return NON_JOB_URL_PATTERNS.some(p => p.test(url));
}

// ─── CATEGORY PAGE DETECTION (crawl INTO, don't treat as job) ─────────────
const CATEGORY_PATTERNS = [
    /\/karriere\/[a-zäöü-]+\/?$/i,           // /karriere/professionals/
    /\/career\/[a-z-]+\/?$/i,                // /career/it-professionals/
    /\/jobs\/[a-z-]+\/?$/i,                  // /jobs/engineering/
    /\/stellenangebote\/[a-z-]+\/?$/i,       // /stellenangebote/it/
    /\/karriere\/(professionals|studium|studierende|ausbildung|praktikum|absolventen|berufserfahrene|schüler|schueler|bewerber|einstieg|führungskräfte|fuehrungskraefte|mitarbeiter)/i,
    /\/karriere\/[a-z-]+\/[a-z-]+\/?$/i,     // /karriere/it/professionals/
    /\/(dein|deine|unsere|ihre)-(studium|praktikum|ausbildung|einstieg|karriere)/i,
    /\/team\/[a-z-]+\/?$/i,                  // /team/engineering/
    /\/jobs\/category\//i,
];

function isCategoryUrl(url) {
    return CATEGORY_PATTERNS.some(p => p.test(url));
}

// ─── STRICT JOB DETAIL URL PATTERNS (must match to accept as job) ─────────
const STRICT_JOB_PATTERNS = [
    /\/job[s]?\/[a-z0-9-_%]+\/?$/i,                  // /job/senior-dev, /jobs/12345
    /\/job[s]?-[a-z0-9-]+/i,                         // /job-senior-dev
    /\/stellenangebot\/[a-z0-9-_%]+/i,               // /stellenangebot/senior-dev
    /\/stelle\/[a-z0-9-_%]+/i,                       // /stelle/xyz
    /\/stellen\/[a-z0-9-_%]+/i,                      // /stellen/xyz
    /\/position[s]?\/[a-z0-9-_%]+/i,                 // /position/xyz
    /\/vacancy\/[a-z0-9-_%]+/i,
    /\/vacancies\/[a-z0-9-_%]+/i,
    /\/apply\/[a-z0-9-_%]+/i,
    /\/bewerbung\/[a-z0-9-_%]+/i,
    /\/offene-stelle[n]?\/[a-z0-9-_%]+/i,
    /\/open-position[s]?\/[a-z0-9-_%]+/i,
    /[?&](job|position|vacancy|stellen)[-_]?id=\d+/i,
    /[?&]id=\d+.*job/i,
    /\/detail\?.*(job|stelle|position)/i,
    /\/detail\/\d+/i,
    /\/(job|stelle|position|vacancy)[s]?\/\d+/i,     // /jobs/12345
    /\/careers?\/[a-z0-9-_%]+\/\d+/i,                // /careers/xyz/12345
    /\/jobs?\/[a-z0-9-_]+\/[a-z0-9-_]+/i,            // /jobs/germany/berlin-senior-dev
];

function isJobDetailUrl(url) {
    if (isNonJobUrl(url)) return false;
    if (isCategoryUrl(url)) return false;
    return STRICT_JOB_PATTERNS.some(p => p.test(url));
}

function isCareerListingUrl(url) {
    if (!url) return false;
    if (isJobDetailUrl(url) || isPdfUrl(url)) return false;
    try {
        const parsed = new URL(url);
        const path = parsed.pathname.replace(/\/+$/, '').toLowerCase();
        const text = `${path} ${parsed.search}`.toLowerCase();
        if (isCategoryUrl(url)) return true;
        if (/\/(career|careers|karriere|jobs|stellenangebote|offene-stellen|vacancies|stellen|bewerbung)$/.test(path)) return true;
        if (/\/(career|careers|karriere|jobs|stellenangebote|offene-stellen|vacancies)\/(search|suche|list|listing|overview|uebersicht|categories|category|bereiche|departments)$/.test(path)) return true;
        if (/(jobs?|stellen|career|karriere).*(page|seite|offset|search|filter)=/.test(text)) return true;
        return false;
    } catch {
        return false;
    }
}

function isLikelyIndividualJobUrl(url) {
    if (!url || isNonJobUrl(url) || isCareerListingUrl(url)) return false;
    if (isJobDetailUrl(url) || isPdfUrl(url)) return true;
    try {
        const parsed = new URL(url);
        const segments = parsed.pathname.split('/').filter(Boolean);
        const last = decodeURIComponent(segments[segments.length - 1] || '');
        return segments.length >= 2 && /\d{3,}|[a-z]+-[a-z]+-[a-z]+/i.test(last) &&
            /(job|jobs|stelle|stellen|position|career|careers|karriere|vacancy|vacancies)/i.test(parsed.pathname);
    } catch {
        return false;
    }
}

// ─── ATS PORTAL DETECTION ─────────────────────────────────────────────────
const ATS_DOMAINS = [
    /\.softgarden\.io/i,
    /\.jobs\.personio\.de/i,
    /\.personio\.de/i,
    /\.myworkdayjobs\.com/i,
    /\.workday\.com/i,
    /\.smartrecruiters\.com/i,
    /\.recruitee\.com/i,
    /\.teamtailor\.com/i,
    /\.successfactors\.com/i,
    /\.sapsf\.com/i,
    /\.umantis\.com/i,
    /\.workwise\.io/i,
    /\.onapply\.com/i,
    /\.rexx-systems\.com/i,
    /\.join\.com/i,
    /\.lever\.co/i,
    /\.greenhouse\.io/i,
    /\.ashbyhq\.com/i,
];

function isAtsUrl(url) {
    return ATS_DOMAINS.some(p => p.test(url));
}

const CAREER_WORDS = [
    'karriere', 'career', 'careers', 'jobs', 'stellenangebote', 'offene stellen',
    'offene-stellen', 'vacancies', 'vacancy', 'bewerbung', 'bewerben',
    'jobangebote', 'stellen', 'join us', 'work with us'
];

const LISTING_WORDS = [
    'alle stellen', 'all jobs', 'job opportunities', 'career opportunities',
    'open positions', 'offene stellen', 'stellenangebote', 'job listings',
    'vacancies', 'category', 'department', 'bereich', 'ausbildung und studium',
    'professionals', 'students', 'graduates', 'entry level'
];

const JOB_EVIDENCE_WORDS = [
    'responsibilities', 'requirements', 'qualifications', 'your tasks', 'your profile',
    'what you bring', 'what we offer', 'apply now', 'apply for this job',
    'job description', 'job id', 'requisition', 'employment type', 'location',
    'aufgaben', 'anforderungen', 'qualifikation', 'profil', 'wir bieten',
    'bewerben', 'jetzt bewerben', 'stellenbeschreibung', 'arbeitsort',
    'vertragsart', 'vollzeit', 'teilzeit', 'befristet', 'unbefristet'
];

const BLOCKED_OR_RETRYABLE_STATUSES = new Set([403, 408, 409, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524]);

const GENERIC_TITLE_PATTERNS = [
    /^career(s)?$/i,
    /^karriere$/i,
    /^jobs?$/i,
    /^stellenangebote$/i,
    /^offene stellen$/i,
    /^open positions?$/i,
    /^job opportunities$/i,
    /^career opportunities$/i,
    /^job opportunities at .+$/i,
    /^ausbildung und studium$/i,
    /^it professionals$/i,
    /^professionals$/i,
    /^students?$/i,
    /^graduates?$/i,
    /^digital solutions/i,
    /^services?$/i,
    /^products?$/i,
    /^departments?$/i
];

function normalizeUrl(rawUrl, baseUrl) {
    if (!rawUrl || typeof rawUrl !== 'string') return null;
    if (/^(mailto|tel|javascript):/i.test(rawUrl) || rawUrl.startsWith('#')) return null;
    try {
        const url = new URL(rawUrl, baseUrl);
        url.hash = '';
        for (const key of [...url.searchParams.keys()]) {
            if (/^(utm_|fbclid$|gclid$|msclkid$|mc_|yclid$)/i.test(key)) {
                url.searchParams.delete(key);
            }
        }
        if ((url.protocol !== 'http:' && url.protocol !== 'https:') || isNonJobUrl(url.href)) return null;
        url.pathname = url.pathname.replace(/\/{2,}/g, '/');
        if (url.pathname.length > 1) url.pathname = url.pathname.replace(/\/+$/, '');
        return url.href;
    } catch {
        return null;
    }
}

function normalizeJobIdentityUrl(rawUrl, baseUrl) {
    const normalized = normalizeUrl(rawUrl, baseUrl);
    if (!normalized) return null;
    try {
        const url = new URL(normalized);
        url.hash = '';
        for (const key of [...url.searchParams.keys()]) {
            if (/^(language|lang)$/i.test(key)) {
                url.searchParams.delete(key);
            }
        }
        if ((url.protocol !== 'http:' && url.protocol !== 'https:') || isNonJobUrl(url.href)) return null;
        url.pathname = url.pathname.replace(/\/{2,}/g, '/');
        if (url.pathname.length > 1) url.pathname = url.pathname.replace(/\/+$/, '');
        return url.href;
    } catch {
        return normalized;
    }
}

function getQueryParam(url, key) {
    try {
        return new URL(url).searchParams.get(key);
    } catch {
        return null;
    }
}

function isEnglishLanguageVariant(url) {
    const lang = (getQueryParam(url, 'language') || getQueryParam(url, 'lang') || '').toLowerCase();
    return lang === 'en' || lang === 'en-us' || lang === 'en-gb';
}

function isGermanLanguageVariant(url) {
    const lang = (getQueryParam(url, 'language') || getQueryParam(url, 'lang') || '').toLowerCase();
    return lang === 'de' || lang === 'de-de' || lang === 'de-at' || lang === 'de-ch';
}

function getDomainRoot(url) {
    try {
        const parts = new URL(url).hostname.replace(/^www\./i, '').split('.');
        return parts.slice(-2).join('.');
    } catch {
        return '';
    }
}

function isSameCompanyUrl(candidateUrl, baseUrl) {
    const root = getDomainRoot(baseUrl);
    if (!root) return true;
    try {
        const host = new URL(candidateUrl).hostname.replace(/^www\./i, '');
        return host === root || host.endsWith('.' + root) || isAtsUrl(candidateUrl);
    } catch {
        return false;
    }
}

function wordCount(text) {
    return String(text || '').trim().split(/\s+/).filter(Boolean).length;
}

function compactText(text, max = 9000) {
    if (!text) return '';
    return String(text)
        .replace(/\s+/g, ' ')
        .replace(/Cookie-Einstellungen|Datenschutzerklaerung|Datenschutzerklärung|Impressum/gi, ' ')
        .trim()
        .slice(0, max);
}

function isGenericJobTitle(title) {
    const cleaned = String(title || '').replace(/\s+/g, ' ').trim();
    if (!cleaned || cleaned.length < 3 || cleaned.length > 180) return true;
    if (GENERIC_TITLE_PATTERNS.some(p => p.test(cleaned))) return true;
    const lower = cleaned.toLowerCase();
    if (LISTING_WORDS.some(w => lower === w || lower.includes(`${w} at `))) return true;
    if (/^(welcome|join|work|working|careers?)\b/i.test(cleaned) && wordCount(cleaned) <= 5) return true;
    return false;
}

function pageHasCareerIntent(url, html) {
    const lowerUrl = String(url || '').toLowerCase();
    const text = compactText(cheerio.load(html || '')('body').text(), 4000).toLowerCase();
    const score =
        CAREER_WORDS.filter(w => lowerUrl.includes(w.replace(/\s+/g, '-')) || text.includes(w)).length +
        JOB_EVIDENCE_WORDS.filter(w => text.includes(w)).length;
    return score >= 2;
}

function isNotFoundReason(reason) {
    return /http_(404|410)|http_(403|429).*scraperapi|scraperapi_failed|blocked_after_scraperapi|no_valid_career_url|resolved_page_not_career_related|career_url_resolved_to_unrelated_page|zero_job_links/i.test(String(reason || ''));
}

function classifyLink(fullUrl, anchorText, contextText, baseUrl) {
    if (!fullUrl || !isSameCompanyUrl(fullUrl, baseUrl)) return 'ignore';
    if (isAtsUrl(fullUrl)) return 'ats';

    const text = `${anchorText || ''} ${contextText || ''}`.toLowerCase();
    const lowerUrl = fullUrl.toLowerCase();
    const lastSegment = (() => {
        try { return decodeURIComponent(new URL(fullUrl).pathname.split('/').filter(Boolean).pop() || ''); }
        catch { return ''; }
    })();

    if (isJobDetailUrl(fullUrl) || isPdfUrl(fullUrl)) return 'job';
    if (isCategoryUrl(fullUrl)) return 'listing';

    const hasCareerUrl = CAREER_WORDS.some(w => lowerUrl.includes(w.replace(/\s+/g, '-')) || lowerUrl.includes(w.replace(/\s+/g, '')));
    const hasListingText = LISTING_WORDS.some(w => text.includes(w));
    const hasJobText = JOB_EVIDENCE_WORDS.some(w => text.includes(w)) || /\b(job|stelle|position|vacancy|bewerb)\b/i.test(text);
    const looksLikeSpecificSlug = /(\d{3,}|[a-z]+-[a-z]+-[a-z]+|[a-z]+_[a-z]+_[a-z]+)/i.test(lastSegment);

    if (hasCareerUrl && (hasListingText || /page=\d+|seite=\d+|offset=\d+|start=\d+/i.test(lowerUrl))) return 'listing';
    if (hasJobText && looksLikeSpecificSlug && !isGenericJobTitle(anchorText)) return 'job';
    if (hasCareerUrl || hasListingText) return 'listing';
    return 'ignore';
}

// ─── CLIENTS ──────────────────────────────────────────────────────────────
const supabase = ENABLE_RUNTIME ? createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
) : null;

const redisConnection = ENABLE_RUNTIME ? new Redis({
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT || '6379', 10),
    maxRetriesPerRequest: null
}) : null;

const openai = ENABLE_RUNTIME ? new OpenAI({ apiKey: process.env.OPENAI_API_KEY }) : null;
if (ENABLE_RUNTIME) logInfo('OPENAI', `Client initialized (${CONFIG.GPT_MODEL})`);

const customCrawlQueue = ENABLE_RUNTIME ? new Queue(QUEUE_NAME, { connection: redisConnection }) : null;

// ─── BROWSER ──────────────────────────────────────────────────────────────
let sharedBrowser = null;
let browserContext = null;
let requestsSinceRestart = 0;

async function getSharedBrowser() {
    if (!sharedBrowser) {
        sharedBrowser = await chromium.launch({ headless: true });
        logInfo('BROWSER', 'Launched');
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
        const prevB = sharedBrowser, prevC = browserContext;
        sharedBrowser = await chromium.launch({ headless: true });
        browserContext = await sharedBrowser.newContext({
            extraHTTPHeaders: { 'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8' },
            viewport: { width: 1280, height: 800 }
        });
        requestsSinceRestart = 0;
        if (prevC) await prevC.close().catch(() => {});
        if (prevB) await prevB.close().catch(() => {});
        logInfo('BROWSER', 'Recycled');
    }
}

// ─── COOKIES ──────────────────────────────────────────────────────────────
const COOKIE_SELECTORS = [
    '#onetrust-accept-btn-handler',
    '#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll',
    '[data-testid="uc-accept-all-button"]',
    'button:has-text("Alle akzeptieren")',
    'button:has-text("Accept all")',
    'button:has-text("Zustimmen")',
    'button:has-text("OK")',
    'button[id*="accept" i]',
    'button[class*="accept" i]',
];

async function acceptCookies(page) {
    for (const sel of COOKIE_SELECTORS) {
        try {
            const btn = page.locator(sel).first();
            if (await btn.isVisible({ timeout: 600 })) {
                await btn.click({ timeout: 1500 });
                await page.waitForTimeout(400);
                return true;
            }
        } catch (e) {}
    }
    return false;
}

// ─── SCROLL ───────────────────────────────────────────────────────────────
async function autoScroll(page, maxSteps = CONFIG.AUTO_SCROLL_MAX_STEPS) {
    let lastH = 0;
    for (let i = 0; i < maxSteps; i++) {
        const h = await page.evaluate(() => document.body.scrollHeight).catch(() => 0);
        if (h === lastH) break;
        lastH = h;
        await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight)).catch(() => {});
        await page.waitForTimeout(600);
    }
}

// ─── FETCH HELPERS ────────────────────────────────────────────────────────
async function fetchWithPlaywright(url, options = {}) {
    const { waitForSelector, timeout = CONFIG.PLAYWRIGHT_TIMEOUT_MS, scroll = false } = options;
    const context = await getBrowserContext();
    const page = await context.newPage();
    try {
        const response = await page.goto(url, { waitUntil: 'domcontentloaded', timeout });
        await acceptCookies(page);
        if (waitForSelector) await page.waitForSelector(waitForSelector, { timeout: CRAWLER_TIMEOUTS.SELECTOR_TIMEOUT_MS }).catch(() => {});
        await page.waitForLoadState('networkidle', { timeout: CRAWLER_TIMEOUTS.LOAD_STATE_TIMEOUT_MS }).catch(() => {});
        await page.waitForTimeout(2000);
        if (scroll) await autoScroll(page);
        const html = await page.content();
        const finalUrl = page.url();
        const status = response ? response.status() : null;
        await page.close().catch(() => {});
        await recycleBrowserIfNeeded();
        return { html, url: finalUrl, status };
    } catch (err) {
        await page.close().catch(() => {});
        await recycleBrowserIfNeeded();
        throw err;
    }
}

async function fetchPageWithFallback(url, options = {}) {
    let playwrightResult = null;
    try {
        playwrightResult = await fetchWithPlaywright(url, options);
        if (!playwrightResult.status || !BLOCKED_OR_RETRYABLE_STATUSES.has(playwrightResult.status)) {
            return playwrightResult;
        }
        logInfo('FETCH', `Playwright HTTP ${playwrightResult.status}: ${url} -> ScraperAPI`);
    } catch (pwErr) {
        logWarn('FETCH', `Playwright failed: ${pwErr.message} → ScraperAPI`);
    }
    try {
        const html = await fetchWithScraperAPI(url, {
            renderJs: true, waitFor: 5000, premium: true, waitForSelector: 'body'
        });
        return { html, url, status: null, usedScraperApi: true };
    } catch (e) {
        logWarn('FETCH', `ScraperAPI failed: ${e.message}`);
        if (playwrightResult) {
            return {
                ...playwrightResult,
                usedScraperApi: false,
                scraperApiFailed: true,
                scraperApiError: e.message
            };
        }
        return {
            html: null,
            url,
            status: null,
            usedScraperApi: false,
            scraperApiFailed: true,
            scraperApiError: e.message
        };
    }
}

// ─── PDF ──────────────────────────────────────────────────────────────────
async function downloadAndParsePDF(url) {
    if (!pdfParse) return null;
    try {
        const r = await axios.get(url, {
            responseType: 'arraybuffer', timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
            headers: { 'User-Agent': 'Mozilla/5.0' }
        });
        const d = await pdfParse(Buffer.from(r.data));
        return d.text || '';
    } catch { return null; }
}

function isPdfUrl(url) {
    return url.toLowerCase().endsWith('.pdf') || url.includes('.pdf?');
}

// ─── CATEGORY DISCOVERY ───────────────────────────────────────────────────
async function discoverCareerPage(baseUrl) {
    const keywords = ['karriere', 'jobs', 'career', 'stellenangebote', 'offene-stellen', 'vacancies'];
    if (keywords.some(k => baseUrl.toLowerCase().includes(k))) return baseUrl;

    let base;
    try { base = new URL(baseUrl).origin; } catch { return null; }

    const paths = [
        '/karriere', '/jobs', '/careers', '/stellenangebote', '/offene-stellen',
        '/en/careers', '/de/karriere', '/about/careers', '/company/careers',
        '/job-angebote', '/vakanz', '/stellen', '/vacancies'
    ];
    for (const p of paths) {
        const testUrl = base + p;
        try {
            const r = await fetchPageWithFallback(testUrl, { waitForSelector: 'body' });
            if (r && r.html && r.html.length > 500) {
                const lower = r.html.toLowerCase();
                if (lower.includes('stellenangebote') || lower.includes('offene stellen') ||
                    lower.includes('karriere') || lower.includes('job')) {
                    return testUrl;
                }
            }
        } catch (e) {}
    }
    return null;
}

// ─── LINK EXTRACTION FROM A PAGE ──────────────────────────────────────────
// Returns: { jobs: Set, categories: Set, subdomains: Set, ats: Set }
async function extractLinksFromPage(baseUrl, companyName) {
    const result = { jobs: new Set(), categories: new Set(), subdomains: new Set(), ats: new Set() };

    try {
        const context = await getBrowserContext();
        const page = await context.newPage();

        await page.goto(baseUrl, { waitUntil: 'domcontentloaded', timeout: CONFIG.PLAYWRIGHT_TIMEOUT_MS });
        await acceptCookies(page);
        await page.waitForLoadState('networkidle', { timeout: CRAWLER_TIMEOUTS.LOAD_STATE_TIMEOUT_MS }).catch(() => {});
        await page.waitForTimeout(2500);

        // Scroll + load-more for lazy-loaded job lists
        await autoScroll(page);
        const loadMoreSelectors = [
            'button:has-text("Mehr laden")', 'button:has-text("Load more")',
            'button:has-text("Weitere anzeigen")', 'button:has-text("Alle anzeigen")',
            'a:has-text("Mehr laden")', '.load-more', '[class*="load-more"]'
        ];
        let clicks = 0, prevH = 0;
        while (clicks < CONFIG.LOAD_MORE_MAX_CLICKS) {
            let clicked = false;
            for (const sel of loadMoreSelectors) {
                try {
                    const btn = page.locator(sel).first();
                    if (await btn.isVisible({ timeout: 800 })) {
                        await btn.click();
                        clicked = true; clicks++;
                        await page.waitForTimeout(2000);
                        const h = await page.evaluate(() => document.body.scrollHeight);
                        if (h === prevH) break;
                        prevH = h;
                        break;
                    }
                } catch (e) {}
            }
            if (!clicked) break;
        }

        const html = await page.content();
        const $ = cheerio.load(html);

        let baseHostname = '';
        try { baseHostname = new URL(baseUrl).hostname; } catch {}

        $('a').each((_, el) => {
            const href = $(el).attr('href');
            const text = $(el).text().trim().toLowerCase();
            if (!href || href.startsWith('#') || href.startsWith('mailto:') || href.startsWith('tel:')) return;

            let fullUrl = href;
            if (!href.startsWith('http')) {
                try { fullUrl = new URL(href, baseUrl).href; } catch { return; }
            }

            if (isNonJobUrl(fullUrl)) return;

            // 1) ATS portal links
            if (isAtsUrl(fullUrl)) {
                result.ats.add(fullUrl.split('?')[0]); // strip query
                return;
            }

            // 2) Subdomain job portals (jobs.example.de, karriere.example.de)
            try {
                const urlObj = new URL(fullUrl);
                const baseRoot = baseHostname.split('.').slice(-2).join('.');
                if (urlObj.hostname !== baseHostname && urlObj.hostname.endsWith(baseRoot)) {
                    if (/^(jobs?|karriere|career|bewerbung|apply|stellen|recruiting|talents)\./i.test(urlObj.hostname)) {
                        result.subdomains.add(urlObj.origin);
                        return;
                    }
                }
            } catch {}

            // 3) Job detail URL
            if (isJobDetailUrl(fullUrl)) {
                result.jobs.add(fullUrl);
                return;
            }

            // 4) Category page (crawl into)
            if (isCategoryUrl(fullUrl)) {
                result.categories.add(fullUrl);
                return;
            }

            // 5) Fallback: strong job-indicator text AND sufficient path depth
            const jobWords = ['stelle', 'job', 'position', 'vacancy', 'bewerbung', 'vakanz'];
            const hasJobText = jobWords.some(w => text.includes(w));
            const pathParts = fullUrl.split('/').filter(Boolean);
            const isDeep = pathParts.length >= 4;
            const looksLikeJobSlug = /\d{3,}|[a-z]+-[a-z]+-[a-z]+/i.test(pathParts[pathParts.length - 1] || '');
            if (hasJobText && isDeep && looksLikeJobSlug) {
                result.jobs.add(fullUrl);
            }
        });

        await page.close().catch(() => {});
        await recycleBrowserIfNeeded();
    } catch (err) {
        console.log(`[EXTRACT] Error on ${baseUrl}: ${err.message}`);
        try {
            const r = await fetchPageWithFallback(baseUrl, { waitForSelector: 'body' });
            if (r && r.html) {
                const $ = cheerio.load(r.html);
                $('a').each((_, el) => {
                    const href = $(el).attr('href');
                    if (!href || href.startsWith('#') || href.startsWith('mailto:')) return;
                    let full = href;
                    if (!href.startsWith('http')) {
                        try { full = new URL(href, baseUrl).href; } catch { return; }
                    }
                    if (isNonJobUrl(full)) return;
                    if (isJobDetailUrl(full)) result.jobs.add(full);
                    else if (isCategoryUrl(full)) result.categories.add(full);
                });
            }
        } catch (e) {}
    }

    return result;
}

// ─── DEEP CRAWL: category → jobs ──────────────────────────────────────────
async function extractAllJobLinks(baseUrl, companyName) {
    logInfo('CRAWL', `Deep-crawling: ${baseUrl}`);
    const allJobs = new Set();
    const allCategories = new Set();
    const allSubdomains = new Set();
    const allAts = new Set();

    // ── PASS 1: Base page ─────────────────────────────────────────────
    const p1 = await extractLinksFromPage(baseUrl, companyName);
    p1.jobs.forEach(u => allJobs.add(u));
    p1.categories.forEach(u => allCategories.add(u));
    p1.subdomains.forEach(u => allSubdomains.add(u));
    p1.ats.forEach(u => allAts.add(u));
    logInfo('PASS 1', `jobs=${p1.jobs.size} categories=${p1.categories.size} subdomains=${p1.subdomains.size} ats=${p1.ats.size}`);

    // ── PASS 2: Crawl INTO each category (max N) ──────────────────────
    const cats = [...allCategories].slice(0, CONFIG.MAX_CATEGORIES_PER_COMPANY);
    for (const catUrl of cats) {
        logInfo('PASS 2', `→ category: ${catUrl}`);
        const p2 = await extractLinksFromPage(catUrl, companyName);
        p2.jobs.forEach(u => allJobs.add(u));
        p2.subdomains.forEach(u => allSubdomains.add(u));
        p2.ats.forEach(u => allAts.add(u));
        // Nested categories (one level deep)
        p2.categories.forEach(u => {
            if (!cats.includes(u) && allCategories.size < CONFIG.MAX_CATEGORIES_PER_COMPANY * 2) {
                allCategories.add(u);
            }
        });
    }

    // ── PASS 2b: Any newly discovered categories ──────────────────────
    const newCats = [...allCategories].filter(u => !cats.includes(u)).slice(0, CONFIG.MAX_CATEGORIES_PER_COMPANY);
    for (const catUrl of newCats) {
        logInfo('PASS 2b', `→ category: ${catUrl}`);
        const p2 = await extractLinksFromPage(catUrl, companyName);
        p2.jobs.forEach(u => allJobs.add(u));
        p2.ats.forEach(u => allAts.add(u));
    }

    // ── PASS 3: Subdomain job portals ─────────────────────────────────
    const subs = [...allSubdomains].slice(0, CONFIG.MAX_SUBDOMAINS_PER_COMPANY);
    for (const subUrl of subs) {
        logInfo('PASS 3', `→ subdomain: ${subUrl}`);
        const p3 = await extractLinksFromPage(subUrl, companyName);
        p3.jobs.forEach(u => allJobs.add(u));
        p3.categories.forEach(u => allCategories.add(u));
    }

    // ── PASS 3b: Crawl into subdomain categories too ──────────────────
    const subCats = [...allCategories].filter(u => !cats.includes(u) && !newCats.includes(u)).slice(0, 5);
    for (const catUrl of subCats) {
        logInfo('PASS 3b', `→ ${catUrl}`);
        const p = await extractLinksFromPage(catUrl, companyName);
        p.jobs.forEach(u => allJobs.add(u));
    }

    // ── PASS 4: ATS portals (take first N as-is; they have their own crawlers) ─
    const atsList = [...allAts].slice(0, 3);
    for (const atsUrl of atsList) {
        logInfo('PASS 4', `ATS detected: ${atsUrl} (handled by dedicated crawlers)`);
        // Not crawled here — dedicated ATS crawlers handle these.
        // But we record the URL so the pipeline knows about it.
    }

    // Filter out garbage
    const finalJobs = [...allJobs].filter(u => !isNonJobUrl(u) && !isCategoryUrl(u));

    logInfo('LINKS', `Final job links: ${finalJobs.length} (scanned ${1 + cats.length + newCats.length + subs.length + subCats.length} pages)`);
    return finalJobs.slice(0, CONFIG.MAX_JOB_LINKS_PER_COMPANY);
}

// ─── CONTAINER FIND ───────────────────────────────────────────────────────
async function validateCareerPage(candidateUrl, companyName) {
    const url = normalizeUrl(candidateUrl);
    if (!url || isNonJobUrl(url)) return null;

    const fetched = await fetchPageWithFallback(url, { waitForSelector: 'body', scroll: true });
    if (!fetched?.html || fetched.html.length < 400) {
        return {
            ok: false,
            url,
            reason: fetched?.scraperApiFailed ? 'scraperapi_failed_empty_or_blocked' : 'career_page_fetch_failed_or_empty'
        };
    }

    const finalUrl = normalizeUrl(fetched.url || url) || url;
    if (fetched.status && fetched.status >= 400) {
        return {
            ok: false,
            url: finalUrl,
            reason: fetched.scraperApiFailed
                ? `career_page_http_${fetched.status}_after_scraperapi_failed`
                : `career_page_http_${fetched.status}`
        };
    }
    if (!pageHasCareerIntent(finalUrl, fetched.html)) {
        return { ok: false, url: finalUrl, reason: 'resolved_page_not_career_related' };
    }

    const $ = cheerio.load(fetched.html);
    const title = compactText($('title').first().text(), 180);
    if (/privacy|datenschutz|impressum|kontakt|contact|blog|news|product|service/i.test(title) &&
        !/career|karriere|job|stellen/i.test(title)) {
        return { ok: false, url: finalUrl, reason: 'career_url_resolved_to_unrelated_page' };
    }

    return { ok: true, url: finalUrl, html: fetched.html };
}

async function discoverCareerPage(baseUrl, companyName = '') {
    const direct = await validateCareerPage(baseUrl, companyName);
    if (direct?.ok) return direct.url;

    let base;
    try { base = new URL(baseUrl).origin; } catch { return null; }

    const paths = [
        '/karriere', '/jobs', '/careers', '/career', '/stellenangebote',
        '/offene-stellen', '/vacancies', '/de/karriere', '/en/careers',
        '/de/jobs', '/en/jobs', '/about/careers', '/company/careers',
        '/job-angebote', '/stellen', '/bewerbung'
    ];

    for (const p of paths) {
        const checked = await validateCareerPage(base + p, companyName);
        if (checked?.ok) return checked.url;
    }
    return null;
}

async function clickLoadMore(page) {
    const selectors = [
        'button:has-text("Mehr laden")', 'button:has-text("Load more")',
        'button:has-text("Weitere anzeigen")', 'button:has-text("Alle anzeigen")',
        'button:has-text("Show more")', 'button:has-text("View more")',
        'a:has-text("Mehr laden")', 'a:has-text("Load more")',
        '.load-more', '[class*="load-more"]', '[data-testid*="load-more"]',
        '[aria-label*="Load more" i]', '[aria-label*="Mehr" i]'
    ];

    let clicks = 0;
    let lastFingerprint = '';
    while (clicks < CONFIG.LOAD_MORE_MAX_CLICKS) {
        let clicked = false;
        for (const sel of selectors) {
            try {
                const btn = page.locator(sel).first();
                if (await btn.isVisible({ timeout: 600 })) {
                    const before = await page.locator('a[href]').count().catch(() => 0);
                    await btn.click({ timeout: CRAWLER_TIMEOUTS.CLICK_TIMEOUT_MS });
                    clicks++;
                    clicked = true;
                    await page.waitForTimeout(1500);
                    await autoScroll(page, 3);
                    const after = await page.locator('a[href]').count().catch(() => before);
                    const fingerprint = `${after}:${await page.evaluate(() => document.body.innerText.length).catch(() => 0)}`;
                    if (after <= before && fingerprint === lastFingerprint) return clicks;
                    lastFingerprint = fingerprint;
                    break;
                }
            } catch {}
        }
        if (!clicked) break;
    }
    return clicks;
}

function extractJsonLdJobUrls($, baseUrl) {
    const urls = new Set();
    $('script[type="application/ld+json"]').each((_, el) => {
        try {
            const raw = $(el).html();
            if (!raw) return;
            const parsed = JSON.parse(raw);
            const stack = Array.isArray(parsed) ? parsed.slice() : [parsed];
            while (stack.length) {
                const item = stack.pop();
                if (!item || typeof item !== 'object') continue;
                if (Array.isArray(item)) {
                    stack.push(...item);
                    continue;
                }
                const type = item['@type'];
                const isJob = type === 'JobPosting' || (Array.isArray(type) && type.includes('JobPosting'));
                if (isJob) {
                    const u = normalizeUrl(item.url || item.sameAs || item.identifier?.value, baseUrl);
                    if (u) urls.add(u);
                }
                for (const value of Object.values(item)) {
                    if (value && typeof value === 'object') stack.push(value);
                }
            }
        } catch {}
    });
    return urls;
}

function addPaginationUrls($, baseUrl, result) {
    $('a[href]').each((_, el) => {
        const text = compactText($(el).text(), 80).toLowerCase();
        const rel = String($(el).attr('rel') || '').toLowerCase();
        const aria = String($(el).attr('aria-label') || '').toLowerCase();
        const full = normalizeUrl($(el).attr('href'), baseUrl);
        if (!full) return;
        const combined = `${text} ${rel} ${aria} ${full}`.toLowerCase();
        if (/next|weiter|naechste|nächste|more|mehr|page=\d+|seite=\d+|offset=\d+|start=\d+/.test(combined)) {
            if (!isNonJobUrl(full) && !isJobDetailUrl(full)) result.listings.add(full);
        }
    });
}

function extractLinksFromHtml(html, pageUrl, rootUrl) {
    const result = { jobs: new Set(), listings: new Set(), ats: new Set() };
    const $ = cheerio.load(html);

    extractJsonLdJobUrls($, pageUrl).forEach(u => result.jobs.add(u));
    addPaginationUrls($, pageUrl, result);

    $('a[href]').each((_, el) => {
        const full = normalizeUrl($(el).attr('href'), pageUrl);
        if (!full || !isSameCompanyUrl(full, rootUrl)) return;

        const text = compactText($(el).text(), 160);
        const context = compactText($(el).closest('li,article,section,div,tr').text(), 1200);
        const kind = classifyLink(full, text, context, rootUrl);

        if (kind === 'job') result.jobs.add(full);
        else if (kind === 'listing') result.listings.add(full);
        else if (kind === 'ats') result.ats.add(full);
    });

    return result;
}

function looksLikeJobCardText(text) {
    const clean = compactText(text, 240);
    if (isGenericJobTitle(clean)) return false;
    if (wordCount(clean) > 18) return false;
    return /(engineer|developer|manager|consultant|analyst|designer|architect|administrator|specialist|lead|director|berater|entwickler|ingenieur|techniker|projektleiter|controller|buchhalter|jurist|devops|data|software|backend|frontend|fullstack|ausbildung|praktikum)/i.test(clean);
}

async function discoverJobDetailUrlsByClicking(page, pageUrl, rootUrl) {
    const found = new Set();
    const locator = page.locator('a, button, [role="button"], [data-href], [data-url]');
    const count = Math.min(await locator.count().catch(() => 0), 120);

    for (let i = 0; i < count; i++) {
        const el = locator.nth(i);
        let text = '';
        let href = null;
        try {
            if (!(await el.isVisible({ timeout: 250 }))) continue;
            text = compactText(await el.innerText({ timeout: 500 }).catch(() => ''), 240);
            href = await el.getAttribute('href')
                || await el.getAttribute('data-href')
                || await el.getAttribute('data-url')
                || await el.getAttribute('aria-label');
        } catch {
            continue;
        }

        const normalizedHref = normalizeUrl(href, pageUrl);
        if (normalizedHref && isLikelyIndividualJobUrl(normalizedHref)) {
            found.add(normalizedHref);
            continue;
        }
        if (!looksLikeJobCardText(text)) continue;

        const beforeUrl = normalizeUrl(page.url()) || pageUrl;
        try {
            const popupPromise = page.context().waitForEvent('page', { timeout: 1500 }).catch(() => null);
            await el.click({ timeout: 2500 });
            const popup = await popupPromise;

            if (popup) {
                await popup.waitForLoadState('domcontentloaded', { timeout: CRAWLER_TIMEOUTS.LOAD_STATE_TIMEOUT_MS }).catch(() => {});
                await popup.waitForTimeout(800).catch(() => {});
                const popupUrl = normalizeUrl(popup.url());
                const popupHtml = await popup.content().catch(() => '');
                if (popupUrl && isSameCompanyUrl(popupUrl, rootUrl) &&
                    (isLikelyIndividualJobUrl(popupUrl) || extractRawJobFromHtml(popupHtml, popupUrl, '').valid)) {
                    found.add(popupUrl);
                }
                await popup.close().catch(() => {});
                continue;
            }

            await page.waitForLoadState('domcontentloaded', { timeout: CRAWLER_TIMEOUTS.LOAD_STATE_TIMEOUT_MS / 2 }).catch(() => {});
            await page.waitForTimeout(800);
            const afterUrl = normalizeUrl(page.url()) || beforeUrl;
            const afterHtml = await page.content().catch(() => '');
            if (afterUrl !== beforeUrl && isSameCompanyUrl(afterUrl, rootUrl) &&
                (isLikelyIndividualJobUrl(afterUrl) || extractRawJobFromHtml(afterHtml, afterUrl, '').valid)) {
                found.add(afterUrl);
            }

            const closeButton = page.locator('button:has-text("Close"), button:has-text("Schließen"), [aria-label*="close" i], [aria-label*="schließen" i]').first();
            if (await closeButton.isVisible({ timeout: 300 }).catch(() => false)) {
                await closeButton.click({ timeout: 1000 }).catch(() => {});
            } else if (afterUrl !== beforeUrl) {
                await page.goBack({ waitUntil: 'domcontentloaded', timeout: CRAWLER_TIMEOUTS.NAVIGATION_TIMEOUT_MS / 12 }).catch(() => {});
                await page.waitForTimeout(500).catch(() => {});
            }
        } catch {
            await page.goto(beforeUrl, { waitUntil: 'domcontentloaded', timeout: CRAWLER_TIMEOUTS.NAVIGATION_TIMEOUT_MS / 12 }).catch(() => {});
        }
    }

    return found;
}

async function extractLinksFromPage(pageUrl, rootUrl) {
    const result = { jobs: new Set(), listings: new Set(), ats: new Set(), finalUrl: pageUrl, failed: false, error: null };
    try {
        const context = await getBrowserContext();
        const page = await context.newPage();
        const response = await page.goto(pageUrl, { waitUntil: 'domcontentloaded', timeout: CONFIG.PLAYWRIGHT_TIMEOUT_MS });
        await acceptCookies(page);
        await page.waitForLoadState('networkidle', { timeout: CRAWLER_TIMEOUTS.LOAD_STATE_TIMEOUT_MS }).catch(() => {});
        await page.waitForTimeout(1500);
        await autoScroll(page);
        await clickLoadMore(page);

        const html = await page.content();
        result.finalUrl = normalizeUrl(page.url()) || pageUrl;
        const status = response ? response.status() : null;
        if (status && status >= 400) {
            await page.close().catch(() => {});
            await recycleBrowserIfNeeded();
            if (BLOCKED_OR_RETRYABLE_STATUSES.has(status)) {
                const fallback = await fetchPageWithFallback(pageUrl, { waitForSelector: 'body', scroll: true });
                if (fallback?.usedScraperApi && fallback.html) {
                    result.finalUrl = normalizeUrl(fallback.url || pageUrl) || pageUrl;
                    const extracted = extractLinksFromHtml(fallback.html, result.finalUrl, rootUrl);
                    extracted.jobs.forEach(u => result.jobs.add(u));
                    extracted.listings.forEach(u => result.listings.add(u));
                    extracted.ats.forEach(u => result.ats.add(u));
                    return result;
                }
                result.failed = true;
                result.error = `http_${status}_after_scraperapi_failed`;
                return result;
            }
            result.failed = true;
            result.error = `http_${status}`;
            return result;
        } else {
            const extracted = extractLinksFromHtml(html, result.finalUrl, rootUrl);
            extracted.jobs.forEach(u => result.jobs.add(u));
            extracted.listings.forEach(u => result.listings.add(u));
            extracted.ats.forEach(u => result.ats.add(u));
            const clickedJobs = await discoverJobDetailUrlsByClicking(page, result.finalUrl, rootUrl);
            clickedJobs.forEach(u => result.jobs.add(u));
        }

        await page.close().catch(() => {});
        await recycleBrowserIfNeeded();
    } catch (err) {
        logWarn('DISCOVERY', `Playwright failed on ${pageUrl}: ${err.message}`);
        try {
            const r = await fetchPageWithFallback(pageUrl, { waitForSelector: 'body', scroll: true });
            if (r?.html) {
                result.finalUrl = normalizeUrl(r.url || pageUrl) || pageUrl;
                const extracted = extractLinksFromHtml(r.html, result.finalUrl, rootUrl);
                extracted.jobs.forEach(u => result.jobs.add(u));
                extracted.listings.forEach(u => result.listings.add(u));
                extracted.ats.forEach(u => result.ats.add(u));
            } else {
                result.failed = true;
                result.error = 'fetch_failed';
            }
        } catch (fallbackErr) {
            result.failed = true;
            result.error = fallbackErr.message;
        }
    }
    return result;
}

async function extractAllJobLinks(baseUrl, companyName) {
    logInfo('CRAWL', `Discovering listings and job details from: ${baseUrl}`);
    const rootUrl = normalizeUrl(baseUrl) || baseUrl;
    const queue = [rootUrl];
    const visited = new Set();
    const jobLinks = new Set();
    const atsLinks = new Set();
    const failedPages = [];
    let listingPagesScanned = 0;

    while (queue.length > 0 && visited.size < CONFIG.MAX_DISCOVERY_PAGES_PER_COMPANY) {
        const current = normalizeUrl(queue.shift(), rootUrl);
        if (!current || visited.has(current) || isNonJobUrl(current) || !isSameCompanyUrl(current, rootUrl)) continue;
        visited.add(current);
        setLogContext({ pageUrl: current, step: 'DISCOVERY' });

        const extracted = await extractLinksFromPage(current, rootUrl);
        listingPagesScanned++;
        if (extracted.failed) failedPages.push({ url: current, reason: extracted.error || 'unknown' });

        extracted.jobs.forEach(u => {
            const normalized = normalizeUrl(u, current);
            if (normalized && !isCategoryUrl(normalized) && !isNonJobUrl(normalized)) jobLinks.add(normalized);
        });
        extracted.ats.forEach(u => atsLinks.add(u));
        extracted.listings.forEach(u => {
            const normalized = normalizeUrl(u, current);
            if (normalized && !visited.has(normalized) && !jobLinks.has(normalized)) queue.push(normalized);
        });

        logInfo('DISCOVERY', `pages=${visited.size} queue=${queue.length} job_links=${jobLinks.size} ats=${atsLinks.size}`);
        if (jobLinks.size >= CONFIG.MAX_JOB_LINKS_PER_COMPANY) {
            logWarn('DISCOVERY', `Hit MAX_JOB_LINKS_PER_COMPANY=${CONFIG.MAX_JOB_LINKS_PER_COMPANY}; increase env var for very large sites.`);
            break;
        }
    }

    if (queue.length > 0) {
        logWarn('DISCOVERY', `Stopped after ${visited.size} listing pages; ${queue.length} pages remain. Increase MAX_DISCOVERY_PAGES_PER_COMPANY for this site.`);
    }
    for (const atsUrl of atsLinks) {
        logInfo('DISCOVERY', `ATS portal found but left for dedicated crawler: ${atsUrl}`);
    }

    const links = [...jobLinks].slice(0, CONFIG.MAX_JOB_LINKS_PER_COMPANY);
    logInfo('LINKS', `job_links=${links.length} listing_pages_scanned=${listingPagesScanned} failed_listing_pages=${failedPages.length}`);
    return {
        links,
        stats: {
            jobLinksFound: links.length,
            listingPagesScanned,
            failedListingPages: failedPages.length,
            atsLinksFound: atsLinks.size,
            stoppedByPageLimit: queue.length > 0,
            stoppedByJobLimit: jobLinks.size >= CONFIG.MAX_JOB_LINKS_PER_COMPANY
        },
        failures: failedPages
    };
}

function findJobContainer($) {
    const sels = [
        '.job-description', '.job-details', '.job-content', '.job-listing',
        '[class*="job-description"]', '[class*="job-detail"]',
        '[class*="job-content"]', '[class*="job-listing"]',
        'article', '.main-content', '#content', '.content-area',
        '.post-content', '.entry-content', '.page-content'
    ];
    for (const s of sels) {
        const el = $(s);
        if (el.length > 0 && el.text().trim().length > 200) return el;
    }
    return $('body');
}

// ─── TITLE EXTRACTION ─────────────────────────────────────────────────────
function extractJobTitleFromContainer(container, $) {
    const sels = ['h1', 'h2', '.job-title', '[class*="job-title"]', '[class*="title"]'];
    for (const s of sels) {
        const el = container.find(s).first();
        if (el.length > 0) {
            const t = el.text().trim();
            if (t.length > 5 && t.length < 200) return t;
        }
    }
    const pt = $('title').text().trim();
    if (pt) {
        const c = pt.replace(/ - (Karriere|Jobs|Stellenangebote|Career|Careers|Startseite|Homepage|Home|Start)$/i, '').trim();
        if (c.length > 5) return c;
    }
    return null;
}

// ─── APPLY URL EXTRACTION ─────────────────────────────────────────────────
function extractApplyUrlFromPage($, pageUrl) {
    const texts = [
        'jetzt bewerben', 'jetzt online bewerben', 'jetzt bewerbung starten',
        'bewerben', 'bewerbung', 'zur bewerbung', 'zur online-bewerbung',
        'apply now', 'apply for this job', 'apply for this position', 'apply'
    ];
    let best = null;
    let bestScore = 0;
    $('a').each((_, el) => {
        const href = $(el).attr('href');
        const text = $(el).text().trim().toLowerCase();
        if (!href || href.startsWith('#') || href.startsWith('mailto:') || href.startsWith('tel:')) return;
        if (texts.some(t => text === t || text.includes(t))) {
            const full = normalizeUrl(href, pageUrl);
            if (!full || isNonJobUrl(full) || isCareerListingUrl(full)) return;

            let score = 1;
            if (isLikelyIndividualJobUrl(full)) score += 4;
            if (/apply|bewerb|application/i.test(full)) score += 2;
            if (getDomainRoot(full) === getDomainRoot(pageUrl)) score += 1;
            if (score > bestScore) {
                best = full;
                bestScore = score;
            }
        }
    });
    return best;
}

function chooseJobPageUrl({ pageUrl, canonicalUrl, jsonUrl, applyUrl }) {
    const candidates = [canonicalUrl, jsonUrl, pageUrl, applyUrl]
        .map(u => normalizeUrl(u, pageUrl))
        .filter(Boolean);
    const german = candidates.find(isGermanLanguageVariant);
    if (german) return german;
    const detail = candidates.find(isLikelyIndividualJobUrl);
    if (detail) return detail;
    return normalizeUrl(pageUrl);
}

function isAcceptableSavedJobUrl(url, rawJob) {
    const normalized = normalizeUrl(url, rawJob?.url || url);
    if (!normalized || isNonJobUrl(normalized) || isCareerListingUrl(normalized)) return false;
    return isLikelyIndividualJobUrl(normalized) || Boolean(rawJob?.valid);
}

// ─── LOCATION EXTRACTION ──────────────────────────────────────────────────
function extractLocationFromContainer(container, $) {
    const text = container.text();
    const jsonLd = $('script[type="application/ld+json"]');
    for (let i = 0; i < jsonLd.length; i++) {
        try {
            const parsed = JSON.parse($(jsonLd[i]).html());
            const items = Array.isArray(parsed) ? parsed : [parsed];
            for (const it of items) {
                if (it && it['@type'] === 'JobPosting' && it.jobLocation) {
                    const loc = it.jobLocation;
                    if (typeof loc === 'string') {
                        const c = extractCity(loc); if (c) return c;
                    }
                    if (loc.address) {
                        const c = loc.address.addressLocality || loc.address.addressRegion;
                        if (c) { const x = extractCity(c); if (x) return x; }
                    }
                }
            }
        } catch {}
    }
    const patterns = [
        /(?:Ort|Standort|Arbeitsort|Location|Stadt|City)[:\s]+([^\n,;.!?]{2,150})/i,
        /(?:Work Location|Job Location)[:\s]+([^\n,;.!?]{2,150})/i
    ];
    for (const p of patterns) {
        const m = text.match(p);
        if (m) { const c = extractCity(m[1].trim()); if (c) return c; }
    }
    return null;
}

function extractCity(text) {
    if (!text || typeof text !== 'string') return null;
    let cleaned = text.replace(/\s+/g, ' ').trim();
    if (cleaned.length < 2 || cleaned.length > 120) return null;
    // Look for known German city names
    const cities = ['Berlin', 'Hamburg', 'Munich', 'München', 'Cologne', 'Köln',
        'Frankfurt', 'Stuttgart', 'Düsseldorf', 'Dortmund', 'Essen', 'Leipzig',
        'Dresden', 'Hannover', 'Hanover', 'Nürnberg', 'Nuremberg', 'Duisburg',
        'Bochum', 'Wuppertal', 'Bielefeld', 'Bonn', 'Mannheim', 'Karlsruhe',
        'Wiesbaden', 'Aachen', 'Kiel', 'Magdeburg', 'Braunschweig', 'Chemnitz',
        'Göttingen', 'Rostock', 'Kassel', 'Saarbrücken', 'Augsburg', 'Ulm',
        'Oldenburg', 'Potsdam', 'Halle', 'Erfurt', 'Jena', 'Ludwigshafen',
        'Trier', 'Freiburg', 'Heidelberg', 'Koblenz', 'Krefeld', 'Neuss',
        'Reutlingen', 'Landshut', 'Passau', 'Regensburg', 'Ingolstadt', 'Fürth',
        'Erlangen', 'Würzburg', 'Darmstadt', 'Mainz', 'Marburg', 'Fulda',
        'Wolfsburg', 'Lübeck', 'Worms', 'Konstanz', 'Kempten', 'Tübingen',
        'Böblingen', 'Ludwigsburg', 'Göppingen', 'Heidenheim', 'Aalen',
        'Vienna', 'Wien', 'Graz', 'Linz', 'Salzburg', 'Innsbruck',
        'Zurich', 'Zürich', 'Geneva', 'Genf', 'Basel', 'Bern', 'Lausanne'];
    for (const c of cities) {
        if (cleaned.includes(c)) return c;
    }
    return null;
}

// ─── DESCRIPTION EXTRACTION ───────────────────────────────────────────────
function extractDescriptionFromHTML(html) {
    const $ = cheerio.load(html);
    const sels = [
        '.job-description', '.job-details', '.description',
        '#job-description', '.job-content', '[class*="job-description"]',
        '[class*="job-detail"]', 'article', '.main-content', '#content',
        '[itemprop="description"]', '[itemprop="jobDescription"]',
        '[class*="stellenanzeige"]', '[class*="aufgaben"]'
    ];
    for (const s of sels) {
        const t = $(s).text().trim();
        if (t && t.length > 300) return cleanDescription(t);
    }
    let text = $('body').text();
    text = text.split('\n')
        .map(l => l.trim())
        .filter(l => l.length > 20)
        .filter(l => !/impressum|datenschutz|agb|cookie|footer|navigation|copyright|©/i.test(l))
        .join('\n');
    return text.length > 300 ? cleanDescription(text) : null;
}

function cleanDescription(text) {
    if (!text) return null;
    text = text.replace(/\s+/g, ' ').trim().replace(/^[\s\-:]+/, '');
    if (text.split(/\s+/).length < 50 || text.length < 300) return null;
    return text.slice(0, 6000);
}

function extractJsonLdJobPostings($) {
    const jobs = [];
    $('script[type="application/ld+json"]').each((_, el) => {
        try {
            const raw = $(el).html();
            if (!raw) return;
            const parsed = JSON.parse(raw);
            const stack = Array.isArray(parsed) ? parsed.slice() : [parsed];
            while (stack.length) {
                const item = stack.pop();
                if (!item || typeof item !== 'object') continue;
                if (Array.isArray(item)) {
                    stack.push(...item);
                    continue;
                }
                const type = item['@type'];
                const isJob = type === 'JobPosting' || (Array.isArray(type) && type.includes('JobPosting'));
                if (isJob) jobs.push(item);
                for (const value of Object.values(item)) {
                    if (value && typeof value === 'object') stack.push(value);
                }
            }
        } catch {}
    });
    return jobs;
}

function extractHiringOrganizationName(jobPosting) {
    const org = jobPosting?.hiringOrganization;
    if (!org) return null;
    if (typeof org === 'string') return org;
    return org.name || org.legalName || null;
}

function extractLocationFromJsonLd(jobPosting) {
    const loc = jobPosting?.jobLocation;
    const items = Array.isArray(loc) ? loc : (loc ? [loc] : []);
    for (const item of items) {
        const address = item?.address || item;
        const city = address?.addressLocality || address?.addressRegion || address?.addressCountry;
        if (city) return extractCity(String(city));
    }
    return null;
}

function scoreJobPage({ url, title, rawText, hasJsonLd, applyUrl }) {
    const lowerText = String(rawText || '').toLowerCase();
    let score = 0;
    if (isJobDetailUrl(url) || isPdfUrl(url)) score += 2;
    if (hasJsonLd) score += 4;
    if (!isGenericJobTitle(title)) score += 2;
    if (applyUrl && applyUrl !== url) score += 1;
    const evidenceHits = JOB_EVIDENCE_WORDS.filter(w => lowerText.includes(w)).length;
    score += Math.min(evidenceHits, 5);
    if (wordCount(rawText) >= CONFIG.MIN_JOB_CONTENT_WORDS) score += 2;
    if (/job\s*id|requisition|referenz|kennziffer|stellen-id|job-id/i.test(rawText || '')) score += 2;
    return score;
}

function extractRawJobFromHtml(html, pageUrl, companyName) {
    const $ = cheerio.load(html);
    const jsonJobs = extractJsonLdJobPostings($);
    const jsonJob = jsonJobs[0] || null;
    const canonicalUrl = normalizeUrl($('link[rel="canonical"]').attr('href'), pageUrl) || pageUrl;
    $('script,style,noscript,nav,footer,header,.cookie-banner,#cookie,[class*="cookie"],[class*="navigation"],[class*="breadcrumb"]').remove();
    const jsonUrl = normalizeUrl(jsonJob?.url, pageUrl);
    const applicationUrl = normalizeUrl(jsonJob?.applicationContact?.url, pageUrl) ||
        extractApplyUrlFromPage($, pageUrl);
    const jobPageUrl = chooseJobPageUrl({ pageUrl, canonicalUrl, jsonUrl, applyUrl: applicationUrl });
    const jsonDescription = jsonJob?.description ? cheerio.load(String(jsonJob.description)).text() : '';

    const container = findJobContainer($);
    const htmlTitle = extractJobTitleFromContainer(container, $);
    const title = compactText(jsonJob?.title || htmlTitle, 220);
    const visibleText = compactText(container.text() || $('body').text(), 12000);
    const rawDescription = cleanDescription([title, jsonDescription, visibleText].filter(Boolean).join('\n\n'));
    const location = extractLocationFromJsonLd(jsonJob) || extractLocationFromContainer(container, $);
    const hiringOrganization = extractHiringOrganizationName(jsonJob);
    const discoveredDetailLinks = extractLinksFromHtml(html, pageUrl, pageUrl).jobs.size;
    const score = scoreJobPage({
        url: pageUrl,
        title,
        rawText: rawDescription || visibleText,
        hasJsonLd: jsonJobs.length > 0,
        applyUrl: applicationUrl || jobPageUrl
    });

    const reasons = [];
    if (!title) reasons.push('missing_title');
    if (isGenericJobTitle(title)) reasons.push('generic_or_category_title');
    if (!rawDescription || wordCount(rawDescription) < CONFIG.MIN_JOB_CONTENT_WORDS) reasons.push('insufficient_job_specific_content');
    if (discoveredDetailLinks >= 3 && score < CONFIG.MIN_JOB_PAGE_SCORE + 2) reasons.push('looks_like_listing_page_not_detail_page');
    if (score < CONFIG.MIN_JOB_PAGE_SCORE) reasons.push(`weak_job_evidence_score_${score}`);
    return {
        valid: reasons.length === 0,
        reasons,
        title,
        rawDescription,
        location,
        applyUrl: jobPageUrl,
        applicationUrl,
        canonicalUrl,
        jobPageUrl,
        score,
        jsonLdCount: jsonJobs.length,
        hiringOrganization
    };
}

function extractRawJobFromPdf(pdfText, pageUrl) {
    const rawDescription = cleanDescription(pdfText);
    const lines = String(pdfText || '').split('\n').map(l => l.trim()).filter(l => l.length > 5);
    const title = compactText(lines.find(l => !isGenericJobTitle(l) && l.length < 180) || '', 180);
    const score = scoreJobPage({ url: pageUrl, title, rawText: rawDescription, hasJsonLd: false, applyUrl: pageUrl });
    const reasons = [];
    if (!title) reasons.push('missing_pdf_title');
    if (isGenericJobTitle(title)) reasons.push('generic_or_category_title');
    if (!rawDescription || wordCount(rawDescription) < CONFIG.MIN_JOB_CONTENT_WORDS) reasons.push('insufficient_pdf_content');
    if (score < CONFIG.MIN_JOB_PAGE_SCORE) reasons.push(`weak_job_evidence_score_${score}`);
    return {
        valid: reasons.length === 0,
        reasons,
        title,
        rawDescription,
        location: null,
        applyUrl: pageUrl,
        applicationUrl: null,
        canonicalUrl: pageUrl,
        jobPageUrl: pageUrl,
        score,
        jsonLdCount: 0,
        hiringOrganization: null
    };
}

function validateStructuredJob(structured, rawJob, companyName) {
    const reasons = [];
    if (!structured) reasons.push('llm_failed');
    if (structured && structured.is_job === false) reasons.push(structured.reason || 'llm_rejected_non_job');
    if (structured && structured.is_relevant === false) reasons.push(structured.relevance_reason || 'llm_rejected_off_division');

    const title = compactText(structured?.cleaned_title || rawJob?.title, 220);
    if (isGenericJobTitle(title)) reasons.push('llm_title_generic_or_category');
    if (!rawJob?.rawDescription || wordCount(rawJob.rawDescription) < CONFIG.MIN_JOB_CONTENT_WORDS) reasons.push('raw_content_too_short');
    if (rawJob?.score < CONFIG.MIN_JOB_PAGE_SCORE) reasons.push('raw_page_lacks_job_evidence');

    return { ok: reasons.length === 0, reasons, title };
}

// ─── GPT: STRUCTURE + VALIDATE + RELEVANCE ────────────────────────────────
async function structureJobWithGPT(rawJobOrTitle, maybeDescription) {
    const rawJob = typeof rawJobOrTitle === 'object'
        ? rawJobOrTitle
        : { title: rawJobOrTitle, rawDescription: maybeDescription };
    const title = rawJob.title || '';
    const description = rawJob.rawDescription || maybeDescription || '';
    const divisionList = Object.keys(DIVISIONS).join(', ');
    const divisionKeywords = Object.entries(DIVISIONS)
        .map(([d, kws]) => `• ${d}: ${kws.slice(0, 25).join(', ')}`)
        .join('\n');

    const prompt = `You are an expert HR data analyst. Structure ONLY the source job posting below.

Source URL: ${rawJob.canonicalUrl || rawJob.url || 'MISSING'}
Company: ${rawJob.companyName || 'MISSING'}
Candidate title from page: ${title || 'MISSING'}
Raw page content:
${(description || '').slice(0, 7000)}

Return ONLY valid JSON:
{
  "is_job": true|false,
  "reason": "if is_job=false, brief reason",
  "is_relevant": true|false,
  "relevance_reason": "if is_relevant=false, why",
  "division": "IT Consulting" | "Business (Finance & Legal)" | "Engineering (Construction)" | null,
  "cleaned_title": "actual position title or null",
  "skills": ["skill1", ...],
  "seniority_level": "junior|mid|senior|lead|executive|null",
  "employment_type": "fulltime|parttime|contract|internship|apprenticeship|null",
  "remote_type": "remote|hybrid|onsite|null",
  "location_city": "city or null"
}

RULES:
1. is_job = FALSE if this is: navigation/category page, legal page (datenschutz/impressum/agb/cookie), marketing page, company culture page, product page, error page, or contains no actual job description.
2. division = the closest matching division name, or null.
3. cleaned_title: clean job role. Remove (m/w/d), (w/m/d), gender tags, location, company name.
4. Do not use category labels, department names, marketing headings, product names, or career-page labels as a title.
5. skills: only skills stated or strongly supported by this source text. Do not invent filler skills.
6. If seniority, remote type, employment type, location, or skills are not present, return null or [].
7. Return ONLY JSON.`;

    try {
        const res = await openai.chat.completions.create({
            model: CONFIG.GPT_MODEL,
            messages: [
                { role: 'system', content: 'Precise job data extractor. Return only valid JSON.' },
                { role: 'user', content: prompt }
            ],
            temperature: 0,
            max_tokens: 700,
            response_format: { type: 'json_object' }
        });
        const content = res.choices[0]?.message?.content?.trim() || '';
        const p = JSON.parse(content);

        const blacklist = ['professional experience', 'general professional skills',
            'team player', 'communication', 'problem solving', 'teamwork',
            'collaboration', 'leadership', 'time management', 'flexibility', 'adaptability'];
        const skills = (Array.isArray(p.skills) ? p.skills : [])
            .map(s => String(s).trim())
            .filter(s => s.length > 1 && !blacklist.includes(s.toLowerCase()))
            .slice(0, 15);

        return {
            is_job: p.is_job !== false,
            reason: p.reason || null,
            is_relevant: p.is_relevant === true,
            relevance_reason: p.relevance_reason || null,
            division: p.division || null,
            cleaned_title: p.cleaned_title || null,
            skills,
            seniority_level: p.seniority_level || null,
            employment_type: p.employment_type || null,
            remote_type: p.remote_type || null,
            location_city: p.location_city || null
        };
    } catch (err) {
        console.warn(`⚠️ GPT error: ${err.message}`);
        return null;
    }
}

// ─── GEOCODING ────────────────────────────────────────────────────────────
const geocodeCache = new Map();
let lastGeo = 0;

async function geocodeCity(city) {
    if (!city || typeof city !== 'string') return { lat: null, lng: null };
    const key = city.toLowerCase().trim();
    if (geocodeCache.has(key)) return geocodeCache.get(key);

    const elapsed = Date.now() - lastGeo;
    if (elapsed < 1100) await new Promise(r => setTimeout(r, 1100 - elapsed));
    lastGeo = Date.now();

    try {
        const r = await axios.get('https://nominatim.openstreetmap.org/search', {
            params: { q: city + ', Germany', format: 'json', limit: 1 },
            headers: { 'User-Agent': CONFIG.NOMINATIM_USER_AGENT },
            timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS
        });
        if (r.data && r.data.length > 0) {
            const result = { lat: parseFloat(r.data[0].lat), lng: parseFloat(r.data[0].lon) };
            geocodeCache.set(key, result);
            return result;
        }
    } catch (e) {}
    const nullR = { lat: null, lng: null };
    geocodeCache.set(key, nullR);
    return nullR;
}

// ─── VOYAGE EMBEDDING ─────────────────────────────────────────────────────
async function embedWithVoyage(text) {
    if (!text || text.length < 10) return null;
    try {
        const r = await axios.post(
            'https://api.voyageai.com/v1/embeddings',
            {
                input: [text.slice(0, 16000)],
                model: CONFIG.VOYAGE_MODEL,
                input_type: 'document'
            },
            {
                headers: {
                    'Authorization': `Bearer ${process.env.VOYAGE_API_KEY}`,
                    'Content-Type': 'application/json'
                },
                timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS
            }
        );
        return r.data?.data?.[0]?.embedding || null;
    } catch (err) {
        console.warn(`[VOYAGE] ${err.message}`);
        return null;
    }
}

// ─── DEDUP ────────────────────────────────────────────────────────────────
function generateExternalJobId(url) {
    const identity = normalizeJobIdentityUrl(url) || String(url || '').trim();
    return crypto.createHash('sha256').update(identity.toLowerCase()).digest('hex').slice(0, 40);
}

// ─── DB ───────────────────────────────────────────────────────────────────
async function markCompanyStatus(companyId, status, { touchTimestamp = true } = {}) {
    const u = { crawl_status: status };
    if (touchTimestamp) u.last_crawled_at = new Date().toISOString();
    const { error } = await supabase.from('companies').update(u).eq('Id', companyId);
    if (error) console.error(`[DB] ${error.message}`);
}

async function batchInsertJobs(rows) {
    if (rows.length === 0) return 0;
    let inserted = 0;
    for (let i = 0; i < rows.length; i += CONFIG.BATCH_INSERT_SIZE) {
        const batch = rows.slice(i, i + CONFIG.BATCH_INSERT_SIZE);
        const { error } = await supabase.from('jobs').upsert(batch, {
            onConflict: 'company_id,external_job_id',
            ignoreDuplicates: false
        });
        if (error) {
            console.error(`[DB] Insert failed for ${batch.length} jobs: ${error.message}`);
            console.error(`[DB] First failed row: company_id=${batch[0]?.company_id || '-'} external_job_id=${batch[0]?.external_job_id || '-'}`);
            throw new Error(`Supabase jobs upsert failed: ${error.message}`);
        }
        inserted += batch.length;
    }
    return inserted;
}

async function logCrawlEvent(companyId, status, payload = {}) {
    const details = {
        ...payload,
        created_at: new Date()
    };
    const row = {
        company_id: companyId,
        status,
        jobs_found: payload.jobs_found ?? payload.jobsSaved ?? null,
        error_message: payload.error_message || payload.reason || null,
        created_at: new Date()
    };
    if (payload.details !== false) row.details = details;
    let { error } = await supabase.from('crawl_logs').insert(row);
    if (error && row.details) {
        delete row.details;
        ({ error } = await supabase.from('crawl_logs').insert(row));
    }
    if (error) console.error(`[DB] crawl_logs: ${error.message}`);
}

async function getActiveJobCount(companyId) {
    const { count, error } = await supabase
        .from('jobs')
        .select('id', { count: 'exact', head: true })
        .eq('company_id', companyId)
        .eq('is_active', true);
    if (error) {
        console.warn(`[DB] active job count failed: ${error.message}`);
        return null;
    }
    return count || 0;
}

// ─── PER-JOB PIPELINE ─────────────────────────────────────────────────────
async function processJobLink(url, companyId, companyName) {
    // 1. Fetch
    let html = null, pdfText = '';
    if (isPdfUrl(url)) {
        pdfText = await downloadAndParsePDF(url) || '';
    } else {
        const r = await fetchPageWithFallback(url, { waitForSelector: 'body' });
        if (r) html = r.html;
        if (!html && !pdfText) return { skip: true, reason: 'fetch_failed' };
    }

    // 2. Extract
    let title, description, location, applyUrl;
    if (pdfText) {
        const lines = pdfText.split('\n').filter(l => l.trim().length > 20);
        title = lines[0]?.trim().slice(0, 255) || 'PDF Job';
        description = pdfText.slice(0, 6000);
        if (!description || description.length < 200) return { skip: true, reason: 'short_pdf' };
        location = null;
        applyUrl = url;
    } else {
        const $ = cheerio.load(html);
        const container = findJobContainer($);
        if (!container || container.text().length < 200) return { skip: true, reason: 'no_container' };
        title = extractJobTitleFromContainer(container, $);
        if (!title || title.length < 5) return { skip: true, reason: 'no_title' };
        description = extractDescriptionFromHTML(html);
        if (!description || description.length < 300) return { skip: true, reason: 'short_desc' };
        location = extractLocationFromContainer(container, $);
        applyUrl = extractApplyUrlFromPage($, url);
    }

    // 3. GPT: validate + relevance + structure
    const s = await structureJobWithGPT(title, description);
    if (!s) return { skip: true, reason: 'gpt_failed' };
    if (!s.is_job) {
        logInfo('JOB', `SKIP not a job: "${title}" (${s.reason || 'rejected'})`);
        return { skip: true, reason: 'not_a_job' };
    }
    // 4. Geocode
    let lat = null, lng = null;
    if (location) {
        const g = await geocodeCity(location);
        lat = g.lat; lng = g.lng;
    }

    // 5. Embed
    const rawDescription = `${s.cleaned_title}\n\n${description}`.trim().slice(0, 6000);
    const embedding = await embedWithVoyage(rawDescription);

    // 6. Return row
    return {
        skip: false,
        row: {
            company_id: companyId,
            external_job_id: generateExternalJobId(url),
            title: s.cleaned_title.slice(0, 255),
            raw_description: rawDescription,
            structured_skills: s.skills.length > 0 ? s.skills : null,
            seniority_level: s.seniority_level,
            location: location ? location.slice(0, 100) : null,
            location_lat: lat,
            location_lng: lng,
            remote_type: s.remote_type,
            employment_type: s.employment_type,
            skill_embedding: embedding,
            apply_url: applyUrl,
            company_name: companyName.slice(0, 100),
            is_active: true,
            first_seen_at: new Date().toISOString(),
            last_seen_at: new Date().toISOString()
        }
    };
}

async function processJobLink(url, companyId, companyName) {
    const normalizedInputUrl = normalizeUrl(url) || url;
    if (isEnglishLanguageVariant(normalizedInputUrl)) {
        logWarn('JOB', `SKIP English variant: ${normalizedInputUrl}`);
        return { skip: true, reason: 'english_language_variant_skipped', url: normalizedInputUrl };
    }
    let html = null;
    let pdfText = '';
    let finalUrl = normalizedInputUrl;

    if (isPdfUrl(normalizedInputUrl)) {
        pdfText = await downloadAndParsePDF(normalizedInputUrl) || '';
    } else {
        const r = await fetchPageWithFallback(normalizedInputUrl, { waitForSelector: 'body', scroll: true });
        if (r) {
            html = r.html;
            finalUrl = normalizeUrl(r.url || normalizedInputUrl) || normalizedInputUrl;
            if (r.scraperApiFailed && (!html || BLOCKED_OR_RETRYABLE_STATUSES.has(r.status))) {
                return { skip: true, reason: r.status ? `http_${r.status}_after_scraperapi_failed` : 'scraperapi_failed_empty_or_blocked', url: finalUrl };
            }
            if (r.status && r.status >= 400) return { skip: true, reason: `http_${r.status}`, url: finalUrl };
        }
        if (!html && !pdfText) return { skip: true, reason: 'fetch_failed', url: finalUrl };
    }

    const rawJob = pdfText
        ? extractRawJobFromPdf(pdfText, finalUrl)
        : extractRawJobFromHtml(html, finalUrl, companyName);
    rawJob.url = finalUrl;
    rawJob.companyName = companyName;

    if (!rawJob.valid) {
        return {
            skip: true,
            reason: rawJob.reasons.join('|') || 'raw_validation_failed',
            url: finalUrl,
            title: rawJob.title
        };
    }

    const structured = await structureJobWithGPT(rawJob);
    const structuredValidation = validateStructuredJob(structured, rawJob, companyName);
    if (!structuredValidation.ok) {
        logInfo('JOB', `SKIP ${rawJob.title || finalUrl} (${structuredValidation.reasons.join('|')})`);
        return {
            skip: true,
            reason: structuredValidation.reasons.join('|') || 'structured_validation_failed',
            url: finalUrl,
            title: rawJob.title
        };
    }

    const location = structured.location_city || rawJob.location;
    let lat = null;
    let lng = null;
    if (location) {
        const geo = await geocodeCity(location);
        lat = geo.lat;
        lng = geo.lng;
    }

    const finalTitle = structuredValidation.title;
    const rawDescription = `${finalTitle}\n\n${rawJob.rawDescription}`.trim().slice(0, 6000);
    const embedding = await embedWithVoyage(rawDescription);
    const jobPageUrl = chooseJobPageUrl({
        pageUrl: finalUrl,
        canonicalUrl: rawJob.jobPageUrl || rawJob.canonicalUrl,
        jsonUrl: rawJob.canonicalUrl,
        applyUrl: rawJob.applyUrl
    });
    if (!isAcceptableSavedJobUrl(jobPageUrl, rawJob)) {
        return {
            skip: true,
            reason: 'job_detail_url_missing_or_points_to_listing',
            url: jobPageUrl || finalUrl,
            title: finalTitle
        };
    }
    const externalSourceId = jobPageUrl || rawJob.canonicalUrl || finalUrl;

    return {
        skip: false,
        row: {
            company_id: companyId,
            external_job_id: generateExternalJobId(`${companyId}:${externalSourceId}`),
            title: finalTitle.slice(0, 255),
            raw_description: rawDescription,
            structured_skills: Array.isArray(structured.skills) && structured.skills.length > 0 ? structured.skills : null,
            seniority_level: structured.seniority_level,
            location: location ? String(location).slice(0, 100) : null,
            location_lat: lat,
            location_lng: lng,
            remote_type: structured.remote_type,
            employment_type: structured.employment_type,
            skill_embedding: embedding,
            apply_url: jobPageUrl,
            company_name: companyName.slice(0, 100),
            ats_source: 'custom',
            is_active: true,
            first_seen_at: new Date().toISOString(),
            last_seen_at: new Date().toISOString()
        },
        meta: {
            url: finalUrl,
            canonicalUrl: rawJob.canonicalUrl,
            applicationUrl: rawJob.applicationUrl,
            rawScore: rawJob.score,
            jsonLdCount: rawJob.jsonLdCount
        }
    };
}

async function withTimeout(promise, ms, label) {
    let h;
    const t = new Promise((_, rej) => { h = setTimeout(() => rej(new Error(`Timeout: ${label}`)), ms); });
    try { return await Promise.race([promise, t]); }
    finally { clearTimeout(h); }
}

// ─── COMPANY PROCESSING ───────────────────────────────────────────────────
async function processCompany(job) {
    const { companyId, companyName, careerUrl } = job.data;
    const startedAt = Date.now();
    logInfo('CRAWL', `Start company="${companyName}" companyId=${companyId} careerUrl=${careerUrl || 'n/a'}`);
    await markCompanyStatus(companyId, 'in_progress', { touchTimestamp: false });

    let effectiveUrl = careerUrl;
    if (!effectiveUrl || effectiveUrl.trim() === '') {
        logWarn('CAREER', `Missing career URL for companyId=${companyId}; skipping instead of guessing from company name`);
        await markCompanyStatus(companyId, 'not_found');
        return { status: 'not_found', companyId };
    }

    logInfo('CAREER', `Using careerUrl=${effectiveUrl}`);

    const links = await extractAllJobLinks(effectiveUrl, companyName);
    logInfo('DISCOVERY', `Found ${links.length} candidate job links for companyId=${companyId}`);
    if (links.length === 0) {
        logWarn('DISCOVERY', `No links discovered for companyId=${companyId}`);
        await markCompanyStatus(companyId, 'no_jobs');
        return { status: 'no_jobs', companyId };
    }

    const rows = [];
    const seen = new Set();
    let skipped = 0;

    let linkIndex = 0;
    for (const link of links) {
        try {
            linkIndex++;
            logInfo('JOB', `Processing ${linkIndex}/${links.length} for companyId=${companyId}`);
            const r = await withTimeout(
                processJobLink(link, companyId, companyName),
                CONFIG.JOB_TIMEOUT_MS,
                `job`
            );
            if (r.skip) { skipped++; continue; }
            if (seen.has(r.row.external_job_id)) continue;
            seen.add(r.row.external_job_id);
            rows.push(r.row);
            logInfo('JOB', `OK ${r.row.title.slice(0, 55)} | ${r.row.location || '-'} | ${r.row.structured_skills?.length || 0} skills`);
        } catch (e) {
            logError('JOB', e.message);
        }
    }

    let saved = 0;
    if (rows.length > 0) saved = await batchInsertJobs(rows);
    logInfo('DB', `Saved ${saved} | Skipped ${skipped} (non-jobs/non-relevant)`);

    await markCompanyStatus(companyId, 'completed');
    logInfo('CRAWL', `Done company="${companyName}" companyId=${companyId} jobsSaved=${saved} skipped=${skipped} elapsedMs=${Date.now() - startedAt}`);
    return { status: 'success', companyId, jobsSaved: saved };
}

// ─── QUEUE ────────────────────────────────────────────────────────────────
async function processCompany(job) {
    const { companyId, companyName, careerUrl } = job.data;
    const metrics = {
        jobLinksFound: 0,
        listingPagesScanned: 0,
        jobPagesFetched: 0,
        jobsStructured: 0,
        invalidRejected: 0,
        duplicateSkipped: 0,
        jobsSaved: 0,
        failedPages: 0,
        activeJobsBefore: null,
        activeJobsAfter: null,
        notFoundReason: null,
        notFoundPages: 0
    };

    setLogContext({
        companyName,
        companyId,
        pageUrl: careerUrl || null,
        jobsSaved: 0,
        jobsFound: 0,
        step: 'START',
    });
    logInfo('CRAWL', `Start company="${companyName}" companyId=${companyId} careerUrl=${careerUrl || 'n/a'}`);
    await markCompanyStatus(companyId, 'in_progress', { touchTimestamp: false });

    metrics.activeJobsBefore = await getActiveJobCount(companyId);

    let effectiveUrl = careerUrl;
    if (effectiveUrl) {
        const validated = await validateCareerPage(effectiveUrl, companyName);
        if (validated?.ok) {
            effectiveUrl = validated.url;
        } else {
            metrics.notFoundReason = validated?.reason || 'invalid_saved_career_url';
            logWarn('CAREER', `Invalid saved URL: ${metrics.notFoundReason}`);
            effectiveUrl = null;
        }
    }

    if (!effectiveUrl) {
        setLogContext({ step: 'DISCOVERY', pageUrl: careerUrl || null });
        logWarn('CAREER', `Missing career URL for companyId=${companyId}; skipping instead of guessing from company name`);
        const reason = isNotFoundReason(metrics.notFoundReason) ? metrics.notFoundReason : 'no_valid_career_url';
        await markCompanyStatus(companyId, 'not_found');
        await logCrawlEvent(companyId, 'not_found', {
            reason,
            error_message: 'No valid career URL was available; existing jobs were preserved.',
            ...metrics
        });
        return { status: 'not_found', companyId, metrics };
    }

    const discovery = await extractAllJobLinks(effectiveUrl, companyName);
    const links = discovery.links || [];
    setLogContext({ step: 'JOBS', pageUrl: effectiveUrl, jobsFound: links.length, jobsSaved: 0 });
    Object.assign(metrics, {
        jobLinksFound: discovery.stats?.jobLinksFound || links.length,
        listingPagesScanned: discovery.stats?.listingPagesScanned || 0,
        failedPages: discovery.stats?.failedListingPages || 0,
        notFoundPages: discovery.failures?.filter(f => isNotFoundReason(f.reason)).length || 0
    });

    if (links.length === 0) {
        const status = 'not_found';
        await markCompanyStatus(companyId, 'not_found');
        await logCrawlEvent(companyId, status, {
            reason: metrics.activeJobsBefore > 0 ? 'zero_job_links_preserved_existing_jobs' : 'zero_job_links',
            error_message: metrics.activeJobsBefore > 0
                ? 'Discovery returned zero job links; existing jobs were preserved.'
                : 'Discovery returned zero job links.',
            careerUrl: effectiveUrl,
            ...metrics
        });
        return { status, companyId, jobsSaved: 0, metrics };
    }

    const rows = [];
    const seen = new Set();
    const rejectedSamples = [];
    const failedSamples = [];
    const rejectionReasons = new Map();

    for (const link of links) {
        try {
            const result = await withTimeout(
                processJobLink(link, companyId, companyName),
                CONFIG.JOB_TIMEOUT_MS,
                `job ${link}`
            );
            if (result.skip) {
                metrics.invalidRejected++;
                if (isNotFoundReason(result.reason)) metrics.notFoundPages++;
                const reason = result.reason || 'unknown_rejection';
                rejectionReasons.set(reason, (rejectionReasons.get(reason) || 0) + 1);
                if (rejectedSamples.length < 20) {
                    rejectedSamples.push({ url: result.url || link, title: result.title || null, reason });
                }
                logWarn('JOB', `REJECT ${result.title || link} (${reason})`);
                continue;
            }

            metrics.jobPagesFetched++;
            metrics.jobsStructured++;
            if (seen.has(result.row.external_job_id)) {
                metrics.duplicateSkipped++;
                continue;
            }
            seen.add(result.row.external_job_id);
            rows.push(result.row);
            setLogContext({ jobsSaved: rows.length });
            logInfo('JOB', `OK ${result.row.title.slice(0, 55)} | ${result.row.location || '-'} | ${result.row.structured_skills?.length || 0} skills`);
        } catch (err) {
            metrics.failedPages++;
            if (failedSamples.length < 20) failedSamples.push({ url: link, reason: err.message });
            logError('JOB', `FAIL ${link}: ${err.message}`);
        }
    }

    logInfo('DB', `rows_ready=${rows.length}`);
    if (rows.length > 0) metrics.jobsSaved = await batchInsertJobs(rows);
    metrics.activeJobsAfter = await getActiveJobCount(companyId);
    setLogContext({ step: 'SAVE', jobsSaved: metrics.jobsSaved });

    const fetchedRatio = links.length > 0 ? metrics.jobPagesFetched / links.length : 0;
    const saveRatio = links.length > 0 ? metrics.jobsSaved / links.length : 0;
    const partial = discovery.stats?.stoppedByPageLimit ||
        metrics.failedPages > 0 ||
        fetchedRatio < CONFIG.PARTIAL_CRAWL_MIN_FETCH_RATIO ||
        saveRatio < CONFIG.PARTIAL_CRAWL_MIN_SAVE_RATIO;
    const allFoundLinksUnavailable = metrics.jobsSaved === 0 &&
        metrics.notFoundPages > 0 &&
        (metrics.notFoundPages + metrics.failedPages + metrics.invalidRejected) >= links.length;

    const rejectionSummary = [...rejectionReasons.entries()]
        .map(([reason, count]) => `${reason}:${count}`)
        .join(', ') || '-';
    logInfo('SUMMARY', `links=${links.length} fetched=${metrics.jobPagesFetched} structured=${metrics.jobsStructured} rejected=${metrics.invalidRejected} duplicates=${metrics.duplicateSkipped} saved=${metrics.jobsSaved} failed=${metrics.failedPages} active_jobs=${metrics.activeJobsAfter ?? '-'}`);
    logInfo('REJECTIONS', rejectionSummary);

    if (allFoundLinksUnavailable) {
        await markCompanyStatus(companyId, 'not_found');
        await logCrawlEvent(companyId, 'not_found', {
            reason: 'all_job_pages_unavailable_after_scraperapi',
            error_message: 'Job links were discovered, but every job page failed or was unavailable after ScraperAPI fallback; existing jobs were preserved.',
            careerUrl: effectiveUrl,
            rejectedSamples,
            failedSamples,
            discovery: discovery.stats,
            ...metrics
        });
        return { status: 'not_found', companyId, jobsSaved: 0, metrics };
    }

    if (partial) {
        await markCompanyStatus(companyId, 'failed');
        await logCrawlEvent(companyId, 'partial', {
            reason: 'partial_custom_crawl_preserved_existing_jobs',
            error_message: 'Custom crawl was incomplete or low-yield; existing jobs were preserved and no deletion was performed.',
            careerUrl: effectiveUrl,
            rejectedSamples,
            failedSamples,
            discovery: discovery.stats,
            ...metrics
        });
        return { status: 'partial', companyId, jobsSaved: metrics.jobsSaved, metrics };
    }

    await markCompanyStatus(companyId, metrics.jobsSaved > 0 ? 'completed' : 'no_jobs');
    await logCrawlEvent(companyId, metrics.jobsSaved > 0 ? 'success' : 'no_jobs', {
        careerUrl: effectiveUrl,
        rejectedSamples,
        failedSamples,
        discovery: discovery.stats,
        ...metrics
    });
    setLogContext({ step: 'DONE', jobsSaved: metrics.jobsSaved, pageUrl: effectiveUrl });
    return { status: metrics.jobsSaved > 0 ? 'success' : 'no_jobs', companyId, jobsSaved: metrics.jobsSaved, metrics };
}

async function resetStuck() {
    const { data } = await supabase.from('companies')
        .update({ crawl_status: 'pending' })
        .eq('ats_type', 'custom')
        .eq('crawl_status', 'in_progress')
        .select('Id');
    if (data?.length) console.log(`[RESET] ${data.length} stuck companies`);
}

async function enqueueCompanies() {
    const cutoff = new Date(Date.now() - CONFIG.RECRAWL_INTERVAL_HOURS * 3600 * 1000).toISOString();
    let page = 0, total = 0, hasMore = true;

    console.log('[QUEUE] Fetching companies...');
    while (hasMore) {
        const start = page * CONFIG.PAGE_SIZE;
        const end = start + CONFIG.PAGE_SIZE - 1;
        const { data, error } = await supabase.from('companies')
            .select('"Id", detected_career_url, "Name"')
            .eq('ats_type', 'custom')
            .not('detected_career_url', 'is', null)
            .neq('crawl_status', 'in_progress')
            .or(`last_crawled_at.is.null,last_crawled_at.lt.${cutoff}`)
            .order('Id', { ascending: true })
            .range(start, end);

        if (error) { console.error(`[QUEUE] ${error.message}`); break; }
        if (!data?.length) { hasMore = false; break; }

        console.log(`[QUEUE] Page ${page + 1}: ${data.length} companies`);
        for (const c of data) {
            await customCrawlQueue.add('crawl-company', {
                companyId: c.Id,
                companyName: c.Name,
                careerUrl: c.detected_career_url
            }, {
                jobId: `company-${c.Id}`,
                attempts: 2,
                backoff: { type: 'exponential', delay: 10000 },
                removeOnComplete: 1000,
                removeOnFail: 5000
            });
            total++;
        }
        if (data.length < CONFIG.PAGE_SIZE) hasMore = false;
        page++;
    }
    console.log(`[QUEUE] Queued ${total}`);
    totalQueued = total;
    return total;
}

// ─── STATS ────────────────────────────────────────────────────────────────
const stats = { processed: 0, failed: 0, partial: 0, not_found: 0, no_jobs: 0, with_jobs: 0, jobs_saved: 0, errors: 0 };
let processedCount = 0, totalQueued = 0;

function printProgress() {
    const pct = totalQueued > 0 ? ((processedCount / totalQueued) * 100).toFixed(1) : 0;
    console.log(`\n[PROGRESS] ${processedCount}/${totalQueued} (${pct}%)`);
    console.log(`  ✅ With jobs: ${stats.with_jobs} | ❌ No jobs: ${stats.no_jobs} | 🚫 Failed: ${stats.failed}`);
    console.log(`  💾 Jobs saved: ${stats.jobs_saved}`);
}

// ─── WORKER ───────────────────────────────────────────────────────────────
const worker = ENABLE_RUNTIME ? new Worker(QUEUE_NAME, async job => {
    return await withTimeout(
        processCompany(job),
        CONFIG.COMPANY_TIMEOUT_MS,
        `company ${job.data.companyName}`
    );
}, {
    connection: redisConnection,
    concurrency: CONFIG.CONCURRENCY,
    limiter: { max: CONFIG.RATE_LIMIT_MAX, duration: CONFIG.RATE_LIMIT_DURATION_MS }
}) : null;

if (worker) worker.on('completed', (job, r) => {
    stats.processed++;
    if (r.status === 'failed_fetch') stats.failed++;
    else if (r.status === 'not_found') stats.not_found++;
    else if (r.status === 'no_jobs') stats.no_jobs++;
    else if (r.status === 'partial') { stats.partial++; stats.jobs_saved += r.jobsSaved || 0; }
    else if (r.status === 'success') { stats.with_jobs++; stats.jobs_saved += r.jobsSaved || 0; }
    else stats.errors++;
    processedCount++;
    if (processedCount % 10 === 0 || processedCount === totalQueued) printProgress();
});

if (worker) worker.on('failed', async (job, err) => {
    console.error(`[FAILED] ${job?.data?.companyName}: ${err.message}`);
    stats.processed++; stats.errors++; processedCount++;
    if (job?.data?.companyId) await markCompanyStatus(job.data.companyId, 'failed');
    if (processedCount % 10 === 0 || processedCount === totalQueued) printProgress();
});

// ─── ORCHESTRATION ────────────────────────────────────────────────────────
async function waitForQueue() {
    return new Promise(resolve => {
        const iv = setInterval(async () => {
            const c = await customCrawlQueue.getJobCounts('waiting', 'active', 'delayed');
            if ((c.waiting + c.active + c.delayed) === 0) { clearInterval(iv); resolve(); }
        }, CONFIG.QUEUE_POLL_INTERVAL_MS);
    });
}

async function shutdown(code = 0) {
    console.log('[SHUTDOWN]');
    try { await worker.close(); } catch {}
    try { await customCrawlQueue.close(); } catch {}
    try { if (sharedBrowser) await sharedBrowser.close(); } catch {}
    try { await redisConnection.quit(); } catch {}
    process.exit(code);
}

function printSummary() {
    console.log('\n' + '═'.repeat(60));
    console.log('CUSTOM CRAWLER v21 — FINAL');
    console.log('═'.repeat(60));
    console.log(`Companies processed : ${stats.processed}`);
    console.log(`  With jobs          : ${stats.with_jobs}`);
    console.log(`  No jobs            : ${stats.no_jobs}`);
    console.log(`  Not found          : ${stats.not_found}`);
    console.log(`  Partial            : ${stats.partial}`);
    console.log(`  Failed             : ${stats.failed}`);
    console.log(`  Errors             : ${stats.errors}`);
    console.log(`Total jobs saved    : ${stats.jobs_saved}`);
    console.log('═'.repeat(60) + '\n');
}

// ─── MAIN ─────────────────────────────────────────────────────────────────
async function run() {
    resetLogContext();
    setLogContext({ step: 'BOOT' });
    console.log('[START] Custom Crawler v21 — Deep Category Crawling + Relevance Filter');
    console.log(`[CONFIG] Concurrency: ${CONFIG.CONCURRENCY} | GPT: ${CONFIG.GPT_MODEL} | Voyage: ${CONFIG.VOYAGE_MODEL}`);
    console.log(`[DIVISIONS] ${Object.keys(DIVISIONS).join(' | ')}`);

    await resetStuck();
    const total = await enqueueCompanies();

    if (total === 0) {
        console.log('[QUEUE] No companies. Exiting.');
        await shutdown(0);
        return;
    }
    console.log(`[QUEUE] Processing ${total} companies...`);
    await waitForQueue();

    resetLogContext();
    setLogContext({ step: 'SUMMARY' });
    console.log('[COMPLETE]');
    printProgress();
    printSummary();
    await shutdown(0);
}

process.on('SIGINT', () => { console.log('\n⏹️  Ctrl+C'); printSummary(); shutdown(0); });
process.on('SIGTERM', () => { console.log('\n⏹️  SIGTERM'); printSummary(); shutdown(0); });
if (ENABLE_RUNTIME) process.on('unhandledRejection', r => console.error('[ERR]', r));
if (ENABLE_RUNTIME) process.on('uncaughtException', e => console.error('[ERR]', e));

if (require.main === module && ENABLE_RUNTIME) {
    run().catch(err => { console.error('[FATAL]', err); shutdown(1); });
}

module.exports = {
    normalizeUrl,
    classifyLink,
    validateCareerPage,
    extractAllJobLinks,
    extractLinksFromHtml,
    extractRawJobFromHtml,
    isAcceptableSavedJobUrl,
    isCareerListingUrl,
    isGenericJobTitle,
    isNotFoundReason,
    validateStructuredJob,
    pageHasCareerIntent
};
