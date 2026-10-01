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
const ws = ENABLE_RUNTIME ? require('ws') : null;
const cheerio = require('cheerio');
const crypto = require('crypto');
const axios = require('axios');
const { AsyncLocalStorage } = require('async_hooks');
const { fetchWithScraperAPI } = TEST_MODE ? { fetchWithScraperAPI: null } : require('../utils/scraperapi-config');
const { CRAWLER_TIMEOUTS } = require('../utils/crawler-timeouts');
const { resolveJobLocation, findCompanyHqLocation, preserveAuthoritativeFieldsForUpsert } = require('../utils/job-enrichment');
const { classifyJobWithLLM } = require('../ai/job-classifier');
const {
    createDiscoveryState,
    fingerprint: discoveryFingerprint,
    validateJobCandidate: validateGenericJobCandidate,
    semanticInteractionScore,
    extractContinuationUrls,
    extractContinuationRequests
} = require('./generic-discovery-engine');
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
    MAX_EXTERNAL_DISCOVERY_PAGES_PER_COMPANY: parseInt(process.env.MAX_EXTERNAL_DISCOVERY_PAGES_PER_COMPANY || '25', 10),
    COMPANY_TIMEOUT_MS: CRAWLER_TIMEOUTS.CUSTOM_CRAWLER_COMPANY_TIMEOUT_MS,
    JOB_TIMEOUT_MS: CRAWLER_TIMEOUTS.JOB_TIMEOUT_MS,
    JOB_DETAIL_CONCURRENCY: Math.max(1, parseInt(process.env.CRAWLER_JOB_DETAIL_CONCURRENCY || '5', 10) || 5),
    PLAYWRIGHT_TIMEOUT_MS: CRAWLER_TIMEOUTS.PAGE_CONTENT_TIMEOUT_MS,
    BROWSER_RESTART_THRESHOLD: parseInt(process.env.BROWSER_RESTART_THRESHOLD || '100', 10),
    QUEUE_POLL_INTERVAL_MS: 5000,
    RATE_LIMIT_MAX: parseInt(process.env.CRAWLER_RATE_LIMIT_MAX || '5', 10),
    RATE_LIMIT_DURATION_MS: parseInt(process.env.CRAWLER_RATE_LIMIT_DURATION_MS || '1000', 10),
    BATCH_INSERT_SIZE: 50,
    LOAD_MORE_MAX_CLICKS: 50,
    AUTO_SCROLL_MAX_STEPS: parseInt(process.env.AUTO_SCROLL_MAX_STEPS || '100', 10),
    MAX_ONCLICK_CANDIDATES_PER_PAGE: parseInt(process.env.MAX_ONCLICK_CANDIDATES_PER_PAGE || '100', 10),
    MAX_EMBEDDED_JSON_SCRIPTS_PER_PAGE: parseInt(process.env.MAX_EMBEDDED_JSON_SCRIPTS_PER_PAGE || '20', 10),
    MAX_EMBEDDED_JSON_BYTES: parseInt(process.env.MAX_EMBEDDED_JSON_BYTES || String(1024 * 1024), 10),
    MAX_EMBEDDED_JSON_CANDIDATES_PER_PAGE: parseInt(process.env.MAX_EMBEDDED_JSON_CANDIDATES_PER_PAGE || '100', 10),
    MAX_DYNAMIC_DOM_CANDIDATES_PER_PAGE: parseInt(process.env.MAX_DYNAMIC_DOM_CANDIDATES_PER_PAGE || '250', 10),
    MAX_IFRAMES_PER_PAGE: parseInt(process.env.MAX_IFRAMES_PER_PAGE || '50', 10),
    MAX_SHADOW_DOM_ROOTS_PER_PAGE: parseInt(process.env.MAX_SHADOW_DOM_ROOTS_PER_PAGE || '50', 10),
    MAX_SHADOW_DOM_CANDIDATES_PER_PAGE: parseInt(process.env.MAX_SHADOW_DOM_CANDIDATES_PER_PAGE || '250', 10),
    MAX_PAGINATION_LINKS_PER_PAGE: parseInt(process.env.MAX_PAGINATION_LINKS_PER_PAGE || '100', 10),
    MAX_JOB_API_RESPONSES_PER_PAGE: parseInt(process.env.MAX_JOB_API_RESPONSES_PER_PAGE || '20', 10),
    MAX_JOB_API_RESPONSE_BYTES: parseInt(process.env.MAX_JOB_API_RESPONSE_BYTES || String(2 * 1024 * 1024), 10),
    MAX_JOB_API_JSON_DEPTH: parseInt(process.env.MAX_JOB_API_JSON_DEPTH || '8', 10),
    API_RESPONSE_DRAIN_TIMEOUT_MS: parseInt(process.env.API_RESPONSE_DRAIN_TIMEOUT_MS || '5000', 10),
    API_RESPONSE_QUIET_WINDOW_MS: parseInt(process.env.API_RESPONSE_QUIET_WINDOW_MS || '150', 10),
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
    /\/faq\b/i, /\/anfahrt\b/i,
    /\/presse\b/i, /\/pressemitteilung/i, /\/news\b/i, /\/blog\b/i,
    /\/galerie\b/i, /\/impressionen\b/i,
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

// URLs that are clearly editorial or asset-oriented are never useful discovery
// targets. Individual job-detail URLs take precedence so a legitimate vacancy
// whose title happens to include one of these terms is not discarded.
const IRRELEVANT_DISCOVERY_URL_WORDS = [
    'blog', 'news', 'story', 'stories', 'podcast', 'media', 'video', 'videos',
    'press', 'presse', 'gallery', 'images', 'events', 'event', 'webinar',
    'download', 'whitepaper', 'magazin'
];

const EXTERNAL_CAREER_SIGNAL_RE = /\b(?:career|careers|karriere|job|jobs|jobangebote?|stellen?|stellenangebote?|vacanc(?:y|ies)|position(?:s)?|bewerbung|bewerben|apply|join(?:-us| us)?|work(?:-with-us| with us))\b/i;
const EXTERNAL_BLOCKED_PATH_RE = /(?:^|[\/_-])(news|blog|event|events|contact|kontakt|legal|privacy|products?|services?|media|social|downloads?)(?:$|[\/_-])/i;
const GENERIC_CAREER_SLUG_RE = /^(?:about|about-us|team|contact|kontakt|company|unternehmen|news|blog|events?|media|products?|services?|privacy|legal|impressum|datenschutz|jobs?|careers?|karriere|stellenangebote?|vacanc(?:y|ies))$/i;
const JOB_TITLE_URL_SIGNAL_RE = /(?:job|stelle|stellen|position|vacanc|bewerb|apply|m-w-d|w-m-d|f-m-d|engineer|developer|administrator|systemadministrator|manager|consultant|analyst|designer|architect|entwickler|ingenieur|techniker|projektleiter|controller|buchhalter|jurist|berater|fachplaner|mitarbeiter|specialist|director|lead)/i;

const HIGH_PRIORITY_DISCOVERY_WORDS = [
    'jobs', 'job', 'career', 'careers', 'career-page', 'karriere', 'stellen',
    'stelle', 'stellenangebote', 'vacancy', 'vacancies', 'position', 'positions',
    'opening', 'openings', 'apply', 'application', 'bewerbung', 'join-us',
    'work-with-us'
];

const MEDIUM_PRIORITY_DISCOVERY_WORDS = [
    'about', 'about-us', 'company', 'unternehmen', 'ueber-uns', 'über-uns',
    'ueber', 'über', 'contact', 'kontakt'
];

function urlContainsDiscoveryWord(url, words) {
    try {
        const parsed = new URL(url);
        const value = decodeURIComponent(`${parsed.pathname} ${parsed.search}`).toLowerCase();
        return words.some(word => value.includes(word));
    } catch {
        return false;
    }
}

function isClearlyIrrelevantDiscoveryUrl(url) {
    if (isJobDetailUrl(url) || isPdfUrl(url)) return false;
    return urlContainsDiscoveryWord(url, IRRELEVANT_DISCOVERY_URL_WORDS);
}

function hasStrongExternalCareerSignal(url, anchorText = '', contextText = '', pageUrl = '') {
    const signalText = `${url || ''} ${anchorText || ''} ${contextText || ''}`;
    let pathAndQuery = '';
    try {
        const parsed = new URL(url);
        pathAndQuery = `${parsed.pathname} ${parsed.search}`.trim();
    } catch {}
    const nonPathSignal = `${anchorText || ''} ${contextText || ''} ${pathAndQuery}`;
    if (EXTERNAL_BLOCKED_PATH_RE.test(pathAndQuery) && !EXTERNAL_CAREER_SIGNAL_RE.test(nonPathSignal)) return false;
    return EXTERNAL_CAREER_SIGNAL_RE.test(signalText) && !isNonJobUrl(url) && !isClearlyIrrelevantDiscoveryUrl(url);
}

function isCareerSlugJobUrl(url) {
    if (!url || isNonJobUrl(url)) return false;
    try {
        const parsed = new URL(url);
        const segments = parsed.pathname.split('/').filter(Boolean).map(segment => decodeURIComponent(segment));
        const careerIndex = segments.findIndex(segment => /^(?:career|careers|karriere|jobs?|stellenangebote?|vacanc(?:y|ies)|position(?:s)?)$/i.test(segment));
        if (careerIndex < 0 || careerIndex >= segments.length - 1) return false;

        const slugSegments = segments.slice(careerIndex + 1);
        const slug = slugSegments.join('-').replace(/[-_]+/g, ' ').trim();
        if (!slug || GENERIC_CAREER_SLUG_RE.test(slug)) return false;
        if (slugSegments.length > 1 && slugSegments.every(segment => GENERIC_CAREER_SLUG_RE.test(segment))) return false;

        const slugLooksSpecific = slug.split(/\s+/).length >= 2 || /\d{3,}/.test(slug);
        return slugLooksSpecific && JOB_TITLE_URL_SIGNAL_RE.test(slug);
    } catch {
        return false;
    }
}

function getDiscoveryUrlPriority(url) {
    if (urlContainsDiscoveryWord(url, HIGH_PRIORITY_DISCOVERY_WORDS)) return 10;
    if (urlContainsDiscoveryWord(url, MEDIUM_PRIORITY_DISCOVERY_WORDS)) return 5;
    return 1;
}

// ─── CATEGORY PAGE DETECTION (crawl INTO, don't treat as job) ─────────────
const CATEGORY_PATTERNS = [
    /\/career\/[a-z-]+\/?$/i,                // /career/it-professionals/
    /\/jobs\/[a-z-]+\/?$/i,                  // /jobs/engineering/
    /\/stellenangebote\/[a-z-]+\/?$/i,       // /stellenangebote/it/
    /\/karriere\/(professionals|studium|studierende|ausbildung|praktikum|absolventen|berufserfahrene|schüler|schueler|bewerber|einstieg|führungskräfte|fuehrungskraefte|mitarbeiter)(?:\/|$)/i,
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
    if (STRICT_JOB_PATTERNS.some(p => p.test(url)) || isCareerSlugJobUrl(url)) return true;
    if (isCategoryUrl(url)) return false;
    return false;
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

const JOB_API_URL_SIGNAL_RE = /(?:api|job|jobs|career|careers|karriere|position|positions|vacanc(?:y|ies)|requisition|stellen|stellenangebote|search|listing|openings?)/i;
const JOB_API_CONTENT_TYPE_RE = /(?:application\/(?:json|ld\+json)|text\/json)/i;
const JOBS_WRAPPER_RE = /<jobs\b[^>]*>([\s\S]*?)<\/jobs>/i;
const JOB_API_TITLE_KEYS = ['title', 'jobTitle', 'position', 'positionTitle', 'jobName', 'name'];
const JOB_API_ID_KEYS = ['jobId', 'job_id', 'requisition', 'requisitionId', 'requisition_id', 'requisitionNumber', 'jobNumber', 'externalJobId', 'external_job_id', 'externalId', 'external_id', 'referenceId', 'reference_id', 'reference', 'id', 'uuid', 'uid'];
const JOB_API_URL_KEYS = ['detailUrl', 'detailURL', 'jobUrl', 'jobURL', 'url', 'jobLink', 'link', 'applyUrl', 'applyURL', 'applicationUrl'];
const JOB_API_LOCATION_KEYS = ['location', 'jobLocation', 'locations', 'city'];
const JOB_API_DESCRIPTION_KEYS = ['description', 'jobDescription', 'descriptionHtml'];
const JOB_API_EMPLOYMENT_KEYS = ['employmentType', 'employment_type'];

const DISCOVERY_COUNTER_KEYS = [
    'html_candidates', 'onclick_candidates', 'embedded_json_candidates',
    'dynamic_dom_candidates', 'iframe_candidates', 'shadow_dom_candidates',
    'api_candidates', 'pagination_candidates', 'load_more_candidates',
    'accepted_job_links', 'duplicate_job_links', 'rejected_job_candidates'
];

function createDiscoveryCounters() {
    return Object.fromEntries(DISCOVERY_COUNTER_KEYS.map(key => [key, 0]));
}

function addDiscoveryCounters(target, source = {}) {
    for (const key of DISCOVERY_COUNTER_KEYS) {
        target[key] = (target[key] || 0) + (Number(source[key]) || 0);
    }
    return target;
}

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

function getDomainHost(url) {
    try {
        const value = String(url || '').trim();
        if (!value) return '';
        return new URL(/^[a-z][a-z0-9+.-]*:\/\//i.test(value) ? value : `https://${value}`)
            .hostname
            .replace(/^www\./i, '')
            .toLowerCase();
    } catch {
        return '';
    }
}

function areRelatedCompanyDomains(firstUrl, secondUrl) {
    const firstHost = getDomainHost(firstUrl);
    const secondHost = getDomainHost(secondUrl);
    if (!firstHost || !secondHost) return false;
    return firstHost === secondHost ||
        firstHost.endsWith(`.${secondHost}`) ||
        secondHost.endsWith(`.${firstHost}`);
}

function isAllowedCareerDomain(url, companyWebsiteUrl) {
    if (!companyWebsiteUrl) return true;
    return isAtsUrl(url) || areRelatedCompanyDomains(url, companyWebsiteUrl);
}

const VERIFIED_CAREER_PAGE_STATUSES = new Set([
    'ok', 'redirect', 'verified', 'blocked_verified', 'verified_detector_error'
]);

function selectOperationalCareerUrl(company = {}) {
    return normalizeUrl(company.detected_career_url) || normalizeUrl(company.career_page_url) || null;
}

function hasVerifiedCareerDetection(company = {}) {
    const genuine = company.detection_signals?.career_discovery?.genuine_career_page === true;
    const status = String(company.career_page_status || '').toLowerCase();
    return genuine || VERIFIED_CAREER_PAGE_STATUSES.has(status);
}

function isTrustedCareerEntryUrl(url, company = {}) {
    const normalized = normalizeUrl(url);
    if (!normalized || !hasVerifiedCareerDetection(company)) return false;
    const verifiedUrls = [company.detected_career_url, company.career_page_url]
        .map(value => normalizeUrl(value))
        .filter(Boolean);
    return verifiedUrls.includes(normalized);
}

function isTrustedCareerRedirect(url, entryUrl) {
    const finalHost = getDomainHost(url);
    const entryHost = getDomainHost(entryUrl);
    if (!finalHost || !entryHost) return false;
    return finalHost === entryHost || finalHost.endsWith(`.${entryHost}`) || entryHost.endsWith(`.${finalHost}`);
}

function isAllowedDiscoveryExternalUrl(url, rootUrl, externalHosts = new Set()) {
    return areRelatedCompanyDomains(url, rootUrl) || isAtsUrl(url) || externalHosts.has(getDomainHost(url));
}

function isAllowedQueuedDiscoveryUrl(url, rootUrl, externalDiscoveryUrls = new Set(), externalHosts = new Set()) {
    if (areRelatedCompanyDomains(url, rootUrl)) return true;
    const host = getDomainHost(url);
    return externalDiscoveryUrls.has(url) && (isAtsUrl(url) || externalHosts.has(host));
}

function logExternalDomainBlocked({ companyName, companyWebsiteUrl, careerUrl, blockedUrl, reason }) {
    logWarn(
        'EXTERNAL_DOMAIN_BLOCKED',
        `company=${truncateForLog(companyName, 70)} companyDomain=${getDomainHost(companyWebsiteUrl) || '-'} ` +
        `careerUrl=${truncateForLog(careerUrl, 140)} blockedDomain=${getDomainHost(blockedUrl) || '-'} reason=${reason}`
    );
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
    return /http_(404|410)|http_(403|429).*scraperapi|scraperapi_failed|blocked_after_scraperapi|no_valid_career_url|resolved_page_not_career_related|career_url_resolved_to_unrelated_page|career_url_(redirected_to_)?external_domain|career_url_external_ats|zero_job_links/i.test(String(reason || ''));
}

function getZeroLinkStatus(discovery = {}) {
    return (discovery.stats?.failedListingPages || 0) > 0 ? 'failed' : 'not_found';
}

function hasTechnicalJobFailure(metrics = {}, rejectionReasons = new Map()) {
    if ((metrics.failedPages || 0) > 0) return true;
    return [...rejectionReasons.keys()].some(reason =>
        /fetch_failed|scraperapi|^http_\d+|timeout|network|browser/i.test(String(reason || ''))
    );
}

function hasSpecificJobTitleEvidence(title) {
    const clean = compactText(title, 180);
    if (isGenericJobTitle(clean) || wordCount(clean) > 18) return false;
    return /\b(mitarbeiter|fachplaner|objektüberwacher|architekt|architect|ingenieur|engineer|entwickler|developer|controller|manager|berater|consultant|analyst|designer|administrator|techniker|projektleiter|sachverständig|supervisor|koordinator|specialist|leiter)\b/i.test(clean) ||
        /\b\(?[mfw]\s*\/\s*[mfw]\s*\/\s*d\)?\b/i.test(clean) ||
        /\b(?:senior|junior)\b/i.test(clean);
}

function hasJobDetailIndicator(fullUrl, anchorText) {
    const lastSegment = (() => {
        try { return decodeURIComponent(new URL(fullUrl).pathname.split('/').filter(Boolean).pop() || ''); }
        catch { return ''; }
    })();
    return /\d{3,}|[a-z]+-[a-z]+(?:-[a-z]+)*/i.test(lastSegment) ||
        /\b\(?[mfw]\s*\/\s*[mfw]\s*\/\s*d\)?\b/i.test(String(anchorText || ''));
}

function classifyLink(fullUrl, anchorText, contextText, baseUrl, options = {}) {
    const { allowUnknownExternal = false, pageUrl = baseUrl, restrictedExternalHost = null } = options;
    if (!fullUrl) return 'ignore';
    const sameCompany = isSameCompanyUrl(fullUrl, baseUrl);
    if (isAtsUrl(fullUrl)) return 'ats';
    if (restrictedExternalHost &&
        getDomainHost(fullUrl) === restrictedExternalHost &&
        !hasStrongExternalCareerSignal(fullUrl, anchorText, contextText, pageUrl)) {
        return 'ignore';
    }
    if (!sameCompany && (!allowUnknownExternal || !hasStrongExternalCareerSignal(fullUrl, anchorText, contextText, pageUrl))) {
        return 'ignore';
    }

    const text = `${anchorText || ''} ${contextText || ''}`.toLowerCase();
    const lowerUrl = fullUrl.toLowerCase();
    const lastSegment = (() => {
        try { return decodeURIComponent(new URL(fullUrl).pathname.split('/').filter(Boolean).pop() || ''); }
        catch { return ''; }
    })();

    if (isJobDetailUrl(fullUrl) || isPdfUrl(fullUrl)) return 'job';

    const hasCareerUrl = CAREER_WORDS.some(w => lowerUrl.includes(w.replace(/\s+/g, '-')) || lowerUrl.includes(w.replace(/\s+/g, '')));
    const hasListingText = LISTING_WORDS.some(w => text.includes(w));
    const hasJobText = JOB_EVIDENCE_WORDS.some(w => text.includes(w)) || /\b(job|stelle|position|vacancy|bewerb)\b/i.test(text);
    const looksLikeSpecificSlug = /(\d{3,}|[a-z]+-[a-z]+-[a-z]+|[a-z]+_[a-z]+_[a-z]+)/i.test(lastSegment);
    const hasSpecificTitle = hasSpecificJobTitleEvidence(anchorText);
    const hasDetailIndicator = hasJobDetailIndicator(fullUrl, anchorText);

    // Career sites often use /karriere/<job-title> URLs. Keep a specific
    // vacancy candidate eligible when its title and URL/context indicate a
    // detail page; processJobLink() still validates the fetched page before
    // it can be structured or upserted.
    if (hasCareerUrl && hasSpecificTitle && (hasDetailIndicator || hasJobText)) return 'job';
    if (hasJobText && looksLikeSpecificSlug && !isGenericJobTitle(anchorText)) return 'job';
    if (isCategoryUrl(fullUrl)) return 'listing';
    if (hasCareerUrl && (hasListingText || /page=\d+|seite=\d+|offset=\d+|start=\d+/i.test(lowerUrl))) return 'listing';
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



const customCrawlQueue = ENABLE_RUNTIME ? new Queue(QUEUE_NAME, { connection: redisConnection }) : null;
const CRAWLER_INSTANCE_LOCK_KEY = `bull:${QUEUE_NAME}:crawler-instance-lock`;
const CRAWLER_INSTANCE_LOCK_TTL_MS = 60000;
const CRAWLER_INSTANCE_LOCK_RENEW_MS = 20000;
let crawlerInstanceLockToken = null;
let crawlerInstanceLockRenewal = null;
let crawlerInstanceLockLost = false;

async function acquireCrawlerInstanceLock() {
    const token = crypto.randomUUID();
    const acquired = await redisConnection.set(
        CRAWLER_INSTANCE_LOCK_KEY,
        token,
        'PX',
        CRAWLER_INSTANCE_LOCK_TTL_MS,
        'NX'
    );
    if (acquired !== 'OK') {
        throw new Error('Another custom crawler instance is already running');
    }

    crawlerInstanceLockToken = token;
    crawlerInstanceLockLost = false;
    crawlerInstanceLockRenewal = setInterval(() => {
        renewCrawlerInstanceLock().catch(error => {
            logError('QUEUE', `Crawler instance lock renewal failed: ${error.message}`);
            if (!crawlerInstanceLockLost) {
                crawlerInstanceLockLost = true;
                shutdown(1);
            }
        });
    }, CRAWLER_INSTANCE_LOCK_RENEW_MS);
}

async function renewCrawlerInstanceLock() {
    if (!crawlerInstanceLockToken) return;
    const renewed = await redisConnection.eval(
        "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end",
        1,
        CRAWLER_INSTANCE_LOCK_KEY,
        crawlerInstanceLockToken,
        String(CRAWLER_INSTANCE_LOCK_TTL_MS)
    );
    if (renewed !== 1) {
        throw new Error('Crawler instance lock was lost');
    }
}

async function releaseCrawlerInstanceLock() {
    if (crawlerInstanceLockRenewal) clearInterval(crawlerInstanceLockRenewal);
    crawlerInstanceLockRenewal = null;
    if (!crawlerInstanceLockToken) return;

    const token = crawlerInstanceLockToken;
    crawlerInstanceLockToken = null;
    crawlerInstanceLockLost = false;
    await redisConnection.eval(
        "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
        1,
        CRAWLER_INSTANCE_LOCK_KEY,
        token
    );
}

// ─── BROWSER ──────────────────────────────────────────────────────────────
let sharedBrowser = null;
let browserContext = null;
let requestsSinceRestart = 0;
let browserRecyclePending = false;
let browserRecyclePromise = null;
const activeCompanyRuns = new Set();
const companyRunContext = new AsyncLocalStorage();

function getAbortError(signal) {
    if (signal?.reason instanceof Error) return signal.reason;

    const error = new Error('Operation aborted');
    error.name = 'AbortError';
    return error;
}

function throwIfAborted(signal) {
    if (signal?.aborted || companyRunContext.getStore()?.timedOut) {
        throw getAbortError(signal);
    }
}

function trackCompanyPage(page) {
    const run = companyRunContext.getStore();
    if (run) run.pages.add(page);
    return page;
}

async function closeTrackedPage(page) {
    const run = companyRunContext.getStore();
    if (run) run.pages.delete(page);
    await page?.close().catch(() => {});
}

async function closeCompanyPages(run) {
    while (run.pages.size > 0) {
        const pages = [...run.pages];
        run.pages.clear();
        await Promise.all(pages.map(page => page.close().catch(() => {})));
    }
}

async function getSharedBrowser() {
    if (sharedBrowser && !sharedBrowser.isConnected()) {
        sharedBrowser = null;
        browserContext = null;
    }
    if (!sharedBrowser) {
        sharedBrowser = await chromium.launch({ headless: true });
        logInfo('BROWSER', 'Launched');
    }
    return sharedBrowser;
}

async function getBrowserContext() {
    if (companyRunContext.getStore()?.timedOut) {
        const error = new Error('Company processing timed out');
        error.name = 'TimeoutError';
        throw error;
    }
    if (browserRecyclePromise) await browserRecyclePromise;
    if (companyRunContext.getStore()?.timedOut) {
        const error = new Error('Company processing timed out');
        error.name = 'TimeoutError';
        throw error;
    }
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
        browserRecyclePending = true;
    }
    await recycleBrowserWhenIdle();
}

function recycleBrowserWhenIdle() {
    if (!browserRecyclePending || activeCompanyRuns.size > 0 || browserRecyclePromise) {
        return browserRecyclePromise || Promise.resolve();
    }

    browserRecyclePromise = (async () => {
        const prevB = sharedBrowser, prevC = browserContext;
        let nextBrowser = null;
        let nextContext = null;
        try {
            nextBrowser = await chromium.launch({ headless: true });
            nextContext = await nextBrowser.newContext({
                extraHTTPHeaders: { 'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8' },
                viewport: { width: 1280, height: 800 }
            });
        } catch (error) {
            if (nextContext) await nextContext.close().catch(() => {});
            if (nextBrowser) await nextBrowser.close().catch(() => {});
            throw error;
        }
        sharedBrowser = nextBrowser;
        browserContext = nextContext;
        requestsSinceRestart = 0;
        browserRecyclePending = false;
        if (prevC) await prevC.close().catch(() => {});
        if (prevB) await prevB.close().catch(() => {});
        logInfo('BROWSER', 'Recycled');
    })().finally(() => {
        browserRecyclePromise = null;
    });

    return browserRecyclePromise;
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
    let lastContentState = '';
    let idleSteps = 0;
    let meaningfulChanges = 0;
    for (let i = 0; i < maxSteps; i++) {
        const state = await page.evaluate(() => ({
            height: document.body?.scrollHeight || 0,
            links: document.querySelectorAll('a[href], [data-url], [data-href]').length,
            records: document.querySelectorAll('article, li, tr, [role="listitem"], [class*="job" i]').length
        })).catch(() => ({ height: 0, links: 0, records: 0 }));
        const contentState = `${state.height}:${state.links}:${state.records}`;
        const changed = contentState !== lastContentState;
        if (changed) meaningfulChanges++;
        idleSteps = changed ? 0 : idleSteps + 1;
        if (idleSteps >= 3) return { steps: i, exhausted: true, limitReached: false, meaningfulChanges };
        lastH = state.height;
        lastContentState = contentState;
        await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight)).catch(() => {});
        await page.waitForTimeout(600);
    }
    return { steps: maxSteps, exhausted: false, limitReached: true, meaningfulChanges };
}

// ─── FETCH HELPERS ────────────────────────────────────────────────────────
async function fetchWithPlaywright(url, options = {}) {
    const { waitForSelector, timeout = CONFIG.PLAYWRIGHT_TIMEOUT_MS, scroll = false, signal } = options;
    throwIfAborted(signal);
    const context = await getBrowserContext();
    throwIfAborted(signal);
    const page = trackCompanyPage(await context.newPage());
    const closeOnAbort = () => page.close().catch(() => {});
    if (signal) signal.addEventListener('abort', closeOnAbort, { once: true });
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
        return { html, url: finalUrl, status };
    } finally {
        if (signal) signal.removeEventListener('abort', closeOnAbort);
        await closeTrackedPage(page);
        await recycleBrowserIfNeeded();
    }
}

async function fetchPageWithFallback(url, options = {}) {
    const { signal } = options;
    throwIfAborted(signal);
    const normalizedUrl = normalizeUrl(url);
    const scraperApiCacheKey = normalizedUrl || null;
    const scraperApiCache = companyRunContext.getStore()?.scraperApiFallbacks;

    if (scraperApiCacheKey && scraperApiCache?.has(scraperApiCacheKey)) {
        logInfo('FETCH', `ScraperAPI cache hit: ${scraperApiCacheKey}`);
        return scraperApiCache.get(scraperApiCacheKey);
    }

    let playwrightResult = null;
    try {
        playwrightResult = await fetchWithPlaywright(url, options);
        if (!playwrightResult.status || !BLOCKED_OR_RETRYABLE_STATUSES.has(playwrightResult.status)) {
            return playwrightResult;
        }
        logInfo('FETCH', `Playwright HTTP ${playwrightResult.status}: ${url} -> ScraperAPI`);
    } catch (pwErr) {
        throwIfAborted(signal);
        logWarn('FETCH', `Playwright failed: ${pwErr.message} → ScraperAPI`);
    }

    throwIfAborted(signal);
    if (scraperApiCacheKey && scraperApiCache?.has(scraperApiCacheKey)) {
        logInfo('FETCH', `ScraperAPI cache hit: ${scraperApiCacheKey}`);
        return scraperApiCache.get(scraperApiCacheKey);
    }

    const scraperApiResult = (async () => {
        try {
            const html = await fetchWithScraperAPI(url, {
                renderJs: true, waitFor: 5000, premium: true, waitForSelector: 'body', signal
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
    })();

    if (scraperApiCacheKey && scraperApiCache) {
        scraperApiCache.set(scraperApiCacheKey, scraperApiResult);
    }

    return scraperApiResult;
}

// ─── PDF ──────────────────────────────────────────────────────────────────
async function downloadAndParsePDF(url, signal) {
    if (!pdfParse) return null;
    throwIfAborted(signal);
    try {
        const r = await axios.get(url, {
            responseType: 'arraybuffer', timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
            headers: { 'User-Agent': 'Mozilla/5.0' }, signal
        });
        const d = await pdfParse(Buffer.from(r.data));
        return d.text || '';
    } catch (err) {
        throwIfAborted(signal);
        return null;
    }
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

// ─── CONTAINER FIND ───────────────────────────────────────────────────────
async function validateCareerPage(candidateUrl, companyName, companyWebsiteUrl, options = {}) {
    const url = normalizeUrl(candidateUrl);
    if (!url || isNonJobUrl(url)) return null;
    if (isAtsUrl(url)) {
        logInfo('ATS', `External ATS career source delegated to dedicated handling: ${url}`);
        return { ok: false, url, reason: 'career_url_external_ats', sourceType: 'external_ats' };
    }
    const trustedEntry = options.trustedCareerUrl &&
        isTrustedCareerEntryUrl(url, options.company || {});
    if (!isAllowedCareerDomain(url, companyWebsiteUrl) && !trustedEntry) {
        logExternalDomainBlocked({
            companyName,
            companyWebsiteUrl,
            careerUrl: url,
            blockedUrl: url,
            reason: 'career_url_external_domain'
        });
        return { ok: false, url, reason: 'career_url_external_domain' };
    }

    const fetched = await fetchPageWithFallback(url, { waitForSelector: 'body', scroll: true });
    if (!fetched?.html || fetched.html.length < 400) {
        return {
            ok: false,
            url,
            reason: fetched?.scraperApiFailed ? 'scraperapi_failed_empty_or_blocked' : 'career_page_fetch_failed_or_empty'
        };
    }

    const finalUrl = normalizeUrl(fetched.url || url) || url;
    if (isAtsUrl(finalUrl)) {
        logInfo('ATS', `Career URL redirected to external ATS source; delegated to dedicated handling: ${finalUrl}`);
        return { ok: false, url: finalUrl, reason: 'career_url_external_ats', sourceType: 'external_ats' };
    }
    if (!isAllowedCareerDomain(finalUrl, companyWebsiteUrl) &&
        !(trustedEntry && isTrustedCareerRedirect(finalUrl, url))) {
        logExternalDomainBlocked({
            companyName,
            companyWebsiteUrl,
            careerUrl: url,
            blockedUrl: finalUrl,
            reason: 'career_url_redirected_to_external_domain'
        });
        return { ok: false, url: finalUrl, reason: 'career_url_redirected_to_external_domain' };
    }
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

function isStrongLoadMoreControl(text = '', aria = '', title = '', className = '') {
    const value = `${text} ${aria} ${title} ${className}`.toLowerCase();
    const label = `${text} ${aria} ${title}`.trim();
    return label.length > 0 && !/cookie|privacy|login|sign\s*in|submit|send|apply|application|contact|register|delete|remove|share|social|language|menu/i.test(value);
}

async function discoverAlternativeCareerUrls(rootUrl, state) {
    let origin;
    try { origin = new URL(rootUrl).origin; } catch { return []; }
    const sitemapUrls = new Set([`${origin}/sitemap.xml`]);
    try {
        const robots = await axios.get(`${origin}/robots.txt`, {
            timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
            responseType: 'text'
        });
        for (const line of String(robots.data || '').split(/\r?\n/)) {
            const match = line.match(/^\s*sitemap:\s*(\S+)/i);
            if (match) sitemapUrls.add(match[1]);
        }
    } catch {}

    const discovered = [];
    for (const sitemapUrl of [...sitemapUrls].slice(0, 10)) {
        try {
            const response = await axios.get(sitemapUrl, {
                timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
                responseType: 'text'
            });
            const locations = [...String(response.data || '').matchAll(/<loc[^>]*>([\s\S]*?)<\/loc>/gi)]
                .map(match => match[1].trim())
                .map(url => state.normalize(url, rootUrl))
                .filter(Boolean)
                .filter(url => !isNonJobUrl(url) && (hasStrongExternalCareerSignal(url, '', '', rootUrl) || isJobDetailUrl(url)));
            discovered.push(...locations.slice(0, 500));
        } catch {}
    }
    return [...new Set(discovered)];
}

async function clickLoadMore(page, options = {}) {
    const selectors = ['button:not([type="submit"]):not([disabled])', '[role="button"]:not([aria-disabled="true"])'];

    let clicks = 0;
    let lastFingerprint = '';
    while (clicks < CONFIG.LOAD_MORE_MAX_CLICKS) {
        let clicked = false;
        const controls = [];
        for (const sel of selectors) {
            try {
                const locator = page.locator(sel);
                const count = Math.min(await locator.count().catch(() => 0), 160);
                for (let index = 0; index < count; index++) controls.push(locator.nth(index));
            } catch {}
        }
        for (const btn of controls) {
            try {
                if (await btn.isVisible({ timeout: 250 })) {
                    const text = await btn.innerText({ timeout: 300 }).catch(() => '');
                    const aria = await btn.getAttribute('aria-label').catch(() => '');
                    const title = await btn.getAttribute('title').catch(() => '');
                    const className = await btn.getAttribute('class').catch(() => '');
                    if (!isStrongLoadMoreControl(text, aria, title, className)) continue;
                    const structuralScore = semanticInteractionScore({
                        role: await btn.getAttribute('role').catch(() => 'button'),
                        tagName: await btn.evaluate(node => node.tagName).catch(() => 'BUTTON'),
                        textContent: text,
                        ariaLabel: aria,
                        title,
                        nearbyRecordCount: await btn.locator('xpath=ancestor::*[self::article or self::li or self::tr or @data-record]').count().catch(() => 0),
                        nearbyLinkCount: await btn.locator('xpath=ancestor::*[self::section or self::main or self::form]//a[@href]').count().catch(() => 0),
                        hasFormControl: await btn.locator('xpath=ancestor::form').count().catch(() => 0) > 0,
                        hasTarget: Boolean(await btn.getAttribute('data-target').catch(() => null) || await btn.getAttribute('aria-controls').catch(() => null))
                    });
                    if (structuralScore < 4) continue;
                    const before = await page.locator('a[href]').count().catch(() => 0);
                    const controlKey = `${text}|${aria}|${title}|${className}|${before}`;
                    if (options.seenControls?.has(controlKey)) continue;
                    options.seenControls?.add(controlKey);
                    if (options.discoveryState) {
                        const state = options.discoveryState.interactionState(
                            page.url(), controlKey, await page.url()
                        );
                        if (!state.fresh) continue;
                    }
                    await btn.click({ timeout: CRAWLER_TIMEOUTS.CLICK_TIMEOUT_MS });
                    clicks++;
                    clicked = true;
                    await page.waitForTimeout(1500);
                    await autoScroll(page, 3);
                    const after = await page.locator('a[href]').count().catch(() => before);
                    const fingerprint = `${after}:${await page.evaluate(() => document.body.innerText.length).catch(() => 0)}`;
                    if (after <= before && fingerprint === lastFingerprint) continue;
                    await options.onClick?.({ page, clicks, before, after });
                    lastFingerprint = fingerprint;
                    break;
                }
            } catch {}
        }
        if (!clicked) break;
    }
    return clicks;
}

async function exploreEmploymentFilters(page, pageUrl, rootUrl, signal, options = {}) {
    const controls = page.locator('select, [role="combobox"]');
    const count = Math.min(await controls.count().catch(() => 0), 20);
    let explored = 0;
    for (let index = 0; index < count; index++) {
        throwIfAborted(signal);
        const control = controls.nth(index);
        try {
            if (!(await control.isVisible({ timeout: 300 }))) continue;
            const optionsData = await control.locator('option').evaluateAll(nodes => nodes.map(node => ({
                value: node.value,
                label: (node.textContent || '').trim(),
                disabled: node.disabled
            }))).catch(() => []);
            const usable = optionsData.filter(option => option.value && !option.disabled);
            const formContext = await control.locator('xpath=ancestor::form | xpath=ancestor::*[self::section or self::main or @role="search"]').count().catch(() => 0);
            const nearbyRecords = await control.locator('xpath=ancestor::*[self::article or self::li or self::tr or @data-record] | xpath=ancestor::*[self::section or self::main]//a[@href]').count().catch(() => 0);
            if (usable.length < 1 || usable.length > 50 || (formContext === 0 && nearbyRecords === 0)) continue;
            for (const option of usable) {
                const stateKey = `${page.url()}|${index}|${option.value}`;
                if (options.discoveryState?.interactionState(page.url(), stateKey, option.value).fresh === false) continue;
                await control.selectOption(option.value).catch(() => null);
                await page.waitForTimeout(700);
                const html = await page.content().catch(() => '');
                if (!html) continue;
                options.onResults?.(extractLinksFromHtml(html, normalizeUrl(page.url()) || pageUrl, rootUrl, {
                    allowUnknownExternal: options.allowUnknownExternal !== false,
                    externalHosts: options.externalHosts || new Set(),
                    restrictedExternalHost: options.restrictedExternalHost || null
                }));
                explored++;
            }
        } catch {}
    }
    return explored;
}

async function clickPaginationControls(page, signal, options = {}) {
    const locator = page.locator(
        'a[rel], a[href], button[data-page], button[data-offset], button[data-cursor], ' +
        '[role="button"], [role="link"]'
    );
    const count = Math.min(await locator.count().catch(() => 0), CONFIG.MAX_PAGINATION_LINKS_PER_PAGE);
    const seenControls = new Set();
    let clicks = 0;

    for (let i = 0; i < count && clicks < CONFIG.MAX_PAGINATION_LINKS_PER_PAGE; i++) {
        throwIfAborted(signal);
        const control = locator.nth(i);
        try {
            if (!(await control.isVisible({ timeout: 300 }))) continue;
            const text = compactText(await control.innerText({ timeout: 300 }).catch(() => ''), 80);
            const aria = await control.getAttribute('aria-label').catch(() => '');
            const title = await control.getAttribute('title').catch(() => '');
            const rel = await control.getAttribute('rel').catch(() => '');
            const href = await control.getAttribute('href').catch(() => '');
            const pageNumber = await control.getAttribute('data-page').catch(() => '');
            const offset = await control.getAttribute('data-offset').catch(() => '');
            const cursor = await control.getAttribute('data-cursor').catch(() => '');
            const signature = `${text}|${aria}|${title}|${pageNumber}|${offset}|${cursor}`;
            if (seenControls.has(signature)) continue;
            seenControls.add(signature);
            const paginationContainer = await control.locator('xpath=ancestor::*[contains(@class,"pagination") or @role="navigation" or @aria-label]').count().catch(() => 0);
            if (!isPaginationControlEvidence({ text, aria, title, pageNumber, offset, rel, href, paginationContainer })) continue;

            const beforeUrl = normalizeUrl(page.url()) || '';
            if (options.discoveryState) {
                const state = options.discoveryState.interactionState(
                    beforeUrl, signature, `${pageNumber}|${offset}|${cursor}`
                );
                if (!state.fresh) continue;
            }
            await control.click({ timeout: CRAWLER_TIMEOUTS.CLICK_TIMEOUT_MS });
            clicks++;
            await page.waitForLoadState('domcontentloaded', { timeout: CRAWLER_TIMEOUTS.LOAD_STATE_TIMEOUT_MS / 2 }).catch(() => {});
            await page.waitForTimeout(600);
            await autoScroll(page, 3);
            await options.onClick?.({ page, clicks, beforeUrl, afterUrl: normalizeUrl(page.url()) || beforeUrl });
        } catch {}
    }
    return clicks;
}

function isPaginationControlEvidence({ text = '', aria = '', title = '', pageNumber = '', offset = '', rel = '', href = '', paginationContainer = 0 } = {}) {
    if (pageNumber || offset) return true;
    if (/\bnext\b|\bweiter\b|nächste|rel\s*next/i.test(`${text} ${aria} ${title} ${rel}`)) return true;
    if (paginationContainer > 0 && /^\d+$/.test(text.trim())) return true;
    return paginationContainer > 0 && /[?&](?:page|seite|offset|start|cursor|after)=/i.test(`${href}`);
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

function extractNavigationUrlsFromOnclick(onclick) {
    const urls = [];
    const source = String(onclick || '');
    const patterns = [
        /(?:window\.)?location(?:\.href)?\s*=\s*["']([^"']+)["']/ig,
        /location\.(?:assign|replace)\s*\(\s*["']([^"']+)["']\s*\)/ig,
        /window\.open\s*\(\s*["']([^"']+)["']/ig
    ];
    for (const pattern of patterns) {
        let match;
        while ((match = pattern.exec(source))) urls.push(match[1]);
    }
    return [...new Set(urls)];
}

function extractOnclickJobUrls($, pageUrl, rootUrl, options = {}) {
    const jobs = new Set();
    let inspected = 0;
    $('[onclick]').each((_, el) => {
        if (inspected >= (options.maxCandidates ?? CONFIG.MAX_ONCLICK_CANDIDATES_PER_PAGE)) return false;
        inspected++;
        const onclick = $(el).attr('onclick');
        const text = compactText($(el).text(), 240);
        const context = compactText($(el).closest('li,article,section,div,tr').text(), 1200);
        for (const rawUrl of extractNavigationUrlsFromOnclick(onclick)) {
            const full = normalizeUrl(rawUrl, pageUrl);
            if (!full) continue;
            const kind = classifyLink(full, text, context, rootUrl, { ...options, pageUrl });
            if (kind === 'job' || isLikelyIndividualJobUrl(full)) jobs.add(full);
        }
    });
    return jobs;
}

function parseEmbeddedJsonText(raw, maxBytes = CONFIG.MAX_EMBEDDED_JSON_BYTES) {
    if (!raw) return null;
    const text = String(raw).trim();
    if (Buffer.byteLength(text, 'utf8') > maxBytes) return null;
    const direct = text.replace(/;\s*$/, '').trim();
    try {
        if (direct.startsWith('{') || direct.startsWith('[')) return JSON.parse(direct);
    } catch {
        return null;
    }

    if (!/(?:__DATA__|__INITIAL_STATE__|__NEXT_DATA__|__NUXT__|jobs?|vacanc(?:y|ies)|requisition)/i.test(text)) return null;
    const starts = [text.indexOf('{'), text.indexOf('[')].filter(index => index >= 0).sort((a, b) => a - b);
    const start = starts[0];
    if (start == null) return null;
    const open = text[start];
    const close = open === '{' ? '}' : ']';
    const end = text.lastIndexOf(close);
    if (end <= start) return null;
    try { return JSON.parse(text.slice(start, end + 1)); } catch { return null; }
}

function extractEmbeddedJsonCandidates($, pageUrl, rootUrl, options = {}) {
    const candidates = [];
    const seen = new Set();
    let inspected = 0;
    $('script').each((_, el) => {
        if (inspected >= (options.maxScripts ?? CONFIG.MAX_EMBEDDED_JSON_SCRIPTS_PER_PAGE)) return false;
        const type = String($(el).attr('type') || '').toLowerCase();
        const id = String($(el).attr('id') || '').toLowerCase();
        const raw = $(el).html() || '';
        if (!raw || /text\/javascript|application\/javascript/i.test(type)) {
            if (!/(?:__data__|__initial_state__|__next_data__|__nuxt__)/i.test(id + raw)) return;
        }
        if (type !== 'application/json' && type !== 'application/ld+json' &&
            !/(?:__data__|__initial_state__|__next_data__|__nuxt__|jobs?|vacanc(?:y|ies)|requisition)/i.test(id + raw)) return;
        inspected++;
        const parsed = parseEmbeddedJsonText(raw, options.maxBytes ?? CONFIG.MAX_EMBEDDED_JSON_BYTES);
        if (parsed == null) return;
        const found = extractJobCandidatesFromApiPayload(parsed, pageUrl, rootUrl, {
            maxDepth: options.maxDepth ?? CONFIG.MAX_JOB_API_JSON_DEPTH,
            maxCandidates: options.maxCandidates ?? CONFIG.MAX_EMBEDDED_JSON_CANDIDATES_PER_PAGE
        });
        for (const candidate of found) {
            const identity = candidate.identity || candidate.detailUrl || candidate.title;
            if (!identity || seen.has(identity)) continue;
            seen.add(identity);
            candidates.push(candidate);
            if (candidates.length >= (options.maxCandidates ?? CONFIG.MAX_EMBEDDED_JSON_CANDIDATES_PER_PAGE)) return false;
        }
    });
    return candidates;
}

function extractJobCardUrls($, pageUrl, rootUrl, options = {}) {
    const jobs = new Set();
    const selectors = 'article, li, [class*="job" i], [class*="position" i], [class*="vacanc" i], [class*="stellen" i], [data-job-id], [data-requisition]';
    let inspected = 0;
    $(selectors).each((_, el) => {
        if (inspected >= (options.maxCandidates ?? CONFIG.MAX_DYNAMIC_DOM_CANDIDATES_PER_PAGE)) return false;
        inspected++;
        const card = $(el);
        const text = compactText(card.text(), 1000);
        const title = compactText(card.find('h1,h2,h3,h4,[class*="title" i],[class*="position" i]').first().text(), 240);
        const evidence = `${title} ${text} ${card.attr('data-job-id') || ''} ${card.attr('data-requisition') || ''}`;
        if (!looksLikeJobCardText(evidence) && !/\b(?:job|position|vacancy|stellenangebot|stelle|requisition)\b/i.test(evidence)) return;
        const rawUrls = [
            card.find('a[href]').first().attr('href'),
            card.attr('data-href'), card.attr('data-url'), card.attr('data-job-url'),
            card.attr('data-detail-url'), card.attr('data-apply-url'),
            card.find('button,[role="button"]').first().attr('data-href'),
            card.find('button,[role="button"]').first().attr('data-url'),
            card.find('button,[role="button"]').first().attr('aria-label')
        ].filter(Boolean);
        for (const rawUrl of rawUrls) {
            const full = normalizeUrl(rawUrl, pageUrl);
            if (!full) continue;
            const kind = classifyLink(full, title || text, evidence, rootUrl, { ...options, pageUrl });
            if (kind === 'job' || isLikelyIndividualJobUrl(full)) jobs.add(full);
        }
    });

    // Structural fallback: many portals use generated class names or no job
    // class at all. Repeated sibling records with distinct terminal links are
    // stronger evidence than a URL keyword, while final detail validation
    // remains authoritative before persistence.
    $('a[href]').each((_, el) => {
        if (inspected >= (options.maxCandidates ?? CONFIG.MAX_DYNAMIC_DOM_CANDIDATES_PER_PAGE)) return false;
        const anchor = $(el);
        const full = normalizeUrl(anchor.attr('href'), pageUrl);
        if (!full || isNonJobUrl(full) || isCareerListingUrl(full)) return;
        const container = anchor.closest('li,article,tr,[role="listitem"],section,div').first();
        const siblingLinks = container.find('a[href]').filter((__, child) => normalizeUrl($(child).attr('href'), pageUrl)).length;
        const parentLinks = anchor.parent().children('a[href]').length;
        const title = compactText(anchor.text(), 240);
        const context = compactText(container.text(), 1200);
        const repeatedRecord = (siblingLinks === 1 || parentLinks >= 2) && container.parent().children().length >= 2;
        if (!repeatedRecord || !title || wordCount(context) > 80) return;
        inspected++;
        const kind = classifyLink(full, title, context, rootUrl, { ...options, pageUrl });
        if (kind === 'job' || isLikelyIndividualJobUrl(full) || !isCareerListingUrl(full)) jobs.add(full);
    });
    return jobs;
}

function getApiFieldValue(record, keys) {
    if (!record || typeof record !== 'object' || Array.isArray(record)) return null;
    for (const key of keys) {
        if (!(key in record)) continue;
        const value = record[key];
        if (typeof value === 'string' || typeof value === 'number') {
            const text = String(value).trim();
            if (text) return text;
        }
        if (Array.isArray(value)) {
            const text = value
                .map(item => typeof item === 'string' || typeof item === 'number' ? String(item) : item?.name || item?.value || '')
                .filter(Boolean)
                .join(', ')
                .trim();
            if (text) return text;
        }
        if (value && typeof value === 'object') {
            const nested = value.url || value.href || value.link || value.value || value.name;
            if (typeof nested === 'string' && nested.trim()) return nested.trim();
        }
    }
    return null;
}

function hasJobApiEvidence(record) {
    if (!record || typeof record !== 'object' || Array.isArray(record)) return false;
    const title = getApiFieldValue(record, JOB_API_TITLE_KEYS);
    if (!title || isGenericJobTitle(title)) return false;

    const titleLooksLikeRole = hasSpecificJobTitleEvidence(title) ||
        /\b(?:accountant|buchhalter|jurist|lawyer|architect|architekt|technician|techniker|designer|coordinator|koordinator|supervisor|specialist|director|leiter|kaufmann|kauffrau|controller|planner|planer|scientist|researcher|product owner|scrum master)\b/i.test(title);

    const hasIdentifier = JOB_API_ID_KEYS.some(key => {
        const value = record[key];
        return (typeof value === 'string' || typeof value === 'number') && String(value).trim().length > 0;
    });
    const hasUrl = JOB_API_URL_KEYS.some(key => {
        const value = record[key];
        return typeof value === 'string' || (value && typeof value === 'object' && Boolean(value.url || value.href || value.link));
    });
    const slug = getApiFieldValue(record, ['slug']);
    const hasLocation = Boolean(getApiFieldValue(record, JOB_API_LOCATION_KEYS));
    const hasDescription = Boolean(getApiFieldValue(record, JOB_API_DESCRIPTION_KEYS));
    const hasEmployment = Boolean(getApiFieldValue(record, JOB_API_EMPLOYMENT_KEYS));
    const hasJobSpecificKey = Object.keys(record).some(key =>
        /(?:responsibilities|requirements|qualifications|department|postingDate|company|apply)/i.test(key)
    );
    const hasRealEmploymentSignal = titleLooksLikeRole || hasEmployment ||
        hasLocation || hasUrl || hasJobSpecificKey;
    if (!hasRealEmploymentSignal) return false;

    // A generic `name` or `title` alone is intentionally insufficient. An ID
    // is also not enough by itself because CMS/navigation payloads commonly
    // contain unrelated IDs.
    return hasIdentifier || hasUrl || hasLocation || hasDescription || hasEmployment || hasJobSpecificKey;
}

function normalizeApiJobRecord(record, responseUrl, baseUrl) {
    if (!hasJobApiEvidence(record)) return null;
    const detailUrl = getApiFieldValue(record, ['detailUrl', 'detailURL']);
    const jobUrl = getApiFieldValue(record, ['jobUrl', 'jobURL']);
    const url = getApiFieldValue(record, ['url']);
    const link = getApiFieldValue(record, ['jobLink', 'link']);
    const applyUrl = getApiFieldValue(record, ['applyUrl', 'applyURL', 'applicationUrl']);
    const urlSource = detailUrl ? 'detailUrl' : jobUrl ? 'jobUrl' : url ? 'url' : link ? 'link' : applyUrl ? 'applyUrl' : null;
    const explicitUrlCandidates = [detailUrl, jobUrl, url, link, applyUrl].filter(Boolean);
    const normalizedUrl = explicitUrlCandidates
        .map(value => normalizeUrl(value, responseUrl || baseUrl) || normalizeUrl(value, baseUrl))
        .find(Boolean) || null;
    const title = getApiFieldValue(record, JOB_API_TITLE_KEYS);
    const jobId = getApiFieldValue(record, ['jobId', 'job_id']);
    const requisitionId = getApiFieldValue(record, ['requisition', 'requisitionId', 'requisition_id', 'requisitionNumber']);
    const externalJobId = getApiFieldValue(record, ['externalJobId', 'external_job_id', 'externalId', 'external_id']);
    const referenceId = getApiFieldValue(record, ['referenceId', 'reference_id', 'reference', 'jobNumber']);
    const stableApiId = getApiFieldValue(record, ['id', 'uuid', 'uid']);
    const identities = [
        externalJobId ? `external_id:${externalJobId}` : null,
        requisitionId ? `requisition:${requisitionId}` : null,
        jobId ? `job_id:${jobId}` : null,
        referenceId ? `reference:${referenceId}` : null,
        stableApiId ? `api_id:${stableApiId}` : null,
        normalizedUrl ? `url:${normalizeJobIdentityUrl(normalizedUrl, baseUrl) || normalizedUrl}` : null,
        `record:${crypto.createHash('sha1').update(JSON.stringify({
            title,
            location: getApiFieldValue(record, JOB_API_LOCATION_KEYS),
            description: getApiFieldValue(record, JOB_API_DESCRIPTION_KEYS)
        })).digest('hex')}`
    ].filter(Boolean);

    return {
        title,
        location: getApiFieldValue(record, JOB_API_LOCATION_KEYS),
        description: getApiFieldValue(record, JOB_API_DESCRIPTION_KEYS),
        jobId,
        requisitionId,
        externalJobId,
        referenceId,
        stableApiId,
        detailUrl: normalizedUrl,
        jobUrl: jobUrl ? normalizeUrl(jobUrl, responseUrl || baseUrl) : null,
        applyUrl: applyUrl ? normalizeUrl(applyUrl, responseUrl || baseUrl) : null,
        locationCount: getApiFieldValue(record, ['count']),
        urlSource,
        isApplyOnly: urlSource === 'applyUrl',
        employmentType: getApiFieldValue(record, JOB_API_EMPLOYMENT_KEYS),
        department: getApiFieldValue(record, ['department', 'team', 'businessUnit', 'category']),
        requirements: getApiFieldValue(record, ['requirements', 'qualifications', 'skills']),
        responsibilities: getApiFieldValue(record, ['responsibilities', 'duties', 'tasks']),
        salary: getApiFieldValue(record, ['salary', 'salaryRange', 'compensation']),
        company: getApiFieldValue(record, ['company', 'companyName', 'hiringOrganization']),
        responseUrl,
        identity: identities[0] || null,
        identities,
    };
}

function extractJobCandidatesFromApiPayload(payload, responseUrl, baseUrl, options = {}) {
    const maxDepth = Math.max(0, options.maxDepth ?? CONFIG.MAX_JOB_API_JSON_DEPTH);
    const maxCandidates = Number.isFinite(options.maxCandidates) ? Math.max(0, options.maxCandidates) : Infinity;
    const candidates = [];
    const seen = new Set();
    const visited = new Set();

    function visit(value, depth) {
        if (candidates.length >= maxCandidates || depth > maxDepth || value == null) return;
        if (typeof value !== 'object') return;
        if (visited.has(value)) return;
        visited.add(value);

        if (!Array.isArray(value)) {
            const candidate = normalizeApiJobRecord(value, responseUrl, baseUrl);
            if (candidate?.identity && !candidate.identities.some(identity => seen.has(identity))) {
                candidate.identities.forEach(identity => seen.add(identity));
                candidates.push(candidate);
                if (candidates.length >= maxCandidates) return;
            }
        }

        for (const child of Array.isArray(value) ? value : Object.values(value)) {
            visit(child, depth + 1);
            if (candidates.length >= maxCandidates) break;
        }
    }

    visit(payload, 0);
    return candidates;
}

function isJobApiResponseMetadata({ url, status, contentType, resourceType } = {}) {
    if (!url) return false;
    const normalizedContentType = String(contentType || '');
    if (!JOB_API_CONTENT_TYPE_RE.test(normalizedContentType)) return false;
    if (Number.isFinite(status) && (status < 200 || status >= 300)) return false;
    const type = String(resourceType || '').toLowerCase();
    return Boolean(JOB_API_URL_SIGNAL_RE.test(url) || type === 'xhr' || type === 'fetch');
}

function isPotentialJobApiResponse(response) {
    try {
        const responseUrl = response.url();
        const status = response.status();
        const resourceType = response.request()?.resourceType?.() || '';
        let contentType = '';
        const headers = response.headers?.() || {};
        contentType = headers['content-type'] || headers['Content-Type'] || '';
        const likelyApiResponse =
            JOB_API_URL_SIGNAL_RE.test(responseUrl) ||
            ['xhr', 'fetch'].includes(String(resourceType).toLowerCase());
        return likelyApiResponse && (!contentType || JOB_API_CONTENT_TYPE_RE.test(contentType)) &&
            status >= 200 && status < 300;
    } catch {
        return false;
    }
}

function parseJobApiResponseBody(body, maxBytes = CONFIG.MAX_JOB_API_RESPONSE_BYTES) {
    if (!body) return null;
    const buffer = Buffer.isBuffer(body) ? body : Buffer.from(String(body));
    if (buffer.length > maxBytes) return null;
    try {
        const text = buffer.toString('utf8').trim();
        const wrapped = text.match(JOBS_WRAPPER_RE);
        return JSON.parse(wrapped ? wrapped[1].trim() : text);
    } catch {
        return null;
    }
}

function acceptApiCandidatesForCompany(candidates, apiState) {
    const unique = [];
    for (const candidate of candidates || []) {
        if (!candidate?.identity || apiState.identities.has(candidate.identity)) continue;
        apiState.identities.add(candidate.identity);
        (candidate.identities || []).forEach(identity => apiState.identities.add(identity));
        apiState.jobsDetected++;
        unique.push(candidate);
    }
    return unique;
}

function syncApiTelemetry(apiState, discoveryState) {
    if (!discoveryState?.metrics) return;
    discoveryState.metrics.apiRequestsAttempted = apiState.requestsAttempted || 0;
    discoveryState.metrics.apiRequestsCompleted = apiState.requestsCompleted || 0;
    discoveryState.metrics.apiRequestsFailed = apiState.requestsFailed || 0;
    discoveryState.metrics.apiRequestsRemaining = apiState.continuationRequests?.length || 0;
}

async function inspectJobApiResponse(response, pageUrl, apiState, pageResponseState) {
    try {
        apiState.requestsAttempted = (apiState.requestsAttempted || 0) + 1;
        syncApiTelemetry(apiState, pageResponseState.discoveryState);
        const headers = await response.allHeaders().catch(() => ({}));
        const contentType = headers['content-type'] || headers['Content-Type'] || '';
        const request = response.request();
        const resourceType = request?.resourceType?.() || '';
        const status = response.status();
        const responseUrl = response.url();
        if (!isJobApiResponseMetadata({ url: responseUrl, status, contentType, resourceType })) return [];
        if (pageResponseState.count >= CONFIG.MAX_JOB_API_RESPONSES_PER_PAGE) {
            pageResponseState.discoveryState?.recordLimit('api_response_budget', CONFIG.MAX_JOB_API_RESPONSES_PER_PAGE);
            return [];
        }
        const contentLength = Number(headers['content-length'] || headers['Content-Length'] || 0);
        if (contentLength > CONFIG.MAX_JOB_API_RESPONSE_BYTES) return [];
        pageResponseState.count++;

        const body = await response.body();
        if (!body || body.length > CONFIG.MAX_JOB_API_RESPONSE_BYTES) return [];
        const payload = parseJobApiResponseBody(body);
        if (payload == null) return [];
        apiState.requestsCompleted = (apiState.requestsCompleted || 0) + 1;
        syncApiTelemetry(apiState, pageResponseState.discoveryState);

        for (const continuationUrl of extractContinuationUrls(payload, responseUrl)) {
            apiState.continuationUrls ??= new Set();
            apiState.continuationUrls.add(continuationUrl);
        }
        apiState.continuationRequests ??= [];
        const requestBody = request?.postData?.() || null;
        for (const continuation of extractContinuationRequests(payload, responseUrl, {
            url: responseUrl,
            method: request?.method?.() || 'GET',
            body: requestBody
        })) {
            const signature = `${continuation.method} ${continuation.url} ${continuation.body || ''}`;
            if (!apiState.requestStates?.has(signature)) {
                apiState.requestStates ??= new Set();
                apiState.continuationRequests.push({ ...continuation, signature });
            }
        }
        const candidates = extractJobCandidatesFromApiPayload(payload, responseUrl, pageUrl);
        return acceptApiCandidatesForCompany(candidates, apiState);
    } catch (error) {
        apiState.requestsFailed = (apiState.requestsFailed || 0) + 1;
        syncApiTelemetry(apiState, pageResponseState.discoveryState);
        pageResponseState.processingFailures = (pageResponseState.processingFailures || 0) + 1;
        logInfo('API_DISCOVERY', `Ignored response ${truncateForLog(response?.url?.() || '', 120)} (${error.message})`);
        return [];
    }
}

function addPaginationUrls($, baseUrl, result) {
    let added = 0;
    $('a[href]').each((_, el) => {
        if (added >= CONFIG.MAX_PAGINATION_LINKS_PER_PAGE) return false;
        const text = compactText($(el).text(), 80).toLowerCase();
        const rel = String($(el).attr('rel') || '').toLowerCase();
        const aria = String($(el).attr('aria-label') || '').toLowerCase();
        const title = String($(el).attr('title') || '').toLowerCase();
        const full = normalizeUrl($(el).attr('href'), baseUrl);
        if (!full) return;
        const combined = `${text} ${rel} ${aria} ${title} ${full}`.toLowerCase();
        const inPaginationContainer = $(el).closest('nav, [role="navigation"], [class*="pagination" i], [class*="pager" i]').length > 0;
        const sequentialPageLink = inPaginationContainer && /^\d+$/.test(text.trim());
        if (rel === 'next' || sequentialPageLink || /\b(?:next|next page|weiter|naechste|nächste|more jobs|mehr stellen|weitere stellen|weitere jobs)\b|(?:[?&](?:page|seite|offset|start|p|cursor|after)=)/i.test(combined)) {
            if (!isNonJobUrl(full) && !isJobDetailUrl(full)) {
                result.listings.add(full);
                added++;
            }
        }
    });
    $('[rel="next" i], [data-page], [data-offset], [data-cursor], [data-url], nav a[href], [role="navigation"] a[href], [class*="pagination" i] a[href]').each((_, el) => {
        if (added >= CONFIG.MAX_PAGINATION_LINKS_PER_PAGE) return false;
        const text = compactText($(el).text(), 80).toLowerCase();
        const rel = String($(el).attr('rel') || '').toLowerCase();
        const raw = $(el).attr('href') || $(el).attr('data-url');
        const full = normalizeUrl(raw, baseUrl);
        if (!full || isJobDetailUrl(full) || isNonJobUrl(full)) return;
        const structuralPagination = $(el).closest('nav, [role="navigation"], [class*="pagination" i], [class*="pager" i]').length > 0;
        if (!structuralPagination && !/next|weiter|naechste|nächste|page|seite|offset|start|pagination|cursor|after/i.test(`${text} ${rel} ${raw || ''}`)) return;
        result.listings.add(full);
        added++;
    });
    result.pagination_candidates = (result.pagination_candidates || 0) + added;
    result.paginationLimitReached = added >= CONFIG.MAX_PAGINATION_LINKS_PER_PAGE;
}

function extractLinksFromHtml(html, pageUrl, rootUrl, options = {}) {
    const result = { jobs: new Set(), listings: new Set(), pages: new Set(), ats: new Set(), embeddedApiCandidates: [], counters: createDiscoveryCounters() };
    const $ = cheerio.load(html);

    const jsonLdJobs = extractJsonLdJobUrls($, pageUrl);
    jsonLdJobs.forEach(u => result.jobs.add(u));
    const embeddedCandidates = extractEmbeddedJsonCandidates($, pageUrl, rootUrl, options);
    result.embeddedApiCandidates.push(...embeddedCandidates);
    for (const candidate of embeddedCandidates) {
        if (candidate.detailUrl || candidate.jobUrl || candidate.applyUrl) {
            const url = normalizeUrl(candidate.detailUrl || candidate.jobUrl || candidate.applyUrl, pageUrl);
            if (url && (isJobDetailUrl(url) || isLikelyIndividualJobUrl(url))) result.jobs.add(url);
        }
    }
    const onclickJobs = extractOnclickJobUrls($, pageUrl, rootUrl, options);
    onclickJobs.forEach(u => result.jobs.add(u));
    extractJobCardUrls($, pageUrl, rootUrl, options).forEach(u => result.jobs.add(u));
    addPaginationUrls($, pageUrl, result);
    result.counters.embedded_json_candidates += embeddedCandidates.length;
    result.counters.onclick_candidates += onclickJobs.size;
    result.counters.html_candidates += jsonLdJobs.size;
    result.counters.pagination_candidates += result.pagination_candidates || 0;

    $('a[href]').each((_, el) => {
        const full = normalizeUrl($(el).attr('href'), pageUrl);
        if (!full) return;

        const text = compactText($(el).text(), 160);
        const context = compactText($(el).closest('li,article,section,div,tr').text(), 1200);
        const rel = String($(el).attr('rel') || '').toLowerCase();
        const aria = String($(el).attr('aria-label') || '').toLowerCase();
        const title = String($(el).attr('title') || '').toLowerCase();
        if (!isJobDetailUrl(full) && (rel === 'next' || rel === 'prev' ||
            $(el).is('[data-page], [data-offset], [data-cursor]') ||
            isPaginationControlEvidence({ text, aria, title }))) {
            result.listings.add(full);
            result.counters.pagination_candidates++;
            return;
        }
        const kind = classifyLink(full, text, context, rootUrl, {
            ...options,
            pageUrl
        });

        if (kind === 'job') result.jobs.add(full);
        else if (kind === 'listing') result.listings.add(full);
        else if (kind === 'ats') result.ats.add(full);
        else if (isSameCompanyUrl(full, rootUrl) && !options.restrictedExternalHost &&
            !isClearlyIrrelevantDiscoveryUrl(full)) result.pages.add(full);
    });

    result.counters.html_candidates += result.jobs.size;

    return result;
}

function looksLikeJobCardText(text) {
    const clean = compactText(text, 240);
    if (isGenericJobTitle(clean)) return false;
    if (wordCount(clean) > 18) return false;
    return /(engineer|developer|manager|consultant|analyst|designer|architect|administrator|specialist|lead|director|berater|entwickler|ingenieur|techniker|projektleiter|controller|buchhalter|jurist|devops|data|software|backend|frontend|fullstack|ausbildung|praktikum)/i.test(clean);
}

async function discoverJobDetailUrlsByClicking(page, pageUrl, rootUrl, signal) {
    const found = new Set();
    const locator = page.locator('a, button, [role="button"], [data-href], [data-url], [onclick]');
    const count = Math.min(await locator.count().catch(() => 0), 120);

    for (let i = 0; i < count; i++) {
        throwIfAborted(signal);
        const el = locator.nth(i);
        let text = '';
        let href = null;
        try {
            if (!(await el.isVisible({ timeout: 250 }))) continue;
            text = compactText(await el.innerText({ timeout: 500 }).catch(() => ''), 240);
            href = await el.getAttribute('href')
                || await el.getAttribute('data-href')
                || await el.getAttribute('data-url');
            const onclick = await el.getAttribute('onclick');
            if (!href && onclick) {
                href = extractNavigationUrlsFromOnclick(onclick)[0] || null;
            }
        } catch {
            continue;
        }

        const normalizedHref = normalizeUrl(href, pageUrl);
        const role = await el.getAttribute('role').catch(() => '');
        const tagName = await el.evaluate(node => node.tagName).catch(() => '');
        const ariaLabel = await el.getAttribute('aria-label').catch(() => '');
        const title = await el.getAttribute('title').catch(() => '');
        const ariaControls = await el.getAttribute('aria-controls').catch(() => '');
        const dataTarget = await el.getAttribute('data-target').catch(() => '');
        const nearbyRecordCount = await el.locator('xpath=ancestor::*[self::article or self::li or self::tr or @data-record]').count().catch(() => 0);
        const nearbyLinkCount = await el.locator('xpath=ancestor::*[self::section or self::main]//a[@href]').count().catch(() => 0);
        const insideForm = await el.locator('xpath=ancestor::form').count().catch(() => 0) > 0;
        const employmentControl = /load\s*more|show\s*(?:more|jobs|positions)|next|weiter|filter|search|location|department|category|vacanc|career|stellen|jobs?/i.test(`${text} ${ariaLabel} ${title}`);
        const interactionScore = semanticInteractionScore({
            role,
            tagName,
            textContent: text,
            ariaLabel,
            title,
            href: normalizedHref,
            nearbyRecordCount,
            nearbyLinkCount,
            hasFormControl: insideForm,
            hasTarget: Boolean(ariaControls || dataTarget)
        });
        if (normalizedHref && isLikelyIndividualJobUrl(normalizedHref)) {
            found.add(normalizedHref);
            continue;
        }
        // Links are discovery candidates, not interaction controls. Only click
        // controls without a direct destination when their surrounding
        // structure or label indicates that they can reveal employment data.
        if (normalizedHref) continue;
        const controlLike = /^(BUTTON|SUMMARY|SELECT|INPUT)$/.test(String(tagName).toUpperCase()) ||
            ['button', 'tab', 'menuitem'].includes(String(role).toLowerCase()) || Boolean(ariaControls || dataTarget);
        if (!controlLike || !employmentControl || interactionScore < 4 ||
            (nearbyRecordCount === 0 && nearbyLinkCount === 0 && !insideForm && !ariaControls && !dataTarget)) continue;
        logInfo('INTERACTION', `clicking employment control score=${interactionScore} reason=${employmentControl ? 'employment_label_and_structure' : 'structure'} text=${truncateForLog(text || ariaLabel || title, 100)}`);

        const beforeUrl = normalizeUrl(page.url()) || pageUrl;
        try {
            const popupPromise = page.context().waitForEvent('page', { timeout: 1500 }).catch(() => null);
            await el.click({ timeout: 2500 });
            const popup = await popupPromise;

            if (popup) {
                trackCompanyPage(popup);
                try {
                    await popup.waitForLoadState('domcontentloaded', { timeout: CRAWLER_TIMEOUTS.LOAD_STATE_TIMEOUT_MS }).catch(() => {});
                    await popup.waitForTimeout(800).catch(() => {});
                    const popupUrl = normalizeUrl(popup.url());
                    const popupHtml = await popup.content().catch(() => '');
                    if (popupUrl && isSameCompanyUrl(popupUrl, rootUrl) &&
                        (isLikelyIndividualJobUrl(popupUrl) || extractRawJobFromHtml(popupHtml, popupUrl, '').valid)) {
                        found.add(popupUrl);
                    }
                } finally {
                    await closeTrackedPage(popup);
                }
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

function mergeLinkExtraction(target, source) {
    if (!source) return target;
    for (const key of ['jobs', 'listings', 'pages', 'ats']) {
        source[key]?.forEach(value => target[key].add(value));
    }
    if (source.embeddedApiCandidates?.length) target.embeddedApiCandidates.push(...source.embeddedApiCandidates);
    addDiscoveryCounters(target.counters, source.counters);
    if (source.paginationLimitReached) target.paginationLimitReached = true;
    return target;
}

async function extractRenderedDomJobUrls(page, pageUrl, rootUrl, options = {}) {
    const rawCandidates = await page.evaluate(maxCandidates => {
        const output = [];
        const selectors = 'a[href], [data-href], [data-url], [data-job-url], [data-detail-url], [data-apply-url], [data-job-id], [data-requisition], button, [role="button"], [onclick]';
        for (const element of document.querySelectorAll(selectors)) {
            if (output.length >= maxCandidates) break;
            const attrs = {};
            for (const name of ['href', 'data-href', 'data-url', 'data-job-url', 'data-detail-url', 'data-apply-url', 'aria-label', 'onclick']) {
                const value = element.getAttribute(name);
                if (value) attrs[name] = value;
            }
            output.push({ text: (element.innerText || element.textContent || '').slice(0, 1200), attrs });
        }
        return output;
    }, options.maxCandidates ?? CONFIG.MAX_DYNAMIC_DOM_CANDIDATES_PER_PAGE).catch(() => []);

    const jobs = new Set();
    for (const candidate of rawCandidates) {
        const text = compactText(candidate.text, 240);
        for (const raw of [
            candidate.attrs.href, candidate.attrs['data-href'], candidate.attrs['data-url'],
            candidate.attrs['data-job-url'], candidate.attrs['data-detail-url'], candidate.attrs['data-apply-url'],
            ...extractNavigationUrlsFromOnclick(candidate.attrs.onclick)
        ].filter(Boolean)) {
            const full = normalizeUrl(raw, pageUrl);
            if (!full) continue;
            const kind = classifyLink(full, text, text, rootUrl, { ...options, pageUrl });
            if (kind === 'job' || isLikelyIndividualJobUrl(full)) jobs.add(full);
        }
    }
    return jobs;
}

async function extractOpenShadowDomJobUrls(page, pageUrl, rootUrl, options = {}) {
    const rawCandidates = await page.evaluate(({ maxRoots, maxCandidates }) => {
        const output = [];
        let roots = 0;
        const visit = root => {
            if (roots >= maxRoots || output.length >= maxCandidates) return;
            for (const element of root.querySelectorAll('*')) {
                if (output.length >= maxCandidates) break;
                if (element.shadowRoot) {
                    roots++;
                    visit(element.shadowRoot);
                }
                if (!element.matches('a[href], [data-href], [data-url], [data-job-url], [data-detail-url], [data-apply-url], button, [role="button"], [onclick]')) continue;
                const attrs = {};
                for (const name of ['href', 'data-href', 'data-url', 'data-job-url', 'data-detail-url', 'data-apply-url', 'aria-label', 'onclick']) {
                    const value = element.getAttribute(name);
                    if (value) attrs[name] = value;
                }
                output.push({ text: (element.innerText || element.textContent || '').slice(0, 1200), attrs });
            }
        };
        visit(document);
        return output;
    }, {
        maxRoots: options.maxRoots ?? CONFIG.MAX_SHADOW_DOM_ROOTS_PER_PAGE,
        maxCandidates: options.maxCandidates ?? CONFIG.MAX_SHADOW_DOM_CANDIDATES_PER_PAGE
    }).catch(() => []);

    const jobs = new Set();
    for (const candidate of rawCandidates) {
        const text = compactText(candidate.text, 240);
        for (const raw of [
            candidate.attrs.href, candidate.attrs['data-href'], candidate.attrs['data-url'],
            candidate.attrs['data-job-url'], candidate.attrs['data-detail-url'], candidate.attrs['data-apply-url'],
            ...extractNavigationUrlsFromOnclick(candidate.attrs.onclick)
        ].filter(Boolean)) {
            const full = normalizeUrl(raw, pageUrl);
            if (!full) continue;
            const kind = classifyLink(full, text, text, rootUrl, { ...options, pageUrl });
            if (kind === 'job' || isLikelyIndividualJobUrl(full)) jobs.add(full);
        }
    }
    return jobs;
}

async function extractBoundedIframeLinks(page, pageUrl, rootUrl, options = {}) {
    const result = { jobs: new Set(), listings: new Set(), pages: new Set(), ats: new Set(), embeddedApiCandidates: [], counters: createDiscoveryCounters() };
    const iframeState = options.iframeState || { inspected: 0, visited: new Set(), limitReached: false };
    const maxIframes = options.maxIframes ?? CONFIG.MAX_IFRAMES_PER_PAGE;
    const frames = page.frames().filter(frame => frame !== page.mainFrame());
    let inspected = 0;
    for (const frame of frames) {
        if (inspected >= maxIframes) {
            iframeState.limitReached = true;
            break;
        }
        const frameUrl = normalizeUrl(frame.url(), pageUrl);
        if (!frameUrl) continue;
        if (iframeState.visited?.has(frameUrl)) continue;
        const allowed = isAllowedDiscoveryExternalUrl(frameUrl, rootUrl, options.externalHosts || new Set()) ||
            hasStrongExternalCareerSignal(frameUrl, '', '', pageUrl);
        if (!allowed) continue;
        try {
            iframeState.visited?.add(frameUrl);
            const html = await frame.content();
            const extracted = extractLinksFromHtml(html, frameUrl, rootUrl, {
                ...options,
                pageUrl: frameUrl,
                allowUnknownExternal: true,
                maxCandidates: CONFIG.MAX_DYNAMIC_DOM_CANDIDATES_PER_PAGE
            });
            mergeLinkExtraction(result, extracted);
            inspected++;
            iframeState.inspected = (iframeState.inspected || 0) + 1;
        } catch {}
    }
    result.counters.iframe_candidates += result.jobs.size;
    result.iframePagesInspected = inspected;
    result.iframeLimitReached = iframeState.limitReached;
    return result;
}

async function extractLinksFromPage(pageUrl, rootUrl, companyName = '', signal, options = {}) {
    throwIfAborted(signal);
    const result = {
        jobs: new Set(), listings: new Set(), pages: new Set(), ats: new Set(),
        apiCandidates: [], embeddedApiCandidates: [], counters: createDiscoveryCounters(),
        finalUrl: pageUrl, failed: false, error: null
    };
    let page = null;
    let closeOnAbort = null;
    const responseTasks = new Set();
    const pageResponseState = { count: 0, discoveryState: options.discoveryState };
    let responseDrainPromise = null;
    let lastRelevantResponseAt = 0;
    let responseListener = null;
    const apiState = options.apiState || { jobsDetected: 0, identities: new Set() };
    apiState.jobsDetected ??= 0;
    apiState.identities ??= new Set();
    const mergeExtracted = extracted => {
        mergeLinkExtraction(result, extracted);
        if (extracted?.paginationLimitReached) options.discoveryState?.recordLimit('pagination_budget', CONFIG.MAX_PAGINATION_LINKS_PER_PAGE);
        if (extracted?.embeddedApiCandidates?.length) {
            result.apiCandidates.push(...acceptApiCandidatesForCompany(extracted.embeddedApiCandidates, apiState));
        }
    };
    const collectApiResults = async () => {
        if (responseDrainPromise) return responseDrainPromise;
        responseDrainPromise = (async () => {
            const deadline = Date.now() + CONFIG.API_RESPONSE_DRAIN_TIMEOUT_MS;
            while (Date.now() < deadline) {
                const pending = [...responseTasks];
                if (pending.length > 0) {
                    const remainingMs = Math.max(1, deadline - Date.now());
                    await Promise.race([
                        Promise.allSettled(pending),
                        new Promise(resolve => setTimeout(resolve, remainingMs))
                    ]);
                }
                const quietFor = lastRelevantResponseAt ? Date.now() - lastRelevantResponseAt : Infinity;
                if (responseTasks.size === 0 && quietFor >= CONFIG.API_RESPONSE_QUIET_WINDOW_MS) break;
                await new Promise(resolve => setTimeout(resolve, Math.min(25, Math.max(1, deadline - Date.now()))));
            }
        })();
        return responseDrainPromise;
    };
    try {
        const context = await getBrowserContext();
        throwIfAborted(signal);
        page = trackCompanyPage(await context.newPage());
        closeOnAbort = () => page.close().catch(() => {});
        if (signal) signal.addEventListener('abort', closeOnAbort, { once: true });
        await page.addInitScript(() => {
            const routes = [];
            const record = () => routes.push(location.href);
            for (const method of ['pushState', 'replaceState']) {
                const original = history[method];
                history[method] = function (...args) {
                    const result = original.apply(this, args);
                    record();
                    return result;
                };
            }
            addEventListener('popstate', record);
            Object.defineProperty(window, '__crawlerRoutes', { value: routes, configurable: true });
        }).catch(() => {});
        // Register before navigation so initial XHR/fetch responses are visible.
        // Bodies are read only after lightweight response metadata passes the
        // JSON/status/resource-type filters in inspectJobApiResponse().
        responseListener = response => {
            if (!isPotentialJobApiResponse(response)) return;

            lastRelevantResponseAt = Date.now();
            const task = inspectJobApiResponse(response, pageUrl, apiState, pageResponseState)
                .then(candidates => {
                    result.apiCandidates.push(...candidates);
                    return candidates;
                })
                .catch(() => []);
            responseTasks.add(task);
            task.finally(() => responseTasks.delete(task)).catch(() => {});
        };
        page.on('response', responseListener);
        const response = await page.goto(pageUrl, { waitUntil: 'domcontentloaded', timeout: CONFIG.PLAYWRIGHT_TIMEOUT_MS });
        await acceptCookies(page);
        const initialHtml = await page.content().catch(() => '');
        await page.waitForLoadState('networkidle', { timeout: CRAWLER_TIMEOUTS.LOAD_STATE_TIMEOUT_MS }).catch(() => {});
        await page.waitForTimeout(1500);
        const scrollResult = await autoScroll(page);
        if (scrollResult?.limitReached) options.discoveryState?.recordLimit('scroll_budget', CONFIG.AUTO_SCROLL_MAX_STEPS);
        const loadMoreClicks = await clickLoadMore(page, {
            discoveryState: options.discoveryState,
            onClick: async () => {
                const iterationHtml = await page.content().catch(() => '');
                if (!iterationHtml) return;
                const jobsBefore = result.jobs.size;
                mergeExtracted(extractLinksFromHtml(iterationHtml, normalizeUrl(page.url()) || pageUrl, rootUrl, {
                    allowUnknownExternal: options.allowUnknownExternal !== false,
                    externalHosts: options.externalHosts || new Set(),
                    restrictedExternalHost: options.restrictedExternalHost || null
                }));
                result.counters.load_more_candidates += Math.max(0, result.jobs.size - jobsBefore);
            }
        });
        if (loadMoreClicks >= CONFIG.LOAD_MORE_MAX_CLICKS) options.discoveryState?.recordLimit('load_more_budget', CONFIG.LOAD_MORE_MAX_CLICKS);
        await exploreEmploymentFilters(page, pageUrl, rootUrl, signal, {
            discoveryState: options.discoveryState,
            allowUnknownExternal: options.allowUnknownExternal !== false,
            externalHosts: options.externalHosts || new Set(),
            restrictedExternalHost: options.restrictedExternalHost || null,
            onResults: extracted => mergeExtracted(extracted)
        });
        const paginationClicks = await clickPaginationControls(page, signal, {
            discoveryState: options.discoveryState,
            onClick: async () => {
                const iterationHtml = await page.content().catch(() => '');
                if (!iterationHtml) return;
                const jobsBefore = result.jobs.size;
                mergeExtracted(extractLinksFromHtml(iterationHtml, normalizeUrl(page.url()) || pageUrl, rootUrl, {
                    allowUnknownExternal: options.allowUnknownExternal !== false,
                    externalHosts: options.externalHosts || new Set(),
                    restrictedExternalHost: options.restrictedExternalHost || null
                }));
                result.counters.pagination_candidates += Math.max(0, result.jobs.size - jobsBefore);
            }
        });
        if (paginationClicks >= CONFIG.MAX_PAGINATION_LINKS_PER_PAGE) options.discoveryState?.recordLimit('pagination_interaction_budget', CONFIG.MAX_PAGINATION_LINKS_PER_PAGE);

        // Drain continuation states discovered in JSON responses. This covers
        // URL, cursor, GraphQL-variable, and JSON POST pagination without
        // assuming a particular parameter name.
        await collectApiResults();
        apiState.continuationRequests ??= [];
        apiState.requestStates ??= new Set();
        let continuationCount = 0;
        while (apiState.continuationRequests.length > 0 &&
            continuationCount < CONFIG.MAX_JOB_API_RESPONSES_PER_PAGE) {
            const continuation = apiState.continuationRequests.shift();
            if (!continuation || apiState.requestStates.has(continuation.signature)) continue;
            apiState.requestStates.add(continuation.signature);
            continuationCount++;
            try {
                apiState.requestsAttempted = (apiState.requestsAttempted || 0) + 1;
                syncApiTelemetry(apiState, options.discoveryState);
                const responsePayload = await page.evaluate(async requestInfo => {
                    const response = await fetch(requestInfo.url, {
                        method: requestInfo.method,
                        headers: requestInfo.method === 'POST' ? { 'content-type': 'application/json' } : undefined,
                        body: requestInfo.method === 'POST' ? requestInfo.body : undefined,
                        credentials: 'include'
                    });
                    return { status: response.status, url: response.url, text: await response.text() };
                }, continuation);
                if (responsePayload.status < 200 || responsePayload.status >= 300) continue;
                const payload = parseJobApiResponseBody(responsePayload.text);
                if (!payload) continue;
                apiState.requestsCompleted = (apiState.requestsCompleted || 0) + 1;
                syncApiTelemetry(apiState, options.discoveryState);
                const candidates = acceptApiCandidatesForCompany(
                    extractJobCandidatesFromApiPayload(payload, responsePayload.url, pageUrl), apiState
                );
                result.apiCandidates.push(...candidates);
                for (const next of extractContinuationRequests(payload, responsePayload.url, continuation)) {
                    if (!apiState.requestStates.has(next.signature || `${next.method} ${next.url} ${next.body || ''}`)) {
                        next.signature ||= `${next.method} ${next.url} ${next.body || ''}`;
                        apiState.continuationRequests.push(next);
                    }
                }
            } catch (error) {
                apiState.requestsFailed = (apiState.requestsFailed || 0) + 1;
                syncApiTelemetry(apiState, options.discoveryState);
                pageResponseState.processingFailures = (pageResponseState.processingFailures || 0) + 1;
                logInfo('API_DISCOVERY', `Continuation failed ${truncateForLog(continuation.url, 120)} (${error.message})`);
            }
        }
        if (apiState.continuationRequests.length > 0) {
            options.discoveryState?.recordLimit('api_response_budget', CONFIG.MAX_JOB_API_RESPONSES_PER_PAGE);
        }

            const html = await page.content();
            result.finalUrl = normalizeUrl(page.url()) || pageUrl;
            const routeUrls = await page.evaluate(() => window.__crawlerRoutes || []).catch(() => []);
            for (const routeUrl of routeUrls.map(url => normalizeUrl(url, result.finalUrl)).filter(Boolean)) {
                if (isJobDetailUrl(routeUrl)) result.jobs.add(routeUrl);
                else result.listings.add(routeUrl);
            }
        const finalHostAllowed = isAllowedDiscoveryExternalUrl(
            result.finalUrl,
            rootUrl,
            options.externalHosts || new Set()
        );
        if (!finalHostAllowed) {
            logExternalDomainBlocked({
                companyName,
                companyWebsiteUrl: rootUrl,
                careerUrl: pageUrl,
                blockedUrl: result.finalUrl,
                reason: 'discovery_page_redirected_to_external_domain'
            });
            result.failed = true;
            result.error = 'discovery_page_redirected_to_external_domain';
            return result;
        }
        const status = response ? response.status() : null;
        if (status && status >= 400) {
            if (BLOCKED_OR_RETRYABLE_STATUSES.has(status)) {
                const fallback = await fetchPageWithFallback(pageUrl, { waitForSelector: 'body', scroll: true, signal });
                if (fallback?.usedScraperApi && fallback.html) {
                    result.finalUrl = normalizeUrl(fallback.url || pageUrl) || pageUrl;
                    const extracted = extractLinksFromHtml(fallback.html, result.finalUrl, rootUrl, {
                        allowUnknownExternal: options.allowUnknownExternal !== false,
                        externalHosts: options.externalHosts || new Set(),
                        restrictedExternalHost: options.restrictedExternalHost || null
                    });
                    mergeExtracted(extracted);
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
            const extracted = extractLinksFromHtml(html, result.finalUrl, rootUrl, {
                allowUnknownExternal: options.allowUnknownExternal !== false,
                externalHosts: options.externalHosts || new Set(),
                restrictedExternalHost: options.restrictedExternalHost || null
            });
            mergeExtracted(extracted);

            const initialExtracted = initialHtml
                ? extractLinksFromHtml(initialHtml, pageUrl, rootUrl, {
                    allowUnknownExternal: options.allowUnknownExternal !== false,
                    externalHosts: options.externalHosts || new Set(),
                    restrictedExternalHost: options.restrictedExternalHost || null
                })
                : null;
            const dynamicJobs = await extractRenderedDomJobUrls(page, result.finalUrl, rootUrl, {
                ...options,
                maxCandidates: CONFIG.MAX_DYNAMIC_DOM_CANDIDATES_PER_PAGE
            });
            for (const url of dynamicJobs) {
                if (!initialExtracted?.jobs.has(url)) result.counters.dynamic_dom_candidates++;
                result.jobs.add(url);
            }

            const clickedJobs = await discoverJobDetailUrlsByClicking(page, result.finalUrl, rootUrl, signal);
            clickedJobs.forEach(u => {
                result.jobs.add(u);
                result.counters.onclick_candidates++;
            });

            const iframeResult = await extractBoundedIframeLinks(page, result.finalUrl, rootUrl, {
                ...options,
                externalHosts: options.externalHosts || new Set(),
                iframeState: options.iframeState,
                maxIframes: options.iframeState
                    ? Math.max(0, CONFIG.MAX_IFRAMES_PER_PAGE - (options.iframeState.inspected || 0))
                    : CONFIG.MAX_IFRAMES_PER_PAGE
            });
            mergeExtracted(iframeResult);
            if (options.iframeState) {
                options.iframeState.inspected = (options.iframeState.inspected || 0) + (iframeResult.iframePagesInspected || 0);
                if (iframeResult.iframeLimitReached) options.discoveryState?.recordLimit('iframe_budget');
            }

            const shadowJobs = await extractOpenShadowDomJobUrls(page, result.finalUrl, rootUrl, {
                ...options,
                maxRoots: CONFIG.MAX_SHADOW_DOM_ROOTS_PER_PAGE,
                maxCandidates: CONFIG.MAX_SHADOW_DOM_CANDIDATES_PER_PAGE
            });
            shadowJobs.forEach(u => {
                result.jobs.add(u);
                result.counters.shadow_dom_candidates++;
            });
        }
    } catch (err) {
        throwIfAborted(signal);
        logWarn('DISCOVERY', `Playwright failed on ${pageUrl}: ${err.message}`);
        try {
            const r = await fetchPageWithFallback(pageUrl, { waitForSelector: 'body', scroll: true, signal });
            if (r?.html) {
                result.finalUrl = normalizeUrl(r.url || pageUrl) || pageUrl;
                const extracted = extractLinksFromHtml(r.html, result.finalUrl, rootUrl, {
                    allowUnknownExternal: options.allowUnknownExternal !== false,
                    externalHosts: options.externalHosts || new Set(),
                    restrictedExternalHost: options.restrictedExternalHost || null
                });
                mergeExtracted(extracted);
            } else {
                result.failed = true;
                result.error = 'fetch_failed';
            }
        } catch (fallbackErr) {
            result.failed = true;
            result.error = fallbackErr.message;
        }
    } finally {
        await collectApiResults();
        for (const continuationUrl of apiState.continuationUrls || []) {
            if (!isJobDetailUrl(continuationUrl) && !isNonJobUrl(continuationUrl)) {
                result.listings.add(continuationUrl);
            }
        }
        if (pageResponseState.processingFailures > 0 && !result.failed) {
            result.failed = true;
            result.error = 'api_response_processing_failed';
        }
        if (page && responseListener) page.off('response', responseListener);
        if (signal && closeOnAbort) signal.removeEventListener('abort', closeOnAbort);
        if (page) await closeTrackedPage(page);
        await recycleBrowserIfNeeded();
    }
    if (result.apiCandidates.length) {
        logInfo('API_DISCOVERY', `page=${truncateForLog(pageUrl, 120)} candidates=${result.apiCandidates.length}`);
    }
    return result;
}

async function extractAllJobLinks(baseUrl, companyName, { onJobLink, onDiscoveryState, signal, companyWebsiteUrl, trustedCareerUrl } = {}) {
    logInfo('CRAWL', `Discovering listings and job details from: ${baseUrl}`);
    const rootUrl = normalizeUrl(baseUrl) || baseUrl;
    const queues = new Map([[10, []], [5, []], [1, []]]);
    const queuedUrls = new Set();
    const visited = new Set();
    const jobLinks = new Set();
    const atsLinks = new Set();
    const failedPages = [];
    const blockedExternalDomains = new Set();
    const externalHosts = new Set();
    const externalDiscoveryUrls = new Set();
    const apiState = { jobsDetected: 0, identities: new Set(), continuationUrls: new Set(), continuationRequests: [], requestStates: new Set(), requestsAttempted: 0, requestsCompleted: 0, requestsFailed: 0 };
    let apiDirectCandidates = 0;
    const iframeState = { inspected: 0, visited: new Set(), limitReached: false };
    const discoveryState = createDiscoveryState(baseUrl);
    onDiscoveryState?.(discoveryState);
    const discoveryCounters = createDiscoveryCounters();
    let listingPagesScanned = 0;
    let externalPagesScanned = 0;
    let verificationPass = false;
    let verificationSnapshot = null;
    let verificationMeaningful = false;

    function snapshotDiscoveryState() {
        return {
            jobs: jobLinks.size,
            apiJobs: apiState.jobsDetected,
            apiIdentities: apiState.identities.size,
            apiRequests: apiState.requestStates.size,
            pending: discoveryState.pendingTasks.size,
            queued: queuedUrls.size,
            interactions: discoveryState.interactionStates.size,
            resultSets: discoveryState.resultHashes.size,
            pagination: discoveryState.metrics.paginationStatesExplored,
            iframes: iframeState.inspected
        };
    }

    function didDiscoveryChange(before, after) {
        if (!before) return true;
        return ['jobs', 'apiJobs', 'apiIdentities', 'apiRequests', 'pending', 'queued', 'interactions',
            'resultSets', 'pagination', 'iframes'].some(key => after[key] > before[key]);
    }

    if (trustedCareerUrl && companyWebsiteUrl &&
        !areRelatedCompanyDomains(trustedCareerUrl, companyWebsiteUrl) &&
        !isAtsUrl(trustedCareerUrl)) {
        externalHosts.add(getDomainHost(trustedCareerUrl));
    }

    function queuedCount() {
        return [...queues.values()].reduce((total, urls) => total + urls.length, 0);
    }

    function enqueueDiscoveryUrl(url, priorityOverride, { external = false } = {}) {
        const normalized = normalizeUrl(url, rootUrl);
        if (!normalized || visited.has(normalized) || queuedUrls.has(normalized) || jobLinks.has(normalized)) return;
        if (isClearlyIrrelevantDiscoveryUrl(normalized)) return;
        if (external && externalPagesScanned + externalDiscoveryUrls.size >= CONFIG.MAX_EXTERNAL_DISCOVERY_PAGES_PER_COMPANY) return;
        const priority = priorityOverride || getDiscoveryUrlPriority(normalized);
        queues.get(priority).push(normalized);
        queuedUrls.add(normalized);
        discoveryState.queuedUrls.add(normalized);
        discoveryState.pendingTasks.set(normalized, { type: 'page', priority, external });
        if (external) {
            externalDiscoveryUrls.add(normalized);
            externalHosts.add(getDomainHost(normalized));
        }
    }

    function dequeueDiscoveryUrl() {
        for (const priority of [10, 5, 1]) {
            if (queues.get(priority).length > 0) return queues.get(priority).shift();
        }
        return null;
    }

    function blockExternalUrl(url, reason) {
        const domain = getDomainHost(url) || url;
        if (blockedExternalDomains.has(domain)) return;
        blockedExternalDomains.add(domain);
        logExternalDomainBlocked({
            companyName,
            companyWebsiteUrl: rootUrl,
            careerUrl: baseUrl,
            blockedUrl: url,
            reason
        });
    }

    enqueueDiscoveryUrl(rootUrl, 10);

    // Sitemaps and robots references are alternative discovery edges. They
    // supplement the browser graph and are never treated as authoritative job
    // records until each URL is fetched and validated.
    for (const sitemapCandidate of await discoverAlternativeCareerUrls(rootUrl, discoveryState)) {
        enqueueDiscoveryUrl(sitemapCandidate);
    }

    while (visited.size < CONFIG.MAX_DISCOVERY_PAGES_PER_COMPANY) {
        throwIfAborted(signal);
        const current = dequeueDiscoveryUrl();
        if (!current) {
            if (!verificationPass) {
                verificationPass = true;
                verificationSnapshot = snapshotDiscoveryState();
                verificationMeaningful = false;
                visited.delete(rootUrl);
                queuedUrls.delete(rootUrl);
                discoveryState.queuedUrls.delete(rootUrl);
                enqueueDiscoveryUrl(rootUrl, 10);
                continue;
            }
            const afterVerification = snapshotDiscoveryState();
            verificationMeaningful = didDiscoveryChange(verificationSnapshot, afterVerification);
            discoveryState.recordDiscoveryCycle({ meaningful: verificationMeaningful });
            if (verificationMeaningful) {
                // New employment data appeared during verification. Drain any
                // newly created tasks and require another measured verification
                // pass before declaring exhaustion.
                verificationPass = false;
                verificationSnapshot = null;
                continue;
            }
            break;
        }
        if (visited.has(current) || isNonJobUrl(current) || isClearlyIrrelevantDiscoveryUrl(current)) continue;
        if (!isAllowedQueuedDiscoveryUrl(current, rootUrl, externalDiscoveryUrls, externalHosts)) {
            blockExternalUrl(current, 'discovery_queue_external_domain');
            continue;
        }
        visited.add(current);
        discoveryState.pendingTasks.delete(current);
        discoveryState.visitedUrls.add(current);
        const currentIsExternal = externalDiscoveryUrls.has(current);
        if (currentIsExternal) externalPagesScanned++;
        setLogContext({ pageUrl: current, step: 'DISCOVERY' });

        const extracted = await extractLinksFromPage(current, rootUrl, companyName, signal, {
            externalHosts,
            apiState,
            iframeState,
            discoveryState,
            allowUnknownExternal: !externalHosts.has(getDomainHost(current)),
            restrictedExternalHost: externalHosts.has(getDomainHost(current))
                ? getDomainHost(current)
                : null
        });
        throwIfAborted(signal);
        addDiscoveryCounters(discoveryCounters, extracted.counters);
        discoveryCounters.api_candidates += extracted.apiCandidates.length;
        listingPagesScanned++;
        discoveryState.metrics.listingPagesDiscovered++;
        discoveryState.pageState(
            extracted.finalUrl || current,
            `${extracted.jobs.size}|${extracted.listings.size}|${extracted.pages.size}|${extracted.apiCandidates.length}`,
            extracted.finalUrl || current
        );
        discoveryState.recordResultSet([
            ...[...extracted.jobs].map(url => ({ identity: normalizeUrl(url, current), url })),
            ...extracted.apiCandidates.map(candidate => ({ identity: candidate.identity || candidate.jobId || candidate.detailUrl, url: candidate.detailUrl }))
        ]);
        discoveryState.metrics.iframeSourcesDiscovered += extracted.counters?.iframe_candidates || 0;
        discoveryState.metrics.apiJobRecordsDiscovered += extracted.apiCandidates.length;
        if (extracted.failed) failedPages.push({ url: current, reason: extracted.error || 'unknown' });

        extracted.jobs.forEach(u => {
            const normalized = normalizeUrl(u, current);
            if (!normalized || isCategoryUrl(normalized) || isNonJobUrl(normalized)) {
                discoveryCounters.rejected_job_candidates++;
                return;
            }
            if (!isAllowedDiscoveryExternalUrl(normalized, rootUrl, externalHosts)) {
                if (isAtsUrl(normalized)) {
                    atsLinks.add(normalized);
                    discoveryCounters.rejected_job_candidates++;
                    return;
                }
                if (hasStrongExternalCareerSignal(normalized, '', '', current)) {
                    externalHosts.add(getDomainHost(normalized));
                } else {
                    blockExternalUrl(normalized, 'discovered_job_link_external_domain');
                    discoveryCounters.rejected_job_candidates++;
                    return;
                }
            }
            if (jobLinks.has(normalized) || jobLinks.size >= CONFIG.MAX_JOB_LINKS_PER_COMPANY) {
                discoveryCounters.duplicate_job_links++;
                return;
            }
            jobLinks.add(normalized);
            discoveryCounters.accepted_job_links++;
            discoveryState.recordCandidate({ detailUrl: normalized }, null);
            setLogContext({ jobsFound: jobLinks.size });
            onJobLink?.(normalized);
        });
        extracted.apiCandidates.forEach(candidate => {
            const normalized = normalizeUrl(candidate.detailUrl || candidate.jobUrl || candidate.applyUrl, current);
            if (jobLinks.size >= CONFIG.MAX_JOB_LINKS_PER_COMPANY) {
                discoveryState.recordLimit('max_job_links');
                discoveryCounters.rejected_job_candidates++;
                return;
            }
            if (normalized && jobLinks.has(normalized)) {
                if (normalized && jobLinks.has(normalized)) discoveryCounters.duplicate_job_links++;
                else discoveryCounters.rejected_job_candidates++;
                return;
            }
            if (normalized && candidate.isApplyOnly && !isLikelyIndividualJobUrl(normalized)) {
                discoveryCounters.rejected_job_candidates++;
                return;
            }

            const allowed = !normalized || isAllowedDiscoveryExternalUrl(normalized, rootUrl, externalHosts);
            if (!allowed) {
                if (isAtsUrl(normalized)) {
                    atsLinks.add(normalized);
                    discoveryCounters.rejected_job_candidates++;
                    return;
                }
                const apiEvidence = `${candidate.title || ''} ${candidate.location || ''} ${candidate.description || ''}`;
                if (!hasStrongExternalCareerSignal(normalized, candidate.title || '', apiEvidence, current)) {
                    blockExternalUrl(normalized, 'api_job_link_external_domain');
                    discoveryCounters.rejected_job_candidates++;
                    return;
                }
                // This permits the candidate URL only. It does not enqueue the
                // returned host as a recursive discovery root.
                externalHosts.add(getDomainHost(normalized));
            }

            if (normalized) {
                jobLinks.add(normalized);
                discoveryCounters.accepted_job_links++;
            } else apiDirectCandidates++;
            discoveryState.recordCandidate(candidate, null);
            setLogContext({ jobsFound: jobLinks.size + apiState.jobsDetected });
            onJobLink?.(candidate);
        });
        if (extracted.apiCandidates.length) {
            const urlCount = extracted.apiCandidates.filter(candidate => candidate.detailUrl || candidate.jobUrl || candidate.applyUrl).length;
            logInfo('API_DISCOVERY', `api_candidates=${extracted.apiCandidates.length} url_candidates=${urlCount}`);
        }
        extracted.ats.forEach(u => atsLinks.add(u));
        [...extracted.listings, ...extracted.pages].forEach(u => {
            const normalized = normalizeUrl(u, current);
            if (!normalized || visited.has(normalized) || jobLinks.has(normalized)) return;
            if (!isAllowedDiscoveryExternalUrl(normalized, rootUrl, externalHosts)) {
                if (isAtsUrl(normalized)) {
                    atsLinks.add(normalized);
                    return;
                }
                if (hasStrongExternalCareerSignal(normalized, '', '', current)) {
                    externalHosts.add(getDomainHost(normalized));
                } else {
                    blockExternalUrl(normalized, 'discovered_listing_link_external_domain');
                    return;
                }
            }
            const external = !areRelatedCompanyDomains(normalized, rootUrl) && !isAtsUrl(normalized);
            enqueueDiscoveryUrl(normalized, undefined, { external });
        });

        logInfo('DISCOVERY', `pages=${visited.size} queue=${queuedCount()} job_links=${jobLinks.size} ats=${atsLinks.size} counters=${JSON.stringify(discoveryCounters)}`);
        if (jobLinks.size >= CONFIG.MAX_JOB_LINKS_PER_COMPANY) {
            logWarn('DISCOVERY', `Hit MAX_JOB_LINKS_PER_COMPANY=${CONFIG.MAX_JOB_LINKS_PER_COMPANY}; increase env var for very large sites.`);
            break;
        }
    }

    const pendingPages = queuedCount();
    const hitPageLimit = visited.size >= CONFIG.MAX_DISCOVERY_PAGES_PER_COMPANY && pendingPages > 0;
    const hitJobLimit = jobLinks.size >= CONFIG.MAX_JOB_LINKS_PER_COMPANY;
    if (hitPageLimit) discoveryState.recordLimit('max_pages', CONFIG.MAX_DISCOVERY_PAGES_PER_COMPANY);
    if (hitJobLimit) discoveryState.recordLimit('max_job_links', CONFIG.MAX_JOB_LINKS_PER_COMPANY);
    discoveryState.updateRemaining();
    discoveryState.finish({ finalCycleComplete: verificationPass && !verificationMeaningful && pendingPages === 0 });
    if (hitPageLimit) {
        logWarn('DISCOVERY', `Stopped at MAX_DISCOVERY_PAGES_PER_COMPANY=${CONFIG.MAX_DISCOVERY_PAGES_PER_COMPANY}; ${pendingPages} pages remain.`);
    }
    if (hitJobLimit) {
        logWarn('DISCOVERY', `Stopped at MAX_JOB_LINKS_PER_COMPANY=${CONFIG.MAX_JOB_LINKS_PER_COMPANY}.`);
    }
    for (const atsUrl of atsLinks) {
        logInfo('DISCOVERY', `ATS portal found but left for dedicated crawler: ${atsUrl}`);
    }

    const links = [...jobLinks].slice(0, CONFIG.MAX_JOB_LINKS_PER_COMPANY);
    discoveryState.metrics.apiRequestsAttempted = apiState.requestsAttempted || 0;
    discoveryState.metrics.apiRequestsCompleted = apiState.requestsCompleted || 0;
    discoveryState.metrics.apiRequestsFailed = apiState.requestsFailed || 0;
    discoveryState.metrics.apiRequestsRemaining = apiState.continuationRequests?.length || 0;
    logInfo('LINKS', `job_links=${links.length} listing_pages_scanned=${listingPagesScanned} failed_listing_pages=${failedPages.length} discovery_counters=${JSON.stringify(discoveryCounters)}`);
    return {
        links,
        stats: {
            jobLinksFound: links.length + apiDirectCandidates,
            listingPagesScanned,
            externalPagesScanned,
            failedListingPages: failedPages.length,
            atsLinksFound: atsLinks.size,
            stoppedByPageLimit: hitPageLimit,
            stoppedByJobLimit: hitJobLimit,
            discoveryExhausted: discoveryState.metrics.discoveryExhausted,
            tasksRemaining: discoveryState.metrics.tasksRemaining,
            apiDirectCandidates,
            telemetry: discoveryState.metrics,
            discoveryCounters,
            iframePagesInspected: iframeState.inspected
        },
        failures: failedPages,
        state: discoveryState
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
    if (rawJob?.sourceType === 'api' && !normalized) {
        return Boolean(rawJob.externalJobId || rawJob.jobId || rawJob.requisitionId || rawJob.referenceId || rawJob.stableApiId);
    }
    if (!normalized || isNonJobUrl(normalized)) return false;
    if (rawJob?.sourceType === 'api') return true;
    if (isCareerListingUrl(normalized)) return false;
    return isLikelyIndividualJobUrl(normalized) || Boolean(rawJob?.valid);
}

// ─── GENERIC JOB LOCATION EVIDENCE ────────────────────────────────────────
const JOB_LOCATION_LABEL_PATTERN = /^(?:location|locations|arbeitsort|arbeitsplatz|arbeitsplatzort|einsatzort|einsatzorte|standort|dienstort|ort|stadt|city|job location|work location|place of work|place of employment|location\(s\))$/i;
const JOB_LOCATION_LABEL_SEARCH_PATTERN = /\b(?:location|locations|arbeitsort|arbeitsplatz|arbeitsplatzort|einsatzort|einsatzorte|standort|dienstort|ort|stadt|city|job location|work location|place of work|place of employment|location\(s\))\b/i;
const NON_LOCATION_VALUE_PATTERN = /^(?:remote|hybrid|homeoffice|home office|vollzeit|teilzeit|full[- ]?time|part[- ]?time|n\/a|none|unknown|unspecified)$/i;
const LOCATION_EXCLUDED_SELECTOR = 'footer, header, nav, aside, [class*="footer"], [id*="footer"], [class*="impressum"], [id*="impressum"], [class*="contact"], [id*="contact"], [class*="cookie"], [id*="cookie"], [class*="breadcrumb"], [id*="breadcrumb"]';

function normalizeLocationEvidenceText(value) {
    return String(value || '')
        .replace(/\u00a0/g, ' ')
        .replace(/\s+/g, ' ')
        .replace(/[|]+/g, ' ')
        .trim();
}

function isJobLocationLabel(value) {
    return JOB_LOCATION_LABEL_PATTERN.test(normalizeLocationEvidenceText(value).replace(/[:：]+$/, ''));
}

function validJobLocationValue(value) {
    const cleaned = normalizeLocationEvidenceText(value)
        .replace(/^(?:[:：\-]\s*)+/, '')
        .replace(/[.,;|]+$/, '')
        .trim();
    if (!cleaned || cleaned.length < 2 || cleaned.length > 220) return null;
    if (NON_LOCATION_VALUE_PATTERN.test(cleaned)) return null;
    if (/^(?:telefon|tel\.?|phone|fax|email|e-mail|register|legal form|rechtsform|supervisory board|aufsichtsrat)\b/i.test(cleaned)) return null;
    if (/^[+\d\s()./-]{6,}$/.test(cleaned) || /@/.test(cleaned)) return null;
    return cleaned;
}

function splitLocationValues(value) {
    const text = normalizeLocationEvidenceText(value);
    if (!text) return [];
    return text
        .split(/\s*(?:\n|\r|•|\u2022|\|)\s*|\s*(?<=\d{4,6}\s+[A-ZÄÖÜÀ-ÖØ-Þ][A-Za-zÄÖÜÀ-öø-ÿ' .-]{2,60})\s+(?=[A-ZÄÖÜÀ-ÖØ-Þ][A-Za-zÄÖÜÀ-öø-ÿ' .-]{2,60},?\s+\d{4,6}\b)/u)
        .map(validJobLocationValue)
        .filter(Boolean);
}

function createJobLocationEvidence() {
    return { values: [], raw_evidence: [], source_types: [] };
}

function addJobLocationEvidence(evidence, value, source, label, confidence) {
    for (const item of splitLocationValues(value)) {
        const duplicate = evidence.values.find(existing => existing.value.toLowerCase() === item.toLowerCase());
        if (duplicate) {
            duplicate.confidence = Math.max(duplicate.confidence, confidence);
            if (!duplicate.source_types.includes(source)) duplicate.source_types.push(source);
            continue;
        }
        evidence.values.push({ value: item, source, label: label || null, confidence, source_types: [source] });
        evidence.raw_evidence.push(label ? `${label}: ${item}` : item);
        if (!evidence.source_types.includes(source)) evidence.source_types.push(source);
    }
}

function locationValueFromStructuredPlace(place) {
    if (typeof place === 'string') return place;
    if (!place || typeof place !== 'object') return null;
    const address = place.address && typeof place.address === 'object' ? place.address : place;
    const street = address.streetAddress || address.street || '';
    const postal = address.postalCode || address.zip || '';
    const locality = address.addressLocality || address.city || '';
    const region = address.addressRegion || address.region || '';
    const country = typeof address.addressCountry === 'object'
        ? (address.addressCountry.name || address.addressCountry.value || '')
        : (address.addressCountry || address.country || '');
    const structured = [street, [postal, locality].filter(Boolean).join(' '), region, country]
        .map(normalizeLocationEvidenceText)
        .filter(Boolean)
        .join(', ');
    return structured || null;
}

function collectJobPostingLocationObjects(value, output = []) {
    if (!value || typeof value !== 'object') return output;
    if (Array.isArray(value)) {
        value.forEach(item => collectJobPostingLocationObjects(item, output));
        return output;
    }
    const type = value['@type'];
    const isJobPosting = type === 'JobPosting' || (Array.isArray(type) && type.includes('JobPosting'));
    if (isJobPosting && value.jobLocation) {
        const locations = Array.isArray(value.jobLocation) ? value.jobLocation : [value.jobLocation];
        locations.forEach(location => output.push(location));
    }
    Object.values(value).forEach(child => collectJobPostingLocationObjects(child, output));
    return output;
}

function addJsonLdJobLocationEvidence($, evidence) {
    $('script[type="application/ld+json"]').each((_, element) => {
        try {
            const parsed = JSON.parse($(element).contents().text().trim());
            for (const location of collectJobPostingLocationObjects(parsed)) {
                addJobLocationEvidence(evidence, locationValueFromStructuredPlace(location), 'json_ld', 'jobLocation', 1);
            }
        } catch { /* malformed or non-JSON-LD scripts are ignored */ }
    });
}

function addMicrodataJobLocationEvidence($, root, evidence) {
    root.find('[itemprop="jobLocation"], [itemprop="location"]').each((_, element) => {
        const root = $(element);
        const fields = {};
        root.find('[itemprop]').addBack('[itemprop]').each((__, field) => {
            const key = $(field).attr('itemprop');
            if (key) fields[key] = $(field).attr('content') || $(field).text();
        });
        addJobLocationEvidence(evidence, locationValueFromStructuredPlace(fields) || root.text(), 'microdata', 'jobLocation', 0.98);
    });
    root.find('[property="jobLocation"], [property="schema:jobLocation"]').each((_, element) => {
        addJobLocationEvidence(evidence, $(element).attr('content') || $(element).text(), 'rdfa', 'jobLocation', 0.96);
    });
}

function elementTextWithSeparators($, element) {
    const clone = $(element).clone();
    const html = String(clone.html() || '')
        .replace(/<br\s*\/?\s*>/gi, ' ')
        .replace(/<\/(?:h[1-6]|p|div|section|article|li|dt|dd|th|td)>/gi, ' ')
        .replace(/<[^>]+>/g, ' ');
    return normalizeLocationEvidenceText(html);
}

function addLabelValueEvidence($, root, evidence) {
    const addPair = (labelElement, valueElements, source, confidence) => {
        const label = normalizeLocationEvidenceText($(labelElement).text()).replace(/[:：]+$/, '');
        if (!isJobLocationLabel(label)) return;
        for (const valueElement of valueElements) {
            addJobLocationEvidence(evidence, elementTextWithSeparators($, valueElement), source, label, confidence);
        }
    };

    root.find('dt').each((_, element) => {
        const values = [];
        let sibling = $(element).next();
        while (sibling.length && sibling.is('dd')) {
            values.push(sibling[0]);
            sibling = sibling.next();
        }
        addPair(element, values, 'definition_list', 0.96);
    });

    root.find('tr').each((_, element) => {
        const cells = $(element).find('th,td').toArray();
        if (cells.length >= 2) addPair(cells[0], cells.slice(1), 'table', 0.95);
    });

    root.find('div, li, p, section, article').each((_, element) => {
        const root = $(element);
        const children = root.children().toArray();
        if (children.length >= 2) {
            const label = normalizeLocationEvidenceText($(children[0]).text()).replace(/[:：]+$/, '');
            if (isJobLocationLabel(label)) addPair(children[0], children.slice(1), 'label_value', 0.92);
        }
        const text = elementTextWithSeparators($, element);
        if (text.length <= 320 && JOB_LOCATION_LABEL_SEARCH_PATTERN.test(text)) {
            const match = text.match(/^(.{2,60}?)\s*[:：-]\s*(.+)$/i);
            if (match && isJobLocationLabel(match[1])) addJobLocationEvidence(evidence, match[2], 'label_value', match[1], 0.9);
        }
    });
}

function addSemanticAddressEvidence($, root, evidence) {
    root.find('address').each((_, element) => {
        const root = $(element);
        const context = `${elementTextWithSeparators($, root.parent())} ${root.attr('class') || ''} ${root.attr('id') || ''}`;
        if (JOB_LOCATION_LABEL_SEARCH_PATTERN.test(context)) {
            addJobLocationEvidence(evidence, elementTextWithSeparators($, element), 'address_element', 'job location', 0.9);
        } else {
            addJobLocationEvidence(evidence, elementTextWithSeparators($, element), 'address_element', null, 0.82);
        }
    });
}

function addDescriptionLocationEvidence(text, evidence) {
    const normalized = String(text || '').replace(/\r/g, '');
    const labelled = /(?:Arbeitsort|Arbeitsplatz|Arbeitsplatzort|Einsatzort|Einsatzorte|Standort|Dienstort|Location(?:s)?|Job Location|Work Location|Place of Work)\s*[:：-]\s*([^\n|]{2,180})/gi;
    for (const match of normalized.matchAll(labelled)) addJobLocationEvidence(evidence, match[1], 'description', match[0].split(/[:：-]/)[0], 0.84);
    const prose = /(?:die stelle|the position|this role|the job)\s+(?:ist|is|liegt|liegt in|is based|based)\s+(?:in|at)\s+([A-ZÄÖÜÀ-ÖØ-Þ][A-Za-zÄÖÜÀ-öø-ÿ' .-]{2,100}?)(?:\s+(?:angesiedelt|located|based))?(?=[.!?,;]|$)/gi;
    for (const match of normalized.matchAll(prose)) addJobLocationEvidence(evidence, match[1], 'description', 'employment location statement', 0.78);
    const based = /\bposition\s+based\s+in\s+([A-ZÄÖÜÀ-ÖØ-Þ][A-Za-zÄÖÜÀ-öø-ÿ' .-]{2,100})/gi;
    for (const match of normalized.matchAll(based)) addJobLocationEvidence(evidence, match[1], 'description', 'position based in', 0.78);
}

function extractJobLocationEvidence(container, $, descriptionText = '') {
    const evidence = createJobLocationEvidence();
    const scoped = container.clone();
    scoped.find(LOCATION_EXCLUDED_SELECTOR).remove();
    addJsonLdJobLocationEvidence($, evidence);
    addMicrodataJobLocationEvidence($, scoped, evidence);
    addSemanticAddressEvidence($, scoped, evidence);
    addLabelValueEvidence($, scoped, evidence);
    addDescriptionLocationEvidence(descriptionText || elementTextWithSeparators($, scoped), evidence);
    evidence.values.sort((a, b) => b.confidence - a.confidence);
    evidence.locations = evidence.values.map(item => item.value);
    evidence.location = evidence.locations.join('; ') || null;
    evidence.raw_evidence = evidence.raw_evidence.join(' | ');
    return evidence;
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
    return text;
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
    const locationContainer = findJobContainer($);
    const locationEvidence = extractJobLocationEvidence(locationContainer, $, null);
    $('script,style,noscript,nav,footer,header,.cookie-banner,#cookie,[class*="cookie"],[class*="navigation"],[class*="breadcrumb"]').remove();
    const jsonUrl = normalizeUrl(jsonJob?.url, pageUrl);
    const applicationUrl = normalizeUrl(jsonJob?.applicationContact?.url, pageUrl) ||
        extractApplyUrlFromPage($, pageUrl);
    const jobPageUrl = chooseJobPageUrl({ pageUrl, canonicalUrl, jsonUrl, applyUrl: applicationUrl });
    const jsonDescription = jsonJob?.description ? cheerio.load(String(jsonJob.description)).text() : '';

    const container = findJobContainer($);
    const htmlTitle = extractJobTitleFromContainer(container, $);
    const title = compactText(jsonJob?.title || htmlTitle, 220);
    const visibleText = compactText(elementTextWithSeparators($, container) || $('body').text(), 12000);
    const rawDescription = cleanDescription([title, jsonDescription, visibleText].filter(Boolean).join('\n\n'));
    locationEvidence.raw_evidence = Array.isArray(locationEvidence.raw_evidence)
        ? locationEvidence.raw_evidence
        : (locationEvidence.raw_evidence ? [locationEvidence.raw_evidence] : []);
    addDescriptionLocationEvidence(rawDescription || visibleText, locationEvidence);
    locationEvidence.values.sort((a, b) => b.confidence - a.confidence);
    locationEvidence.locations = locationEvidence.values.map(item => item.value);
    locationEvidence.location = locationEvidence.locations.join('; ') || null;
    locationEvidence.raw_evidence = locationEvidence.raw_evidence.join(' | ');
    const location = locationEvidence.location;
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
        locationEvidence,
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
async function structureJobWithGPT(rawJobOrTitle, maybeDescription, signal) {
    throwIfAborted(signal);
    const rawJob = typeof rawJobOrTitle === 'object'
        ? rawJobOrTitle
        : { title: rawJobOrTitle, rawDescription: maybeDescription };
    const result = await classifyJobWithLLM({
        company_name: rawJob.companyName,
        title: rawJob.title || '',
        raw_description: rawJob.rawDescription || maybeDescription || '',
        source_url: rawJob.canonicalUrl || rawJob.url || null,
        crawler_location: rawJob.location || null,
        location_evidence: rawJob.locationEvidence?.raw_evidence || null,
        structured_location: rawJob.structuredLocation || null,
        company_hq: rawJob.companyHq || await findCompanyHqLocation(rawJob.companyWebsiteUrl, { signal }),
        model: CONFIG.GPT_MODEL
    }, { signal });
    if (!result.ok) return null;
    return result.data;
}

// ─── GEOCODING ────────────────────────────────────────────────────────────
const geocodeCache = new Map();
let lastGeo = 0;

async function geocodeCity(city, signal) {
    if (!city || typeof city !== 'string') return { lat: null, lng: null };
    throwIfAborted(signal);
    const key = city.toLowerCase().trim();
    if (geocodeCache.has(key)) return geocodeCache.get(key);

    const elapsed = Date.now() - lastGeo;
    if (elapsed < 1100) await new Promise(r => setTimeout(r, 1100 - elapsed));
    throwIfAborted(signal);
    lastGeo = Date.now();

    try {
        const r = await axios.get('https://nominatim.openstreetmap.org/search', {
            params: { q: city + ', Germany', format: 'json', limit: 1 },
            headers: { 'User-Agent': CONFIG.NOMINATIM_USER_AGENT },
            timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
            signal
        });
        if (r.data && r.data.length > 0) {
            const result = { lat: parseFloat(r.data[0].lat), lng: parseFloat(r.data[0].lon) };
            geocodeCache.set(key, result);
            return result;
        }
    } catch (e) {
        throwIfAborted(signal);
    }
    const nullR = { lat: null, lng: null };
    geocodeCache.set(key, nullR);
    return nullR;
}

// ─── VOYAGE EMBEDDING ─────────────────────────────────────────────────────
async function embedWithVoyage(text, signal) {
    throwIfAborted(signal);
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
                timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
                signal
            }
        );
        return r.data?.data?.[0]?.embedding || null;
    } catch (err) {
        throwIfAborted(signal);
        console.warn(`[VOYAGE] ${err.message}`);
        return null;
    }
}

// ─── DEDUP ────────────────────────────────────────────────────────────────
function generateExternalJobId(value) {
    if (value && typeof value === 'object') {
        const identity = value.externalJobId || value.requisitionId || value.apiJobId || value.canonicalUrl;
        if (identity) {
            const companyPrefix = value.companyId ? `${value.companyId}:` : '';
            return crypto.createHash('sha256').update(`${companyPrefix}${identity}`.toLowerCase()).digest('hex').slice(0, 40);
        }
        value = value.canonicalUrl;
    }
    const identity = normalizeJobIdentityUrl(value) || String(value || '').trim();
    return crypto.createHash('sha256').update(identity.toLowerCase()).digest('hex').slice(0, 40);
}

// ─── DB ───────────────────────────────────────────────────────────────────
async function markCompanyStatus(companyId, status, { touchTimestamp = true, error: companyError } = {}) {
    if (companyRunContext.getStore()?.timedOut && !['failed', 'partial'].includes(status)) return;
    const u = { crawl_status: status };
    if (touchTimestamp) u.last_crawled_at = new Date().toISOString();
    if (companyError) {
        u.last_error = companyError.message;
        u.last_error_type = companyError.name || companyError.code || 'UnknownError';
    }
    const { error } = await supabase.from('companies').update(u).eq('Id', companyId);
    if (error) console.error(`[DB] ${error.message}`);
}

async function batchInsertJobs(rows) {
    if (rows.length === 0) return 0;
    let inserted = 0;
    for (let i = 0; i < rows.length; i += CONFIG.BATCH_INSERT_SIZE) {
        const batch = rows.slice(i, i + CONFIG.BATCH_INSERT_SIZE);
        const preparedBatch = await Promise.all(batch.map(row => preserveAuthoritativeFieldsForUpsert(supabase, row)));
        const { error } = await supabase.from('jobs').upsert(preparedBatch, {
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

async function logCrawlEvent(companyId, status, payload = {}, { allowWhenTimedOut = false } = {}) {
    if (companyRunContext.getStore()?.timedOut && !allowWhenTimedOut) return;
    const row = {
        company_id: companyId,
        crawl_type: 'custom',
        status,
        jobs_found: payload.jobs_found ?? payload.jobsSaved ?? null,
        error_message: payload.error_message || payload.reason || null,
        duration_ms: payload.duration_ms ?? payload.elapsedMs ?? null,
        created_at: new Date()
    };
    const { error } = await supabase.from('crawl_logs').insert(row);
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
async function processJobLink(input, companyId, companyName, signal, companyWebsiteUrl) {
    throwIfAborted(signal);
    const apiCandidate = input && typeof input === 'object' ? input : null;
    const url = apiCandidate?.detailUrl || apiCandidate?.jobUrl || apiCandidate?.applyUrl || apiCandidate?.responseUrl || input;
    const normalizedInputUrl = normalizeUrl(url) || url;
    if (isEnglishLanguageVariant(normalizedInputUrl)) {
        logWarn('JOB', `SKIP English variant: ${normalizedInputUrl}`);
        return { skip: true, reason: 'english_language_variant_skipped', url: normalizedInputUrl };
    }
    let html = null;
    let pdfText = '';
    let finalUrl = normalizedInputUrl;

    if (apiCandidate) {
        // Complete API records are valid job sources even when no HTML detail
        // page exists. They use the same validator/classifier/upsert path below.
        finalUrl = normalizeUrl(apiCandidate.detailUrl || apiCandidate.jobUrl || apiCandidate.applyUrl || apiCandidate.responseUrl) || normalizedInputUrl;
    } else if (isPdfUrl(normalizedInputUrl)) {
        pdfText = await downloadAndParsePDF(normalizedInputUrl, signal) || '';
    } else {
        const r = await fetchPageWithFallback(normalizedInputUrl, { waitForSelector: 'body', scroll: true, signal });
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

    const rawJob = apiCandidate
        ? {
            sourceType: 'api',
            valid: true,
            reasons: [],
            title: apiCandidate.title,
            rawDescription: apiCandidate.description || apiCandidate.rawDescription,
            location: apiCandidate.location,
            applyUrl: apiCandidate.applyUrl || apiCandidate.detailUrl || apiCandidate.jobUrl || apiCandidate.responseUrl,
            applicationUrl: apiCandidate.applyUrl,
            canonicalUrl: apiCandidate.detailUrl || apiCandidate.jobUrl || apiCandidate.responseUrl || finalUrl,
            jobPageUrl: apiCandidate.detailUrl || apiCandidate.jobUrl || apiCandidate.applyUrl || apiCandidate.responseUrl || finalUrl,
            score: 10,
            jsonLdCount: 0,
            hiringOrganization: apiCandidate.company,
            employmentType: apiCandidate.employmentType,
            department: apiCandidate.department,
            requirements: apiCandidate.requirements,
            responsibilities: apiCandidate.responsibilities,
            externalJobId: apiCandidate.externalJobId,
            requisitionId: apiCandidate.requisitionId,
            jobId: apiCandidate.jobId,
            referenceId: apiCandidate.referenceId,
            stableApiId: apiCandidate.stableApiId
        }
        : pdfText
        ? extractRawJobFromPdf(pdfText, finalUrl)
        : extractRawJobFromHtml(html, finalUrl, companyName);
    throwIfAborted(signal);
    rawJob.url = finalUrl;
    rawJob.companyName = companyName;
    rawJob.companyWebsiteUrl = companyWebsiteUrl;

    const genericValidation = validateGenericJobCandidate({
        sourceType: rawJob.sourceType,
        apiRecord: rawJob.sourceType === 'api',
        title: rawJob.title,
        rawDescription: rawJob.rawDescription,
        location: rawJob.location,
        detailUrl: rawJob.jobPageUrl || rawJob.canonicalUrl || finalUrl,
        applicationUrl: rawJob.applicationUrl || rawJob.applyUrl,
        jsonLd: rawJob.jsonLdCount > 0,
        metadata: rawJob.hiringOrganization || rawJob.department || rawJob.requirements || rawJob.responsibilities,
        department: rawJob.department,
        requirements: rawJob.requirements,
        responsibilities: rawJob.responsibilities,
        employmentType: rawJob.employmentType,
        externalJobId: rawJob.externalJobId,
        jobId: rawJob.jobId,
        requisitionId: rawJob.requisitionId,
        referenceId: rawJob.referenceId,
        stableApiId: rawJob.stableApiId
    });

    if (!genericValidation.valid) {
        return {
            skip: true,
            reason: `generic_job_evidence_failed:${genericValidation.reasons.join('|')}`,
            url: finalUrl,
            title: rawJob.title,
            evidence: genericValidation.evidence
        };
    }

    if (!rawJob.valid) {
        return {
            skip: true,
            reason: rawJob.reasons.join('|') || 'raw_validation_failed',
            url: finalUrl,
            title: rawJob.title
        };
    }

    const structured = await structureJobWithGPT(rawJob, undefined, signal);
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

    const companyHq = rawJob.companyHq || await findCompanyHqLocation(companyWebsiteUrl, { signal });
    const deterministicLocation = rawJob.locationEvidence?.location || rawJob.location || null;
    const resolvedLocation = await resolveJobLocation({
        location: deterministicLocation || structured.job_location,
        company_website: companyWebsiteUrl,
        company_hq: companyHq,
        raw_description: rawJob.rawDescription,
        description: rawJob.rawDescription,
        location_evidence: rawJob.locationEvidence?.raw_evidence || null,
        title: structuredValidation.title,
        employment_type: structured.employment_type,
        remote_evidence: structured.remote_type === 'hybrid' ? 'hybrid' : '',
        requirements: rawJob.requirements,
        responsibilities: rawJob.responsibilities,
        location_lat: null,
        location_lng: null
    }, { signal, companyHq });
    const location = resolvedLocation.location;
    const lat = resolvedLocation.location_lat;
    const lng = resolvedLocation.location_lng;

    const finalTitle = structuredValidation.title;
    const rawDescription = `${finalTitle}\n\n${rawJob.rawDescription}`.trim().slice(0, 6000);
    const embedding = await embedWithVoyage(rawDescription, signal);
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
    const externalSourceId = {
        companyId,
        externalJobId: rawJob.externalJobId,
        requisitionId: rawJob.requisitionId,
        apiJobId: rawJob.jobId || rawJob.stableApiId || rawJob.referenceId,
        canonicalUrl: jobPageUrl || rawJob.canonicalUrl || finalUrl
    };
    throwIfAborted(signal);

    return {
        skip: false,
        row: {
            company_id: companyId,
            external_job_id: generateExternalJobId(externalSourceId),
            title: finalTitle.slice(0, 255),
            raw_description: rawDescription,
            structured_skills: Array.isArray(structured.skills) && structured.skills.length > 0 ? structured.skills : null,
            seniority_level: structured.seniority_level,
            location: location ? String(location) : null,
            location_lat: lat,
            location_lng: lng,
            remote_type: resolvedLocation.remote_type,
            _classification_source: 'authoritative',
            _location_source: resolvedLocation.source,
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

async function withTimeout(task, ms, label, onTimeout, parentSignal) {
    const controller = new AbortController();
    let timedOut = false;
    let timeoutCleanup = Promise.resolve();
    const timeoutError = new Error(`Timeout: ${label}`);
    timeoutError.name = 'TimeoutError';

    const timer = setTimeout(() => {
        timedOut = true;
        controller.abort(timeoutError);
        timeoutCleanup = Promise.resolve(onTimeout?.(timeoutError))
            .catch(cleanupError => logError('TIMEOUT', `Cleanup failed: ${cleanupError.message}`));
    }, ms);
    const abortFromParent = () => controller.abort(getAbortError(parentSignal));
    if (parentSignal) {
        if (parentSignal.aborted) abortFromParent();
        else parentSignal.addEventListener('abort', abortFromParent, { once: true });
    }

    try {
        const result = await task(controller.signal);
        if (timedOut) {
            await timeoutCleanup;
            throw timeoutError;
        }
        return result;
    } catch (err) {
        if (timedOut) {
            await timeoutCleanup;
            throw timeoutError;
        }
        throw err;
    } finally {
        clearTimeout(timer);
        if (parentSignal) parentSignal.removeEventListener('abort', abortFromParent);
    }
}

function createStreamingJobProcessor({
    companyId,
    companyName,
    companyWebsiteUrl,
    signal,
    metrics,
    seen,
    rejectedSamples,
    failedSamples,
    rejectionReasons
}) {
    const queuedLinks = new Set();
    const pendingLinks = [];
    const activeTasks = new Set();
    let fatalError = null;

    const isCancelled = () => signal?.aborted || companyRunContext.getStore()?.timedOut;
    const candidateKey = candidate => typeof candidate === 'object'
        ? (candidate.identity || candidate.jobId || candidate.requisitionId || candidate.detailUrl || candidate.applyUrl || candidate.responseUrl)
        : candidate;

    async function processOne(link) {
        let result;
        try {
            result = await withTimeout(
                jobSignal => processJobLink(link, companyId, companyName, jobSignal, companyWebsiteUrl),
                CONFIG.JOB_TIMEOUT_MS,
                `job ${candidateKey(link)}`,
                undefined,
                signal
            );
        } catch (error) {
            metrics.failedPages++;
            if (failedSamples.length < 20) failedSamples.push({ url: candidateKey(link), reason: error.message });
            logError('JOB', `FAIL ${candidateKey(link)}: ${error.message}`);
            return;
        }

        if (result.skip) {
            metrics.invalidRejected++;
            companyRunContext.getStore()?.discoveryState?.recordValidation({
                detailUrl: result.url || candidateKey(link),
                title: result.title
            }, false);
            if (isNotFoundReason(result.reason)) metrics.notFoundPages++;
            const reason = result.reason || 'unknown_rejection';
            rejectionReasons.set(reason, (rejectionReasons.get(reason) || 0) + 1);
            if (rejectedSamples.length < 20) {
                rejectedSamples.push({ url: result.url || candidateKey(link), title: result.title || null, reason });
            }
            logWarn('JOB', `REJECT ${result.title || candidateKey(link)} (${reason})`);
            return;
        }

        metrics.jobPagesFetched++;
        metrics.jobsStructured++;
        companyRunContext.getStore()?.discoveryState?.recordValidation({
            identity: result.row.external_job_id,
            detailUrl: result.meta?.url,
            title: result.row.title,
            location: result.row.location,
            description: result.row.raw_description
        }, true);
        if (seen.has(result.row.external_job_id)) {
            metrics.duplicateSkipped++;
            return;
        }
        seen.add(result.row.external_job_id);

        // Persist each completed job before its worker slot is released. This
        // preserves completed work if the company deadline later cancels the
        // remaining discovery or job-detail tasks.
        try {
            const saved = await batchInsertJobs([result.row]);
            metrics.jobsSaved += saved;
            setLogContext({ jobsSaved: metrics.jobsSaved });
            logInfo('DB', `Saved incremental batch=${saved} total=${metrics.jobsSaved}`);
            logInfo('JOB', `OK ${result.row.title.slice(0, 55)} | ${result.row.location || '-'} | ${result.row.structured_skills?.length || 0} skills`);
        } catch (error) {
            fatalError = fatalError || error;
            logError('DB', `Incremental save failed for ${candidateKey(link)}: ${error.message}`);
        }
    }

    function pump() {
        if (fatalError || isCancelled()) {
            pendingLinks.length = 0;
            return;
        }
        while (activeTasks.size < CONFIG.JOB_DETAIL_CONCURRENCY && pendingLinks.length > 0) {
            const link = pendingLinks.shift();
            const task = processOne(link).catch(error => {
                fatalError = fatalError || error;
                logError('JOB', `Unexpected streaming failure for ${candidateKey(link)}: ${error.message}`);
            });
            activeTasks.add(task);
            task.finally(() => {
                activeTasks.delete(task);
                pump();
            }).catch(() => {});
        }
    }

    function enqueue(link) {
        const key = candidateKey(link);
        if (!key || isCancelled() || fatalError || queuedLinks.has(key)) return false;
        queuedLinks.add(key);
        pendingLinks.push(link);
        pump();
        return true;
    }

    async function drain({ cancelPending = false } = {}) {
        if (cancelPending || isCancelled() || fatalError) pendingLinks.length = 0;
        else pump();

        while (activeTasks.size > 0) {
            await Promise.all([...activeTasks]);
            if (cancelPending || isCancelled() || fatalError) pendingLinks.length = 0;
            else pump();
        }

        if (!cancelPending && !isCancelled() && !fatalError && pendingLinks.length > 0) {
            return drain();
        }
        if (fatalError) throw fatalError;
    }

    return { enqueue, drain };
}

// ─── QUEUE ────────────────────────────────────────────────────────────────
async function processCompany(job, signal) {
    throwIfAborted(signal);
    const {
        companyId,
        companyName,
        companyWebsiteUrl,
        companyIndex,
        companyTotal,
        careerPageUrl,
        detectedCareerUrl,
        careerPageStatus,
        detectionSignals
    } = job.data;
    const careerUrl = selectOperationalCareerUrl({
        career_page_url: careerPageUrl,
        detected_career_url: detectedCareerUrl || job.data.careerUrl
    });
    const careerRecord = {
        career_page_url: careerPageUrl,
        detected_career_url: detectedCareerUrl || job.data.careerUrl,
        career_page_status: careerPageStatus,
        detection_signals: detectionSignals
    };
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
    const runContext = companyRunContext.getStore();
    if (runContext) runContext.metrics = metrics;

    setLogContext({
        companyName,
        companyId,
        pageUrl: careerUrl || null,
        jobsSaved: 0,
        jobsFound: 0,
        step: 'START',
    });
    logInfo('CRAWL', `Start ${companyLabel(companyName, companyIndex, companyTotal)} | companyId=${companyId} | careerUrl=${careerUrl || 'n/a'}`);
    await markCompanyStatus(companyId, 'in_progress', { touchTimestamp: false });

    metrics.activeJobsBefore = await getActiveJobCount(companyId);
    const startedAt = Date.now();

    let effectiveUrl = careerUrl;
    if (effectiveUrl) {
        const validated = await validateCareerPage(effectiveUrl, companyName, companyWebsiteUrl, {
            trustedCareerUrl: effectiveUrl,
            company: careerRecord
        });
        if (validated?.sourceType === 'external_ats') {
            metrics.notFoundReason = validated.reason;
            logInfo('ATS', `Skipping custom discovery for external ATS source: ${validated.url || effectiveUrl}`);
            effectiveUrl = null;
        } else if (validated?.ok) {
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
        const externalDomainBlocked = /career_url_(redirected_to_)?external_domain/.test(reason);
        const externalAtsSource = reason === 'career_url_external_ats';
        await markCompanyStatus(companyId, 'not_found');
        await logCrawlEvent(companyId, 'not_found', {
            reason,
            error_message: externalAtsSource
                ? 'Career URL is hosted on an external ATS source and was left for dedicated ATS handling; existing jobs were preserved.'
                : externalDomainBlocked
                    ? 'Career URL redirected to or is hosted on an unrelated external domain; existing jobs were preserved.'
                    : 'No valid career URL was available; existing jobs were preserved.',
            duration_ms: Date.now() - startedAt,
            ...metrics
        });
        return { status: 'not_found', companyId, companyName, companyIndex, companyTotal, jobsSaved: 0, elapsedMs: Date.now() - startedAt, metrics };
    }

    const seen = new Set();
    const rejectedSamples = [];
    const failedSamples = [];
    const rejectionReasons = new Map();
    const jobProcessor = createStreamingJobProcessor({
        companyId,
        companyName,
        companyWebsiteUrl,
        signal,
        metrics,
        seen,
        rejectedSamples,
        failedSamples,
        rejectionReasons
    });

    let discovery;
    try {
        discovery = await extractAllJobLinks(effectiveUrl, companyName, {
            signal,
            companyWebsiteUrl,
            trustedCareerUrl: isTrustedCareerEntryUrl(effectiveUrl, careerRecord) ? effectiveUrl : null,
            onJobLink: link => jobProcessor.enqueue(link),
            onDiscoveryState: state => { companyRunContext.getStore().discoveryState = state; }
        });
        companyRunContext.getStore().discoveryState = discovery.state;
        await jobProcessor.drain();
    } catch (error) {
        // A company deadline drops work that has not started, but waits for
        // already-active work to settle so successfully completed rows remain
        // in Supabase before the timeout is reported to BullMQ.
        await jobProcessor.drain({ cancelPending: true }).catch(cleanupError => {
            logError('JOB', `Streaming cleanup failed: ${cleanupError.message}`);
        });
        throw error;
    }
    throwIfAborted(signal);
    const links = discovery.links || [];
    setLogContext({ step: 'JOBS', pageUrl: effectiveUrl, jobsFound: links.length, jobsSaved: 0 });
    Object.assign(metrics, {
        jobLinksFound: discovery.stats?.jobLinksFound || links.length,
        listingPagesScanned: discovery.stats?.listingPagesScanned || 0,
        failedPages: metrics.failedPages + (discovery.stats?.failedListingPages || 0),
        notFoundPages: metrics.notFoundPages + (discovery.failures?.filter(f => isNotFoundReason(f.reason)).length || 0)
    });

    const discoveredJobCount = links.length + (discovery.stats?.apiDirectCandidates || 0);
    if (discoveredJobCount === 0) {
        const status = getZeroLinkStatus(discovery);
        const technicalFailure = status === 'failed';
        const reason = technicalFailure ? 'discovery_failed_without_job_links' : 'zero_job_links';
        const errorMessage = technicalFailure
            ? 'Job discovery encountered technical failures before any usable job links were recovered.'
            : metrics.activeJobsBefore > 0
                ? 'Discovery returned zero job links; existing jobs were preserved.'
                : 'Discovery returned zero job links.';
        await markCompanyStatus(companyId, status, technicalFailure
            ? { error: new Error(errorMessage) }
            : {});
        await logCrawlEvent(companyId, status, {
            reason,
            error_message: errorMessage,
            careerUrl: effectiveUrl,
            duration_ms: Date.now() - startedAt,
            ...metrics
        });
        return { status, companyId, companyName, companyIndex, companyTotal, jobsSaved: 0, elapsedMs: Date.now() - startedAt, metrics };
    }

    logInfo('DB', `streaming_saved=${metrics.jobsSaved}`);
    metrics.activeJobsAfter = await getActiveJobCount(companyId);
    setLogContext({ step: 'SAVE', jobsSaved: metrics.jobsSaved });

    const fetchedRatio = discoveredJobCount > 0 ? metrics.jobPagesFetched / discoveredJobCount : 0;
    const saveRatio = discoveredJobCount > 0 ? metrics.jobsSaved / discoveredJobCount : 0;
    const partial = discovery.stats?.stoppedByPageLimit ||
        discovery.stats?.stoppedByJobLimit ||
        discovery.stats?.discoveryExhausted === false ||
        metrics.failedPages > 0 ||
        fetchedRatio < CONFIG.PARTIAL_CRAWL_MIN_FETCH_RATIO ||
        saveRatio < CONFIG.PARTIAL_CRAWL_MIN_SAVE_RATIO;
    const partialReasons = [];
    if (discovery.stats?.stoppedByPageLimit) partialReasons.push('discovery stopped at the page safety limit');
    if (discovery.stats?.stoppedByJobLimit) partialReasons.push('discovery stopped at the job safety limit');
    if (discovery.stats?.discoveryExhausted === false) partialReasons.push(`${discovery.stats?.tasksRemaining || 0} discovery task(s) remained`);
    if (metrics.failedPages > 0) partialReasons.push(`${metrics.failedPages} job page(s) failed`);
    if (fetchedRatio < CONFIG.PARTIAL_CRAWL_MIN_FETCH_RATIO) partialReasons.push(`fetch ratio ${fetchedRatio.toFixed(2)}`);
    if (saveRatio < CONFIG.PARTIAL_CRAWL_MIN_SAVE_RATIO) partialReasons.push(`save ratio ${saveRatio.toFixed(2)}`);
    const allDiscoveredJobsTechnicallyUnavailable = metrics.jobsSaved === 0 &&
        discoveredJobCount > 0 &&
        hasTechnicalJobFailure(metrics, rejectionReasons);

    const rejectionSummary = [...rejectionReasons.entries()]
        .map(([reason, count]) => `${reason}:${count}`)
        .join(', ') || '-';
    logInfo('SUMMARY', `links=${links.length} fetched=${metrics.jobPagesFetched} structured=${metrics.jobsStructured} rejected=${metrics.invalidRejected} duplicates=${metrics.duplicateSkipped} saved=${metrics.jobsSaved} failed=${metrics.failedPages} active_jobs=${metrics.activeJobsAfter ?? '-'}`);
    logInfo('REJECTIONS', rejectionSummary);

    if (allDiscoveredJobsTechnicallyUnavailable) {
        await markCompanyStatus(companyId, 'failed', {
            error: new Error('Discovered job pages were unavailable after technical fetch attempts.')
        });
        await logCrawlEvent(companyId, 'failed', {
            reason: 'all_job_pages_unavailable_after_scraperapi',
            error_message: 'Job links were discovered, but every job page failed or was unavailable after ScraperAPI fallback; existing jobs were preserved.',
            careerUrl: effectiveUrl,
            duration_ms: Date.now() - startedAt,
            rejectedSamples,
            failedSamples,
            discovery: discovery.stats,
            discovery_telemetry: discovery.stats?.telemetry || null,
            ...metrics
        });
        return { status: 'failed', companyId, companyName, companyIndex, companyTotal, jobsSaved: 0, elapsedMs: Date.now() - startedAt, metrics };
    }

    if (partial) {
        await markCompanyStatus(companyId, 'partial');
        await logCrawlEvent(companyId, 'partial', {
            reason: 'partial_custom_crawl_preserved_existing_jobs',
            error_message: `Partial crawl: ${partialReasons.join('; ')}. Existing jobs were preserved and no deletion was performed.`,
            careerUrl: effectiveUrl,
            duration_ms: Date.now() - startedAt,
            rejectedSamples,
            failedSamples,
            discovery: discovery.stats,
            discovery_telemetry: discovery.stats?.telemetry || null,
            ...metrics
        });
        return { status: 'partial', companyId, companyName, companyIndex, companyTotal, jobsSaved: metrics.jobsSaved, elapsedMs: Date.now() - startedAt, metrics };
    }

    await markCompanyStatus(companyId, metrics.jobsSaved > 0 ? 'completed' : 'no_jobs');
    await logCrawlEvent(companyId, metrics.jobsSaved > 0 ? 'completed' : 'no_jobs', {
        careerUrl: effectiveUrl,
        duration_ms: Date.now() - startedAt,
        rejectedSamples,
        failedSamples,
        discovery: discovery.stats,
        discovery_telemetry: discovery.stats?.telemetry || null,
        ...metrics
    });
    setLogContext({ step: 'DONE', jobsSaved: metrics.jobsSaved, pageUrl: effectiveUrl });
    return {
        status: metrics.jobsSaved > 0 ? 'completed' : 'no_jobs',
        companyId,
        companyName,
        companyIndex,
        companyTotal,
        jobsSaved: metrics.jobsSaved,
        elapsedMs: Date.now() - startedAt,
        metrics
    };
}

async function resetStuck() {
    const { data } = await supabase.from('companies')
        .update({ crawl_status: 'pending' })
        .eq('ats_type', 'custom')
        .eq('crawl_status', 'in_progress')
        .select('Id');
    if (data?.length) console.log(`[RESET] ${data.length} stuck companies`);
}

async function rebuildQueue() {
    if (!crawlerInstanceLockToken) throw new Error('Crawler instance lock is required before rebuilding the queue');
    await customCrawlQueue.obliterate({ force: true });
    console.log('[QUEUE] Cleared stale Redis jobs; rebuilding from company database state');
}

async function enqueueCompanies() {
    let page = 0, total = 0, hasMore = true;
    let totalEligible = null;

    console.log('[QUEUE] Fetching companies...');
    const { count: countResult, error: countError } = await supabase.from('companies')
        .select('"Id"', { count: 'exact', head: true })
        .eq('ats_type', 'custom')
        .or('detected_career_url.not.is.null,career_page_url.not.is.null');

    if (countError) {
        console.warn(`[QUEUE] Count query failed: ${countError.message}`);
    } else {
        totalEligible = countResult ?? null;
        if (Number.isFinite(totalEligible)) {
            console.log(`[QUEUE] Eligible companies: ${totalEligible}`);
        }
    }

    while (hasMore) {
        const start = page * CONFIG.PAGE_SIZE;
        const end = start + CONFIG.PAGE_SIZE - 1;
        const { data, error } = await supabase.from('companies')
            .select('"Id", detected_career_url, career_page_url, career_page_status, detection_signals, "Name", "Website"')
            .eq('ats_type', 'custom')
            .or('detected_career_url.not.is.null,career_page_url.not.is.null')
            .order('Id', { ascending: true })
            .range(start, end);

        if (error) { console.error(`[QUEUE] ${error.message}`); break; }
        if (!data?.length) { hasMore = false; break; }

        console.log(`[QUEUE] Page ${page + 1}: ${data.length} companies`);
        for (const c of data) {
            const companyIndex = total + 1;
            await customCrawlQueue.add('crawl-company', {
                companyId: c.Id,
                companyName: c.Name,
                careerUrl: selectOperationalCareerUrl(c),
                careerPageUrl: c.career_page_url,
                detectedCareerUrl: c.detected_career_url,
                careerPageStatus: c.career_page_status,
                detectionSignals: c.detection_signals,
                companyWebsiteUrl: c.Website,
                companyIndex,
                companyTotal: totalEligible
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
    console.log(`[QUEUE] Queued ${total}${Number.isFinite(totalEligible) ? ` of ${totalEligible}` : ''}`);
    totalQueued = total;
    return total;
}

// ─── STATS ────────────────────────────────────────────────────────────────
const stats = { processed: 0, failed: 0, partial: 0, not_found: 0, no_jobs: 0, with_jobs: 0, jobs_saved: 0, errors: 0 };
let processedCount = 0, totalQueued = 0;

function printProgress() {
    const pct = totalQueued > 0 ? ((processedCount / totalQueued) * 100).toFixed(1) : 0;
    console.log(
        `\n[PROGRESS] done=${processedCount}/${totalQueued} (${pct}%) | ` +
        `success=${stats.with_jobs} | no_jobs=${stats.no_jobs} | ` +
        `partial=${stats.partial} | failed=${stats.failed} | ` +
        `errors=${stats.errors} | jobs_saved=${stats.jobs_saved}`
    );
}

function companyLabel(companyName, companyIndex, companyTotal) {
    if (Number.isFinite(companyIndex) && Number.isFinite(companyTotal) && companyTotal > 0) {
        return `${companyIndex}/${companyTotal} ${companyName}`;
    }
    return companyName;
}

function logCompanyResult(jobData, result) {
    const label = companyLabel(jobData.companyName, jobData.companyIndex, jobData.companyTotal);
    const elapsedMs = result.elapsedMs ?? '-';
    const jobsSaved = result.jobsSaved ?? 0;
    const status = result.status || 'unknown';
    const detailBits = [];

    if (result.metrics?.jobLinksFound != null) detailBits.push(`links=${result.metrics.jobLinksFound}`);
    if (result.metrics?.listingPagesScanned != null) detailBits.push(`pages=${result.metrics.listingPagesScanned}`);
    if (result.metrics?.jobPagesFetched != null) detailBits.push(`fetched=${result.metrics.jobPagesFetched}`);
    if (result.metrics?.jobsStructured != null) detailBits.push(`structured=${result.metrics.jobsStructured}`);

    console.log(
        `[DONE] ${label} | status=${status} | jobs_saved=${jobsSaved} | elapsed=${elapsedMs}ms` +
        (detailBits.length > 0 ? ` | ${detailBits.join(' | ')}` : '')
    );
}

// ─── WORKER ───────────────────────────────────────────────────────────────
const worker = ENABLE_RUNTIME ? new Worker(QUEUE_NAME, async job => {
    const run = { pages: new Set(), scraperApiFallbacks: new Map(), timedOut: false };
    activeCompanyRuns.add(run);
    return companyRunContext.run(run, async () => {
        try {
            try {
                return await withTimeout(
                    signal => processCompany(job, signal),
                    CONFIG.COMPANY_TIMEOUT_MS,
                    `company ${job.data.companyName}`,
                    async () => {
                        run.timedOut = true;
                        await closeCompanyPages(run);
                    }
                );
            } catch (error) {
                if (error?.name !== 'TimeoutError') throw error;
                const telemetry = {
                    ...(run.discoveryState?.metrics || {}),
                    ...(run.metrics || {}),
                    timed_out: true,
                    timeout_reason: error.message,
                    discovery_tasks_remaining: run.discoveryState?.metrics?.tasksRemaining || 0,
                    api_requests_attempted: run.discoveryState?.metrics?.apiRequestsAttempted || 0,
                    api_requests_completed: run.discoveryState?.metrics?.apiRequestsCompleted || 0,
                    api_requests_failed: run.discoveryState?.metrics?.apiRequestsFailed || 0,
                    api_requests_remaining: run.discoveryState?.metrics?.apiRequestsRemaining || 0
                };
                const result = {
                    status: 'partial',
                    companyId: job.data.companyId,
                    companyName: job.data.companyName,
                    jobsSaved: run.metrics?.jobsSaved || 0,
                    elapsedMs: Date.now() - (job.processedOn || Date.now()),
                    metrics: { ...telemetry, discovery: telemetry, partialReason: 'company_timeout' }
                };
                await markCompanyStatus(job.data.companyId, 'partial');
                await logCrawlEvent(job.data.companyId, 'partial', {
                    reason: 'company_timeout',
                    error_message: error.message,
                    discovery: telemetry,
                    duration_ms: result.elapsedMs,
                    ...telemetry
                }, { allowWhenTimedOut: true });
                return result;
            }
        } finally {
            await closeCompanyPages(run);
            activeCompanyRuns.delete(run);
            await recycleBrowserWhenIdle();
        }
    });
}, {
    connection: redisConnection,
    concurrency: CONFIG.CONCURRENCY,
    limiter: { max: CONFIG.RATE_LIMIT_MAX, duration: CONFIG.RATE_LIMIT_DURATION_MS },
    autorun: false
}) : null;

if (worker) worker.on('completed', (job, r) => {
    stats.processed++;
    if (r.status === 'failed_fetch') stats.failed++;
    else if (r.status === 'not_found') stats.not_found++;
    else if (r.status === 'no_jobs') stats.no_jobs++;
    else if (r.status === 'partial') { stats.partial++; stats.jobs_saved += r.jobsSaved || 0; }
    else if (r.status === 'completed') { stats.with_jobs++; stats.jobs_saved += r.jobsSaved || 0; }
    else stats.errors++;
    processedCount++;
    logCompanyResult(job.data, r);
    if (processedCount % 10 === 0 || processedCount === totalQueued) printProgress();
});

if (worker) worker.on('failed', async (job, err) => {
    const label = companyLabel(job?.data?.companyName, job?.data?.companyIndex, job?.data?.companyTotal);
    console.error(`[FAILED] ${label}: ${err.message}`);
    stats.processed++; stats.failed++; stats.errors++; processedCount++;
    if (job?.data?.companyId) {
        await markCompanyStatus(job.data.companyId, 'failed', { error: err });
        await logCrawlEvent(job.data.companyId, 'failed', {
            error_message: err.message,
            duration_ms: job.processedOn ? Date.now() - job.processedOn : null
        });
    }
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
    try { await releaseCrawlerInstanceLock(); } catch {}
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
    console.log(`[CONFIG] Concurrency: ${CONFIG.CONCURRENCY} | Job detail concurrency: ${CONFIG.JOB_DETAIL_CONCURRENCY} | GPT: ${CONFIG.GPT_MODEL} | Voyage: ${CONFIG.VOYAGE_MODEL}`);
    console.log(`[DIVISIONS] ${Object.keys(DIVISIONS).join(' | ')}`);

    await acquireCrawlerInstanceLock();
    await resetStuck();
    await rebuildQueue();
    const total = await enqueueCompanies();

    if (total === 0) {
        console.log('[QUEUE] No companies. Exiting.');
        await shutdown(0);
        return;
    }
    console.log(`[QUEUE] Processing ${total} companies...`);
    worker.run().catch(err => console.error(`[WORKER] ${err.message}`));
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
    isJobDetailUrl,
    isLikelyIndividualJobUrl,
    selectOperationalCareerUrl,
    hasVerifiedCareerDetection,
    isTrustedCareerEntryUrl,
    isTrustedCareerRedirect,
    hasStrongExternalCareerSignal,
    isAllowedQueuedDiscoveryUrl,
    isPaginationControlEvidence,
    validateCareerPage,
    extractAllJobLinks,
    extractLinksFromHtml,
    extractJobCandidatesFromApiPayload,
    normalizeApiJobRecord,
    hasJobApiEvidence,
    isJobApiResponseMetadata,
    isPotentialJobApiResponse,
    parseJobApiResponseBody,
    acceptApiCandidatesForCompany,
    API_DISCOVERY_CONFIG: {
        MAX_JOB_API_RESPONSES_PER_PAGE: CONFIG.MAX_JOB_API_RESPONSES_PER_PAGE,
        MAX_JOB_API_RESPONSE_BYTES: CONFIG.MAX_JOB_API_RESPONSE_BYTES,
        MAX_JOB_API_JSON_DEPTH: CONFIG.MAX_JOB_API_JSON_DEPTH,
    },
    extractRawJobFromHtml,
    extractJobLocationEvidence,
    splitLocationValues,
    isAcceptableSavedJobUrl,
    isCareerListingUrl,
    isGenericJobTitle,
    isNotFoundReason,
    getZeroLinkStatus,
    hasTechnicalJobFailure,
    validateStructuredJob,
    pageHasCareerIntent,
    withTimeout
};
