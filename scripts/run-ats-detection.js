#!/usr/bin/env node
/**
 * ATS Detection Runner v8 — Strict Genuine Career Pages + Client ATS Allowlist
 *
 * Main improvements over v5:
 *   - Writes BOTH career_page_url and detected_career_url
 *   - Reprocesses old rows where career_page_url is still null
 *   - Validates existing career URLs before trusting them
 *   - Discovers career pages from homepage links
 *   - Detects embedded / hosted ATS URLs in homepage HTML
 *   - Checks sitemap.xml and robots.txt sitemap declarations
 *   - Probes common career paths as a fallback
 *   - Uses the final redirected URL as the canonical career URL
 *   - Stores discovery evidence inside detection_signals
 *   - Never stores a company homepage as career_page_url
 *   - Preserves HTTPS → HTTP fallback, retries, checkpointing, batching,
 *     pagination, Claude fallback, and strict client ATS classification
 *
 * Usage:
 *   node scripts/run-ats-detection.js
 *   node scripts/run-ats-detection.js --retry
 *   node scripts/run-ats-detection.js --all
 *   node scripts/run-ats-detection.js --limit 100
 *   node scripts/run-ats-detection.js --concurrency 10
 *   node scripts/run-ats-detection.js --resume
 *   node scripts/run-ats-detection.js --dry-run
 */

'use strict';

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const { detectATS } = require('../src/ats-adapters/ats-detector');
const { fetchWithMetadata } = require('../src/utils/http-fetcher');

// ─── CONFIG ────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);

const CONFIG = {
  concurrency:         parseInt(getArg('--concurrency') || '5', 10),
  limit:               parseInt(getArg('--limit') || '0', 10),
  retryErrors:         args.includes('--retry') || args.includes('--all'),
  rerunAll:            args.includes('--all'),
  resume:              args.includes('--resume'),
  dryRun:              args.includes('--dry-run'),

  delayMs:             1000,
  maxRetries:          3,
  retryBaseDelay:      3000,

  batchSize:           50,
  checkpointEvery:     100,
  cacheTTL:            3600000,

  requestTimeout:      30000,
  discoveryTimeout:    15000,
  maxRedirects:        8,

  pageSize:            1000,
  maxAnchorCandidates: 18,
  maxSitemapCandidates: 12,
  maxCommonPathProbes: 24,

  // Optional search fallback. It only runs after direct website discovery fails.
  // Set SERPER_API_KEY on Contabo / DigitalOcean to enable it.
  searchFallback:       Boolean(process.env.SERPER_API_KEY) && !args.includes('--no-search'),
  searchTimeout:        15000,
  maxSearchCandidates:  10,

  // Claude is retained as the final discovery / classification fallback.
  // Any URL suggested by Claude is independently validated before storage.
  claudeCareerFallback: Boolean(process.env.ANTHROPIC_API_KEY) && !args.includes('--no-claude-career'),
  claudeTimeout:        20000,
  claudeModel:          process.env.ANTHROPIC_MODEL || 'claude-3-5-haiku-latest',
  maxClaudeLinks:       120,

  // Useful in development when the Anthropic key is invalid.
  // This bypasses ../src/ats-adapters/ats-detector and uses the local
  // fingerprints + job-content heuristic in this runner.
  noAI:                 args.includes('--no-ai') || process.env.ATS_DISABLE_AI === 'true',

  checkpointFile: path.join(__dirname, '.ats-checkpoint.json'),
};

function getArg(name) {
  const idx = args.indexOf(name);
  return idx !== -1 ? args[idx + 1] : null;
}

// ─── SUPABASE CLIENT ──────────────────────────────────────────────────────

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY,
  { global: { fetch }, realtime: { transport: ws } }
);

// ─── CACHE ─────────────────────────────────────────────────────────────────

const cache = {
  careerUrl: new Map(),
};

function cacheGet(map, key) {
  const entry = map.get(key);
  if (!entry) return null;

  if (Date.now() - entry.timestamp > CONFIG.cacheTTL) {
    map.delete(key);
    return null;
  }

  return entry.value;
}

function cacheSet(map, key, value) {
  map.set(key, { value, timestamp: Date.now() });
}

// ─── CAREER DISCOVERY CONSTANTS ────────────────────────────────────────────

const CAREER_TERMS = [
  // English
  'career',
  'careers',
  'job',
  'jobs',
  'vacancy',
  'vacancies',
  'open position',
  'open positions',
  'job opening',
  'job openings',
  'opportunities',
  'join us',
  'join our team',
  'work with us',
  'working at',
  'employment',
  'we are hiring',
  "we're hiring",
  'hiring',

  // German
  'karriere',
  'stellenangebot',
  'stellenangebote',
  'stellenanzeige',
  'stellenanzeigen',
  'offene stelle',
  'offene stellen',
  'jobangebot',
  'jobangebote',
  'bewerbung',
  'bewerben',
  'arbeiten bei',
  'arbeitgeber',
  'werde teil',
  'komm ins team',
  'komm in unser team',
  'einstieg',
  'ausbildung',
  'duales studium',

  // French
  'carrière',
  'carrières',
  'recrutement',
  "offres d'emploi",
  "offre d'emploi",
  'emploi',
  'rejoignez-nous',

  // Dutch
  'vacature',
  'vacatures',
  'werken bij',
  'solliciteren',

  // Spanish
  'empleo',
  'empleos',
  'vacantes',
  'trabaja con nosotros',

  // Italian
  'lavora con noi',
  'posizioni aperte',

  // Polish / Nordic
  'kariera',
  'oferty pracy',
  'lediga jobb',
];

const COMMON_CAREER_PATHS = [
  // English
  '/careers',
  '/careers/',
  '/jobs',
  '/jobs/',
  '/career',
  '/join-us',
  '/join-our-team',
  '/work-with-us',
  '/vacancies',
  '/open-positions',
  '/job-openings',
  '/company/careers',
  '/about/careers',
  '/about-us/careers',

  // German
  '/karriere',
  '/karriere/',
  '/jobs-karriere',
  '/stellenangebote',
  '/stellenangebote/',
  '/stellen',
  '/offene-stellen',
  '/jobangebote',
  '/unternehmen/karriere',
  '/unternehmen/jobs',
  '/ueber-uns/karriere',
  '/über-uns/karriere',
  '/de/karriere',
  '/de-de/karriere',
  '/de/jobs',
  '/de-de/jobs',

  // Other common European variants
  '/recrutement',
  '/carrieres',
  '/carriere',
  '/vacatures',
  '/werken-bij',
  '/empleo',
  '/lavora-con-noi',
  '/kariera',
];

const ATS_FINGERPRINTS = [
  {
    type: 'greenhouse',
    confidence: 0.99,
    url: [/greenhouse\.io/i, /boards\.greenhouse/i, /job-boards\.greenhouse/i],
    html: [/greenhouse\.io/i, /gh_jid/i, /greenhouse-job-board/i],
  },
  {
    type: 'lever',
    confidence: 0.99,
    url: [/jobs\.lever\.co/i, /lever\.co/i],
    html: [/jobs\.lever\.co/i, /lever-jobs/i, /lever\.co/i],
  },
  {
    type: 'workday',
    confidence: 0.98,
    url: [/myworkdayjobs\.com/i, /workdayjobs\.com/i],
    html: [/myworkdayjobs\.com/i, /\/wday\/cxs\//i, /workdayjobs\.com/i],
  },
  {
    type: 'personio',
    confidence: 0.99,
    url: [/personio\.(?:de|com)/i, /jobs\.personio\./i],
    html: [/personio\.(?:de|com)/i, /personio-jobs/i, /personio career/i],
  },
  {
    type: 'softgarden',
    confidence: 0.99,
    url: [/softgarden\.(?:io|de)/i],
    html: [/softgarden\.(?:io|de)/i, /softgarden/i],
  },
  {
    type: 'recruitee',
    confidence: 0.99,
    url: [/recruitee\.com/i],
    html: [/recruitee\.com/i, /recruitee/i],
  },
  {
    type: 'smartrecruiters',
    confidence: 0.99,
    url: [/smartrecruiters\.com/i],
    html: [/smartrecruiters\.com/i, /jobs\.smartrecruiters/i],
  },
  {
    type: 'teamtailor',
    confidence: 0.99,
    url: [/teamtailor\.com/i],
    html: [/teamtailor\.com/i, /teamtailor/i],
  },
  {
    type: 'workable',
    confidence: 0.99,
    url: [/apply\.workable\.com/i, /workable\.com/i],
    html: [/apply\.workable\.com/i, /workable\.com\/j/i],
  },
  {
    type: 'bamboohr',
    confidence: 0.98,
    url: [/bamboohr\.com\/careers/i, /bamboohr\.com\/jobs/i],
    html: [/bamboohr\.com\/careers/i, /bamboohr\.com\/jobs/i],
  },
  {
    type: 'ashby',
    confidence: 0.99,
    url: [/ashbyhq\.com/i],
    html: [/ashbyhq\.com/i, /jobs\.ashbyhq/i],
  },
  {
    type: 'breezy',
    confidence: 0.99,
    url: [/breezy\.hr/i],
    html: [/breezy\.hr/i],
  },
  {
    type: 'jobvite',
    confidence: 0.99,
    url: [/jobvite\.com/i],
    html: [/jobvite\.com/i],
  },
  {
    type: 'icims',
    confidence: 0.99,
    url: [/icims\.com/i],
    html: [/icims\.com/i, /iCIMS/i],
  },
  {
    type: 'successfactors',
    confidence: 0.99,
    url: [/successfactors\./i, /successfactors\.com/i],
    html: [/successfactors/i, /career\d*\.successfactors/i],
  },
  {
    type: 'oracle_recruiting',
    confidence: 0.98,
    url: [/oraclecloud\.com\/hcmui/i, /oraclecloud\.com\/hcmUI/i],
    html: [/\/hcmUI\/CandidateExperience/i, /oraclecloud\.com\/hcmui/i],
  },
  {
    type: 'taleo',
    confidence: 0.99,
    url: [/taleo\.net/i],
    html: [/taleo\.net/i],
  },
  {
    type: 'comeet',
    confidence: 0.99,
    url: [/comeet\.(?:co|com)/i],
    html: [/comeet\.(?:co|com)/i],
  },
  {
    type: 'jazzhr',
    confidence: 0.98,
    url: [/applytojob\.com/i, /jazzhr\.com/i, /jazz\.co/i],
    html: [/applytojob\.com/i, /jazzhr/i],
  },
  {
    type: 'pinpoint',
    confidence: 0.99,
    url: [/pinpointhq\.com/i],
    html: [/pinpointhq\.com/i],
  },
  {
    type: 'rippling',
    confidence: 0.98,
    url: [/ats\.rippling\.com/i, /rippling\.com\/jobs/i],
    html: [/ats\.rippling\.com/i, /rippling\.com\/jobs/i],
  },

  // Germany / DACH focused ATS platforms
  {
    type: 'onlyfy',
    confidence: 0.99,
    url: [/onlyfy\.(?:io|com)/i, /prescreen\.io/i],
    html: [/onlyfy\.(?:io|com)/i, /prescreen\.io/i, /onlyfy/i],
  },
  {
    type: 'dvinci',
    confidence: 0.99,
    url: [/dvinci(?:-easy)?\.(?:de|com)/i],
    html: [/dvinci(?:-easy)?\.(?:de|com)/i, /d\.vinci/i],
  },
  {
    type: 'rexx',
    confidence: 0.98,
    url: [/rexx-systems\.com/i, /rexx-recruitment\.com/i],
    html: [/rexx-systems\.com/i, /rexx recruitment/i],
  },
  {
    type: 'coveto',
    confidence: 0.99,
    url: [/coveto\.de/i],
    html: [/coveto\.de/i, /coveto/i],
  },
  {
    type: 'concludis',
    confidence: 0.99,
    url: [/concludis\.(?:de|com)/i],
    html: [/concludis\.(?:de|com)/i, /concludis/i],
  },
  {
    type: 'hr4you',
    confidence: 0.99,
    url: [/hr4you\.(?:de|com)/i],
    html: [/hr4you\.(?:de|com)/i, /hr4you/i],
  },
  {
    type: 'umantis',
    confidence: 0.99,
    url: [/umantis\.com/i],
    html: [/umantis\.com/i, /umantis/i],
  },
  {
    type: 'jacando',
    confidence: 0.99,
    url: [/jacando\.com/i],
    html: [/jacando\.com/i, /jacando/i],
  },
  {
    type: 'heavenhr',
    confidence: 0.99,
    url: [/heavenhr\.com/i],
    html: [/heavenhr\.com/i, /heavenhr/i],
  },
  {
    type: 'talentsconnect',
    confidence: 0.99,
    url: [/talentsconnect\.com/i],
    html: [/talentsconnect\.com/i, /talentsconnect/i],
  },
  {
    type: 'perbit',
    confidence: 0.98,
    url: [/perbit\.(?:com|de)/i],
    html: [/perbit\.(?:com|de)/i, /perbit/i],
  },
  {
    type: 'easycruit',
    confidence: 0.98,
    url: [/easycruit\.com/i],
    html: [/easycruit\.com/i, /easycruit/i],
  },
  {
    type: 'cornerstone',
    confidence: 0.98,
    url: [/csod\.com/i, /cornerstoneondemand\.com/i],
    html: [/csod\.com/i, /cornerstoneondemand/i],
  },
  {
    type: 'avature',
    confidence: 0.98,
    url: [/avature\.net/i],
    html: [/avature\.net/i, /avature/i],
  },
  {
    type: 'dayforce',
    confidence: 0.98,
    url: [/dayforcehcm\.com/i, /jobs\.dayforcehcm/i],
    html: [/dayforcehcm\.com/i, /dayforce/i],
  },
  {
    type: 'ukg',
    confidence: 0.97,
    url: [/ultipro\.com/i, /ukg\.com/i],
    html: [/ultipro\.com/i, /ukg careers/i],
  },
  {
    type: 'adp',
    confidence: 0.97,
    url: [/workforcenow\.adp\.com/i, /adp\.com\/mascsr/i],
    html: [/workforcenow\.adp\.com/i, /adp\.com\/mascsr/i],
  },
  {
    type: 'jobylon',
    confidence: 0.98,
    url: [/jobylon\.com/i],
    html: [/jobylon\.com/i, /jobylon/i],
  },
  {
    type: 'jobadder',
    confidence: 0.97,
    url: [/jobadder\.com/i],
    html: [/jobadder\.com/i, /jobadder/i],
  },

  {
    type: 'join',
    confidence: 0.99,
    url: [
      /join\.com\/companies\//i,
      /join\.com\/company\//i,
      /join\.com\/jobs\//i,
    ],
    html: [
      /join\.com\/companies\//i,
      /powered\s+by\s+join/i,
      /join solutions/i,
    ],
  },
  {
    type: 'onapply',
    confidence: 0.99,
    url: [
      /(?:^|\/\/)(?:[^/]+\.)?onapply\.(?:de|com)(?:\/|$)/i,
    ],
    html: [
      /onapply\.(?:de|com)/i,
      /powered\s+by\s+onapply/i,
    ],
  },
  {
    type: 'workwise',
    confidence: 0.99,
    url: [
      /(?:^|\/\/)(?:[^/]+\.)?workwise\.(?:io|de)(?:\/|$)/i,
      /jobs\.workwise\.(?:io|de)/i,
    ],
    html: [
      /workwise\.(?:io|de)/i,
      /powered\s+by\s+workwise/i,
    ],
  },
];

const ATS_URL_PATTERNS = ATS_FINGERPRINTS.flatMap(item => item.url || []);

// ─── CLIENT ATS CLASSIFICATION RULES ─────────────────────────────────────
//
// Only these ATS values are allowed to be stored by vendor name.
// Every genuine career page using any other system is classified as custom.

const CLIENT_ATS_TYPES = new Set([
  'personio',
  'softgarden',
  'concludis',
  'join',
  'onapply',
  'recruitee',
  'rexx',
  'smartrecruiters',
  'successfactors',
  'teamtailor',
  'umantis',
  'workday',
  'workwise',
]);

const CLIENT_ATS_ALIASES = new Map([
  ['personio', 'personio'],
  ['softgarden', 'softgarden'],
  ['concludis', 'concludis'],
  ['join', 'join'],
  ['join.com', 'join'],
  ['onapply', 'onapply'],
  ['recruitee', 'recruitee'],
  ['rexx', 'rexx'],
  ['rexx systems', 'rexx'],
  ['smartrecruiters', 'smartrecruiters'],
  ['smart recruiters', 'smartrecruiters'],
  ['successfactors', 'successfactors'],
  ['success factors', 'successfactors'],
  ['sap successfactors', 'successfactors'],
  ['teamtailor', 'teamtailor'],
  ['umantis', 'umantis'],
  ['workday', 'workday'],
  ['workwise', 'workwise'],
]);

function canonicalClientAts(rawType) {
  if (!rawType) return null;

  const normalized = String(rawType)
    .trim()
    .toLowerCase()
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ');

  if (CLIENT_ATS_ALIASES.has(normalized)) {
    return CLIENT_ATS_ALIASES.get(normalized);
  }

  if (CLIENT_ATS_TYPES.has(normalized)) {
    return normalized;
  }

  return null;
}

function classifyForClient(rawType, careerUrl, html = '') {
  // Deterministic URL / HTML evidence has priority over a generic AI answer.
  const fingerprint = detectKnownATS(careerUrl, html);
  const fingerprintCanonical = canonicalClientAts(fingerprint?.ats_type);

  if (fingerprintCanonical) {
    return {
      ats_type: fingerprintCanonical,
      reason: 'client_supported_fingerprint',
      original_ats_type: rawType || fingerprint?.ats_type || null,
    };
  }

  const detectorCanonical = canonicalClientAts(rawType);
  if (detectorCanonical) {
    return {
      ats_type: detectorCanonical,
      reason: 'client_supported_detector_result',
      original_ats_type: rawType,
    };
  }

  // If a real career page has been verified and it is not one of the
  // supported ATS platforms, the client's required value is custom.
  return {
    ats_type: 'custom',
    reason:
      rawType && !['unknown', 'error', 'no_url', 'null'].includes(String(rawType).toLowerCase())
        ? 'unsupported_ats_to_custom'
        : 'unknown_to_custom',
    original_ats_type: rawType || null,
  };
}


// ─── GENERAL HELPERS ───────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function formatDuration(ms) {
  const s = Math.round(ms / 1000);
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
}

function exponentialBackoffWithJitter(attempt) {
  const base = CONFIG.retryBaseDelay * Math.pow(2, attempt);
  const jitter = Math.random() * 0.3 * base;
  return base + jitter;
}

function normalizeUrl(input) {
  if (!input || typeof input !== 'string') return null;

  let value = input.trim();
  if (!value) return null;

  if (value.startsWith('//')) {
    value = `https:${value}`;
  } else if (!/^https?:\/\//i.test(value)) {
    value = `https://${value}`;
  }

  try {
    const url = new URL(value);
    url.hash = '';
    return url.toString();
  } catch {
    return null;
  }
}

function safeUrl(input, baseUrl = null) {
  if (!input) return null;

  try {
    const resolved = baseUrl ? new URL(input, baseUrl) : new URL(normalizeUrl(input));
    if (!/^https?:$/i.test(resolved.protocol)) return null;

    resolved.hash = '';
    return resolved.toString();
  } catch {
    return null;
  }
}

function stripHtml(value = '') {
  return String(value)
    .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&#39;/gi, "'")
    .replace(/&quot;/gi, '"')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractTitle(html) {
  if (!html) return null;
  const match = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  return match ? stripHtml(match[1]).trim() : null;
}

function extractSubdomain(url) {
  try {
    const host = new URL(url).hostname;
    const parts = host.split('.');
    if (parts.length > 2) return parts[0];
    return null;
  } catch {
    return null;
  }
}

function getHostname(url) {
  try {
    return new URL(url).hostname.toLowerCase().replace(/^www\./, '');
  } catch {
    return null;
  }
}

function detectKnownATS(url = '', html = '') {
  const urlText = String(url || '');
  const htmlText = String(html || '');

  for (const fingerprint of ATS_FINGERPRINTS) {
    const urlHit = (fingerprint.url || []).some(pattern => pattern.test(urlText));
    const htmlHit = (fingerprint.html || []).some(pattern => pattern.test(htmlText));

    if (urlHit || htmlHit) {
      return {
        ats_type: fingerprint.type,
        ats_confidence: fingerprint.confidence || 0.95,
        detection_method: urlHit
          ? 'runner_url_fingerprint'
          : 'runner_html_fingerprint',
        signals: {
          runner_fingerprint: fingerprint.type,
          url_match: urlHit,
          html_match: htmlHit,
        },
      };
    }
  }

  return null;
}

function isKnownAtsUrl(url) {
  if (!url) return false;
  return Boolean(detectKnownATS(url, ''));
}

function hasCareerTerm(value = '') {
  const haystack = String(value).toLowerCase();
  return CAREER_TERMS.some(term => haystack.includes(term));
}

function hasStrongJobContent(html = '') {
  const text = stripHtml(String(html || '').slice(0, 500000)).toLowerCase();

  const phrases = [
    'open positions',
    'current openings',
    'job openings',
    'join our team',
    'work with us',
    'view jobs',
    'search jobs',
    'apply now',
    'available positions',
    'careers at',
    'working at',

    // German
    'offene stellen',
    'stellenangebote',
    'stellenanzeigen',
    'jetzt bewerben',
    'online bewerben',
    'karriere bei',
    'arbeiten bei',
    'werde teil',
    'komm ins team',
    'unsere jobs',
    'unsere stellenangebote',

    // Other common European wording
    "offres d'emploi",
    'rejoignez-nous',
    'werken bij',
    'vacatures',
    'lavora con noi',
    'posizioni aperte',
    'oferty pracy',
  ];

  let hits = 0;
  for (const phrase of phrases) {
    if (text.includes(phrase)) hits++;
  }

  if (hits >= 2) return true;

  const applyCount = (text.match(/\bapply\b/g) || []).length;
  const jobCount = (text.match(/\bjobs?\b/g) || []).length;
  const karriereCount = (text.match(/\bkarriere\b/g) || []).length;
  const stellenCount = (text.match(/\bstellen(?:angebote|anzeigen)?\b/g) || []).length;

  return (
    (applyCount >= 2 && jobCount >= 2) ||
    karriereCount >= 2 ||
    stellenCount >= 2
  );
}

function isLikelyNonCareerUrl(url) {
  const lower = String(url).toLowerCase();

  return (
    /^mailto:|^tel:|^javascript:/.test(lower) ||
    /\.(?:jpg|jpeg|png|gif|webp|svg|pdf|zip|docx?|xlsx?|pptx?)(?:\?|$)/i.test(lower) ||
    /\/(?:privacy|terms|legal|cookie|contact|support|login|signin|sign-in|register)(?:\/|$|\?)/i.test(lower)
  );
}

function rootDomainApprox(hostname) {
  if (!hostname) return null;
  const parts = hostname.replace(/^www\./, '').split('.').filter(Boolean);

  if (parts.length <= 2) return parts.join('.');

  const knownSecondLevel = new Set([
    'co.uk', 'org.uk', 'ac.uk',
    'com.au', 'net.au', 'org.au',
    'co.nz',
    'co.za',
    'com.br',
    'com.mx',
    'co.in',
    'com.sg',
  ]);

  const last2 = parts.slice(-2).join('.');
  const last3 = parts.slice(-3).join('.');

  if (knownSecondLevel.has(last2) && parts.length >= 3) return last3;
  return last2;
}

function isSameCompanyDomain(candidateUrl, websiteUrl) {
  const a = getHostname(candidateUrl);
  const b = getHostname(websiteUrl);
  if (!a || !b) return false;

  if (a === b) return true;
  if (a.endsWith(`.${b}`) || b.endsWith(`.${a}`)) return true;

  return rootDomainApprox(a) === rootDomainApprox(b);
}

function dedupeByUrl(items) {
  const seen = new Set();
  const output = [];

  for (const item of items) {
    const normalized = safeUrl(item.url);
    if (!normalized) continue;

    const key = normalized.replace(/\/+$/, '').toLowerCase();
    if (seen.has(key)) continue;

    seen.add(key);
    output.push({ ...item, url: normalized });
  }

  return output;
}

// ─── CHECKPOINT / RESUME ──────────────────────────────────────────────────

function loadCheckpoint() {
  try {
    if (fs.existsSync(CONFIG.checkpointFile)) {
      return JSON.parse(fs.readFileSync(CONFIG.checkpointFile, 'utf8'));
    }
  } catch {
    // ignore bad checkpoint
  }

  return null;
}

function saveCheckpoint(checkpoint) {
  try {
    fs.writeFileSync(
      CONFIG.checkpointFile,
      JSON.stringify(checkpoint, null, 2)
    );
  } catch {
    // ignore checkpoint write error
  }
}

function clearCheckpoint() {
  try {
    if (fs.existsSync(CONFIG.checkpointFile)) {
      fs.unlinkSync(CONFIG.checkpointFile);
    }
  } catch {
    // ignore
  }
}

// ─── FETCH COMPANIES — PAGINATED ──────────────────────────────────────────

async function fetchCompanies(checkpoint = null) {
  const allCompanies = [];
  let page = 0;
  const PAGE_SIZE = CONFIG.pageSize;
  let hasMore = true;
  let totalFetched = 0;

  const processedSet = new Set(checkpoint?.processedIds || []);

  console.log(`   📋 Fetching companies (paginated, ${PAGE_SIZE} per page)...`);

  while (hasMore) {
    let query = supabase
      .from('companies')
      .select(`
        Id,
        Name,
        Website,
        career_page_url,
        detected_career_url,
        career_page_status,
        ats_type,
        ats_api_url,
        crawl_status
      `)
      .order('Id', { ascending: true })
      .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1);

    if (CONFIG.rerunAll) {
      // intentionally no filter
    } else if (CONFIG.retryErrors) {
      query = query.or(
        'crawl_status.eq.pending,' +
        'crawl_status.eq.failed,' +
        'crawl_status.eq.no_url,' +
        'crawl_status.is.null,' +
        'ats_type.eq.unknown,' +
        'ats_type.eq.error,' +
        'ats_type.is.null,' +
        'career_page_url.is.null,' +
        'career_page_status.eq.homepage_fallback,' +
        'crawl_status.eq.career_fallback'
      );
    } else {
      // Important: include old records where ATS may already be detected
      // but career_page_url was never populated by v5.
      query = query.or(
        'crawl_status.eq.pending,' +
        'crawl_status.is.null,' +
        'career_page_url.is.null'
      );
    }

    const { data, error } = await query;

    if (error) {
      throw new Error(`Supabase fetch error (page ${page + 1}): ${error.message}`);
    }

    if (!data || data.length === 0) {
      hasMore = false;
      break;
    }

    for (const company of data) {
      if (CONFIG.resume && processedSet.has(company.Id)) continue;

      const hasAnyUrl =
        normalizeUrl(company.career_page_url) ||
        normalizeUrl(company.detected_career_url) ||
        normalizeUrl(company.Website);

      if (!hasAnyUrl) {
        // Keep it. processCompany will mark it no_url.
        allCompanies.push(company);
      } else {
        allCompanies.push(company);
      }

      totalFetched++;

      if (CONFIG.limit > 0 && totalFetched >= CONFIG.limit) {
        hasMore = false;
        break;
      }
    }

    page++;

    if (CONFIG.limit > 0 && totalFetched >= CONFIG.limit) break;
    if (data.length < PAGE_SIZE) hasMore = false;
  }

  console.log(`   ✅ Fetched ${allCompanies.length} companies (${page} pages)`);
  return allCompanies;
}

// ─── BATCH UPSERT ─────────────────────────────────────────────────────────

async function batchUpsertCompanies(updates) {
  if (updates.length === 0) return;

  const updateData = updates.map(({ id, fields }) => ({
    Id: id,
    ...fields,
    updated_at: new Date().toISOString(),
  }));

  const { error } = await supabase
    .from('companies')
    .upsert(updateData, { onConflict: 'Id' });

  if (error) {
    console.error(`  ⚠️ Batch upsert error: ${error.message}`);
    console.log('  🔄 Falling back to individual updates...');

    for (const item of updateData) {
      const { error: singleError } = await supabase
        .from('companies')
        .update({
          ...item,
          updated_at: new Date().toISOString(),
        })
        .eq('Id', item.Id);

      if (singleError) {
        console.error(`    ❌ Failed to update ${item.Id}: ${singleError.message}`);
      }
    }
  }
}

// ─── HOMEPAGE LINK EXTRACTION ─────────────────────────────────────────────

function extractAnchors(html, baseUrl) {
  if (!html) return [];

  const anchors = [];
  const regex = /<a\b([^>]*?)href\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))([^>]*)>([\s\S]*?)<\/a>/gi;

  let match;

  while ((match = regex.exec(html)) !== null) {
    const rawHref = match[2] || match[3] || match[4] || '';
    const attrsText = `${match[1] || ''} ${match[5] || ''}`;

    const ariaLabel =
      attrsText.match(/\baria-label\s*=\s*(?:"([^"]*)"|'([^']*)')/i)?.slice(1).find(Boolean) || '';

    const titleAttr =
      attrsText.match(/\btitle\s*=\s*(?:"([^"]*)"|'([^']*)')/i)?.slice(1).find(Boolean) || '';

    const text = [
      stripHtml(match[6] || ''),
      stripHtml(ariaLabel),
      stripHtml(titleAttr),
    ].filter(Boolean).join(' ');

    const url = safeUrl(rawHref.replace(/&amp;/gi, '&'), baseUrl);

    if (!url || isLikelyNonCareerUrl(url)) continue;

    anchors.push({
      url,
      text,
      rawHref,
    });
  }

  return anchors;
}

function scoreCareerCandidate(candidate, websiteUrl) {
  const url = candidate.url || '';
  const text = candidate.text || '';
  const source = candidate.source || '';

  const lowerUrl = url.toLowerCase();
  const lowerText = text.toLowerCase();

  let score = 0;

  if (isKnownAtsUrl(url)) score += 80;
  if (hasCareerTerm(url)) score += 45;
  if (hasCareerTerm(text)) score += 40;

  if (/\/careers?(?:\/|$|\?|#)/i.test(lowerUrl)) score += 55;
  if (/\/jobs?(?:\/|$|\?|#)/i.test(lowerUrl)) score += 50;
  if (/\/vacanc(?:y|ies)(?:\/|$|\?|#)/i.test(lowerUrl)) score += 45;
  if (/\/(?:join-us|join-our-team|work-with-us|open-positions|job-openings)(?:\/|$|\?|#)/i.test(lowerUrl)) score += 45;

  if (/\bcareers?\b/i.test(lowerText)) score += 45;
  if (/\bjobs?\b/i.test(lowerText)) score += 38;
  if (/\bjoin (?:us|our team)\b/i.test(lowerText)) score += 40;
  if (/\bwork with us\b/i.test(lowerText)) score += 40;
  if (/\bvacanc(?:y|ies)\b/i.test(lowerText)) score += 35;
  if (/\bopen positions?\b/i.test(lowerText)) score += 35;
  if (/\bopportunities\b/i.test(lowerText)) score += 20;
  if (/\bhiring\b/i.test(lowerText)) score += 18;

  if (isSameCompanyDomain(url, websiteUrl)) score += 10;
  if (source === 'existing_career_page_url') score += 25;
  if (source === 'existing_detected_career_url') score += 20;
  if (source === 'homepage_embedded_ats') score += 25;
  if (source === 'homepage_resource') score += 18;
  if (source === 'homepage_meta_refresh') score += 18;
  if (source === 'sitemap') score += 8;
  if (source === 'common_path') score += 5;
  if (source === 'search_result') score += 12;

  if (/\/(?:blog|news|press|privacy|terms|contact|login|signin|register)\b/i.test(lowerUrl)) {
    score -= 50;
  }

  return score;
}

function extractKnownAtsUrlsFromHtml(html, baseUrl) {
  if (!html) return [];

  const urls = [];

  const absoluteMatches = html.match(/https?:\/\/[^\s"'<>\\)]+/gi) || [];
  for (const raw of absoluteMatches) {
    const clean = raw.replace(/&amp;/gi, '&').replace(/[),.;]+$/, '');
    const url = safeUrl(clean);
    if (url && isKnownAtsUrl(url)) {
      urls.push({
        url,
        text: '',
        source: 'homepage_embedded_ats',
      });
    }
  }

  const protocolRelativeMatches = html.match(/\/\/[^\s"'<>\\)]+/gi) || [];
  for (const raw of protocolRelativeMatches) {
    const clean = raw.replace(/&amp;/gi, '&').replace(/[),.;]+$/, '');
    const url = safeUrl(`https:${clean}`);
    if (url && isKnownAtsUrl(url)) {
      urls.push({
        url,
        text: '',
        source: 'homepage_embedded_ats',
      });
    }
  }

  // Some sites store ATS links as JSON escaped strings.
  const unescaped = html
    .replace(/\\u002F/gi, '/')
    .replace(/\\\//g, '/');

  const escapedMatches = unescaped.match(/https?:\/\/[^\s"'<>\\)]+/gi) || [];
  for (const raw of escapedMatches) {
    const clean = raw.replace(/[),.;]+$/, '');
    const url = safeUrl(clean);
    if (url && isKnownAtsUrl(url)) {
      urls.push({
        url,
        text: '',
        source: 'homepage_embedded_ats',
      });
    }
  }

  return dedupeByUrl(urls);
}

function extractNonAnchorCareerUrlsFromHtml(html, baseUrl) {
  if (!html) return [];

  const candidates = [];
  const attrs = /(?:src|action|data-href|data-url|data-link|data-careers-url|data-jobs-url)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))/gi;
  let match;

  while ((match = attrs.exec(html)) !== null) {
    const raw = match[1] || match[2] || match[3] || '';
    const url = safeUrl(raw.replace(/&amp;/gi, '&'), baseUrl);

    if (!url || isLikelyNonCareerUrl(url)) continue;

    if (isKnownAtsUrl(url) || hasCareerTerm(url)) {
      candidates.push({
        url,
        text: '',
        source: 'homepage_resource',
      });
    }
  }

  // Meta refresh redirects are common on lightweight career landing pages.
  const metaRefreshRegex = /<meta\b[^>]*http-equiv\s*=\s*(?:"refresh"|'refresh'|refresh)[^>]*content\s*=\s*(?:"([^"]*)"|'([^']*)'|([^>]+))[^>]*>/gi;
  while ((match = metaRefreshRegex.exec(html)) !== null) {
    const content = match[1] || match[2] || match[3] || '';
    const urlPart = content.match(/url\s*=\s*([^;]+)/i)?.[1]?.trim().replace(/^['"]|['"]$/g, '');
    const url = safeUrl(urlPart, baseUrl);

    if (url && (isKnownAtsUrl(url) || hasCareerTerm(url))) {
      candidates.push({
        url,
        text: '',
        source: 'homepage_meta_refresh',
      });
    }
  }

  // Look for relative URLs embedded inside JS or JSON.
  const unescaped = html
    .replace(/\\u002F/gi, '/')
    .replace(/\\\//g, '/');

  const relativeMatches = unescaped.match(/["'](\/[^"'<>]{1,240})["']/g) || [];
  for (const rawMatch of relativeMatches) {
    const raw = rawMatch.slice(1, -1);
    if (!hasCareerTerm(raw)) continue;

    const url = safeUrl(raw, baseUrl);
    if (!url || isLikelyNonCareerUrl(url)) continue;

    candidates.push({
      url,
      text: '',
      source: 'homepage_resource',
    });
  }

  return dedupeByUrl(candidates);
}



function getCareerSubdomainSignal(url) {
  try {
    const host = new URL(url).hostname.toLowerCase();
    const first = host.split('.')[0] || '';
    return /^(?:career|careers|job|jobs|karriere|stellen|recruiting|recruitment)$/.test(first);
  } catch {
    return false;
  }
}

function isRootLikeUrl(url) {
  try {
    const parsed = new URL(url);
    const pathname = parsed.pathname.replace(/\/+/g, '/');
    return pathname === '/' || pathname === '';
  } catch {
    return false;
  }
}

function isSoft404(html = '', title = '') {
  const titleText = String(title || '').toLowerCase();
  const bodyText = stripHtml(String(html || '').slice(0, 180000)).toLowerCase();

  const negatives = [
    '404 not found',
    'page not found',
    'seite nicht gefunden',
    '404 -',
    '404 |',
    'error 404',
    'the page you requested could not be found',
    'die von ihnen gesuchte seite',
  ];

  return negatives.some(term => titleText.includes(term) || bodyText.includes(term));
}

function genuineCareerSignals(url, html = '', title = '', websiteUrl = null) {
  const atsFingerprint = detectKnownATS(url, html);
  const titleHasCareer = hasCareerTerm(title || '');
  const urlHasCareer = hasCareerTerm(url || '');
  const careerSubdomain = getCareerSubdomainSignal(url);
  const rootLike = isRootLikeUrl(url);

  const body = stripHtml(String(html || '').slice(0, 550000)).toLowerCase();

  const strongPhrases = [
    'open positions',
    'current openings',
    'job openings',
    'available positions',
    'search jobs',
    'view jobs',
    'join our team',
    'work with us',
    'careers at',
    'working at',
    'apply for this job',
    'apply now',
    'offene stellen',
    'stellenangebote',
    'stellenanzeigen',
    'jetzt bewerben',
    'online bewerben',
    'karriere bei',
    'arbeiten bei',
    'unsere jobs',
    'unsere stellenangebote',
    'komm ins team',
    'werde teil',
    "offres d'emploi",
    'rejoignez-nous',
    'vacatures',
    'werken bij',
    'trabaja con nosotros',
    'lavora con noi',
    'posizioni aperte',
    'oferty pracy',
  ];

  let strongPhraseCount = 0;
  for (const phrase of strongPhrases) {
    if (body.includes(phrase)) strongPhraseCount++;
  }

  const jobLinkMatches =
    String(html || '').match(
      /href\s*=\s*["'][^"']*(?:job|jobs|career|careers|karriere|stelle|stellenangebot|vacan|apply|bewerb)[^"']*["']/gi
    ) || [];

  const applicationFormSignal =
    /(?:application\/ld\+json[\s\S]{0,4000}"@type"\s*:\s*"JobPosting")/i.test(html || '') ||
    /(?:jobposting|job-position|job_position|job-listing|job_listing)/i.test(html || '');

  const sameCompanyDomain = websiteUrl
    ? isSameCompanyDomain(url, websiteUrl)
    : false;

  // Known ATS evidence is the strongest proof that this is a real jobs page.
  if (atsFingerprint) {
    return {
      genuine: true,
      reason: 'ats_fingerprint',
      confidence: 0.99,
      strongPhraseCount,
      jobLinkCount: jobLinkMatches.length,
      atsFingerprint: atsFingerprint.ats_type,
    };
  }

  // Dedicated career or jobs subdomains are accepted when there is at least
  // one additional content signal.
  if (
    careerSubdomain &&
    (titleHasCareer || strongPhraseCount >= 1 || jobLinkMatches.length >= 1 || applicationFormSignal)
  ) {
    return {
      genuine: true,
      reason: 'career_subdomain_plus_content',
      confidence: 0.96,
      strongPhraseCount,
      jobLinkCount: jobLinkMatches.length,
      atsFingerprint: null,
    };
  }

  // Career paths must also contain career content. This prevents common
  // paths that return the company homepage or a generic CMS page.
  if (
    urlHasCareer &&
    (
      titleHasCareer ||
      strongPhraseCount >= 1 ||
      jobLinkMatches.length >= 2 ||
      applicationFormSignal
    )
  ) {
    return {
      genuine: true,
      reason: 'career_url_plus_content',
      confidence: 0.94,
      strongPhraseCount,
      jobLinkCount: jobLinkMatches.length,
      atsFingerprint: null,
    };
  }

  // Some legitimate career pages have neutral URLs. Require substantially
  // stronger page evidence in that case.
  if (
    titleHasCareer &&
    (
      strongPhraseCount >= 2 ||
      jobLinkMatches.length >= 2 ||
      applicationFormSignal
    )
  ) {
    return {
      genuine: true,
      reason: 'career_title_plus_content',
      confidence: 0.92,
      strongPhraseCount,
      jobLinkCount: jobLinkMatches.length,
      atsFingerprint: null,
    };
  }

  if (
    strongPhraseCount >= 3 &&
    (jobLinkMatches.length >= 2 || applicationFormSignal)
  ) {
    return {
      genuine: true,
      reason: 'strong_job_content',
      confidence: 0.90,
      strongPhraseCount,
      jobLinkCount: jobLinkMatches.length,
      atsFingerprint: null,
    };
  }

  // A company's normal root homepage is never accepted just because its
  // footer contains a Careers link.
  if (rootLike && sameCompanyDomain && !careerSubdomain) {
    return {
      genuine: false,
      reason: 'company_homepage_rejected',
      confidence: 0,
      strongPhraseCount,
      jobLinkCount: jobLinkMatches.length,
      atsFingerprint: null,
    };
  }

  return {
    genuine: false,
    reason: 'insufficient_career_evidence',
    confidence: 0,
    strongPhraseCount,
    jobLinkCount: jobLinkMatches.length,
    atsFingerprint: null,
  };
}

// ─── CAREER PAGE VALIDATION ────────────────────────────────────────────────

function careerEvidenceScore(url, html, title, source, websiteUrl) {
  let score = 0;

  const localAts = detectKnownATS(url, html);
  if (localAts) score += 95;

  if (isKnownAtsUrl(url)) score += 80;
  if (hasCareerTerm(url)) score += 40;
  if (hasCareerTerm(title || '')) score += 35;

  const text = stripHtml((html || '').slice(0, 500000)).toLowerCase();

  const strongPhrases = [
    // English
    'open positions',
    'current openings',
    'job openings',
    'join our team',
    'work with us',
    'view jobs',
    'search jobs',
    'apply now',
    'available positions',
    'careers at',
    'working at',

    // German
    'offene stellen',
    'stellenangebote',
    'stellenanzeigen',
    'jetzt bewerben',
    'online bewerben',
    'karriere bei',
    'arbeiten bei',
    'werde teil',
    'komm ins team',
    'unsere jobs',
    'unsere stellenangebote',

    // Other European languages
    "offres d'emploi",
    'rejoignez-nous',
    'werken bij',
    'vacatures',
    'trabaja con nosotros',
    'lavora con noi',
    'posizioni aperte',
    'oferty pracy',
  ];

  for (const phrase of strongPhrases) {
    if (text.includes(phrase)) score += 12;
  }

  if (hasCareerTerm(text)) score += 18;
  if (/\bcareers?\b/i.test(text)) score += 12;
  if (/\bjobs?\b/i.test(text)) score += 10;
  if (/\bvacanc(?:y|ies)\b/i.test(text)) score += 10;
  if (/\bapply\b/i.test(text)) score += 6;
  if (/\bkarriere\b/i.test(text)) score += 12;
  if (/\bstellenangebote\b/i.test(text)) score += 12;
  if (/\bbewerben\b/i.test(text)) score += 8;

  if (source === 'existing_career_page_url') score += 15;
  if (source === 'existing_detected_career_url') score += 12;
  if (source === 'homepage_anchor') score += 10;
  if (source === 'homepage_embedded_ats') score += 15;
  if (source === 'homepage_resource') score += 12;
  if (source === 'homepage_meta_refresh') score += 12;
  if (source === 'sitemap') score += 8;
  if (source === 'common_path') score += 5;
  if (source === 'search_result') score += 10;
  if (source === 'claude_link_selection') score += 10;
  if (source === 'ats_detector') score += 12;

  if (websiteUrl && isSameCompanyDomain(url, websiteUrl)) score += 5;

  return score;
}

async function validateCareerCandidate(candidate, websiteUrl, workerId) {
  const candidateUrl = safeUrl(candidate.url);
  if (!candidateUrl) return null;

  try {
    const fetched = await fetchWithMetadata(candidateUrl, {
      timeout: CONFIG.discoveryTimeout,
      maxRedirects: CONFIG.maxRedirects,
      protocols: ['https', 'http'],
    });

    const finalUrl = safeUrl(fetched.finalUrl || candidateUrl) || candidateUrl;
    const title = extractTitle(fetched.html);
    const httpOk = fetched.status >= 200 && fetched.status < 400;
    const blockedOrRateLimited = [401, 403, 429].includes(fetched.status);

    if (isSoft404(fetched.html, title)) return null;

    const evidence = genuineCareerSignals(
      finalUrl,
      fetched.html || '',
      title || '',
      websiteUrl
    );

    const trustedBlockedUrl =
      blockedOrRateLimited &&
      (
        Boolean(detectKnownATS(finalUrl, fetched.html || '')) ||
        getCareerSubdomainSignal(finalUrl) ||
        (
          hasCareerTerm(finalUrl) &&
          !isRootLikeUrl(finalUrl)
        )
      );

    if (!httpOk && !trustedBlockedUrl) return null;

    // For normal 2xx / 3xx responses, strict content validation is required.
    // For 401 / 403 / 429, a highly credible career URL or ATS URL is enough.
    if (!evidence.genuine && !trustedBlockedUrl) return null;

    // Reject a career candidate that redirected back to the ordinary company
    // homepage unless the final URL is a dedicated career subdomain or ATS.
    if (
      websiteUrl &&
      isSameCompanyDomain(finalUrl, websiteUrl) &&
      isRootLikeUrl(finalUrl) &&
      !getCareerSubdomainSignal(finalUrl) &&
      !detectKnownATS(finalUrl, fetched.html || '')
    ) {
      return null;
    }

    return {
      url: finalUrl,
      source: candidate.source,
      score: careerEvidenceScore(
        finalUrl,
        fetched.html || '',
        title || '',
        candidate.source,
        websiteUrl
      ),
      httpStatus: fetched.status,
      protocolUsed: fetched.protocolUsed,
      title,
      redirects: fetched.redirects || [],
      metadata: fetched,
      localAts: detectKnownATS(finalUrl, fetched.html || ''),
      genuineCareer: true,
      validationReason: evidence.genuine
        ? evidence.reason
        : 'blocked_but_credible',
      validationConfidence: evidence.genuine
        ? evidence.confidence
        : 0.88,
      validationSignals: evidence,
    };
  } catch {
    return null;
  }
}

// ─── SITEMAP DISCOVERY ────────────────────────────────────────────────────

function extractSitemapLocs(xml, baseUrl) {
  if (!xml) return [];

  const urls = [];
  const locRegex = /<loc[^>]*>([\s\S]*?)<\/loc>/gi;
  let match;

  while ((match = locRegex.exec(xml)) !== null) {
    const raw = stripHtml(match[1]).replace(/&amp;/gi, '&');
    const url = safeUrl(raw, baseUrl);

    if (!url) continue;

    urls.push(url);
  }

  return [...new Set(urls)];
}

async function fetchTextLike(url) {
  try {
    return await fetchWithMetadata(url, {
      timeout: CONFIG.discoveryTimeout,
      maxRedirects: CONFIG.maxRedirects,
      protocols: ['https', 'http'],
    });
  } catch {
    return null;
  }
}

async function discoverFromSitemaps(websiteUrl, workerId) {
  const origin = new URL(websiteUrl).origin;
  const sitemapUrls = new Set([
    `${origin}/sitemap.xml`,
    `${origin}/sitemap_index.xml`,
  ]);

  // robots.txt may declare a nonstandard sitemap location.
  const robots = await fetchTextLike(`${origin}/robots.txt`);
  if (robots?.html) {
    const sitemapRegex = /^\s*Sitemap:\s*(.+)\s*$/gim;
    let match;

    while ((match = sitemapRegex.exec(robots.html)) !== null) {
      const sitemapUrl = safeUrl(match[1].trim(), origin);
      if (sitemapUrl) sitemapUrls.add(sitemapUrl);
    }
  }

  const candidateUrls = [];

  for (const sitemapUrl of [...sitemapUrls].slice(0, 4)) {
    const sitemap = await fetchTextLike(sitemapUrl);
    if (!sitemap?.html) continue;

    const locs = extractSitemapLocs(sitemap.html, sitemapUrl);

    for (const loc of locs) {
      if (
        hasCareerTerm(loc) ||
        isKnownAtsUrl(loc)
      ) {
        candidateUrls.push({
          url: loc,
          text: '',
          source: 'sitemap',
        });
      }
    }

    // If this is a sitemap index, inspect a few child sitemaps whose
    // URLs themselves suggest jobs / careers / pages.
    const childSitemaps = locs
      .filter(loc =>
        /\.xml(?:\?|$)/i.test(loc) &&
        (
          /career|job|page|post/i.test(loc) ||
          candidateUrls.length === 0
        )
      )
      .slice(0, 3);

    for (const child of childSitemaps) {
      const childResult = await fetchTextLike(child);
      if (!childResult?.html) continue;

      for (const loc of extractSitemapLocs(childResult.html, child)) {
        if (hasCareerTerm(loc) || isKnownAtsUrl(loc)) {
          candidateUrls.push({
            url: loc,
            text: '',
            source: 'sitemap',
          });
        }
      }
    }
  }

  const candidates = dedupeByUrl(candidateUrls)
    .map(item => ({
      ...item,
      score: scoreCareerCandidate(item, websiteUrl),
    }))
    .sort((a, b) => b.score - a.score)
    .slice(0, CONFIG.maxSitemapCandidates);

  for (const candidate of candidates) {
    const validated = await validateCareerCandidate(candidate, websiteUrl, workerId);
    if (validated) return validated;
  }

  return null;
}

// ─── COMMON PATH DISCOVERY ─────────────────────────────────────────────────

async function discoverFromCommonPaths(websiteUrl, workerId) {
  const origin = new URL(websiteUrl).origin;

  const candidates = COMMON_CAREER_PATHS
    .slice(0, CONFIG.maxCommonPathProbes)
    .map(pathname => ({
      url: new URL(pathname, origin).toString(),
      text: pathname,
      source: 'common_path',
    }))
    .map(item => ({
      ...item,
      score: scoreCareerCandidate(item, websiteUrl),
    }))
    .sort((a, b) => b.score - a.score);

  for (const candidate of candidates) {
    const validated = await validateCareerCandidate(candidate, websiteUrl, workerId);
    if (validated) return validated;
  }

  return null;
}

// ─── OPTIONAL SEARCH FALLBACK ─────────────────────────────────────────────

function companyNameTokens(name = '') {
  return String(name)
    .toLowerCase()
    .replace(/&amp;/g, '&')
    .replace(/\b(gmbh|ag|kg|mbh|gbr|ug|se|inc|llc|ltd|limited|corp|corporation|company|co)\b/g, ' ')
    .replace(/[^a-z0-9äöüß]+/gi, ' ')
    .split(/\s+/)
    .filter(token => token.length >= 3)
    .slice(0, 6);
}

function searchCandidateMatchesCompany(candidate, company, websiteUrl) {
  if (!candidate?.url) return false;

  if (isSameCompanyDomain(candidate.url, websiteUrl)) return true;
  if (isKnownAtsUrl(candidate.url)) {
    const haystack = `${candidate.url} ${candidate.text || ''}`.toLowerCase();
    const tokens = companyNameTokens(company.Name);

    // External ATS links should contain at least one meaningful company token.
    return tokens.some(token => haystack.includes(token));
  }

  return false;
}

async function serperSearch(query) {
  if (!CONFIG.searchFallback || !process.env.SERPER_API_KEY) return [];

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), CONFIG.searchTimeout);

  try {
    const response = await fetch('https://google.serper.dev/search', {
      method: 'POST',
      headers: {
        'X-API-KEY': process.env.SERPER_API_KEY,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        q: query,
        num: 10,
      }),
      signal: controller.signal,
    });

    if (!response.ok) return [];

    const payload = await response.json();
    return Array.isArray(payload.organic) ? payload.organic : [];
  } catch {
    return [];
  } finally {
    clearTimeout(timer);
  }
}

async function discoverFromSearch(company, websiteUrl, workerId) {
  if (!CONFIG.searchFallback) return null;

  const domain = getHostname(websiteUrl);
  if (!domain) return null;

  const companyName = String(company.Name || '').trim();

  const queries = [
    `site:${domain} careers jobs karriere stellenangebote`,
    `site:${domain} karriere jobs`,
  ];

  if (companyName) {
    queries.push(`"${companyName}" careers jobs karriere stellenangebote`);
  }

  const rawCandidates = [];

  for (const query of queries) {
    const results = await serperSearch(query);

    for (const item of results) {
      const url = safeUrl(item.link);
      if (!url || isLikelyNonCareerUrl(url)) continue;

      const text = `${item.title || ''} ${item.snippet || ''}`;

      if (
        !hasCareerTerm(url) &&
        !hasCareerTerm(text) &&
        !isKnownAtsUrl(url)
      ) {
        continue;
      }

      const candidate = {
        url,
        text,
        source: 'search_result',
      };

      if (!searchCandidateMatchesCompany(candidate, company, websiteUrl)) {
        continue;
      }

      rawCandidates.push(candidate);
    }

    if (rawCandidates.length >= CONFIG.maxSearchCandidates) break;
  }

  const candidates = dedupeByUrl(rawCandidates)
    .map(item => ({
      ...item,
      score: scoreCareerCandidate(item, websiteUrl),
    }))
    .sort((a, b) => b.score - a.score)
    .slice(0, CONFIG.maxSearchCandidates);

  for (const candidate of candidates) {
    const validated = await validateCareerCandidate(
      candidate,
      websiteUrl,
      workerId
    );

    if (validated) return validated;
  }

  return null;
}



async function callClaudeForCareerLink(company, websiteUrl, homepageHtml) {
  if (!CONFIG.claudeCareerFallback || !homepageHtml) return null;

  const allLinks = extractAnchors(homepageHtml, websiteUrl)
    .filter(item => item.url)
    .slice(0, CONFIG.maxClaudeLinks)
    .map((item, index) => ({
      id: index + 1,
      url: item.url,
      text: String(item.text || '').slice(0, 180),
    }));

  if (allLinks.length === 0) return null;

  const prompt = [
    'You are selecting the genuine careers or jobs page for a company.',
    'Choose only from the supplied links. Never invent a URL.',
    'Return JSON only using this schema:',
    '{"link_id": number|null, "confidence": number, "reason": string}',
    '',
    `Company: ${company.Name || ''}`,
    `Website: ${websiteUrl}`,
    '',
    'Links:',
    JSON.stringify(allLinks),
    '',
    'A valid choice should be a company careers page, jobs page, recruiting page,',
    'or hosted applicant tracking system page. Do not choose the normal homepage,',
    'contact page, news page, privacy page, generic about page, or social profile.',
    'If no supplied link is a genuine career page, return link_id as null.',
  ].join('\n');

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), CONFIG.claudeTimeout);

  try {
    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        model: CONFIG.claudeModel,
        max_tokens: 220,
        temperature: 0,
        messages: [
          {
            role: 'user',
            content: prompt,
          },
        ],
      }),
      signal: controller.signal,
    });

    if (!response.ok) {
      const body = await response.text().catch(() => '');
      console.log(
        `  Claude career discovery HTTP ${response.status}: ${body.slice(0, 180)}`
      );
      return null;
    }

    const payload = await response.json();
    const rawText = Array.isArray(payload.content)
      ? payload.content
          .filter(part => part.type === 'text')
          .map(part => part.text)
          .join('\n')
      : '';

    const jsonMatch = rawText.match(/\{[\s\S]*\}/);
    if (!jsonMatch) return null;

    let parsed;
    try {
      parsed = JSON.parse(jsonMatch[0]);
    } catch {
      return null;
    }

    if (!Number.isInteger(parsed.link_id)) return null;

    const chosen = allLinks.find(item => item.id === parsed.link_id);
    if (!chosen) return null;

    return {
      url: chosen.url,
      text: chosen.text,
      source: 'claude_link_selection',
      claudeConfidence:
        typeof parsed.confidence === 'number'
          ? parsed.confidence
          : null,
      claudeReason: parsed.reason || null,
    };
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

async function discoverFromClaudeLinks(company, websiteUrl, homepageHtml, workerId) {
  const candidate = await callClaudeForCareerLink(
    company,
    websiteUrl,
    homepageHtml
  );

  if (!candidate) return null;

  // Claude is never trusted directly. The selected URL goes through exactly
  // the same strict HTTP and career content validation as every other source.
  const validated = await validateCareerCandidate(
    candidate,
    websiteUrl,
    workerId
  );

  if (!validated) return null;

  return {
    ...validated,
    claudeConfidence: candidate.claudeConfidence,
    claudeReason: candidate.claudeReason,
  };
}

// ─── MAIN CAREER PAGE DISCOVERY ────────────────────────────────────────────

async function discoverCareerPage(company, workerId) {
  const websiteUrl = normalizeUrl(company.Website);

  const storedCareer = normalizeUrl(company.career_page_url);
  const storedDetected = normalizeUrl(company.detected_career_url);

  // Do not trust an old v6 homepage fallback as if it were a verified
  // career page. Re-discover it from scratch.
  const oldHomepageFallback =
    company.career_page_status === 'homepage_fallback';

  const existingCareer = oldHomepageFallback
    ? null
    : storedCareer;

  const existingDetected = oldHomepageFallback
    ? null
    : storedDetected;

  const cacheKey = websiteUrl || existingCareer || existingDetected;
  const cached = cacheKey ? cacheGet(cache.careerUrl, cacheKey) : null;

  if (cached) {
    return {
      ...cached,
      fromCache: true,
    };
  }

  // 1. Validate previously stored career URLs first.
  const existingCandidates = dedupeByUrl([
    existingCareer
      ? {
          url: existingCareer,
          text: '',
          source: 'existing_career_page_url',
        }
      : null,
    existingDetected
      ? {
          url: existingDetected,
          text: '',
          source: 'existing_detected_career_url',
        }
      : null,
  ].filter(Boolean))
    .map(item => ({
      ...item,
      score: scoreCareerCandidate(item, websiteUrl || item.url),
    }))
    .sort((a, b) => b.score - a.score);

  for (const candidate of existingCandidates) {
    const validated = await validateCareerCandidate(
      candidate,
      websiteUrl || candidate.url,
      workerId
    );

    if (validated) {
      const result = {
        ...validated,
        fallback: false,
        discoveryMethod: validated.source,
      };

      if (cacheKey) cacheSet(cache.careerUrl, cacheKey, result);
      return result;
    }
  }

  if (!websiteUrl) {
    return {
      url: null,
      source: 'no_website',
      score: 0,
      fallback: false,
      discoveryMethod: 'no_website',
      metadata: null,
      genuineCareer: false,
    };
  }

  // 2. Fetch homepage once and inspect links / HTML.
  let homepageResult = null;

  try {
    homepageResult = await fetchWithMetadata(websiteUrl, {
      timeout: CONFIG.requestTimeout,
      maxRedirects: CONFIG.maxRedirects,
      protocols: ['https', 'http'],
    });
  } catch {
    homepageResult = null;
  }

  const homepageFinalUrl =
    safeUrl(homepageResult?.finalUrl || websiteUrl) ||
    websiteUrl;

  if (homepageResult?.html) {
    const anchorCandidates = extractAnchors(
      homepageResult.html,
      homepageFinalUrl
    )
      .filter(item =>
        hasCareerTerm(item.text) ||
        hasCareerTerm(item.url) ||
        isKnownAtsUrl(item.url)
      )
      .map(item => ({
        ...item,
        source: 'homepage_anchor',
      }));

    const embeddedAtsCandidates = extractKnownAtsUrlsFromHtml(
      homepageResult.html,
      homepageFinalUrl
    );

    const resourceCandidates = extractNonAnchorCareerUrlsFromHtml(
      homepageResult.html,
      homepageFinalUrl
    );

    const candidates = dedupeByUrl([
      ...embeddedAtsCandidates,
      ...resourceCandidates,
      ...anchorCandidates,
    ])
      .map(item => ({
        ...item,
        score: scoreCareerCandidate(item, homepageFinalUrl),
      }))
      .sort((a, b) => b.score - a.score)
      .slice(0, CONFIG.maxAnchorCandidates);

    for (const candidate of candidates) {
      const validated = await validateCareerCandidate(
        candidate,
        homepageFinalUrl,
        workerId
      );

      if (validated) {
        const result = {
          ...validated,
          fallback: false,
          discoveryMethod: validated.source,
          homepageUrl: homepageFinalUrl,
        };

        cacheSet(cache.careerUrl, websiteUrl, result);
        return result;
      }
    }
  }

  // 3. Search sitemap URLs.
  const sitemapResult = await discoverFromSitemaps(homepageFinalUrl, workerId);

  if (sitemapResult) {
    const result = {
      ...sitemapResult,
      fallback: false,
      discoveryMethod: 'sitemap',
      homepageUrl: homepageFinalUrl,
    };

    cacheSet(cache.careerUrl, websiteUrl, result);
    return result;
  }

  // 4. Probe standard career paths.
  const commonPathResult = await discoverFromCommonPaths(
    homepageFinalUrl,
    workerId
  );

  if (commonPathResult) {
    const result = {
      ...commonPathResult,
      fallback: false,
      discoveryMethod: 'common_path',
      homepageUrl: homepageFinalUrl,
    };

    cacheSet(cache.careerUrl, websiteUrl, result);
    return result;
  }

  // 5. Optional search fallback.
  //
  // This only runs when SERPER_API_KEY is configured and all direct
  // website methods above failed.
  const searchResult = await discoverFromSearch(
    company,
    homepageFinalUrl,
    workerId
  );

  if (searchResult) {
    const result = {
      ...searchResult,
      fallback: false,
      discoveryMethod: 'search_result',
      homepageUrl: homepageFinalUrl,
    };

    cacheSet(cache.careerUrl, websiteUrl, result);
    return result;
  }

  // 6. Claude link selection fallback.
  //
  // Claude may select only from actual links found on the company homepage.
  // Its selection still must pass strict career page validation.
  const claudeResult = await discoverFromClaudeLinks(
    company,
    homepageFinalUrl,
    homepageResult?.html || '',
    workerId
  );

  if (claudeResult) {
    const result = {
      ...claudeResult,
      fallback: false,
      discoveryMethod: 'claude_link_selection',
      homepageUrl: homepageFinalUrl,
    };

    cacheSet(cache.careerUrl, websiteUrl, result);
    return result;
  }

  // 7. Nothing genuine was verified.
  //
  // Never store the homepage as a career page. A null career URL is safer
  // than an incorrect URL because invalid career URLs were the client's
  // original problem.
  const notFoundResult = {
    url: null,
    source: 'not_found',
    score: 0,
    fallback: false,
    discoveryMethod: 'not_found',
    homepageUrl: homepageFinalUrl,
    httpStatus: homepageResult?.status || null,
    protocolUsed: homepageResult?.protocolUsed || null,
    title: homepageResult?.html ? extractTitle(homepageResult.html) : null,
    redirects: homepageResult?.redirects || [],
    metadata: homepageResult,
    genuineCareer: false,
  };

  cacheSet(cache.careerUrl, websiteUrl, notFoundResult);
  return notFoundResult;
}

// ─── PROCESS A SINGLE COMPANY ──────────────────────────────────────────────

async function processCompany(company, workerId) {
  const startTime = Date.now();
  let retryCount = 0;
  let lastError = null;

  const hasAnyUrl =
    normalizeUrl(company.career_page_url) ||
    normalizeUrl(company.detected_career_url) ||
    normalizeUrl(company.Website);

  if (!hasAnyUrl) {
    return {
      status: 'no_url',
      result: {
        career_page_url: null,
        detected_career_url: null,
        ats_type: 'no_url',
        ats_confidence: 0,
        ats_api_url: null,
        crawl_status: 'no_url',
        career_page_status: 'no_url',
        last_crawled_at: new Date().toISOString(),
        retry_count: 0,
        last_error: 'No Website, career_page_url, or detected_career_url available',
        last_error_type: 'NoUrl',
      },
      metadata: {
        retryCount: 0,
        error: 'No URL available',
      },
    };
  }

  // ─── STEP 1: Discover the best career page first ─────────────────────
  const careerDiscovery = await discoverCareerPage(company, workerId);

  const discoveredUrl = normalizeUrl(careerDiscovery?.url);

  if (!discoveredUrl || !careerDiscovery?.genuineCareer) {
    return {
      status: 'no_url',
      result: {
        career_page_url: null,
        detected_career_url: null,
        ats_type: null,
        ats_confidence: 0,
        ats_api_url: null,
        crawl_status: 'career_not_found',
        career_page_status: 'not_found',
        last_crawled_at: new Date().toISOString(),
        retry_count: 0,
        last_error: 'No genuine public career page could be verified',
        last_error_type: 'CareerPageNotFound',
        detection_signals: {
          career_discovery: {
            source: careerDiscovery?.source || 'not_found',
            method: careerDiscovery?.discoveryMethod || 'not_found',
            homepage_url:
              careerDiscovery?.homepageUrl || normalizeUrl(company.Website),
            genuine_career_page: false,
          },
        },
      },
      metadata: {
        retryCount: 0,
        error: 'No genuine career page verified',
      },
    };
  }

  for (let attempt = 0; attempt < CONFIG.maxRetries; attempt++) {
    try {
      if (attempt > 0) {
        const delay = exponentialBackoffWithJitter(attempt);
        await sleep(delay);
        retryCount++;
      }

      // ─── STEP 2: Fetch discovered career URL with HTTP/HTTPS fallback
      const fetchResult = await fetchWithMetadata(discoveredUrl, {
        timeout: CONFIG.requestTimeout,
        maxRedirects: CONFIG.maxRedirects,
        protocols: ['https', 'http'],
      });

      const fetchedFinalUrl =
        safeUrl(fetchResult.finalUrl || discoveredUrl) ||
        discoveredUrl;

      // ─── STEP 3: ATS detection
      //
      // First use deterministic fingerprints in this runner. This catches
      // common ATS platforms without spending an AI call and avoids Claude
      // becoming a bottleneck for obvious providers such as Personio,
      // Softgarden, Workday, onlyfy, d.vinci, rexx, etc.
      let detectionResult = detectKnownATS(
        fetchedFinalUrl,
        fetchResult.html || ''
      );

      if (detectionResult) {
        detectionResult = {
          ...detectionResult,
          career_page_url: fetchedFinalUrl,
          ats_api_url: null,
        };

        console.log(
          `  [W${workerId}] ${company.Name} → ${detectionResult.ats_type} ` +
          `(runner fingerprint, conf: ${detectionResult.ats_confidence})`
        );
      } else if (fetchResult.html && CONFIG.noAI) {
        const customCareer = hasStrongJobContent(fetchResult.html);

        detectionResult = {
          ats_type: customCareer ? 'custom' : 'unknown',
          ats_confidence: customCareer ? 0.72 : 0.30,
          career_page_url: fetchedFinalUrl,
          ats_api_url: null,
          detection_method: customCareer
            ? 'runner_job_content_heuristic'
            : 'runner_no_ai_unknown',
          signals: {
            no_ai_mode: true,
            job_content_heuristic: customCareer,
          },
        };
      } else if (fetchResult.html) {
        detectionResult = await detectATS(company.Id, fetchedFinalUrl);

        // If the shared detector still returns unknown/custom, check the
        // fetched HTML one more time before accepting a generic result.
        const postDetectorFingerprint = detectKnownATS(
          normalizeUrl(detectionResult?.career_page_url) || fetchedFinalUrl,
          fetchResult.html
        );

        if (
          postDetectorFingerprint &&
          (
            !detectionResult?.ats_type ||
            ['unknown', 'custom', 'error'].includes(detectionResult.ats_type)
          )
        ) {
          detectionResult = {
            ...detectionResult,
            ...postDetectorFingerprint,
            career_page_url:
              normalizeUrl(detectionResult?.career_page_url) ||
              fetchedFinalUrl,
            ats_api_url: detectionResult?.ats_api_url || null,
          };
        }
      } else {
        detectionResult = {
          ats_type: 'error',
          ats_confidence: 0,
          career_page_url: fetchedFinalUrl,
          ats_api_url: null,
          detection_method: 'http_fetch_failed',
          http_status: fetchResult.status,
        };
      }

      // The discovered URL was already verified as a genuine career page.
      // If the shared detector returns another career URL, validate that URL
      // independently before replacing the verified one.
      let finalCareerUrl = fetchedFinalUrl;

      const detectorCareerUrl = normalizeUrl(detectionResult?.career_page_url);

      if (
        detectorCareerUrl &&
        detectorCareerUrl.replace(/\/+$/, '') !== fetchedFinalUrl.replace(/\/+$/, '')
      ) {
        const validatedDetectorUrl = await validateCareerCandidate(
          {
            url: detectorCareerUrl,
            text: '',
            source: 'ats_detector',
          },
          normalizeUrl(company.Website) || careerDiscovery?.homepageUrl || fetchedFinalUrl,
          workerId
        );

        if (validatedDetectorUrl?.genuineCareer) {
          finalCareerUrl = validatedDetectorUrl.url;
        }
      }

      // Client classification rule:
      // only the explicitly supported ATS platforms retain their names.
      // Unknown, unsupported, or any other ATS becomes custom because the
      // career page itself has already been verified as genuine.
      const clientClassification = classifyForClient(
        detectionResult?.ats_type,
        finalCareerUrl,
        fetchResult.html || ''
      );

      const rawAtsType = detectionResult?.ats_type || null;

      detectionResult = {
        ...detectionResult,
        ats_type: clientClassification.ats_type,
        ats_confidence:
          clientClassification.ats_type === 'custom'
            ? Math.max(detectionResult?.ats_confidence || 0, 0.70)
            : Math.max(detectionResult?.ats_confidence || 0, 0.90),
        career_page_url: finalCareerUrl,
        detection_method:
          detectionResult?.detection_method ||
          clientClassification.reason,
        signals: {
          ...(detectionResult?.signals || {}),
          client_classification: {
            original_ats_type: rawAtsType,
            final_ats_type: clientClassification.ats_type,
            reason: clientClassification.reason,
            allowed_named_ats: [...CLIENT_ATS_TYPES],
          },
        },
      };

      if (detectionResult.ats_type === 'custom') {
        // Custom crawler should own unsupported ATS implementations.
        detectionResult.ats_api_url = null;
      }

      const elapsed = Date.now() - startTime;

      const detectionSignals = {
        ats: detectionResult.signals || null,
        career_discovery: {
          source: careerDiscovery?.source || null,
          method: careerDiscovery?.discoveryMethod || null,
          score: careerDiscovery?.score ?? null,
          fallback: Boolean(careerDiscovery?.fallback),
          homepage_url: careerDiscovery?.homepageUrl || normalizeUrl(company.Website),
          discovered_url: discoveredUrl,
          final_url: finalCareerUrl,
          validation_http_status: careerDiscovery?.httpStatus ?? null,
          validation_title: careerDiscovery?.title || null,
          validation_reason: careerDiscovery?.validationReason || null,
          validation_confidence: careerDiscovery?.validationConfidence ?? null,
          genuine_career_page: true,
        },
      };

      let careerPageStatus;

      if ([401, 403, 429].includes(fetchResult.status)) {
        careerPageStatus = 'blocked_verified';
      } else if (fetchResult.status >= 200 && fetchResult.status < 300) {
        careerPageStatus = 'ok';
      } else if (fetchResult.status >= 300 && fetchResult.status < 400) {
        careerPageStatus = 'redirect';
      } else if (fetchResult.status >= 400 && fetchResult.status < 500) {
        careerPageStatus = 'client_error';
      } else if (fetchResult.status >= 500) {
        careerPageStatus = 'server_error';
      } else {
        careerPageStatus = 'verified';
      }

      const fields = {
        // CRITICAL FIX:
        // Populate BOTH columns from your schema.
        career_page_url: finalCareerUrl,
        detected_career_url: finalCareerUrl,

        ats_type: detectionResult.ats_type || 'unknown',
        ats_confidence: detectionResult.ats_confidence || 0,
        ats_api_url: detectionResult.ats_api_url || null,

        crawl_status:
          detectionResult.ats_type === 'custom'
            ? 'custom_detected'
            : 'ats_detected',

        last_crawled_at: new Date().toISOString(),
        crawl_time_ms: elapsed,
        retry_count: retryCount,

        last_error: fetchResult.error
          ? fetchResult.error.message
          : null,

        last_error_type: fetchResult.error
          ? (
              fetchResult.error.code ||
              fetchResult.error.name ||
              'Unknown'
            )
          : null,

        career_page_http_status: fetchResult.status,
        career_page_redirects:
          fetchResult.redirects?.length > 0
            ? fetchResult.redirects
            : null,

        redirect_chain:
          fetchResult.redirects?.length > 0
            ? fetchResult.redirects
            : null,

        response_time_ms: fetchResult.responseTimeMs,
        ttfb_ms: fetchResult.ttfbMs,
        content_type: fetchResult.contentType,
        content_length: fetchResult.contentLength,

        career_page_title: fetchResult.html
          ? extractTitle(fetchResult.html)
          : careerDiscovery?.title || null,

        career_page_status: careerPageStatus,

        detected_host: getHostname(finalCareerUrl),
        detected_subdomain: extractSubdomain(finalCareerUrl),

        detection_signals: detectionSignals,
        confidence_breakdown: detectionResult.confidence_breakdown || null,
        protocol_used: fetchResult.protocolUsed,
      };

      if (normalizeUrl(company.Website)) {
        cacheSet(cache.careerUrl, normalizeUrl(company.Website), {
          ...careerDiscovery,
          url: finalCareerUrl,
        });
      }

      console.log(
        `  [W${workerId}] ${company.Name} → ` +
        `${fields.ats_type} | career=${careerDiscovery?.discoveryMethod || 'unknown'} ` +
        `| ${fetchResult.status} [${fetchResult.protocolUsed}] in ${elapsed}ms`
      );

      return {
        status: 'success',
        result: fields,
        metadata: {
          retryCount,
          error: null,
          elapsed,
          httpStatus: fetchResult.status,
          careerDiscovery: careerDiscovery?.discoveryMethod || null,
          careerVerified: true,
        },
      };

    } catch (err) {
      lastError = err;

      if (attempt === CONFIG.maxRetries - 1) {
        const elapsed = Date.now() - startTime;

        // Career discovery already verified this URL. Preserve the genuine
        // career URL, but classify it through the client rule even when a
        // later ATS detector or metadata request fails.
        const finalCareerUrl = discoveredUrl;
        const failureClassification = classifyForClient(
          null,
          finalCareerUrl,
          ''
        );

        const fields = {
          career_page_url: finalCareerUrl,
          detected_career_url: finalCareerUrl,

          ats_type: failureClassification.ats_type,
          ats_confidence: 0.70,
          ats_api_url: null,

          crawl_status: 'custom_detected',
          last_crawled_at: new Date().toISOString(),
          crawl_time_ms: elapsed,
          retry_count: retryCount + 1,

          last_error: err.message,
          last_error_type: err.code || err.name || 'UnknownError',

          career_page_http_status: err.response?.status || 0,
          career_page_status: 'verified_detector_error',

          detected_host: getHostname(finalCareerUrl),
          detected_subdomain: extractSubdomain(finalCareerUrl),

          detection_signals: {
            ats: null,
            career_discovery: {
              source: careerDiscovery?.source || null,
              method: careerDiscovery?.discoveryMethod || null,
              score: careerDiscovery?.score ?? null,
              fallback: Boolean(careerDiscovery?.fallback),
              homepage_url: careerDiscovery?.homepageUrl || normalizeUrl(company.Website),
              discovered_url: finalCareerUrl,
              final_url: finalCareerUrl,
            },
          },
        };

        console.log(
          `  [W${workerId}] ❌ ${company.Name} failed: ${err.message}`
        );

        return {
          status: 'success',
          result: fields,
          metadata: {
            retryCount: retryCount + 1,
            error: err.message,
            elapsed,
          },
        };
      }
    }
  }

  return {
    status: 'success',
    result: {
      career_page_url: discoveredUrl,
      detected_career_url: discoveredUrl,
      ats_type: 'custom',
      ats_confidence: 0.70,
      crawl_status: 'custom_detected',
      last_crawled_at: new Date().toISOString(),
      last_error: lastError ? lastError.message : 'Unknown error',
      last_error_type: lastError
        ? (lastError.code || lastError.name || 'UnknownError')
        : 'UnknownError',
    },
    metadata: {
      retryCount,
      error: lastError ? lastError.message : 'Unknown error',
    },
  };
}

// ─── WORKER POOL ──────────────────────────────────────────────────────────

async function runWorkerPool(companies, concurrency) {
  const total = companies.length;
  const queue = [...companies];

  const results = {
    success: 0,
    error: 0,
    skipped: 0,
    no_url: 0,
  };

  let processed = 0;
  let batchUpdates = [];

  const startTime = Date.now();

  const checkpoint = CONFIG.resume ? loadCheckpoint() : null;
  const processedIds = checkpoint?.processedIds || [];
  const processedIdSet = new Set(processedIds);

  let lastCheckpointCount = processedIds.length;

  function printProgress() {
    const pct = total > 0
      ? ((processed / total) * 100).toFixed(1)
      : '0.0';

    const elapsed = Date.now() - startTime;
    const rate = processed > 0
      ? processed / Math.max(elapsed / 1000, 0.001)
      : 0;

    const remaining = Math.max(0, total - processed);
    const eta = rate > 0 ? remaining / rate : 0;

    process.stdout.write(
      `\rProgress: ${processed}/${total} (${pct}%) | ` +
      `✅ ${results.success} ❌ ${results.error} ` +
      `⏭ ${results.skipped} 🚫 ${results.no_url} | ` +
      `ETA: ${formatDuration(eta * 1000)}    `
    );
  }

  async function flushBatchIfNeeded(force = false) {
    if (!force && batchUpdates.length < CONFIG.batchSize) return;
    if (batchUpdates.length === 0) return;

    const batch = batchUpdates;
    batchUpdates = [];
    await batchUpsertCompanies(batch);
  }

  async function worker(id) {
    while (queue.length > 0) {
      const company = queue.shift();
      if (!company) break;

      if (CONFIG.resume && processedIdSet.has(company.Id)) {
        processed++;
        printProgress();
        continue;
      }

      const outcome = await processCompany(company, id);

      if (outcome.status === 'success') {
        results.success++;
      } else if (outcome.status === 'error') {
        results.error++;
      } else if (outcome.status === 'no_url') {
        results.no_url++;
      } else {
        results.skipped++;
      }

      if (outcome.result) {
        batchUpdates.push({
          id: company.Id,
          fields: outcome.result,
        });
      }

      processed++;

      if (!processedIdSet.has(company.Id)) {
        processedIds.push(company.Id);
        processedIdSet.add(company.Id);
      }

      printProgress();

      await flushBatchIfNeeded(false);

      if (processed - lastCheckpointCount >= CONFIG.checkpointEvery) {
        lastCheckpointCount = processed;

        saveCheckpoint({
          processedIds,
          timestamp: Date.now(),
        });

        await flushBatchIfNeeded(true);
      }

      await sleep(CONFIG.delayMs);
    }
  }

  const workers = Array.from(
    { length: Math.min(concurrency, Math.max(1, total)) },
    (_, i) => worker(i + 1)
  );

  await Promise.all(workers);

  await flushBatchIfNeeded(true);

  if (processed === total) {
    clearCheckpoint();
  }

  console.log('');

  return {
    results,
    processed,
    elapsed: Date.now() - startTime,
  };
}

// ─── SUMMARY REPORT ───────────────────────────────────────────────────────

async function printSummary(processed, results, elapsed, companies) {
  console.log('\n\n══════════════════════════════════════════');
  console.log('  ATS DETECTION COMPLETE');
  console.log('══════════════════════════════════════════');
  console.log(`  Duration       : ${formatDuration(elapsed)}`);
  console.log(`  Companies      : ${companies.length} total, ${processed} processed`);
  console.log(`  ✅ Success     : ${results.success}`);
  console.log(`  ❌ Errors      : ${results.error}`);
  console.log(`  ⏭ Skipped     : ${results.skipped}`);
  console.log(`  🚫 No URL     : ${results.no_url}`);

  // Career discovery coverage.
  const { data: careerCoverage } = await supabase
    .from('companies')
    .select('career_page_url, career_page_status')
    .not('Website', 'is', null);

  if (careerCoverage) {
    const withCareerUrl = careerCoverage.filter(
      r => Boolean(r.career_page_url)
    ).length;

    const notFoundCount = careerCoverage.filter(
      r => r.career_page_status === 'not_found'
    ).length;

    const verifiedCount = careerCoverage.filter(
      r =>
        Boolean(r.career_page_url) &&
        r.career_page_status !== 'not_found'
    ).length;

    console.log('\n  Career URL Coverage:');
    console.log(`    Genuine career URLs      : ${verifiedCount}/${careerCoverage.length}`);
    console.log(`    Career pages not found   : ${notFoundCount}`);
    console.log(`    Non-null career_page_url : ${withCareerUrl}/${careerCoverage.length}`);
  }

  // HTTP Status Distribution
  const { data: statusData } = await supabase
    .from('companies')
    .select('career_page_http_status')
    .not('career_page_http_status', 'is', null);

  if (statusData && statusData.length > 0) {
    const dist = {};

    statusData.forEach(row => {
      const status = row.career_page_http_status || 'unknown';
      dist[status] = (dist[status] || 0) + 1;
    });

    console.log('\n  HTTP Status Distribution:');

    Object.entries(dist)
      .sort((a, b) => b[1] - a[1])
      .forEach(([status, count]) => {
        const bar = '█'.repeat(
          Math.min(Math.round(count / 3), 30)
        );

        console.log(
          `    ${String(status).padEnd(6)} ` +
          `${String(count).padStart(4)}  ${bar}`
        );
      });
  }

  // ATS Distribution
  const { data: atsData } = await supabase
    .from('companies')
    .select('ats_type');

  if (atsData) {
    const dist = {};

    atsData.forEach(row => {
      const key = row.ats_type || 'null';
      dist[key] = (dist[key] || 0) + 1;
    });

    console.log('\n  ATS Distribution:');

    Object.entries(dist)
      .sort((a, b) => b[1] - a[1])
      .forEach(([ats, count]) => {
        const bar = '█'.repeat(
          Math.min(Math.round(count / 3), 30)
        );

        console.log(
          `    ${ats.padEnd(18)} ` +
          `${String(count).padStart(4)}  ${bar}`
        );
      });
  }

  console.log('\n  Performance:');
  console.log(
    `    Avg crawl time : ${formatDuration(
      elapsed / Math.max(1, processed)
    )}`
  );
  console.log(
    `    Throughput      : ${(
      processed / Math.max(elapsed / 1000, 0.001)
    ).toFixed(2)} companies/sec`
  );

  console.log('══════════════════════════════════════════\n');
}

// ─── MAIN ─────────────────────────────────────────────────────────────────

async function main() {
  console.log('\n🔍 ATS Detection Runner (v7 — Multilingual Career Discovery)');
  console.log(`   Concurrency : ${CONFIG.concurrency} workers`);
  console.log(`   Delay       : ${CONFIG.delayMs}ms`);
  console.log(`   Retries     : ${CONFIG.maxRetries}`);
  console.log(`   Page size   : ${CONFIG.pageSize} companies`);
  console.log(`   Resume      : ${CONFIG.resume ? 'enabled' : 'disabled'}`);
  console.log(`   Search      : ${CONFIG.searchFallback ? 'enabled (Serper)' : 'disabled'}`);
  console.log(`   AI detector : ${CONFIG.noAI ? 'disabled (local mode)' : 'enabled'}`);
  console.log(`   Dry run     : ${CONFIG.dryRun ? 'ON' : 'OFF'}`);
  console.log(`   Search      : ${CONFIG.searchFallback ? 'enabled' : 'disabled'}`);
  console.log(`   Claude      : ${CONFIG.claudeCareerFallback ? 'enabled' : 'disabled for career discovery'}`);
  console.log(`   ATS rule    : 13 named ATS platforms, everything else = custom`);
  console.log(`   Career rule : genuine verified pages only, no homepage fallback\n`);

  const checkpoint = CONFIG.resume
    ? loadCheckpoint()
    : null;

  if (checkpoint) {
    console.log(
      `   ℹ️ Resuming from checkpoint ` +
      `(${checkpoint.processedIds.length} companies already processed)`
    );
  }

  const companies = await fetchCompanies(checkpoint);

  if (companies.length === 0) {
    console.log(
      '✅ No companies to process. Use --retry or --all to re-scan.'
    );
    return;
  }

  console.log(`   Found ${companies.length} companies to process\n`);

  if (CONFIG.dryRun) {
    console.log('🔎 DRY RUN — would process the following companies:');

    companies.slice(0, 10).forEach(company => {
      console.log(
        `   • ${company.Name} | ` +
        `website=${company.Website || 'null'} | ` +
        `career=${company.career_page_url || company.detected_career_url || 'null'}`
      );
    });

    if (companies.length > 10) {
      console.log(`   ... and ${companies.length - 10} more`);
    }

    return;
  }

  const {
    results,
    processed,
    elapsed,
  } = await runWorkerPool(
    companies,
    CONFIG.concurrency
  );

  await printSummary(
    processed,
    results,
    elapsed,
    companies
  );
}

main().catch(err => {
  console.error('\n❌ Fatal error:', err.message);
  process.exit(1);
});
