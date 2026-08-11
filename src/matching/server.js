#!/usr/bin/env node
'use strict';

/**
 * PRODUCTION CUSTOMER MATCHING ENGINE v6.0
 *
 * Main changes from v5.3
 *
 * 1. Uses the complete candidate payload for semantic matching.
 *    This includes MCG content, Frontsheet content, summaries, experience,
 *    education, certifications, industries, languages and any other
 *    professional fields supplied by the upstream workflow.
 *
 * 2. Recursively reads nested JSON objects and arrays, so Finance and Legal
 *    Frontsheets do not need to share one rigid schema.
 *
 * 3. Uses the complete stored candidate row as a fallback when fields already
 *    exist in Supabase.
 *
 * 4. Rebuilds the candidate embedding on every matching request so changes in
 *    any candidate content can affect the result, not only summary changes.
 *
 * 5. Splits long profiles into chunks, embeds every chunk, then creates one
 *    weighted profile embedding. This avoids silently dropping long MCG or
 *    Frontsheet content.
 *
 * 6. Separates IT Consulting, Construction, Business, Finance and Legal.
 *    Accounting, Audit and Tax are normalized into Finance.
 *
 * 7. Removes the old early gate that discarded jobs before semantic profile
 *    similarity could be considered.
 *
 * 8. Fixes overly broad garbage title regexes that rejected valid job titles.
 *
 * 9. Uses dynamic scoring. Missing skill or title data does not give a free
 *    score and does not block semantic matching. Its unused weight is
 *    redistributed across the signals that are actually available.
 *
 * 10. Always clears stale stored matches before writing a new result set.
 *
 * v6.0.1 - matches table write fix
 *    The `matches` insert payload is now aligned with the real
 *    public.matches schema:
 *      - location_distance_km is TEXT in the DB, so it is stringified
 *        instead of sent as a number.
 *      - top_skills is TEXT[] in the DB, so it is sent as an array instead
 *        of a comma-joined string.
 *      - The apply URL column is actually named "Apply URL" (capitalized,
 *        with a space), so it is written with that exact quoted key.
 *      - career_page_url, last_crawled_at and crawl_status do not exist as
 *        columns on matches, so they are no longer sent in the insert
 *        (they are still returned in the API response/matches array,
 *        just not persisted to that table).
 */

const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const axios = require('axios');
const fs = require('fs');
const https = require('https');
const { v4: uuidv4 } = require('uuid');
require('dotenv').config();

// Configuration
const PORT = Number.parseInt(process.env.PORT || '3000', 10);
const HOST = process.env.HOST || '0.0.0.0';
const MAX_CONCURRENT_MATCHES = Number.parseInt(process.env.MAX_CONCURRENT_MATCHES || '20', 10);
const JOB_CACHE_TTL_MS = Number.parseInt(process.env.JOB_CACHE_TTL_MS || '600000', 10);
const TIMEOUT_MS = Number.parseInt(process.env.TIMEOUT_MS || '120000', 10);
const JSON_LIMIT = process.env.JSON_LIMIT || '25mb';
const MATCH_WEBHOOK_SECRET = process.env.MATCH_WEBHOOK_SECRET || '';

// Semantic profile embedding configuration
const EMBED_CHUNK_CHARS = Math.max(2000, Number.parseInt(process.env.EMBED_CHUNK_CHARS || '8000', 10));
const EMBED_BATCH_SIZE = Math.max(1, Number.parseInt(process.env.EMBED_BATCH_SIZE || '8', 10));
const MAX_PROFILE_CHARS = Math.max(0, Number.parseInt(process.env.MAX_PROFILE_CHARS || '0', 10));

// Scoring weights
const SEMANTIC_WEIGHT = Number.parseFloat(process.env.SEMANTIC_WEIGHT || '0.50');
const SKILL_WEIGHT = Number.parseFloat(process.env.SKILL_WEIGHT || '0.20');
const TITLE_WEIGHT = Number.parseFloat(process.env.TITLE_WEIGHT || '0.20');
const DIVISION_WEIGHT = Number.parseFloat(process.env.DIVISION_WEIGHT || '0.10');
const TITLE_BONUS = Number.parseFloat(process.env.TITLE_BONUS || '0.03');

const MIN_SEMANTIC_SIMILARITY = Number.parseFloat(process.env.MIN_SEMANTIC_SIMILARITY || '0.01');
const DEFAULT_RADIUS_KM = Number.parseFloat(process.env.DEFAULT_RADIUS_KM || '50');
const DEFAULT_TOP_K = Number.parseInt(process.env.DEFAULT_TOP_K || '30', 10);
const MAX_TOP_K = Math.max(1, Number.parseInt(process.env.MAX_TOP_K || '100', 10));

const WEIGHT_SUM = SEMANTIC_WEIGHT + SKILL_WEIGHT + TITLE_WEIGHT + DIVISION_WEIGHT;
if (!Number.isFinite(WEIGHT_SUM) || WEIGHT_SUM <= 0) {
  console.error('Invalid matching weights. Check SEMANTIC_WEIGHT, SKILL_WEIGHT, TITLE_WEIGHT and DIVISION_WEIGHT.');
  process.exit(1);
}

// Division configuration
const DIVISION_CONFIG = {
  'IT Consulting': {
    minSkillOverlap: 0,
    requireSkillMatch: false,
  },
  Construction: {
    minSkillOverlap: 0,
    requireSkillMatch: false,
  },
  Business: {
    minSkillOverlap: 0,
    requireSkillMatch: false,
  },
  Finance: {
    minSkillOverlap: 0,
    requireSkillMatch: false,
  },
  Legal: {
    minSkillOverlap: 0,
    requireSkillMatch: false,
  },
  default: {
    minSkillOverlap: 0,
    requireSkillMatch: false,
  },
};

// Garbage title detection
// Important: do not use generic patterns such as /^[a-z]+\s+[a-z]+/ because
// they classify valid titles such as "Senior Accountant" as garbage.
const GARBAGE_TITLE_PATTERNS = [
  /^karriere$/i,
  /^career$/i,
  /^careers$/i,
  /^jobs$/i,
  /^job offers$/i,
  /^stellenangebote$/i,
  /^offene stellen$/i,
  /^open positions$/i,
  /^career opportunities$/i,
  /^join us$/i,
  /^join our team$/i,
  /^work with us$/i,
  /^jobs at .+$/i,
  /^karriere bei .+$/i,
  /^jobportal$/i,
  /^jobsportal$/i,
  /^job board$/i,
  /^job portal$/i,
  /^stellenmarkt$/i,
  /^jobmarkt$/i,
  /^initiativbewerbung$/i,
  /^proactive application$/i,
  /^proactive application\s*\/\s*referral$/i,
  /^online(?:-| )bewerbung$/i,
  /^referral$/i,
  /great to have you here/i,
  /super, dass du hier bist/i,
  /wir suchen dich/i,
  /are you looking for new challenges/i,
  /dein traumjob/i,
  /willkommen in ihrer zukunft/i,
  /bewerbungsprozess/i,
  /bewerben\.\s*begegnen\.\s*beginnen\./i,
  /see beyond\.\s*secure beyond\./i,
  /passioniert,\s*eigenverantwortlich/i,
  /wir freuen uns auf ihre anfrage/i,
  /menschlichkeit pragmatismus nachhaltigkeit/i,
  /^grüne baustelle$/i,
  /^dein weg zu uns$/i,
  /^fachabteilungen\s*&\s*organisation$/i,
  /^regionaldirektionen$/i,
  /^geschäftsstellen$/i,
  /^partner-?\/mitgliedschaften$/i,
  /^kontakte\s*&\s*standorte$/i,
];

function isGarbageTitle(title) {
  if (!title) return true;
  const trimmed = String(title).trim();
  if (trimmed.length < 3) return true;
  return GARBAGE_TITLE_PATTERNS.some((pattern) => pattern.test(trimmed));
}

// Stopwords for title and lexical processing
const STOPWORDS = new Set([
  'der', 'die', 'das', 'und', 'oder', 'für', 'mit', 'von', 'zu', 'im', 'am',
  'als', 'auch', 'auf', 'bei', 'durch', 'in', 'nach', 'um', 'über', 'unter',
  'ohne', 'des', 'dem', 'den', 'ein', 'eine', 'eines', 'einer', 'einem', 'einen',
  'the', 'and', 'or', 'for', 'with', 'without', 'of', 'to', 'from', 'by',
  'at', 'on', 'into', 'through', 'during', 'including', 'per', 'via', 'a', 'an',
  'is', 'are', 'be', 'as', 'our', 'your', 'we', 'you', 'this', 'that', 'role',
  'position', 'job', 'candidate', 'profile', 'summary',
]);

function normalizeText(value) {
  return String(value ?? '')
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9äöüß+#.]+/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractKeywords(text) {
  const normalized = normalizeText(text);
  if (!normalized) return [];
  const words = normalized.split(/\s+/g);
  return [...new Set(words.filter((word) => word.length > 2 && !STOPWORDS.has(word)))];
}

function clamp01(value) {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(1, value));
}

function hasMeaningfulValue(value) {
  if (value === undefined || value === null) return false;
  if (typeof value === 'string') return value.trim().length > 0;
  if (Array.isArray(value)) return value.length > 0;
  if (typeof value === 'object') return Object.keys(value).length > 0;
  return true;
}

function preferIncoming(incoming, existing, fallback = null) {
  return hasMeaningfulValue(incoming) ? incoming : (hasMeaningfulValue(existing) ? existing : fallback);
}


function getFirstValue(object, keys, fallback = null) {
  if (!object || typeof object !== 'object') return fallback;

  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(object, key) && hasMeaningfulValue(object[key])) {
      return object[key];
    }
  }

  const lowerMap = new Map(
    Object.keys(object).map((key) => [String(key).toLowerCase().replace(/[\s_-]+/g, ''), key])
  );

  for (const key of keys) {
    const normalized = String(key).toLowerCase().replace(/[\s_-]+/g, '');
    const actualKey = lowerMap.get(normalized);
    if (actualKey && hasMeaningfulValue(object[actualKey])) return object[actualKey];
  }

  return fallback;
}

function toFiniteNumber(value) {
  if (value === null || value === undefined || value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

// Division handling
const DIVISION_TERMS = {
  'IT Consulting': [
    'software', 'developer', 'development', 'devops', 'cloud', 'cyber',
    'cybersecurity', 'security engineer', 'network', 'netzwerk', 'system engineer',
    'system administrator', 'administrator', 'infrastructure', 'server', 'linux',
    'windows', 'programming', 'coding', 'sap', 'data engineer', 'data scientist',
    'database', 'sql', 'azure', 'aws', 'gcp', 'kubernetes', 'docker', 'java',
    'javascript', 'typescript', 'python', '.net', 'c#', 'frontend', 'backend',
    'fullstack', 'full stack', 'it consultant', 'technical consultant',
  ],
  Construction: [
    'construction', 'bau', 'hochbau', 'tiefbau', 'architekt', 'architect',
    'architecture', 'civil engineer', 'building', 'baustelle', 'bauleiter',
    'projektleiter bau', 'project manager construction', 'planung', 'bim',
    'immobilie', 'real estate development', 'structural engineer',
  ],
  Finance: [
    'finance', 'financial', 'accounting', 'accountant', 'buchhaltung',
    'buchhalter', 'controller', 'controlling', 'audit', 'auditor', 'tax',
    'steuer', 'steuerberater', 'treasury', 'ifrs', 'gaap', 'payroll',
    'accounts payable', 'accounts receivable', 'fp&a', 'financial analyst',
    'investment', 'banking', 'credit', 'risk management',
  ],
  Legal: [
    'legal', 'lawyer', 'attorney', 'anwalt', 'rechtsanwalt', 'jurist',
    'juristin', 'counsel', 'general counsel', 'legal counsel', 'recht',
    'compliance', 'contract law', 'contracts', 'datenschutz', 'privacy law',
    'litigation', 'corporate law', 'arbeitsrecht', 'gesellschaftsrecht',
  ],
  Business: [
    'business development', 'sales', 'account manager', 'key account',
    'marketing', 'procurement', 'einkauf', 'operations', 'strategy',
    'human resources', 'hr manager', 'recruiter', 'recruitment', 'talent',
    'supply chain', 'logistics', 'customer success', 'customer service',
    'project manager', 'product manager', 'management consultant',
  ],
};

function normalizeDivision(rawDivision, contextText = '') {
  const raw = normalizeText(rawDivision);
  const context = normalizeText(contextText);

  if (raw) {
    if (/^(it|it consulting|technology|tech|information technology)$/.test(raw)) return 'IT Consulting';
    if (/^(construction|bau|building|engineering construction)$/.test(raw)) return 'Construction';
    if (/^(finance|financial|accounting|audit|tax|business finance|finance business)$/.test(raw)) return 'Finance';
    if (/^(legal|law|business legal|legal business)$/.test(raw)) return 'Legal';

    // Ambiguous umbrella divisions are resolved from the candidate content.
    if (/^(business|business finance legal|finance legal)$/.test(raw)) {
      const inferred = inferDivisionFromText(context);
      if (inferred === 'Finance' || inferred === 'Legal') return inferred;
      return 'Business';
    }
  }

  return inferDivisionFromText(context);
}

function containsTerm(text, term) {
  const normalizedText = normalizeText(text);
  const normalizedTerm = normalizeText(term);
  if (!normalizedText || !normalizedTerm) return false;
  return ` ${normalizedText} `.includes(` ${normalizedTerm} `);
}

function inferDivisionFromText(text) {
  const normalized = normalizeText(text);
  if (!normalized) return 'default';

  const scores = new Map();
  for (const [division, terms] of Object.entries(DIVISION_TERMS)) {
    let score = 0;
    for (const term of terms) {
      if (containsTerm(normalized, term)) {
        score += term.includes(' ') ? 2 : 1;
      }
    }
    scores.set(division, score);
  }

  const ranked = [...scores.entries()].sort((a, b) => b[1] - a[1]);
  if (ranked.length === 0 || ranked[0][1] === 0) return 'default';

  // When Finance and Legal tie inside the Business umbrella, keep Business
  // rather than forcing an arbitrary specialization.
  if (ranked.length > 1 && ranked[0][1] === ranked[1][1] && ranked[0][1] > 0) {
    const tied = ranked.filter((entry) => entry[1] === ranked[0][1]).map((entry) => entry[0]);
    if (tied.includes('Finance') && tied.includes('Legal')) return 'Business';
  }

  return ranked[0][0];
}

function divisionCompatibility(candidateDivision, jobDivision) {
  const candidate = candidateDivision || 'default';
  const job = jobDivision || 'default';

  if (candidate === 'default' || job === 'default') return null;
  if (candidate === job) return 1;

  if (candidate === 'Business' && (job === 'Finance' || job === 'Legal')) return 0.75;
  if (job === 'Business' && (candidate === 'Finance' || candidate === 'Legal')) return 0.70;

  return 0.05;
}

// Express app
const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: JSON_LIMIT }));
app.use(express.urlencoded({ extended: true, limit: JSON_LIMIT }));

// Optional webhook authentication. It is backward compatible when unset.
function authenticateMatchWebhook(req, res, next) {
  if (!MATCH_WEBHOOK_SECRET) return next();

  const bearer = String(req.headers.authorization || '').replace(/^Bearer\s+/i, '').trim();
  const headerSecret = String(req.headers['x-match-secret'] || '').trim();
  if (bearer === MATCH_WEBHOOK_SECRET || headerSecret === MATCH_WEBHOOK_SECRET) return next();

  return res.status(401).json({
    success: false,
    error: 'Unauthorized',
    timestamp: new Date().toISOString(),
  });
}

// Supabase
const supabaseUrl = process.env.SUPABASE_URL;
const serviceKey = process.env.SUPABASE_SERVICE_KEY;
if (!supabaseUrl || !serviceKey) {
  console.error('SUPABASE_URL or SUPABASE_SERVICE_KEY missing in .env');
  process.exit(1);
}
const supabase = createClient(supabaseUrl, serviceKey, {
  realtime: { transport: ws },
  auth: { persistSession: false, autoRefreshToken: false },
});

// Voyage AI
const VOYAGE_API_KEY = process.env.VOYAGE_API_KEY;
const VOYAGE_URL = 'https://api.voyageai.com/v1/embeddings';
const VOYAGE_MODEL = process.env.VOYAGE_MODEL || 'voyage-3-large';
if (!VOYAGE_API_KEY) {
  console.error('VOYAGE_API_KEY missing in .env');
  process.exit(1);
}

// Semaphore
class Semaphore {
  constructor(limit) {
    this.limit = Math.max(1, limit);
    this.running = 0;
    this.queue = [];
  }

  async acquire() {
    if (this.running < this.limit) {
      this.running += 1;
      return;
    }
    await new Promise((resolve) => this.queue.push(resolve));
    this.running += 1;
  }

  release() {
    this.running = Math.max(0, this.running - 1);
    const next = this.queue.shift();
    if (next) next();
  }

  async run(fn) {
    await this.acquire();
    try {
      return await fn();
    } finally {
      this.release();
    }
  }
}

const matchSemaphore = new Semaphore(MAX_CONCURRENT_MATCHES);

// Helper: set statement_timeout if RPC exists
let timeoutWarned = false;
async function setStatementTimeout(seconds = 300) {
  try {
    const { error } = await supabase.rpc('set_config', {
      setting_name: 'statement_timeout',
      setting_value: `${seconds}s`,
      is_local: true,
    });
    if (error) throw error;
  } catch (error) {
    if (!timeoutWarned) {
      console.warn(`set_config RPC not available. Continuing without it: ${error.message}`);
      timeoutWarned = true;
    }
  }
}

// Job cache
let cachedJobs = null;
let jobsCacheTimestamp = 0;
let jobsFetchInFlight = null;

async function fetchJobsFromSupabase() {
  const start = Date.now();
  console.log('Fetching jobs with keyset pagination...');
  await setStatementTimeout(300);

  const jobs = [];
  const PAGE_SIZE = 500;
  let lastId = null;
  let page = 0;

  while (true) {
    let query = supabase
      .from('jobs')
      .select(
        'id, title, company_id, company_name, apply_url, location, location_lat, location_lng, remote_type, seniority_level, structured_skills, skill_embedding'
      )
      .not('skill_embedding', 'is', null)
      .order('id', { ascending: true })
      .limit(PAGE_SIZE)
      .abortSignal(AbortSignal.timeout(300000));

    if (lastId !== null) query = query.gt('id', lastId);

    const { data, error } = await query;
    if (error) {
      console.error(`Supabase jobs query failed on page ${page + 1}:`, error);
      throw error;
    }
    if (!data || data.length === 0) break;

    jobs.push(...data);
    lastId = data[data.length - 1].id;
    page += 1;

    if (data.length < PAGE_SIZE) break;
  }

  console.log(`Loaded ${jobs.length} jobs in ${Date.now() - start}ms`);
  return jobs;
}

async function getCachedJobs() {
  const now = Date.now();
  if (cachedJobs && (now - jobsCacheTimestamp) < JOB_CACHE_TTL_MS) return cachedJobs;
  if (jobsFetchInFlight) return jobsFetchInFlight;

  jobsFetchInFlight = (async () => {
    try {
      const jobs = await fetchJobsFromSupabase();
      cachedJobs = jobs;
      jobsCacheTimestamp = Date.now();
      return jobs;
    } finally {
      jobsFetchInFlight = null;
    }
  })();

  return jobsFetchInFlight;
}

// Company cache
let companiesMap = null;
let companiesCacheTimestamp = 0;
let companiesFetchInFlight = null;

async function fetchCompaniesFromSupabase() {
  const start = Date.now();
  console.log('Fetching companies...');

  const { data, error } = await supabase
    .from('companies')
    .select('"Id", "Name", "detected_career_url", "last_crawled_at", "crawl_status"')
    .abortSignal(AbortSignal.timeout(60000));

  if (error) throw error;

  const byId = new Map();
  const byName = new Map();

  for (const company of data || []) {
    const info = {
      Id: company.Id,
      Name: company.Name,
      career_page_url: company.detected_career_url || null,
      last_crawled_at: company.last_crawled_at || null,
      crawl_status: company.crawl_status || null,
    };

    byId.set(company.Id, info);
    if (company.Name) {
      const key = company.Name.toLowerCase().trim();
      if (!byName.has(key)) byName.set(key, info);
    }
  }

  console.log(`Loaded ${(data || []).length} companies in ${Date.now() - start}ms`);
  return { byId, byName };
}

async function getCompaniesMap() {
  const now = Date.now();
  if (companiesMap && (now - companiesCacheTimestamp) < JOB_CACHE_TTL_MS) return companiesMap;
  if (companiesFetchInFlight) return companiesFetchInFlight;

  companiesFetchInFlight = (async () => {
    try {
      const map = await fetchCompaniesFromSupabase();
      companiesMap = map;
      companiesCacheTimestamp = Date.now();
      return map;
    } finally {
      companiesFetchInFlight = null;
    }
  })();

  return companiesFetchInFlight;
}

async function warmupCache(retries = 3) {
  console.log('Warming matching caches...');

  for (let attempt = 1; attempt <= retries; attempt += 1) {
    try {
      await Promise.all([getCachedJobs(), getCompaniesMap()]);
      console.log('Cache warmup complete.');
      return true;
    } catch (error) {
      console.error(`Cache warmup attempt ${attempt} failed: ${error.message}`);
      if (attempt < retries) await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }

  return false;
}

// Embedding helpers
function parseEmbedding(embedding) {
  if (!embedding) return null;
  if (Array.isArray(embedding)) return embedding.map(Number);
  if (typeof embedding === 'string') {
    try {
      const parsed = JSON.parse(embedding);
      return Array.isArray(parsed) ? parsed.map(Number) : null;
    } catch {
      return null;
    }
  }
  return null;
}

function parseSkillScores(raw) {
  if (!raw) return {};

  let value = raw;
  if (typeof value === 'string') {
    try {
      value = JSON.parse(value);
    } catch {
      return {};
    }
  }

  if (typeof value !== 'object' || value === null || Array.isArray(value)) return {};

  const output = {};
  for (const [skill, rawScore] of Object.entries(value)) {
    if (!skill || !String(skill).trim()) continue;
    const score = Number(rawScore);
    output[String(skill).trim()] = Number.isFinite(score) ? score : 1;
  }
  return output;
}

function parseStructuredSkills(raw) {
  if (!raw) return [];

  let value = raw;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) return [];
    try {
      value = JSON.parse(trimmed);
    } catch {
      value = trimmed.split(/[,;|\n]/g).map((part) => part.trim()).filter(Boolean);
    }
  }

  if (Array.isArray(value)) {
    return value
      .flatMap((item) => {
        if (typeof item === 'string') return [item];
        if (item && typeof item === 'object') {
          return [item.name, item.skill, item.label, item.value].filter(Boolean);
        }
        return [];
      })
      .map((item) => String(item).trim())
      .filter(Boolean);
  }

  if (typeof value === 'object' && value !== null) {
    return Object.keys(value).map((item) => item.trim()).filter(Boolean);
  }

  return [];
}

function cosineSimilarity(vecA, vecB) {
  const a = parseEmbedding(vecA);
  const b = parseEmbedding(vecB);
  if (!a || !b || a.length === 0 || a.length !== b.length) return 0;

  let dot = 0;
  let magA = 0;
  let magB = 0;

  for (let i = 0; i < a.length; i += 1) {
    const av = Number(a[i]);
    const bv = Number(b[i]);
    if (!Number.isFinite(av) || !Number.isFinite(bv)) return 0;
    dot += av * bv;
    magA += av * av;
    magB += bv * bv;
  }

  if (magA === 0 || magB === 0) return 0;
  return clamp01(dot / (Math.sqrt(magA) * Math.sqrt(magB)));
}

function weightedAverageEmbeddings(items) {
  if (!Array.isArray(items) || items.length === 0) return null;

  const valid = items.filter((item) => Array.isArray(item.embedding) && item.embedding.length > 0 && item.weight > 0);
  if (valid.length === 0) return null;

  const dimension = valid[0].embedding.length;
  if (!valid.every((item) => item.embedding.length === dimension)) {
    throw new Error('Voyage AI returned embeddings with inconsistent dimensions');
  }

  const output = new Array(dimension).fill(0);
  let totalWeight = 0;

  for (const item of valid) {
    const weight = Number(item.weight) || 1;
    totalWeight += weight;
    for (let i = 0; i < dimension; i += 1) {
      output[i] += Number(item.embedding[i]) * weight;
    }
  }

  if (totalWeight <= 0) return null;
  for (let i = 0; i < dimension; i += 1) output[i] /= totalWeight;

  // Normalize the average vector to unit length.
  const magnitude = Math.sqrt(output.reduce((sum, value) => sum + value * value, 0));
  if (magnitude > 0) {
    for (let i = 0; i < output.length; i += 1) output[i] /= magnitude;
  }

  return output;
}

function splitTextIntoChunks(text, maxChars = EMBED_CHUNK_CHARS) {
  const cleaned = String(text || '').replace(/\r\n/g, '\n').trim();
  if (!cleaned) return [];
  if (cleaned.length <= maxChars) return [cleaned];

  const chunks = [];
  let remaining = cleaned;

  while (remaining.length > maxChars) {
    let splitAt = remaining.lastIndexOf('\n', maxChars);
    if (splitAt < Math.floor(maxChars * 0.60)) splitAt = remaining.lastIndexOf('. ', maxChars);
    if (splitAt < Math.floor(maxChars * 0.60)) splitAt = remaining.lastIndexOf(' ', maxChars);
    if (splitAt < Math.floor(maxChars * 0.60)) splitAt = maxChars;

    chunks.push(remaining.slice(0, splitAt).trim());
    remaining = remaining.slice(splitAt).trim();
  }

  if (remaining) chunks.push(remaining);
  return chunks.filter(Boolean);
}

async function embedTexts(texts) {
  if (!Array.isArray(texts) || texts.length === 0) throw new Error('No text supplied for embedding');

  const allEmbeddings = [];

  for (let offset = 0; offset < texts.length; offset += EMBED_BATCH_SIZE) {
    const batch = texts.slice(offset, offset + EMBED_BATCH_SIZE);

    try {
      const response = await axios.post(
        VOYAGE_URL,
        {
          model: VOYAGE_MODEL,
          input: batch,
        },
        {
          headers: {
            Authorization: `Bearer ${VOYAGE_API_KEY}`,
            'Content-Type': 'application/json',
          },
          timeout: 60000,
        }
      );

      const data = response.data?.data;
      if (!Array.isArray(data) || data.length !== batch.length) {
        throw new Error(`Expected ${batch.length} embeddings but received ${Array.isArray(data) ? data.length : 0}`);
      }

      for (const row of data) {
        if (!Array.isArray(row.embedding) || row.embedding.length === 0) {
          throw new Error('Voyage AI returned an empty embedding');
        }
        allEmbeddings.push(row.embedding);
      }
    } catch (error) {
      const details = error.response?.data ? JSON.stringify(error.response.data).slice(0, 1000) : error.message;
      throw new Error(`Voyage AI error: ${details}`);
    }
  }

  return allEmbeddings;
}

// Full candidate profile extraction
const SEMANTIC_SKIP_KEYS = new Set([
  'id',
  'salesforce_contact_id',
  'skill_embedding',
  'embedding',
  'created_at',
  'updated_at',
  'deleted_at',
  'location_lat',
  'location_lng',
  'latitude',
  'longitude',
  'radius',
  'radius_km',
  'top_k',
  'topk',
  'request_id',
  'is_active',
]);

const CORE_PROFILE_KEYS = new Set([
  'position',
  'name',
  'current_employer',
  'salary_expectation',
  'language',
  'languages',
  'seniority_level',
  'remote_preference',
  'location',
  'summary',
  'skill_scores',
  'division',
]);

function isSensitiveKey(key) {
  const normalized = String(key || '').toLowerCase();
  return /(^|_)(password|passwd|secret|token|authorization|api_key|apikey|access_key|private_key)($|_)/i.test(normalized);
}

function looksLikeBase64Blob(value) {
  const text = String(value || '').trim();
  if (text.length < 1000) return false;
  if (/^data:[^;]+;base64,/i.test(text)) return true;
  const sample = text.slice(0, Math.min(4000, text.length));
  const compact = sample.replace(/\s+/g, '');
  if (compact.length < 1000) return false;
  return /^[A-Za-z0-9+/=]+$/.test(compact) && compact.length / sample.length > 0.95;
}

function humanizeKey(key) {
  return String(key || '')
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/[_\-.]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function maybeParseJsonString(value) {
  if (typeof value !== 'string') return value;
  const trimmed = value.trim();
  if (!trimmed || !((trimmed.startsWith('{') && trimmed.endsWith('}')) || (trimmed.startsWith('[') && trimmed.endsWith(']')))) {
    return value;
  }

  try {
    return JSON.parse(trimmed);
  } catch {
    return value;
  }
}

function flattenSemanticContent(value, path = [], output = [], seen = new WeakSet(), depth = 0) {
  if (depth > 12 || value === undefined || value === null) return output;

  if (typeof value === 'object' && value !== null) {
    if (seen.has(value)) return output;
    seen.add(value);
  }

  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index += 1) {
      flattenSemanticContent(value[index], path, output, seen, depth + 1);
    }
    return output;
  }

  if (typeof value === 'object' && value !== null) {
    for (const [rawKey, rawValue] of Object.entries(value)) {
      const key = String(rawKey).trim();
      const normalizedKey = key.toLowerCase();
      if (!key || SEMANTIC_SKIP_KEYS.has(normalizedKey) || isSensitiveKey(normalizedKey)) continue;

      const parsedValue = maybeParseJsonString(rawValue);
      flattenSemanticContent(parsedValue, [...path, key], output, seen, depth + 1);
    }
    return output;
  }

  const text = String(value).replace(/\s+/g, ' ').trim();
  if (!text || looksLikeBase64Blob(text)) return output;

  const label = path.map(humanizeKey).filter(Boolean).join(' > ');
  output.push(label ? `${label}: ${text}` : text);
  return output;
}

function buildCoreProfileText(profile) {
  const parts = [];

  const position = preferIncoming(profile.Position, profile.position, null);
  const name = preferIncoming(profile.name, profile.Name, null);
  const employer = preferIncoming(profile.current_employer, profile.currentEmployer, null);
  const salary = preferIncoming(profile.salary_expectation, profile.salaryExpectation, null);
  const languages = preferIncoming(profile.language, profile.languages, null);
  const seniority = preferIncoming(profile.seniority_level, profile.seniorityLevel, null);
  const remote = preferIncoming(profile.remote_preference, profile.remotePreference, null);
  const location = profile.location;
  const summary = preferIncoming(profile.summary, profile.candidate_summary, null);
  const division = preferIncoming(profile.Division, profile.division, null);
  const skillScores = parseSkillScores(profile.skill_scores);

  if (position) parts.push(`Current or target position: ${position}`);
  if (name) parts.push(`Candidate name: ${name}`);
  if (employer) parts.push(`Current employer: ${employer}`);
  if (seniority) parts.push(`Seniority: ${seniority}`);
  if (division) parts.push(`Division: ${division}`);
  if (location) parts.push(`Location: ${location}`);
  if (remote) parts.push(`Remote preference: ${remote}`);
  if (languages) parts.push(`Languages: ${typeof languages === 'string' ? languages : JSON.stringify(languages)}`);
  if (salary) parts.push(`Salary expectation: ${salary}`);

  if (Object.keys(skillScores).length > 0) {
    const skills = Object.entries(skillScores)
      .map(([skill, score]) => `${skill} (${score})`)
      .join(', ');
    parts.push(`Skills and proficiency: ${skills}`);
  }

  // The summary is intentionally prominent because the client specifically
  // requested it to influence matching across every division.
  if (summary) parts.push(`Candidate summary: ${summary}`);

  return parts.join('\n');
}

function buildFullSourceText(profile) {
  const filtered = {};

  for (const [key, value] of Object.entries(profile || {})) {
    const normalizedKey = key.toLowerCase();
    if (SEMANTIC_SKIP_KEYS.has(normalizedKey) || isSensitiveKey(normalizedKey)) continue;

    // Canonical fields are already represented in the high priority core
    // section. Other MCG and Frontsheet fields remain here in full.
    if (CORE_PROFILE_KEYS.has(normalizedKey)) continue;

    filtered[key] = value;
  }

  return flattenSemanticContent(filtered).join('\n');
}

function buildCandidateSemanticProfile(profile) {
  const coreText = buildCoreProfileText(profile);
  const fullSourceText = buildFullSourceText(profile);

  let totalText = [coreText, fullSourceText].filter(Boolean).join('\n\n');
  let truncated = false;

  if (MAX_PROFILE_CHARS > 0 && totalText.length > MAX_PROFILE_CHARS) {
    totalText = totalText.slice(0, MAX_PROFILE_CHARS);
    truncated = true;
  }

  // Recreate sections after optional global cap so no hidden text is embedded.
  const effectiveCore = coreText.slice(0, totalText.length);
  const remainingChars = Math.max(0, totalText.length - effectiveCore.length - (effectiveCore && fullSourceText ? 2 : 0));
  const effectiveFull = remainingChars > 0 ? fullSourceText.slice(0, remainingChars) : '';

  const sections = [];
  if (effectiveCore) sections.push({ name: 'core', text: effectiveCore, weight: 2.0 });
  if (effectiveFull) sections.push({ name: 'full_source', text: effectiveFull, weight: 1.0 });

  return {
    sections,
    totalText,
    totalCharacters: totalText.length,
    truncated,
    hasFullSourceContent: Boolean(effectiveFull),
  };
}

async function embedCandidateProfile(profile) {
  const built = buildCandidateSemanticProfile(profile);
  if (!built.totalText || built.totalText.length < 5) throw new Error('No valid candidate content to embed');

  const chunkDescriptors = [];

  for (const section of built.sections) {
    const chunks = splitTextIntoChunks(section.text, EMBED_CHUNK_CHARS);
    for (const chunk of chunks) {
      chunkDescriptors.push({
        text: chunk,
        weight: section.weight * Math.max(1, chunk.length / 1000),
        section: section.name,
      });
    }
  }

  if (chunkDescriptors.length === 0) throw new Error('Candidate semantic profile produced no embedding chunks');

  const embeddings = await embedTexts(chunkDescriptors.map((item) => item.text));
  const weighted = weightedAverageEmbeddings(
    embeddings.map((embedding, index) => ({
      embedding,
      weight: chunkDescriptors[index].weight,
    }))
  );

  if (!weighted) throw new Error('Unable to build candidate profile embedding');

  return {
    embedding: weighted,
    characters: built.totalCharacters,
    chunks: chunkDescriptors.length,
    truncated: built.truncated,
    hasFullSourceContent: built.hasFullSourceContent,
  };
}

function mergeProfileData(existingCandidate, incomingCandidate) {
  const merged = { ...(existingCandidate || {}) };

  for (const [key, value] of Object.entries(incomingCandidate || {})) {
    if (value !== undefined && value !== null) merged[key] = value;
  }

  return merged;
}

// Skill matching
function normalizeSkillName(skill) {
  return normalizeText(skill)
    .replace(/\bmicrosoft\b/g, 'ms')
    .replace(/\s+/g, ' ')
    .trim();
}

function skillNamesEquivalent(a, b) {
  const left = normalizeSkillName(a);
  const right = normalizeSkillName(b);
  if (!left || !right) return false;
  if (left === right) return true;

  const aliasPairs = [
    ['ms excel', 'excel'],
    ['microsoft excel', 'excel'],
    ['ms azure', 'azure'],
    ['amazon web services', 'aws'],
    ['google cloud platform', 'gcp'],
    ['javascript', 'js'],
    ['typescript', 'ts'],
    ['nodejs', 'node js'],
    ['reactjs', 'react'],
    ['postgresql', 'postgres'],
  ];

  for (const [x, y] of aliasPairs) {
    if ((left === x && right === y) || (left === y && right === x)) return true;
  }

  // Safe containment for longer multiword skills only.
  if (left.length >= 6 && right.length >= 6) {
    if (left.includes(right) || right.includes(left)) return true;
  }

  return false;
}

function calculateSkillOverlap(candidateSkillScores, jobStructuredSkills) {
  const candidateEntries = Object.entries(candidateSkillScores || {});
  const jobSkills = parseStructuredSkills(jobStructuredSkills);

  if (candidateEntries.length === 0 || jobSkills.length === 0) {
    return { available: false, score: null, matchedSkills: [] };
  }

  let totalWeight = 0;
  let matchedWeight = 0;
  const matchedSkills = [];

  for (const [candidateSkill, rawScore] of candidateEntries) {
    const weight = Math.max(0, Number(rawScore) || 0);
    totalWeight += weight;

    const matched = jobSkills.some((jobSkill) => skillNamesEquivalent(candidateSkill, jobSkill));
    if (matched) {
      matchedWeight += weight;
      matchedSkills.push(candidateSkill);
    }
  }

  if (totalWeight <= 0) {
    return { available: false, score: null, matchedSkills: [] };
  }

  return {
    available: true,
    score: clamp01(matchedWeight / totalWeight),
    matchedSkills,
  };
}

// Title matching
function calculateTitleMatch(candidatePosition, jobTitle) {
  const candidateKeywords = extractKeywords(candidatePosition);
  const jobKeywords = extractKeywords(jobTitle);

  if (candidateKeywords.length === 0 || jobKeywords.length === 0) {
    return { available: false, score: null, exactPhrase: false };
  }

  const candidateSet = new Set(candidateKeywords);
  const jobSet = new Set(jobKeywords);

  let intersection = 0;
  for (const keyword of candidateSet) {
    if (jobSet.has(keyword)) intersection += 1;
  }

  const candidateCoverage = intersection / candidateSet.size;
  const jobCoverage = intersection / jobSet.size;
  const score = clamp01((candidateCoverage * 0.65) + (jobCoverage * 0.35));

  const normalizedPosition = normalizeText(candidatePosition);
  const normalizedTitle = normalizeText(jobTitle);
  const exactPhrase = Boolean(
    normalizedPosition && normalizedTitle &&
    (normalizedTitle.includes(normalizedPosition) || normalizedPosition.includes(normalizedTitle))
  );

  return {
    available: true,
    score: exactPhrase ? Math.max(score, 0.95) : score,
    exactPhrase,
  };
}

// Dynamic scoring
function calculateDynamicFinalScore({ semantic, skill, title, division }) {
  const signals = [
    { name: 'semantic', score: semantic, weight: SEMANTIC_WEIGHT },
    { name: 'skill', score: skill, weight: SKILL_WEIGHT },
    { name: 'title', score: title, weight: TITLE_WEIGHT },
    { name: 'division', score: division, weight: DIVISION_WEIGHT },
  ];

  let weightedScore = 0;
  let activeWeight = 0;

  for (const signal of signals) {
    if (signal.score === null || signal.score === undefined || !Number.isFinite(signal.score)) continue;
    weightedScore += clamp01(signal.score) * signal.weight;
    activeWeight += signal.weight;
  }

  if (activeWeight <= 0) return 0;
  return clamp01(weightedScore / activeWeight);
}

// Location helpers
function haversine(lat1, lon1, lat2, lon2) {
  const aLat = toFiniteNumber(lat1);
  const aLon = toFiniteNumber(lon1);
  const bLat = toFiniteNumber(lat2);
  const bLon = toFiniteNumber(lon2);
  if (aLat === null || aLon === null || bLat === null || bLon === null) return null;

  const earthRadiusKm = 6371;
  const toRad = (deg) => deg * Math.PI / 180;
  const dLat = toRad(bLat - aLat);
  const dLon = toRad(bLon - aLon);

  const h = Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(aLat)) * Math.cos(toRad(bLat)) * Math.sin(dLon / 2) ** 2;

  return earthRadiusKm * 2 * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
}

function normalizeCity(value) {
  return normalizeText(value)
    .replace(/\b(germany|deutschland|austria|osterreich|österreich|switzerland|schweiz)\b/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function citiesMatch(cityA, cityB) {
  if (!cityA || !cityB) return false;
  const a = normalizeCity(cityA);
  const b = normalizeCity(cityB);
  if (!a || !b) return false;
  return a === b || a.includes(b) || b.includes(a);
}

function isRemoteJob(remoteType) {
  const value = normalizeText(remoteType);
  return value === 'remote' || value === 'fully remote' || value.includes('100 remote');
}

// Company extraction
function extractCompanyNameFromTitle(title) {
  if (!title) return null;

  const patterns = [
    /[—–\-]\s*(.+?)(?:\s*\(|$)/,
    /\|\s*(.+?)(?:\s*\(|$)/,
    /bei\s+(.+?)(?:\s*\(|$)/i,
    /(?:für|an|mit)\s+(.+?)(?:\s*\(|$)/i,
  ];

  for (const pattern of patterns) {
    const match = String(title).match(pattern);
    if (match && match[1]) {
      const name = match[1].trim();
      if (name.length > 2 && name.length < 100) return name;
    }
  }

  const companyWords = ['GmbH', 'AG', 'KG', 'SE', 'e.V.', 'UG', 'GbR', 'OHG'];
  const parts = String(title).split(/[—–\-|\/]/);

  for (const part of parts.reverse()) {
    const trimmed = part.trim();
    if (companyWords.some((word) => trimmed.includes(word))) return trimmed;
  }

  return null;
}

// Main matching function
async function matchCandidate(incomingData, radius, topK = DEFAULT_TOP_K) {
  const salesforceContactId = incomingData.salesforce_contact_id;
  if (!salesforceContactId) throw new Error('Missing salesforce_contact_id');

  // Read the full candidate row. This is deliberate. If Finance, Legal, IT or
  // Construction specific fields already exist in Supabase, they become part
  // of the semantic profile automatically without another code change.
  let { data: existingCandidate, error: fetchError } = await supabase
    .from('candidates')
    .select('*')
    .eq('salesforce_contact_id', salesforceContactId)
    .maybeSingle()
    .abortSignal(AbortSignal.timeout(30000));

  if (fetchError) throw new Error(`Supabase candidate fetch error: ${fetchError.message}`);

  const existing = existingCandidate || {};

  const incomingPosition = getFirstValue(incomingData, ['Position', 'position', 'current_position', 'current position', 'job_title', 'job title'], null);
  const incomingName = getFirstValue(incomingData, ['name', 'Name', 'full_name', 'full name', 'candidate_name', 'candidate name'], null);
  const incomingEmployer = getFirstValue(incomingData, ['current_employer', 'current employer', 'current_company', 'current company', 'employer'], null);
  const incomingSummary = getFirstValue(incomingData, ['summary', 'Summary', 'candidate_summary', 'candidate summary', 'profile_summary', 'profile summary', 'professional_summary', 'professional summary'], null);
  const incomingSalary = getFirstValue(incomingData, ['salary_expectation', 'salary expectation', 'expected_salary', 'expected salary'], null);
  const incomingLanguage = getFirstValue(incomingData, ['language', 'languages', 'Language', 'Languages'], null);
  const incomingSeniority = getFirstValue(incomingData, ['seniority_level', 'seniority level', 'seniority'], null);
  const incomingRemote = getFirstValue(incomingData, ['remote_preference', 'remote preference', 'remote'], null);
  const incomingLocation = getFirstValue(incomingData, ['location', 'Location', 'city', 'City'], null);
  const incomingLat = getFirstValue(incomingData, ['location_lat', 'latitude', 'lat'], null);
  const incomingLng = getFirstValue(incomingData, ['location_lng', 'longitude', 'lng', 'lon'], null);

  const resolvedPosition = String(preferIncoming(incomingPosition, existing.Position, '') || '').trim();
  const resolvedName = String(preferIncoming(incomingName, existing.name, '') || '').trim();
  const resolvedCurrentEmployer = preferIncoming(incomingEmployer, existing.current_employer, null);
  const resolvedSummary = preferIncoming(incomingSummary, existing.summary, null);
  const resolvedSalary = preferIncoming(incomingSalary, existing.salary_expectation, null);
  const resolvedLanguage = preferIncoming(incomingLanguage, existing.language, null);
  const resolvedSeniority = preferIncoming(incomingSeniority, existing.seniority_level, null);
  const resolvedRemotePreference = preferIncoming(incomingRemote, existing.remote_preference, null);
  const resolvedLocation = preferIncoming(incomingLocation, existing.location, null);
  const resolvedLocationLat = preferIncoming(incomingLat, existing.location_lat, null);
  const resolvedLocationLng = preferIncoming(incomingLng, existing.location_lng, null);

  const incomingSkills = parseSkillScores(incomingData.skill_scores);
  const existingSkills = parseSkillScores(existing.skill_scores);
  const resolvedSkillScores = Object.keys(incomingSkills).length > 0 ? incomingSkills : existingSkills;

  // Merge the whole incoming body with the full stored candidate row.
  // Unknown MCG and Frontsheet fields are intentionally preserved here.
  const mergedProfile = mergeProfileData(existing, incomingData);
  mergedProfile.Position = resolvedPosition;
  mergedProfile.name = resolvedName;
  mergedProfile.current_employer = resolvedCurrentEmployer;
  mergedProfile.summary = resolvedSummary;
  mergedProfile.salary_expectation = resolvedSalary;
  mergedProfile.language = resolvedLanguage;
  mergedProfile.seniority_level = resolvedSeniority;
  mergedProfile.remote_preference = resolvedRemotePreference;
  mergedProfile.location = resolvedLocation;
  mergedProfile.skill_scores = resolvedSkillScores;

  const divisionContext = [
    resolvedPosition,
    resolvedSummary,
    buildFullSourceText(mergedProfile),
  ].filter(Boolean).join('\n');

  const incomingDivision = getFirstValue(incomingData, ['Division', 'division', 'business_division', 'business division'], null);
  const resolvedDivision = normalizeDivision(
    preferIncoming(incomingDivision, preferIncoming(existing.Division, existing.division, ''), ''),
    divisionContext
  );

  mergedProfile.Division = resolvedDivision;

  console.log(`[${salesforceContactId}] Division: ${resolvedDivision}`);
  console.log(`[${salesforceContactId}] Skills: ${Object.keys(resolvedSkillScores).length}`);

  // Persist only columns known to exist in the current schema used by v5.3.
  // The full source payload is still used for matching even if the candidates
  // table does not have dedicated MCG or Frontsheet JSON columns.
  const upsertPayload = {
    skill_scores: resolvedSkillScores,
    seniority_level: resolvedSeniority,
    remote_preference: resolvedRemotePreference,
    location: resolvedLocation,
    location_lat: toFiniteNumber(resolvedLocationLat),
    location_lng: toFiniteNumber(resolvedLocationLng),
    Position: resolvedPosition || null,
    summary: resolvedSummary || null,
    salary_expectation: resolvedSalary,
    language: resolvedLanguage,
    updated_at: new Date().toISOString(),
  };

  if (resolvedCurrentEmployer !== undefined) upsertPayload.current_employer = resolvedCurrentEmployer;
  if (incomingData.is_active !== undefined) upsertPayload.is_active = incomingData.is_active;

  let candidateId;

  if (existingCandidate) {
    candidateId = existingCandidate.id;
    const { error: updateError } = await supabase
      .from('candidates')
      .update(upsertPayload)
      .eq('id', candidateId)
      .abortSignal(AbortSignal.timeout(30000));

    if (updateError) throw new Error(`Supabase candidate update error: ${updateError.message}`);
  } else {
    const { data: newCandidate, error: insertError } = await supabase
      .from('candidates')
      .insert({
        salesforce_contact_id: salesforceContactId,
        ...upsertPayload,
        created_at: new Date().toISOString(),
      })
      .select('id')
      .single()
      .abortSignal(AbortSignal.timeout(30000));

    if (insertError) throw new Error(`Supabase candidate insert error: ${insertError.message}`);
    candidateId = newCandidate.id;
  }

  // Always rebuild from the complete effective profile.
  // This fixes the v5.3 behavior where only a summary change triggered a new
  // embedding and changes in Frontsheet or other MCG fields could be ignored.
  const profileEmbeddingResult = await embedCandidateProfile(mergedProfile);
  const skillEmbedding = profileEmbeddingResult.embedding;

  const { error: embeddingUpdateError } = await supabase
    .from('candidates')
    .update({
      skill_embedding: skillEmbedding,
      updated_at: new Date().toISOString(),
    })
    .eq('id', candidateId)
    .abortSignal(AbortSignal.timeout(30000));

  if (embeddingUpdateError) {
    throw new Error(`Supabase embedding update error: ${embeddingUpdateError.message}`);
  }

  console.log(
    `[${salesforceContactId}] Semantic profile: ${profileEmbeddingResult.characters} chars, ` +
    `${profileEmbeddingResult.chunks} chunks, full source=${profileEmbeddingResult.hasFullSourceContent}, ` +
    `truncated=${profileEmbeddingResult.truncated}`
  );

  const [jobs, companies] = await Promise.all([getCachedJobs(), getCompaniesMap()]);
  const config = DIVISION_CONFIG[resolvedDivision] || DIVISION_CONFIG.default;

  const startMatch = Date.now();
  const matches = [];
  const seenJobIds = new Set();
  const seenJobKey = new Set();

  let skippedGarbage = 0;
  let skippedDuplicate = 0;
  let skippedLowSemantic = 0;
  let skippedLowSkill = 0;
  let skippedLocation = 0;
  let missingGeo = 0;

  const candidateLat = toFiniteNumber(resolvedLocationLat);
  const candidateLng = toFiniteNumber(resolvedLocationLng);
  const parsedRadius = Number(radius);
  const effectiveRadius = Number.isFinite(parsedRadius) ? Math.max(0, parsedRadius) : DEFAULT_RADIUS_KM;

  for (const job of jobs) {
    if (isGarbageTitle(job.title)) {
      skippedGarbage += 1;
      continue;
    }

    if (seenJobIds.has(job.id)) {
      skippedDuplicate += 1;
      continue;
    }
    seenJobIds.add(job.id);

    const locationKey = normalizeText(job.location || '');
    const dedupKey = `${job.company_id || ''}|${normalizeText(job.title || '')}|${locationKey}`;
    if (seenJobKey.has(dedupKey)) {
      skippedDuplicate += 1;
      continue;
    }
    seenJobKey.add(dedupKey);

    // Semantic profile signal. No title or skill gate is allowed before this.
    const semanticSimilarity = cosineSimilarity(skillEmbedding, job.skill_embedding);
    if (semanticSimilarity < MIN_SEMANTIC_SIMILARITY) {
      skippedLowSemantic += 1;
      continue;
    }

    const titleMatch = calculateTitleMatch(resolvedPosition, job.title);
    const skillMatch = calculateSkillOverlap(resolvedSkillScores, job.structured_skills);

    if (
      config.requireSkillMatch &&
      skillMatch.available &&
      skillMatch.score <= 0
    ) {
      skippedLowSkill += 1;
      continue;
    }

    if (
      skillMatch.available &&
      Number.isFinite(config.minSkillOverlap) &&
      skillMatch.score < config.minSkillOverlap
    ) {
      skippedLowSkill += 1;
      continue;
    }

    const jobDivisionText = [job.title, ...parseStructuredSkills(job.structured_skills)].join(' ');
    const jobDivision = inferDivisionFromText(jobDivisionText);
    const divisionScore = divisionCompatibility(resolvedDivision, jobDivision);

    let finalScore = calculateDynamicFinalScore({
      semantic: semanticSimilarity,
      skill: skillMatch.available ? skillMatch.score : null,
      title: titleMatch.available ? titleMatch.score : null,
      division: divisionScore,
    });

    if (titleMatch.exactPhrase) finalScore = clamp01(finalScore + TITLE_BONUS);
    if (finalScore <= 0) continue;

    // Location filter
    const jobLat = toFiniteNumber(job.location_lat);
    const jobLng = toFiniteNumber(job.location_lng);
    const remote = isRemoteJob(job.remote_type);
    let distance = null;
    let includeByLocation = false;

    if (candidateLat !== null && candidateLng !== null && jobLat !== null && jobLng !== null) {
      distance = haversine(candidateLat, candidateLng, jobLat, jobLng);
    } else {
      missingGeo += 1;
    }

    const candidateLocationKnown = (candidateLat !== null && candidateLng !== null) || Boolean(resolvedLocation);

    if (!candidateLocationKnown) {
      // If the candidate has no usable location data, do not suppress otherwise
      // strong semantic matches. Location filtering only applies when candidate
      // location is actually known.
      includeByLocation = true;
    } else if (remote) {
      includeByLocation = true;
    } else if (distance !== null && distance <= effectiveRadius) {
      includeByLocation = true;
    } else if (distance === null && resolvedLocation && job.location && citiesMatch(resolvedLocation, job.location)) {
      includeByLocation = true;
    } else if (distance === null && !resolvedLocation && !job.location) {
      includeByLocation = true;
    }

    if (!includeByLocation) {
      skippedLocation += 1;
      continue;
    }

    // Company information
    let companyName = job.company_name || null;
    let careerPageUrl = null;
    let lastCrawledAt = null;
    let crawlStatus = null;

    if (job.company_id) {
      const info = companies.byId.get(job.company_id);
      if (info) {
        companyName = companyName || info.Name || null;
        careerPageUrl = info.career_page_url || null;
        lastCrawledAt = info.last_crawled_at || null;
        crawlStatus = info.crawl_status || null;
      }
    }

    if (!companyName) {
      const extracted = extractCompanyNameFromTitle(job.title);
      if (extracted) {
        const key = extracted.toLowerCase().trim();
        let matchedCompany = companies.byName.get(key);

        if (!matchedCompany) {
          const clean = key.replace(/\s*(gmbh|ag|kg|se|e\.v\.|ug|gbr|ohg)\s*$/i, '').trim();
          if (clean !== key) matchedCompany = companies.byName.get(clean);
        }

        if (matchedCompany) {
          companyName = matchedCompany.Name;
          careerPageUrl = careerPageUrl || matchedCompany.career_page_url || null;
          lastCrawledAt = lastCrawledAt || matchedCompany.last_crawled_at || null;
          crawlStatus = crawlStatus || matchedCompany.crawl_status || null;
        }
      }
    }

    const structuredSkills = parseStructuredSkills(job.structured_skills);

    matches.push({
      job_id: job.id,
      job_title: job.title || 'Untitled',
      company_id: job.company_id,
      company_name: companyName,
      apply_url: job.apply_url || null,
      career_page_url: careerPageUrl,
      last_crawled_at: lastCrawledAt,
      crawl_status: crawlStatus,
      location: job.location,
      remote_type: job.remote_type,
      seniority_level: job.seniority_level,
      top_skills: structuredSkills.slice(0, 5).join(', '),
      matched_candidate_skills: skillMatch.matchedSkills.slice(0, 10),
      candidate_division: resolvedDivision,
      job_division: jobDivision,
      similarity_score: semanticSimilarity,
      semantic_score: semanticSimilarity,
      skill_overlap: skillMatch.available ? skillMatch.score : null,
      title_match: titleMatch.available ? titleMatch.score : null,
      division_match: divisionScore,
      final_score: Math.round(finalScore * 10000) / 100,
      location_distance_km: distance === null ? null : Math.round(distance * 10) / 10,
    });
  }

  console.log(
    `[${salesforceContactId}] Skipped jobs: ` +
    `${skippedGarbage} garbage, ${skippedDuplicate} duplicate, ` +
    `${skippedLowSemantic} low semantic, ${skippedLowSkill} low skill, ${skippedLocation} location`
  );
  if (missingGeo > 0) console.warn(`[${salesforceContactId}] ${missingGeo} jobs had incomplete coordinates.`);

  matches.sort((a, b) => {
    if (b.final_score !== a.final_score) return b.final_score - a.final_score;
    if (b.semantic_score !== a.semantic_score) return b.semantic_score - a.semantic_score;
    return (a.location_distance_km ?? Infinity) - (b.location_distance_km ?? Infinity);
  });

  const safeTopK = Math.min(Math.max(1, Number.parseInt(topK, 10) || DEFAULT_TOP_K), MAX_TOP_K);
  const topMatches = matches.slice(0, safeTopK);

  console.log(
    `[${salesforceContactId}] Matching loop took ${Date.now() - startMatch}ms for ` +
    `${jobs.length} jobs and returned ${topMatches.length} matches.`
  );

  // Always delete old matches first. v5.3 left stale matches in the database
  // whenever a new run returned zero matches.
  const { error: deleteMatchesError } = await supabase
    .from('matches')
    .delete()
    .eq('candidate_id', candidateId)
    .abortSignal(AbortSignal.timeout(30000));

  if (deleteMatchesError) {
    throw new Error(`Supabase old match deletion error: ${deleteMatchesError.message}`);
  }

  if (topMatches.length > 0) {
    // NOTE: this payload is shaped to match public.matches exactly:
    //   - location_distance_km is TEXT in the DB -> stringify the number.
    //   - top_skills is TEXT[] in the DB -> send an array, not a joined string.
    //   - the apply URL column is literally named "Apply URL" -> quoted key.
    //   - career_page_url / last_crawled_at / crawl_status are NOT columns
    //     on this table, so they are intentionally left out of the insert
    //     (they still exist on the in-memory `match` object / API response).
    const rows = topMatches.map((match) => ({
      candidate_id: candidateId,
      job_id: match.job_id,
      similarity_score: Math.round(match.similarity_score * 10000) / 100,
      location_distance_km: match.location_distance_km === null || match.location_distance_km === undefined
        ? null
        : String(match.location_distance_km),
      final_score: match.final_score,
      company_name: match.company_name,
      job_title: match.job_title,
      job_location: match.location,
      remote_type: match.remote_type,
      seniority_level: match.seniority_level,
      top_skills: match.top_skills
        ? match.top_skills.split(',').map((skill) => skill.trim()).filter(Boolean)
        : [],
      'Apply URL': match.apply_url || null,
      created_at: new Date().toISOString(),
    }));

    const { error: insertMatchesError } = await supabase
      .from('matches')
      .insert(rows)
      .abortSignal(AbortSignal.timeout(30000));

    if (insertMatchesError) {
      throw new Error(`Supabase match insert error: ${insertMatchesError.message}`);
    }
  }

  return {
    candidate_id: candidateId,
    salesforce_contact_id: salesforceContactId,
    candidate_name: resolvedName,
    position_used: resolvedPosition || null,
    division_used: resolvedDivision || null,
    semantic_profile: {
      characters_used: profileEmbeddingResult.characters,
      chunks_embedded: profileEmbeddingResult.chunks,
      full_source_content_included: profileEmbeddingResult.hasFullSourceContent,
      truncated: profileEmbeddingResult.truncated,
    },
    scoring: {
      semantic_weight: SEMANTIC_WEIGHT,
      skill_weight: SKILL_WEIGHT,
      title_weight: TITLE_WEIGHT,
      division_weight: DIVISION_WEIGHT,
      dynamic_weight_redistribution: true,
    },
    matches_count: topMatches.length,
    matches: topMatches,
  };
}

// Webhook handler
app.post('/webhook/match-candidate', authenticateMatchWebhook, async (req, res) => {
  const requestId = uuidv4();
  const startTime = Date.now();
  console.log(`[${requestId}] Match request received`);

  try {
    if (!req.body || typeof req.body !== 'object' || Array.isArray(req.body)) {
      throw new Error('Request body must be a JSON object');
    }

    const incoming = { ...req.body };
    const salesforceContactId = incoming.salesforce_contact_id;
    if (!salesforceContactId) throw new Error('Missing salesforce_contact_id');

    const parsedSkills = parseSkillScores(incoming.skill_scores);
    if (Object.keys(parsedSkills).length === 0) {
      console.warn(`[${requestId}] skill_scores is empty. Semantic full profile matching will still run.`);
    }

    const parsedRadius = Number(incoming.radius ?? DEFAULT_RADIUS_KM);
    const radius = Number.isFinite(parsedRadius) ? Math.max(0, parsedRadius) : DEFAULT_RADIUS_KM;
    const topK = Math.min(
      Math.max(1, Number.parseInt(incoming.top_k ?? DEFAULT_TOP_K, 10) || DEFAULT_TOP_K),
      MAX_TOP_K
    );

    // Keep the whole body for matching. Only radius and top_k are operational
    // controls and are ignored by the semantic flattener automatically.
    const result = await matchSemaphore.run(() => matchCandidate(incoming, radius, topK));

    const elapsed = Date.now() - startTime;
    console.log(`[${requestId}] Completed in ${elapsed}ms with ${result.matches_count} matches`);

    res.status(200).json({
      success: true,
      request_id: requestId,
      ...result,
      elapsed_ms: elapsed,
      message: `Successfully matched candidate with ${result.matches_count} job(s)`,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    const elapsed = Date.now() - startTime;
    console.error(`[${requestId}] Error after ${elapsed}ms: ${error.stack || error.message}`);

    res.status(500).json({
      success: false,
      request_id: requestId,
      error: error.message,
      elapsed_ms: elapsed,
      timestamp: new Date().toISOString(),
    });
  }
});

app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'ok',
    version: '6.0.0',
    cache_warm: Boolean(cachedJobs),
    jobs_cached: cachedJobs ? cachedJobs.length : 0,
    profile_embedding: {
      model: VOYAGE_MODEL,
      chunk_chars: EMBED_CHUNK_CHARS,
      batch_size: EMBED_BATCH_SIZE,
      max_profile_chars: MAX_PROFILE_CHARS === 0 ? 'unlimited' : MAX_PROFILE_CHARS,
    },
    divisions: ['IT Consulting', 'Construction', 'Business', 'Finance', 'Legal'],
    timestamp: new Date().toISOString(),
  });
});

// Start server
const SSL_KEY_PATH = process.env.SSL_KEY || '/etc/ssl/private/server.key';
const SSL_CERT_PATH = process.env.SSL_CERT || '/etc/ssl/certs/server.crt';

let useHttps = false;
try {
  useHttps = fs.existsSync(SSL_KEY_PATH) && fs.existsSync(SSL_CERT_PATH);
} catch {
  useHttps = false;
}

async function startServer() {
  const warmed = await warmupCache();
  if (!warmed) {
    console.error('Cache warmup failed. Exiting so the process manager can restart the service.');
    process.exit(1);
  }

  const server = useHttps
    ? https.createServer(
      {
        key: fs.readFileSync(SSL_KEY_PATH),
        cert: fs.readFileSync(SSL_CERT_PATH),
      },
      app
    )
    : app;

  server.requestTimeout = TIMEOUT_MS;
  server.headersTimeout = Math.max(65000, Math.min(TIMEOUT_MS, 120000));

  server.listen(PORT, HOST, () => {
    console.log(`${useHttps ? 'HTTPS' : 'HTTP'} matching engine v6.0.0 running on ${HOST}:${PORT}`);
    console.log('POST /webhook/match-candidate');
    console.log('GET  /health');
    console.log(`Concurrency: ${MAX_CONCURRENT_MATCHES}`);
    console.log(`Cache TTL: ${JOB_CACHE_TTL_MS / 1000}s`);
    console.log(`Request timeout: ${TIMEOUT_MS / 1000}s`);
    console.log(
      `Weights: semantic ${SEMANTIC_WEIGHT}, skills ${SKILL_WEIGHT}, ` +
      `title ${TITLE_WEIGHT}, division ${DIVISION_WEIGHT}`
    );
    console.log(
      `Full profile embedding: chunk size ${EMBED_CHUNK_CHARS} chars, ` +
      `batch size ${EMBED_BATCH_SIZE}, max profile chars ${MAX_PROFILE_CHARS || 'unlimited'}`
    );
  });
}

startServer().catch((error) => {
  console.error('Fatal startup error:', error);
  process.exit(1);
});
