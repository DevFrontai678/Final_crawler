#!/usr/bin/env node
/**
 * ═══════════════════════════════════════════════════════════════════════
 *  PRODUCTION MATCHING ENGINE – v5.2
 * ═══════════════════════════════════════════════════════════════════════
 *
 *  Weights:
 *    - Embedding (profile): 40%
 *    - Skills: 30%
 *    - Title match: 30%
 *
 *  All jobs passing filters are considered and scored.
 *  No minimum skill overlap required (minSkillOverlap = 0).
 */

const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const axios = require('axios');
const fs = require('fs');
const https = require('https');
const { v4: uuidv4 } = require('uuid');
require('dotenv').config();

// ─── Configuration ──────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
const HOST = process.env.HOST || '0.0.0.0';
const MAX_CONCURRENT_MATCHES = parseInt(process.env.MAX_CONCURRENT_MATCHES || '20', 10);
const JOB_CACHE_TTL_MS = parseInt(process.env.JOB_CACHE_TTL_MS || '600000', 10);
const TIMEOUT_MS = parseInt(process.env.TIMEOUT_MS || '120000', 10);

// ─── Weights (sum = 1.0) ──────────────────────────────────────────────
const SIMILARITY_WEIGHT = 0.40;
const SKILL_WEIGHT = 0.30;
const TITLE_WEIGHT = 0.30;          // ⬆️ increased
const TITLE_BONUS = 0.05;

// ─── Division‑specific config (only skillWeight matters) ──────────────
const DIVISION_CONFIG = {
  IT: {
    minSkillOverlap: 0.0,
    skillWeight: SKILL_WEIGHT,
    requireSkillMatch: false,
  },
  Construction: {
    minSkillOverlap: 0.0,
    skillWeight: SKILL_WEIGHT,
    requireSkillMatch: false,
  },
  Legal: {
    minSkillOverlap: 0.0,
    skillWeight: SKILL_WEIGHT,
    requireSkillMatch: false,
  },
  Accounting: {
    minSkillOverlap: 0.0,
    skillWeight: SKILL_WEIGHT,
    requireSkillMatch: false,
  },
  default: {
    minSkillOverlap: 0.0,
    skillWeight: SKILL_WEIGHT,
    requireSkillMatch: false,
  },
};

// ─── Garbage patterns (unchanged) ──────────────────────────────────────
const GARBAGE_TITLE_PATTERNS = [
  /karriere/i, /career/i, /great to have you here/i, /super, dass du hier bist/i,
  /wir suchen dich/i, /initiativbewerbung/i, /are you looking for new challenges/i,
  /jobs at/i, /dein traumjob/i, /willkommen in ihrer zukunft/i,
  /bewerbungsprozess/i, /neustart/i, /karriere -/i, /career -/i,
  /stellenangebote/i, /job offers/i, /join/i, /career opportunities/i,
  /work with us/i, /come join us/i, /offene stellen/i, /jobs/i,
  /stellenangebot/i, /karriere bei/i, /join us/i, /open positions/i,
  /join our team/i, /careers/i,
  /bewerben\.\s*begegnen\.\s*beginnen\./i, /see beyond\.\s*secure beyond\./i,
  /passioniert,\s*eigenverantwortlich/i, /wir freuen uns auf ihre anfrage/i,
  /menschlichkeit pragmatismus nachhaltigkeit/i, /grüne baustelle/i,
  /dein weg zu uns/i, /proactive application/i, /online-bewerbung/i,
  /referral/i, /proactive application \/ referral/i,
  /jobportal/i, /jobsportal/i, /job board/i, /job portal/i,
  /stellenmarkt/i, /jobmarkt/i,
  /fachabteilungen\s*&\s*organisation/i, /regionaldirektionen/i,
  /geschäftsstellen/i, /partner-\/mitgliedschaften/i,
  /kontakte\s*&\s*standorte/i, /veranstaltungskauffrau/i,
  /^[a-z]{3,12}$/i,
  /^[a-zäöüß]+\s+[a-zäöüß]+/i,
  /^[a-zäöüß]{2,8}$/i,
];

function isGarbageTitle(title) {
  if (!title) return true;
  const trimmed = title.trim();
  if (trimmed.length < 4) return true;
  return GARBAGE_TITLE_PATTERNS.some(p => p.test(trimmed));
}

// ─── Stopwords (unchanged) ──────────────────────────────────────────
const STOPWORDS = new Set([
  'der', 'die', 'das', 'und', 'oder', 'für', 'mit', 'von', 'zu', 'im', 'am',
  'als', 'auch', 'auf', 'bei', 'durch', 'in', 'nach', 'um', 'über', 'unter',
  'für', 'ohne', 'mit', 'von', 'zu', 'des', 'dem', 'den', 'ein', 'eine',
  'eines', 'einer', 'einem', 'einen', 'der', 'die', 'das', 'und', 'oder',
  'the', 'and', 'or', 'for', 'with', 'without', 'of', 'to', 'from', 'by',
  'at', 'on', 'into', 'through', 'during', 'including', 'per', 'via',
]);

function extractKeywords(text) {
  if (!text) return [];
  const words = text.toLowerCase().split(/[\s\-_\/,()&+]+/);
  return words.filter(w => w.length > 2 && !STOPWORDS.has(w));
}

// ─── Division inference ──────────────────────────────────────────────
function inferDivision(position, name) {
  const text = (position + ' ' + name).toLowerCase();
  if (/it|cloud|cyber|software|netzwerk|system|admin|devops|developer|infrastructure|security|server|linux|windows|coding|programming/.test(text)) {
    return 'IT';
  } else if (/bau|construction|architekt|ingenieur|building|planung|immobilie|hochbau|tiefbau|architektur/.test(text)) {
    return 'Construction';
  } else if (/legal|recht|anwalt|jurist|compliance|steuer|buchhaltung|accounting|audit|tax|finance|controller/.test(text)) {
    return 'Legal';
  }
  return 'default';
}

// ─── Express app ──────────────────────────────────────────────────────
const app = express();
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

// ─── Supabase ────────────────────────────────────────────────────────
const supabaseUrl = process.env.SUPABASE_URL;
const serviceKey = process.env.SUPABASE_SERVICE_KEY;
if (!supabaseUrl || !serviceKey) {
  console.error('❌ SUPABASE_URL or SUPABASE_SERVICE_KEY missing in .env');
  process.exit(1);
}
const supabase = createClient(supabaseUrl, serviceKey, { realtime: { transport: ws } });

// ─── Voyage AI ──────────────────────────────────────────────────────
const VOYAGE_API_KEY = process.env.VOYAGE_API_KEY;
const VOYAGE_URL = 'https://api.voyageai.com/v1/embeddings';
const VOYAGE_MODEL = process.env.VOYAGE_MODEL || 'voyage-3-large';

// ─── Semaphore ──────────────────────────────────────────────────────
class Semaphore {
  constructor(limit) {
    this.limit = limit;
    this.running = 0;
    this.queue = [];
  }
  async acquire() {
    if (this.running < this.limit) {
      this.running++;
      return;
    }
    return new Promise((resolve) => this.queue.push(resolve));
  }
  release() {
    if (this.queue.length > 0) {
      this.queue.shift()();
    } else {
      this.running--;
    }
  }
  async run(fn) {
    await this.acquire();
    try { return await fn(); } finally { this.release(); }
  }
}
const matchSemaphore = new Semaphore(MAX_CONCURRENT_MATCHES);

// ─── Helper: set statement_timeout ──────────────────────────────
let timeoutWarned = false;
async function setStatementTimeout(seconds = 300) {
  try {
    await supabase.rpc('set_config', {
      setting_name: 'statement_timeout',
      setting_value: `${seconds}s`,
      is_local: true,
    });
  } catch (e) {
    if (!timeoutWarned) {
      console.warn(`⚠️ set_config RPC not available (ignored): ${e.message}`);
      timeoutWarned = true;
    }
  }
}

// ─── Job Cache ──────────────────────────────────────────────────────
let cachedJobs = null;
let jobsCacheTimestamp = 0;
let jobsFetchInFlight = null;

async function fetchJobsFromSupabase() {
  const start = Date.now();
  console.log(`⏳ Fetching jobs (keyset, PAGE_SIZE=500)...`);
  await setStatementTimeout(300);

  const jobs = [];
  const PAGE_SIZE = 500;
  let lastId = null;
  let page = 0;

  while (true) {
    const startPage = Date.now();
    console.log(`   📄 Page ${page + 1} (lastId: ${lastId || 'start'})...`);

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
      console.error(`❌ Supabase query error on page ${page + 1}:`, error);
      throw error;
    }
    if (!data || data.length === 0) break;
    jobs.push(...data);
    lastId = data[data.length - 1].id;
    console.log(`   ✅ Page ${page + 1} loaded ${data.length} jobs in ${Date.now() - startPage}ms`);
    page++;
    if (data.length < PAGE_SIZE) break;
  }

  console.log(`✅ Loaded ${jobs.length} jobs in ${Date.now() - start}ms`);
  return jobs;
}

async function getCachedJobs() {
  const now = Date.now();
  if (cachedJobs && (now - jobsCacheTimestamp) < JOB_CACHE_TTL_MS) {
    return cachedJobs;
  }
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

// ─── Companies Cache ──────────────────────────────────────────────
let companiesMap = null;
let companiesCacheTimestamp = 0;
let companiesFetchInFlight = null;

async function fetchCompaniesFromSupabase() {
  const start = Date.now();
  console.log(`⏳ Fetching companies...`);
  const { data, error } = await supabase
    .from('companies')
    .select('"Id", "Name", "detected_career_url", "last_crawled_at", "crawl_status"')
    .abortSignal(AbortSignal.timeout(60000));
  if (error) throw error;

  const byId = new Map();
  const byName = new Map();
  data.forEach(c => {
    byId.set(c.Id, {
      Name: c.Name,
      career_page_url: c.detected_career_url || null,
      last_crawled_at: c.last_crawled_at || null,
      crawl_status: c.crawl_status || null,
    });
    const key = c.Name.toLowerCase().trim();
    if (!byName.has(key)) byName.set(key, c);
  });
  console.log(`✅ Loaded ${data.length} companies in ${Date.now() - start}ms`);
  return { byId, byName };
}

async function getCompaniesMap() {
  const now = Date.now();
  if (companiesMap && (now - companiesCacheTimestamp) < JOB_CACHE_TTL_MS) {
    return companiesMap;
  }
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

// ─── Warm‑up ──────────────────────────────────────────────────────────
async function warmupCache(retries = 3) {
  console.log('🔥 Warming up cache...');
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      await getCachedJobs();
      await getCompaniesMap();
      console.log('✅ Cache warm‑up complete.');
      return true;
    } catch (err) {
      console.error(`❌ Cache warm‑up attempt ${attempt} failed:`, err.message);
      if (attempt < retries) {
        console.log(`⏳ Retrying in 5s...`);
        await new Promise(r => setTimeout(r, 5000));
      }
    }
  }
  return false;
}

// ─── Helpers (unchanged) ──────────────────────────────────────────
function parseEmbedding(embedding) {
  if (!embedding) return null;
  if (Array.isArray(embedding)) return embedding;
  if (typeof embedding === 'string') {
    try { return JSON.parse(embedding); } catch { return null; }
  }
  return null;
}

function parseSkillScores(raw) {
  if (!raw) return {};
  let value = raw;
  if (typeof value === 'string') {
    try { value = JSON.parse(value); } catch { return {}; }
  }
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return {};
  }
  return value;
}

function cosineSimilarity(vecA, vecB) {
  const a = parseEmbedding(vecA);
  const b = parseEmbedding(vecB);
  if (!a || !b || a.length !== b.length) return 0;
  let dot = 0, magA = 0, magB = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    magA += a[i] * a[i];
    magB += b[i] * b[i];
  }
  if (magA === 0 || magB === 0) return 0;
  const sim = dot / (Math.sqrt(magA) * Math.sqrt(magB));
  return isNaN(sim) ? 0 : Math.max(0, Math.min(sim, 1));
}

function haversine(lat1, lon1, lat2, lon2) {
  if (!lat1 || !lon1 || !lat2 || !lon2) return Infinity;
  const R = 6371;
  const toRad = deg => deg * Math.PI / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a = Math.sin(dLat/2) ** 2 +
            Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
            Math.sin(dLon/2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function citiesMatch(cityA, cityB) {
  if (!cityA || !cityB) return false;
  const a = cityA.toLowerCase().trim();
  const b = cityB.toLowerCase().trim();
  return a === b || a.includes(b) || b.includes(a);
}

function candidateToText(candidate) {
  const parts = [];
  if (candidate.Position) parts.push(`Position: ${candidate.Position}`);
  if (candidate.name) parts.push(`Name: ${candidate.name}`);
  if (candidate.current_employer) parts.push(`Current employer: ${candidate.current_employer}`);
  if (candidate.skill_scores && typeof candidate.skill_scores === 'object') {
    const entries = Object.entries(candidate.skill_scores);
    if (entries.length > 0) {
      parts.push(`Skills: ${entries.map(([s, score]) => `${s} (${score}/5)`).join(', ')}`);
    }
  }
  if (candidate.seniority_level) parts.push(`Seniority: ${candidate.seniority_level}`);
  if (candidate.remote_preference) parts.push(`Remote: ${candidate.remote_preference}`);
  if (candidate.location) parts.push(`Location: ${candidate.location}`);
  return parts.join('. ') || 'No data available';
}

async function embedCandidate(candidate) {
  const text = candidateToText(candidate);
  if (!text || text.length < 5) throw new Error('No valid text to embed');
  try {
    const response = await axios.post(
      VOYAGE_URL,
      { model: VOYAGE_MODEL, input: [text] },
      {
        headers: { 'Authorization': `Bearer ${VOYAGE_API_KEY}`, 'Content-Type': 'application/json' },
        timeout: 60000,
      }
    );
    const embedding = response.data?.data?.[0]?.embedding;
    if (!Array.isArray(embedding) || embedding.length === 0) {
      throw new Error('Voyage AI returned an empty or invalid embedding');
    }
    return embedding;
  } catch (err) {
    throw new Error(`Voyage AI error: ${err.message}`);
  }
}

function extractCompanyNameFromTitle(title) {
  if (!title) return null;
  const patterns = [
    /[—–\-]\s*(.+?)(?:\s*\(|$)/,
    /\|\s*(.+?)(?:\s*\(|$)/,
    /–\s*(.+?)(?:\s*\(|$)/,
    /bei\s+(.+?)(?:\s*\(|$)/i,
    /(?:für|an|mit)\s+(.+?)(?:\s*\(|$)/i,
  ];
  for (const p of patterns) {
    const m = title.match(p);
    if (m && m[1]) {
      const name = m[1].trim();
      if (name.length > 2 && name.length < 60) return name;
    }
  }
  const companyWords = ['GmbH', 'AG', 'KG', 'SE', 'e.V.', 'UG', 'GbR', 'OHG'];
  const parts = title.split(/[—–\-|\/]/);
  for (const part of parts.reverse()) {
    const trimmed = part.trim();
    if (companyWords.some(w => trimmed.includes(w))) return trimmed;
  }
  return null;
}

// ─── Main matching function ──────────────────────────────────────
async function matchCandidate(candidateData, radius, topK = 30) {
  const {
    salesforce_contact_id,
    name,
    skill_scores,
    seniority_level,
    remote_preference,
    location,
    location_lat,
    location_lng,
    Position,
    Division,
    current_employer,
    salary_expectation,
    summary,
    language,
    is_active,
  } = candidateData;

  if (!Position || !String(Position).trim()) {
    console.warn(`⚠️ [${salesforce_contact_id}] No 'Position' provided.`);
  }

  // ── 1. Fetch existing candidate ──
  let { data: existingCandidate, error: fetchError } = await supabase
    .from('candidates')
    .select('id, skill_embedding, skill_scores, current_employer, "Position"')
    .eq('salesforce_contact_id', salesforce_contact_id)
    .single()
    .abortSignal(AbortSignal.timeout(30000));

  if (fetchError && fetchError.code !== 'PGRST116') {
    throw new Error(`Supabase fetch error: ${fetchError.message}`);
  }

  const resolvedPosition = (Position && String(Position).trim()) || existingCandidate?.Position || '';
  const resolvedName = (name && String(name).trim()) || '';
  const resolvedCurrentEmployer = current_employer || existingCandidate?.current_employer || null;
  const resolvedSkillScores = Object.keys(parseSkillScores(skill_scores)).length > 0
    ? parseSkillScores(skill_scores)
    : parseSkillScores(existingCandidate?.skill_scores);

  let resolvedDivision = (Division && String(Division).trim()) || null;
  if (!resolvedDivision) {
    resolvedDivision = inferDivision(resolvedPosition, resolvedName);
    console.log(`🔍 Inferred division: ${resolvedDivision}`);
  }

  // ── 2. Upsert candidate ──
  const upsertPayload = {
    skill_scores: resolvedSkillScores,
    seniority_level,
    remote_preference,
    location,
    location_lat: location_lat || null,
    location_lng: location_lng || null,
    "Position": resolvedPosition || null,
    updated_at: new Date().toISOString(),
  };
  if (current_employer !== undefined) upsertPayload.current_employer = current_employer;
  if (salary_expectation !== undefined) upsertPayload.salary_expectation = salary_expectation;
  if (summary !== undefined) upsertPayload.summary = summary;
  if (language !== undefined) upsertPayload.language = language;
  if (is_active !== undefined) upsertPayload.is_active = is_active;

  let candidateId;
  if (existingCandidate) {
    candidateId = existingCandidate.id;
    const { error: updateError } = await supabase
      .from('candidates')
      .update(upsertPayload)
      .eq('id', candidateId)
      .abortSignal(AbortSignal.timeout(30000));
    if (updateError) throw new Error(`Supabase update error: ${updateError.message}`);
  } else {
    const { data: newCandidate, error: insertError } = await supabase
      .from('candidates')
      .insert({ salesforce_contact_id, ...upsertPayload, created_at: new Date().toISOString() })
      .select('id')
      .single()
      .abortSignal(AbortSignal.timeout(30000));
    if (insertError) throw new Error(`Supabase insert error: ${insertError.message}`);
    candidateId = newCandidate.id;
  }

  // ── 3. skill_embedding ──
  let skillEmbedding = parseEmbedding(existingCandidate?.skill_embedding);
  if (!skillEmbedding) {
    const candidateObj = {
      Position: resolvedPosition,
      name: resolvedName,
      current_employer: resolvedCurrentEmployer,
      skill_scores: resolvedSkillScores,
      seniority_level,
      remote_preference,
      location,
    };
    skillEmbedding = await embedCandidate(candidateObj);
    await supabase
      .from('candidates')
      .update({ skill_embedding: skillEmbedding, updated_at: new Date().toISOString() })
      .eq('id', candidateId)
      .abortSignal(AbortSignal.timeout(30000));
  }
  if (!skillEmbedding || !Array.isArray(skillEmbedding) || skillEmbedding.length === 0) {
    throw new Error('skill_embedding is required');
  }

  // ── 4. Fetch jobs & companies ──
  const [jobs, companies] = await Promise.all([getCachedJobs(), getCompaniesMap()]);

  // ── 5. Division config ──
  const config = DIVISION_CONFIG[resolvedDivision] || DIVISION_CONFIG['default'];
  console.log(`📊 Config: ${resolvedDivision} (minOverlap=${config.minSkillOverlap}, skillWeight=${config.skillWeight})`);

  // ── 6. Candidate data ──
  const candidateSkills = new Set(Object.keys(resolvedSkillScores).map(s => s.toLowerCase().trim()));
  const positionKeywords = extractKeywords(resolvedPosition);
  const nameKeywords = extractKeywords(resolvedName);

  console.log(`🔍 Candidate skills (${candidateSkills.size}): ${Array.from(candidateSkills).slice(0, 10).join(', ')}`);

  let totalCandidateScore = 0;
  for (const score of Object.values(resolvedSkillScores)) {
    totalCandidateScore += Number(score) || 0;
  }

  // ── 7. Matching loop ──
  const startMatch = Date.now();
  const matches = [];
  const seenJobIds = new Set();
  const seenJobKey = new Set();

  let skippedNoCompany = 0, skippedGarbage = 0, skippedDup = 0, skippedNoSkill = 0,
      skippedLowOverlap = 0, skippedTitleMismatch = 0, missingGeo = 0;

  for (const job of jobs) {
    if (!job.company_name || job.company_name.trim() === '') { skippedNoCompany++; continue; }
    if (isGarbageTitle(job.title)) { skippedGarbage++; continue; }
    if (seenJobIds.has(job.id)) { skippedDup++; continue; }
    seenJobIds.add(job.id);
    const locationKey = job.location ? job.location.trim().toLowerCase() : '';
    const dedupKey = `${job.company_id}|${job.title?.trim().toLowerCase()}|${locationKey}`;
    if (seenJobKey.has(dedupKey)) { skippedDup++; continue; }
    seenJobKey.add(dedupKey);

    // Title match
    let titleScore = 0;
    if (positionKeywords.length > 0 && job.title) {
      const titleLower = job.title.toLowerCase();
      let matchCount = 0;
      for (const kw of positionKeywords) if (titleLower.includes(kw)) matchCount++;
      titleScore = Math.min(matchCount / positionKeywords.length, 1);
    } else if (nameKeywords.length > 0 && job.title) {
      const titleLower = job.title.toLowerCase();
      let matchCount = 0;
      for (const kw of nameKeywords) if (titleLower.includes(kw)) matchCount++;
      titleScore = Math.min(matchCount / nameKeywords.length, 1);
    }

    if (candidateSkills.size === 0 && titleScore === 0) {
      skippedTitleMismatch++;
      continue;
    }

    // Skill overlap
    const jobStructuredSkills = job.structured_skills || [];
    const jobSkillSet = new Set(jobStructuredSkills.map(s => s.toLowerCase().trim()));

    let matchedWeightedScore = 0;
    if (candidateSkills.size > 0 && jobSkillSet.size > 0) {
      for (const [skill, score] of Object.entries(resolvedSkillScores)) {
        if (jobSkillSet.has(skill.toLowerCase().trim())) {
          matchedWeightedScore += Number(score) || 0;
        }
      }
    }

    let skillMatchFound = matchedWeightedScore > 0;
    if (config.requireSkillMatch && candidateSkills.size > 0 && !skillMatchFound) {
      skippedNoSkill++;
      continue;
    }
    if (candidateSkills.size > 0 && jobSkillSet.size === 0) {
      if (config.requireSkillMatch) {
        skippedNoSkill++;
        continue;
      } else {
        matchedWeightedScore = 0;
      }
    }

    // Weighted overlap (now min is 0, so no job skipped due to low overlap)
    let overlapRatio = 0;
    if (candidateSkills.size > 0 && totalCandidateScore > 0) {
      overlapRatio = matchedWeightedScore / totalCandidateScore;
    } else if (candidateSkills.size === 0) {
      overlapRatio = 1;
    }

    if (candidateSkills.size > 0 && overlapRatio < config.minSkillOverlap) {
      skippedLowOverlap++;
      continue;
    }

    // Embedding similarity
    let sim = cosineSimilarity(skillEmbedding, job.skill_embedding);
    if (sim < 0.01) continue;

    // ─── Final score (using updated weights) ──────────────────────────
    const skillWeight = config.skillWeight; // = SKILL_WEIGHT (0.30)
    let finalScore = (sim * SIMILARITY_WEIGHT) +
                     (overlapRatio * skillWeight) +
                     (titleScore * TITLE_WEIGHT);

    // Ensure at least a tiny positive score
    finalScore = Math.max(finalScore, 0.001);

    // Title bonus (extra if exact keyword match)
    if (job.title && positionKeywords.length > 0) {
      const titleLower = job.title.toLowerCase();
      for (const kw of positionKeywords) {
        if (titleLower.includes(kw)) {
          finalScore = Math.min(finalScore + TITLE_BONUS, 1.0);
          break;
        }
      }
    }

    if (finalScore < 0.001) continue;

    // Distance
    const isRemote = job.remote_type && job.remote_type.toLowerCase() === 'remote';
    let include = false, distance = null;
    if (location_lat && location_lng && job.location_lat && job.location_lng) {
      distance = haversine(location_lat, location_lng, job.location_lat, job.location_lng);
    } else { missingGeo++; if (missingGeo <= 5) console.warn(`⚠️ Missing location for job "${job.title}"`); }

    if (isRemote) include = true;
    else if (distance !== null && distance <= radius) include = true;
    else if (distance === null && location && job.location) { if (citiesMatch(location, job.location)) include = true; }
    else if (distance === null && !location && !job.location) include = true;

    if (!include) continue;

    // Company info
    let companyName = job.company_name || null;
    let careerPageUrl = null, lastCrawledAt = null, crawlStatus = null;
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
        let matched = companies.byName.get(key);
        if (!matched) {
          const clean = key.replace(/\s*(gmbh|ag|kg|se|e\.v\.|ug|gbr|ohg)\s*$/, '').trim();
          if (clean !== key) matched = companies.byName.get(clean);
        }
        if (matched) {
          companyName = matched.Name;
          careerPageUrl = careerPageUrl || matched.detected_career_url || null;
          lastCrawledAt = lastCrawledAt || matched.last_crawled_at || null;
          crawlStatus = crawlStatus || matched.crawl_status || null;
        }
      }
    }

    const topSkills = (job.structured_skills || []).slice(0, 5).join(', ');

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
      top_skills: topSkills,
      similarity_score: sim,
      skill_overlap: overlapRatio,
      title_match: titleScore,
      final_score: Math.round(finalScore * 10000) / 100,
      location_distance_km: distance,
    });
  }

  console.log(`🔍 Skipped: ${skippedNoCompany} no company, ${skippedGarbage} garbage, ${skippedDup} duplicates, ${skippedNoSkill} no skill, ${skippedLowOverlap} low overlap, ${skippedTitleMismatch} title mismatch`);
  if (missingGeo > 0) console.warn(`⚠️ ${missingGeo} jobs missing location coordinates.`);

  matches.sort((a, b) => {
    if (b.final_score !== a.final_score) return b.final_score - a.final_score;
    return b.similarity_score - a.similarity_score;
  });

  const topMatches = matches.slice(0, topK);
  console.log(`⏱️ Matching loop took ${Date.now() - startMatch}ms for ${jobs.length} jobs → ${topMatches.length} matches`);

  // ── 8. Store matches ──
  if (topMatches.length > 0) {
    await supabase
      .from('matches')
      .delete()
      .eq('candidate_id', candidateId)
      .abortSignal(AbortSignal.timeout(30000));

    const rows = topMatches.map(m => ({
      candidate_id: candidateId,
      job_id: m.job_id,
      similarity_score: Math.round(m.similarity_score * 10000) / 100,
      location_distance_km: m.location_distance_km,
      final_score: m.final_score,
      company_name: m.company_name,
      apply_url: m.apply_url || null,
      career_page_url: m.career_page_url || null,
      job_title: m.job_title,
      job_location: m.location,
      remote_type: m.remote_type,
      seniority_level: m.seniority_level,
      top_skills: m.top_skills,
      created_at: new Date().toISOString(),
    }));
    await supabase.from('matches').insert(rows);
  }

  return {
    candidate_id: candidateId,
    salesforce_contact_id,
    candidate_name: resolvedName,
    position_used: resolvedPosition || null,
    division_used: resolvedDivision || null,
    matches_count: topMatches.length,
    matches: topMatches,
  };
}

// ─── Webhook handler ──────────────────────────────────────────────
app.post('/webhook/match-candidate', async (req, res) => {
  const requestId = uuidv4();
  const startTime = Date.now();
  console.log(`[${requestId}] Received request`);

  try {
    const {
      salesforce_contact_id,
      name,
      Position,
      Division,
      current_employer,
      salary_expectation,
      summary,
      language,
      is_active,
      skill_scores,
      seniority_level,
      remote_preference,
      location,
      location_lat,
      location_lng,
      radius = 50,
      top_k = 30,
    } = req.body;

    if (!salesforce_contact_id) throw new Error('Missing salesforce_contact_id');
    if (!skill_scores || typeof skill_scores !== 'object' || Object.keys(skill_scores).length === 0) {
      console.warn(`[${requestId}] Warning: skill_scores empty`);
    }

    const result = await matchSemaphore.run(() =>
      matchCandidate(
        {
          salesforce_contact_id,
          name,
          Position,
          Division,
          current_employer,
          salary_expectation,
          summary,
          language,
          is_active,
          skill_scores,
          seniority_level,
          remote_preference,
          location,
          location_lat,
          location_lng,
        },
        radius,
        top_k
      )
    );

    const elapsed = Date.now() - startTime;
    console.log(`[${requestId}] Completed in ${elapsed}ms, found ${result.matches_count} matches`);
    res.status(200).json({
      success: true,
      ...result,
      message: `Successfully matched candidate with ${result.matches_count} job(s)`,
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    const elapsed = Date.now() - startTime;
    console.error(`[${requestId}] Error after ${elapsed}ms: ${err.message}`);
    res.status(500).json({
      success: false,
      error: err.message,
      timestamp: new Date().toISOString(),
    });
  }
});

app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'ok',
    cache_warm: !!cachedJobs,
    jobs_cached: cachedJobs ? cachedJobs.length : 0,
    timestamp: new Date().toISOString(),
  });
});

// ─── Start server ──────────────────────────────────────────────
const SSL_KEY_PATH = process.env.SSL_KEY || '/etc/ssl/private/server.key';
const SSL_CERT_PATH = process.env.SSL_CERT || '/etc/ssl/certs/server.crt';

let useHttps = false;
try {
  if (fs.existsSync(SSL_KEY_PATH) && fs.existsSync(SSL_CERT_PATH)) useHttps = true;
} catch (e) {}

async function startServer() {
  const warmed = await warmupCache();
  if (!warmed) {
    console.error('❌ Cache warm‑up failed. Exiting for PM2 restart.');
    process.exit(1);
  }

  const server = useHttps
    ? https.createServer({ key: fs.readFileSync(SSL_KEY_PATH), cert: fs.readFileSync(SSL_CERT_PATH) }, app)
    : app;

  server.listen(PORT, HOST, () => {
    console.log(`${useHttps ? '🔒 HTTPS' : '🚀 HTTP'} server running on ${useHttps ? 'https' : 'http'}://${HOST}:${PORT}`);
    console.log(`📍 POST to /webhook/match-candidate`);
    console.log(`💚 Health check: /health`);
    console.log(`⚡ Concurrency: ${MAX_CONCURRENT_MATCHES}, Cache TTL: ${JOB_CACHE_TTL_MS/1000}s, Timeout: ${TIMEOUT_MS/1000}s`);
    console.log(`✅ Weights: Profile 40% | Skills 30% | Title 30%`);
  });
}

startServer().catch(err => {
  console.error('Fatal startup error:', err);
  process.exit(1);
});
