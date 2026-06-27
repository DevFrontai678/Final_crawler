/**
 * Matching Engine — Location-Aware Job Matching
 * 
 * 🔥 FIX: company_name always gets a name (extracted from title or 'Unknown Company'), never ID.
 */

const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
require('dotenv').config();

// ─── CONFIG ──────────────────────────────────────────────────────────────
const TOP_K = parseInt(process.argv.find(a => a.startsWith('--top='))?.split('=')[1] || 10);
const MAX_DISTANCE_KM = parseInt(process.argv.find(a => a.startsWith('--max-dist='))?.split('=')[1] || 50);
const DRY_RUN = process.argv.includes('--dry-run');
const LIMIT = parseInt(process.argv.find(a => a.startsWith('--limit='))?.split('=')[1] || 0);

// ─── SUPABASE ────────────────────────────────────────────────────────────
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY,
  { realtime: { transport: ws } }
);

// ─── HELPERS ──────────────────────────────────────────────────────────────
function parseEmbedding(embedding) {
  if (!embedding) return null;
  if (Array.isArray(embedding)) return embedding;
  if (typeof embedding === 'string') {
    try { return JSON.parse(embedding); } catch { return null; }
  }
  return null;
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
  const toRad = (deg) => deg * Math.PI / 180;
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
  if (a === b) return true;
  return a.includes(b) || b.includes(a);
}

// ─── EXTRACT COMPANY NAME FROM JOB TITLE ──────────────────────────────
function extractCompanyNameFromTitle(title) {
  if (!title) return null;
  
  // Try patterns: "Job Title — Company Name" or "Job Title | Company"
  const patterns = [
    /[—–\-]\s*(.+?)(?:\s*\(|$)/,
    /\|\s*(.+?)(?:\s*\(|$)/,
    /–\s*(.+?)(?:\s*\(|$)/,
    /bei\s+(.+?)(?:\s*\(|$)/i,
    /(?:für|an|mit)\s+(.+?)(?:\s*\(|$)/i,
  ];
  for (const pattern of patterns) {
    const match = title.match(pattern);
    if (match && match[1]) {
      let name = match[1].trim();
      if (name.length > 2 && name.length < 60) return name;
    }
  }
  
  // Check for GmbH, AG, etc.
  const companyWords = ['GmbH', 'AG', 'KG', 'SE', 'e.V.', 'UG', 'GbR', 'OHG'];
  const parts = title.split(/[—–\-|\/]/);
  for (const part of parts.reverse()) {
    const trimmed = part.trim();
    if (companyWords.some(w => trimmed.includes(w))) {
      return trimmed;
    }
  }
  
  return null;
}

// ─── FETCH ALL JOBS ──────────────────────────────────────────────────────
async function fetchAllJobs() {
  let allJobs = [];
  let page = 0;
  const PAGE_SIZE = 1000;
  let hasMore = true;

  console.log('📋 Fetching all jobs (paginated)...');

  while (hasMore) {
    const start = page * PAGE_SIZE;
    const end = start + PAGE_SIZE - 1;

    const { data: jobs, error } = await supabase
      .from('jobs')
      .select('id, title, company_id, location, location_lat, location_lng, remote_type, seniority_level, structured_skills, skill_embedding')
      .not('skill_embedding', 'is', null)
      .order('id', { ascending: true })
      .range(start, end);

    if (error) throw error;

    if (!jobs || jobs.length === 0) {
      hasMore = false;
    } else {
      for (const job of jobs) {
        job.skill_embedding = parseEmbedding(job.skill_embedding);
        if (typeof job.structured_skills === 'string') {
          try { job.structured_skills = JSON.parse(job.structured_skills); } catch { job.structured_skills = []; }
        }
        if (!Array.isArray(job.structured_skills)) job.structured_skills = [];
      }
      allJobs = allJobs.concat(jobs);
      console.log(`  Fetched page ${page + 1}: ${jobs.length} jobs (total so far: ${allJobs.length})`);
      page++;
      if (jobs.length < PAGE_SIZE) hasMore = false;
    }
  }

  console.log(`✅ Total jobs fetched: ${allJobs.length}\n`);
  return allJobs;
}

// ─── FETCH COMPANIES ──────────────────────────────────────────────────────
async function fetchCompanies() {
  const { data: companies, error } = await supabase
    .from('companies')
    .select('"Id", "Name"');
  if (error) throw error;
  const map = new Map();
  companies.forEach(c => map.set(c.Id, c.Name));
  console.log(`📋 Fetched ${map.size} companies for name mapping`);
  return map;
}

// ─── FETCH CANDIDATES ────────────────────────────────────────────────────
async function fetchCandidates() {
  let query = supabase
    .from('candidates')
    .select('id, name, salesforce_id, location, location_lat, location_lng, remote_preference, skill_embedding')
    .not('skill_embedding', 'is', null);

  if (LIMIT > 0) query = query.limit(LIMIT);

  const { data: candidates, error } = await query;
  if (error) throw error;

  for (const candidate of candidates) {
    candidate.skill_embedding = parseEmbedding(candidate.skill_embedding);
  }

  console.log(`📋 Fetched ${candidates.length} candidates\n`);
  return candidates;
}

// ─── MATCH SINGLE CANDIDATE ──────────────────────────────────────────────
function matchCandidate(candidate, jobs, companiesMap, topK) {
  const matches = [];

  for (const job of jobs) {
    if (!job.skill_embedding) continue;

    const sim = cosineSimilarity(candidate.skill_embedding, job.skill_embedding);
    if (sim < 0.01) continue;

    // ─── Distance ────────────────────────────────────────────────────────
    let distance = null;
    if (candidate.location_lat && candidate.location_lng && job.location_lat && job.location_lng) {
      distance = haversine(
        candidate.location_lat, candidate.location_lng,
        job.location_lat, job.location_lng
      );
    }

    // ─── Location Penalty ────────────────────────────────────────────────
    let penalty = 0;
    const isRemote = job.remote_type && job.remote_type.toLowerCase() === 'remote';

    if (!isRemote) {
      if (distance !== null && distance > MAX_DISTANCE_KM && isFinite(distance)) {
        const extra = distance - MAX_DISTANCE_KM;
        penalty = Math.min(extra / 10 * 0.02, 0.3);
      } else if (candidate.location && job.location) {
        if (!citiesMatch(candidate.location, job.location)) {
          penalty = 0.1;
        }
      }
    }

    const finalScore = Math.max(0, sim - penalty);

    // ─── Company Name ────────────────────────────────────────────────────
    // 🔥 FIX: Always try to get a name — never use ID.
    let companyName = companiesMap.get(job.company_id);
    if (!companyName) {
      companyName = extractCompanyNameFromTitle(job.title);
    }
    if (!companyName) {
      companyName = 'Unknown Company';
    }

    // ─── Top Skills ─────────────────────────────────────────────────────
    const topSkillsArray = (job.structured_skills || []).slice(0, 5);
    const topSkillsString = topSkillsArray.join(', ');

    matches.push({
      candidate_id: candidate.id,
      job_id: job.id,
      similarity_score: sim,
      location_distance_km: distance,
      final_score: finalScore,
      company_name: companyName,
      job_title: job.title || null,
      job_location: job.location,
      remote_type: job.remote_type,
      seniority_level: job.seniority_level,
      top_skills: topSkillsArray,
      // For logging:
      _job_title: job.title,
      _company_name_log: companyName,
      _top_skills_string: topSkillsString,
      sim,
      penalty,
    });
  }

  matches.sort((a, b) => b.final_score - a.final_score);
  return matches.slice(0, topK);
}

// ─── SAVE MATCHES ──────────────────────────────────────────────────────────
async function saveMatches(candidateId, matches) {
  if (DRY_RUN) {
    console.log(`  [DRY] Would save ${matches.length} matches for candidate ${candidateId}`);
    return;
  }

  await supabase
    .from('matches')
    .delete()
    .eq('candidate_id', candidateId);

  if (matches.length === 0) return;

  const rows = matches.map(m => ({
    candidate_id: m.candidate_id,
    job_id: m.job_id,
    similarity_score: Math.round(m.similarity_score * 10000) / 100,
    location_distance_km: m.location_distance_km,
    final_score: Math.round(m.final_score * 10000) / 100,
    company_name: m.company_name,
    job_title: m.job_title,
    job_location: m.job_location,
    remote_type: m.remote_type,
    seniority_level: m.seniority_level,
    top_skills: m.top_skills,
  }));

  const { error } = await supabase
    .from('matches')
    .insert(rows);

  if (error) {
    console.error(`  ❌ Insert error: ${error.message}`);
  } else {
    console.log(`  ✅ Saved ${rows.length} matches`);
  }
}

// ─── MAIN ──────────────────────────────────────────────────────────────────
async function runMatching() {
  console.log('\n🔍 Starting location-aware matching...');
  console.log(`   Top K          : ${TOP_K}`);
  console.log(`   Max distance   : ${MAX_DISTANCE_KM} km`);
  console.log(`   Dry run        : ${DRY_RUN}`);
  console.log(`   Limit          : ${LIMIT || 'All candidates'}\n`);

  const jobs = await fetchAllJobs();
  const companiesMap = await fetchCompanies();
  const candidates = await fetchCandidates();

  if (candidates.length === 0) {
    console.log('❌ No candidates with embeddings found.');
    return;
  }

  let totalMatches = 0;

  for (const candidate of candidates) {
    console.log(`\n👤 Candidate: ${candidate.name || candidate.salesforce_id || candidate.id}`);
    const matches = matchCandidate(candidate, jobs, companiesMap, TOP_K);
    totalMatches += matches.length;

    console.log(`  Top matches:`);
    matches.slice(0, 3).forEach((m, i) => {
      const simPct = (m.sim * 100).toFixed(1);
      const finalPct = (m.final_score * 100).toFixed(1);
      console.log(`    ${i+1}. [${simPct}% → ${finalPct}%] ${m._job_title || 'Untitled'}`);
      console.log(`       Company: ${m._company_name_log || 'N/A'} | Loc: ${m.job_location || 'N/A'} | Remote: ${m.remote_type || 'N/A'}`);
      console.log(`       Distance: ${m.location_distance_km !== null ? m.location_distance_km.toFixed(1) + ' km' : 'N/A'}`);
      console.log(`       Skills: ${m._top_skills_string || 'N/A'}`);
    });

    await saveMatches(candidate.id, matches);
  }

  console.log(`\n✅ Matching complete!`);
  console.log(`   Candidates processed : ${candidates.length}`);
  console.log(`   Total matches saved  : ${totalMatches}`);
}

// ─── RUN ──────────────────────────────────────────────────────────────────
runMatching().catch(err => {
  console.error('❌ Fatal error:', err.message);
  process.exit(1);
});
