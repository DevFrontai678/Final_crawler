// src/matching/match-engine.js
const { createClient } = require('@supabase/supabase-js');
const pLimit = require('p-limit');
const ws = require('ws');                        // ← WebSocket fix
require('dotenv').config();

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY,
  { realtime: { transport: ws } }                // ← WebSocket fix
);

// ─── CONFIG ────────────────────────────────────────────────────────────────────
const CONFIG = {
  topK: 10,
  concurrency: 5,
  skipIfExists: false,
  dryRun: false,
};

// ─── CHECK IF CANDIDATE HAS EXISTING MATCHES ──────────────────────────────
async function hasExistingMatches(candidateId) {
  const { count, error } = await supabase
    .from('matches')
    .select('*', { count: 'exact', head: true })
    .eq('candidate_id', candidateId);
  if (error) throw error;
  return count > 0;
}

// ─── MATCH SINGLE CANDIDATE ──────────────────────────────────────────────────
async function matchCandidate(candidate, topK = CONFIG.topK) {
  console.log(`🔄 Processing: ${candidate.name} (${candidate.salesforce_id})`);

  const { data: matches, error } = await supabase.rpc(
    'match_jobs_for_candidate',
    {
      candidate_embedding: candidate.skill_embedding,
      match_count: topK,
    }
  );

  if (error) {
    console.error(`  ❌ RPC error: ${error.message}`);
    return null;
  }

  if (!matches || matches.length === 0) {
    console.log(`  ⚠️ No matches found`);
    return [];
  }

  console.log(`  ✅ Found ${matches.length} matches`);

  return matches.map(m => ({
    candidate_id: candidate.id,
    job_id: m.id,
    similarity_score: parseFloat((m.similarity * 100).toFixed(2)),
    final_score: parseFloat((m.similarity * 100).toFixed(2)),
    job_title: m.title,
    company_name: m.company_name || 'N/A',
    job_location: m.location || 'N/A',
    remote_type: m.remote_type || 'N/A',
    seniority_level: m.seniority_level || 'N/A',
    top_skills: (m.structured_skills || []).slice(0, 6),
  }));
}

// ─── SAVE MATCHES IN BATCH ──────────────────────────────────────────────────
async function saveMatchesBatch(allMatches) {
  if (allMatches.length === 0) return 0;

  const { error } = await supabase
    .from('matches')
    .insert(allMatches);

  if (error) {
    console.error('❌ Batch insert error:', error.message);
    let saved = 0;
    for (const match of allMatches) {
      const { error: singleError } = await supabase
        .from('matches')
        .insert(match);
      if (!singleError) saved++;
    }
    return saved;
  }
  return allMatches.length;
}

// ─── MAIN FUNCTION ────────────────────────────────────────────────────────────
async function matchAllCandidates() {
  console.log('\n🚀 Starting matching for all candidates...\n');

  const { data: candidates, error: candError } = await supabase
    .from('candidates')
    .select('id, name, salesforce_id, location, seniority_level, remote_preference, skill_embedding')
    .not('skill_embedding', 'is', null');

  if (candError) throw candError;
  console.log(`📋 Found ${candidates.length} candidates with embeddings\n`);

  let candidatesToProcess = candidates;
  if (CONFIG.skipIfExists) {
    console.log('🔍 Checking existing matches...');
    const filtered = [];
    for (const c of candidates) {
      const hasMatches = await hasExistingMatches(c.id);
      if (!hasMatches) filtered.push(c);
    }
    candidatesToProcess = filtered;
    console.log(`   ${candidates.length - filtered.length} skipped (already matched)`);
    console.log(`   ${filtered.length} candidates to process\n`);
  }

  if (candidatesToProcess.length === 0) {
    console.log('✅ All candidates already matched!');
    return;
  }

  const limit = pLimit(CONFIG.concurrency);
  const startTime = Date.now();
  let processed = 0;
  let totalMatches = 0;

  const promises = candidatesToProcess.map(async (candidate) => {
    const matches = await matchCandidate(candidate);
    processed++;
    const remaining = candidatesToProcess.length - processed;
    console.log(`   Progress: ${processed}/${candidatesToProcess.length} | ETA: ${(remaining * 0.5 / 60).toFixed(1)} min`);
    if (matches && matches.length > 0) {
      totalMatches += matches.length;
      return matches;
    }
    return [];
  });

  const allMatches = await Promise.all(promises);
  const flatMatches = allMatches.flat();

  console.log(`\n💾 Saving ${flatMatches.length} matches to database...`);

  if (CONFIG.dryRun) {
    console.log('   DRY RUN – Not saving to database');
    console.log(`   Would save ${flatMatches.length} matches`);
  } else {
    const candidateIds = candidatesToProcess.map(c => c.id);
    const { error: deleteError } = await supabase
      .from('matches')
      .delete()
      .in('candidate_id', candidateIds);

    if (deleteError) {
      console.error('❌ Delete error:', deleteError.message);
    } else {
      console.log(`   ✅ Deleted old matches for ${candidateIds.length} candidates`);
    }

    const batchSize = 1000;
    let saved = 0;
    for (let i = 0; i < flatMatches.length; i += batchSize) {
      const batch = flatMatches.slice(i, i + batchSize);
      const count = await saveMatchesBatch(batch);
      saved += count;
      console.log(`   💾 Saved ${saved}/${flatMatches.length} matches`);
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log('\n' + '═'.repeat(60));
  console.log(`✅ Matching complete!`);
  console.log(`   Candidates processed : ${candidatesToProcess.length}`);
  console.log(`   Total matches saved  : ${flatMatches.length}`);
  console.log(`   Time taken          : ${elapsed} min`);
  console.log('═'.repeat(60) + '\n');
}

matchAllCandidates().catch(console.error);
