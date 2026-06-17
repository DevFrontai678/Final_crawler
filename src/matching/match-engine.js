// src/matching/match-engine.js
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');                         // WebSocket fix
require('dotenv').config();

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY,
  { realtime: { transport: ws } }
);

async function matchAllCandidates(topK = 10) {
  console.log('\n🚀 Starting matching for all candidates...\n');

  const { data: candidates, error: candError } = await supabase
    .from('candidates')
    .select('id, name, salesforce_id, location, seniority_level, remote_preference, skill_embedding')
    .not('skill_embedding', 'is', null);   // ← FIXED: removed extra quote and semicolon

  if (candError) throw candError;
  console.log(`Found ${candidates.length} candidates with embeddings\n`);

  let totalMatches = 0;

  for (const candidate of candidates) {
    console.log(`\n📋 Processing: ${candidate.name} (${candidate.salesforce_id})`);

    const { data: matches, error: matchError } = await supabase.rpc(
      'match_jobs_for_candidate',
      {
        candidate_embedding: candidate.skill_embedding,
        match_count: topK,
      }
    );

    if (matchError) {
      console.error(`  ❌ Match error: ${matchError.message}`);
      continue;
    }

    console.log(`  ✅ Found ${matches.length} matches`);

    // Delete old matches
    await supabase
      .from('matches')
      .delete()
      .eq('candidate_id', candidate.id);

    // Insert new matches
    for (const match of matches) {
      const { error: saveError } = await supabase
        .from('matches')
        .insert({
          candidate_id: candidate.id,
          job_id: match.id,
          similarity_score: parseFloat((match.similarity * 100).toFixed(2)),
          final_score: parseFloat((match.similarity * 100).toFixed(2)),
        });

      if (saveError) {
        console.error(`    ❌ Save error: ${saveError.message}`);
      }
    }

    // Print top 5
    console.log(`\n  Top 5 matches:`);
    console.log('  ' + '─'.repeat(60));
    matches.slice(0, 5).forEach((m, i) => {
      const score = (m.similarity * 100).toFixed(1);
      console.log(`  ${i + 1}. [${score}%] ${m.title}`);
      console.log(`     Company: ${m.company_name || 'N/A'} | ${m.location || 'N/A'} | ${m.remote_type || 'N/A'}`);
      console.log(`     Skills : ${(m.structured_skills || []).slice(0, 4).join(', ')}`);
    });

    totalMatches += matches.length;
  }

  console.log('\n' + '═'.repeat(60));
  console.log(`✅ Matching complete!`);
  console.log(`   Candidates processed : ${candidates.length}`);
  console.log(`   Total matches saved  : ${totalMatches}`);
}

matchAllCandidates(10).catch(console.error);

