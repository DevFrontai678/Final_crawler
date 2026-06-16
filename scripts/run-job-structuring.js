require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const { structureJob } = require('../src/ai/job-structurer');
const ws = require('ws');

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY,
  { realtime: { transport: ws } }
);

async function run() {
  const { data: jobs, error } = await supabase
    .from('jobs')
    .select('*')
    .is('structured_skills', null)
    .limit(500);

  if (error) {
    console.error('❌ Supabase error:', error.message);
    return;
  }

  if (!jobs || jobs.length === 0) {
    console.log('✅ All jobs already structured!');
    return;
  }

  console.log(`\n🤖 Structuring ${jobs.length} jobs with Claude...\n`);

  let success = 0;
  let failed = 0;

  for (const job of jobs) {
    console.log(`\n📋 ${job.title}`);
    console.log(`   Company ID: ${job.company_id}`);

    try {
      const structured = await structureJob(job);

      if (!structured) {
        console.log(`   ⚠️ No structured data returned`);
        failed++;
        continue;
      }

      console.log(`   ✅ Skills: ${structured.skills?.join(', ') || 'none'}`);
      console.log(`   📊 Seniority: ${structured.seniority_level || 'N/A'}`);
      console.log(`   🏠 Remote: ${structured.remote_type || 'N/A'}`);

      const { error: updateError } = await supabase
        .from('jobs')
        .update({
          structured_skills: structured.skills || [],
          seniority_level: structured.seniority_level || null,
          remote_type: structured.remote_type || null,
          employment_type: structured.employment_type || job.employment_type || null,
          location: structured.location_city || job.location || null,
          last_seen_at: new Date().toISOString()
        })
        .eq('id', job.id);

      if (updateError) {
        console.log(`   ❌ Save error: ${updateError.message}`);
        failed++;
      } else {
        console.log(`   💾 Saved!`);
        success++;
      }
    } catch (err) {
      console.log(`   ❌ Structure error: ${err.message}`);
      failed++;
    }

    await new Promise(r => setTimeout(r, 1000));
  }

  console.log(`\n✅ Done! Success: ${success}, Failed: ${failed}`);

  const { data: sample } = await supabase
    .from('jobs')
    .select('title, structured_skills, seniority_level, remote_type')
    .not('structured_skills', 'is', null)
    .limit(5);

  if (sample && sample.length) {
    console.log('\n📊 Sample structured jobs:');
    sample.forEach(j => {
      console.log(`\n  ${j.title}`);
      console.log(`  Skills: ${j.structured_skills?.join(', ') || 'none'}`);
      console.log(`  Level: ${j.seniority_level || 'N/A'} | Remote: ${j.remote_type || 'N/A'}`);
    });
  }
}

run();
