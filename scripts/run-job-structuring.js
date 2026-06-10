require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const { structureJob } = require('../src/ai/job-structurer');

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

async function run() {
  // Jobs lo jo abhi structured nahi hain
  const { data: jobs, error } = await supabase
    .from('jobs')
    .select('*')
    .is('structured_skills', null)
    .limit(50);

  if (!jobs || jobs.length === 0) {
    console.log('No jobs to structure!');
    return;
  }

  console.log(`\n🤖 Structuring ${jobs.length} jobs with Claude...\n`);

  let success = 0;
  let failed = 0;

  for (const job of jobs) {
    console.log(`\n📋 ${job.title}`);
    console.log(`   Company ID: ${job.company_id}`);

    const structured = await structureJob(job);

    if (!structured) {
      failed++;
      continue;
    }

    console.log(`   ✅ Skills: ${structured.skills?.join(', ')}`);
    console.log(`   📊 Seniority: ${structured.seniority_level}`);
    console.log(`   🏠 Remote: ${structured.remote_type}`);

    // Supabase update karo
    const { error: updateError } = await supabase
      .from('jobs')
      .update({
        structured_skills: structured.skills || [],
        seniority_level: structured.seniority_level,
        remote_type: structured.remote_type,
        employment_type: structured.employment_type || job.employment_type,
        location: structured.location_city || job.location,
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

    // Rate limiting — Claude API
    await new Promise(r => setTimeout(r, 500));
  }

  console.log(`\n✅ Done! Success: ${success}, Failed: ${failed}`);

  // Sample result dikhao
  const { data: sample } = await supabase
    .from('jobs')
    .select('title, structured_skills, seniority_level, remote_type')
    .not('structured_skills', 'is', null)
    .limit(5);

  console.log('\n📊 Sample structured jobs:');
  sample?.forEach(j => {
    console.log(`\n  ${j.title}`);
    console.log(`  Skills: ${j.structured_skills?.join(', ')}`);
    console.log(`  Level: ${j.seniority_level} | Remote: ${j.remote_type}`);
  });
}

run();