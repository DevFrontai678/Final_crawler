require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const { processSoftgardenCompany } = require('../src/ats-adapters/softgarden-adapter');

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

async function run() {
  const { data: companies } = await supabase
    .from('companies')
    .select('*')
    .eq('ats_type', 'softgarden');

  if (!companies || companies.length === 0) {
    console.log('❌ No Softgarden companies yet — ATS detection chal rahi hogi!');
    return;
  }

  console.log(`\n🚀 Processing ${companies.length} Softgarden companies...\n`);

  let totalJobs = 0;

  for (const company of companies) {
    const result = await processSoftgardenCompany(company);

    if (result.jobs.length === 0) continue;

    for (const job of result.jobs) {
      const { error } = await supabase
        .from('jobs')
        .upsert({
          company_id: company.Id,
          external_job_id: job.external_job_id,
          title: job.title,
          location: job.location,
          employment_type: job.employment_type,
          raw_description: job.raw_description,
          is_active: true,
          first_seen_at: new Date().toISOString(),
          last_seen_at: new Date().toISOString()
        }, { onConflict: 'company_id,external_job_id' });

      if (error) console.log(`    ❌ Save error: ${error.message}`);
    }

    totalJobs += result.jobs.length;
    console.log(`   💾 ${result.jobs.length} jobs saved!`);

    await new Promise(r => setTimeout(r, 1000));
  }

  console.log(`\n✅ Done! Total jobs: ${totalJobs}`);
}

run();