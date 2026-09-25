require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const { processPersonioCompany, closePersonioBrowser } = require('../src/ats-adapters/personio-adapter');
const { enrichJobForStorage } = require('../src/utils/job-enrichment');
const { runCompaniesInBatches } = require('../src/utils/company-batch-runner');

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

async function run() {
    // Fetch Personio companies
    const { data: companies, error } = await supabase
        .from('companies')
        .select('"Id", "Name", "Website", detected_career_url')
        .eq('ats_type', 'personio')
        .eq('crawl_status', 'pending');

    if (error) {
        console.error('❌ Supabase error:', error.message);
        return;
    }

    if (!companies || companies.length === 0) {
        console.log('No Personio companies found.');
        return;
    }

    console.log(`📋 Processing ${companies.length} Personio companies in batches of 10...\n`);

    const { jobs: totalJobs } = await runCompaniesInBatches(companies, {
        batchSize: parseInt(process.env.CRAWLER_COMPANY_BATCH_SIZE || '10', 10),
        label: 'PERSONIO',
        handler: async (company, meta) => {
            const result = await processPersonioCompany(company);

            if (result.error) {
                console.log(`   ❌ [${meta.companyIndex}/${meta.companyTotal}] ${company.Name} | ${result.error}`);
                await supabase.from('crawl_logs').insert({
                    company_id: company.Id,
                    status: 'failed',
                    error_message: result.error,
                    created_at: new Date()
                });
                await supabase.from('companies')
                    .update({ crawl_status: 'failed' })
                    .eq('Id', company.Id);
                return { status: 'failed', jobs: [] };
            }

            if (result.jobs.length === 0) {
                console.log(`   ⚠️ [${meta.companyIndex}/${meta.companyTotal}] ${company.Name} | no jobs found`);
                await supabase.from('crawl_logs').insert({
                    company_id: company.Id,
                    status: 'failed',
                    error_message: 'No jobs found',
                    created_at: new Date()
                });
                await supabase.from('companies')
                    .update({ crawl_status: 'failed' })
                    .eq('Id', company.Id);
                return { status: 'no_jobs', jobs: [] };
            }

            let missingDescCount = 0;
            let missingLocationCount = 0;

            for (const job of result.jobs) {
                if (!job.raw_description) missingDescCount++;
                if (!job.location) missingLocationCount++;

                const storageJob = await enrichJobForStorage({
                    company_id: company.Id,
                    company_name: job.company_name || company.Name || null,
                    company_website: company.Website || null,
                    external_job_id: job.external_job_id,
                    title: job.title || null,
                    location: job.location || null,
                    employment_type: job.employment_type || null,
                    raw_description: job.raw_description || null,
                    apply_url: job.apply_url,
                    ats_source: job.ats_source || 'personio',
                    is_active: true
                });

                const { error: insertError } = await supabase
                    .from('jobs')
                    .upsert({
                        ...storageJob,
                        first_seen_at: new Date(),
                        last_seen_at: new Date()
                    }, { onConflict: 'company_id,external_job_id' });

                if (insertError) {
                    console.error(`   ❌ Save error for job ${job.title}: ${insertError.message}`);
                }
            }

            console.log(`   💾 [${meta.companyIndex}/${meta.companyTotal}] ${company.Name} | saved=${result.jobs.length} | missing_desc=${missingDescCount} | missing_location=${missingLocationCount}`);

            await supabase.from('crawl_logs').insert({
                company_id: company.Id,
                status: 'success',
                jobs_found: result.jobs.length,
                created_at: new Date()
            });

            await supabase.from('companies')
                .update({ crawl_status: 'ats_detected' })
                .eq('Id', company.Id);

            return { status: 'success', jobs: result.jobs };
        }
    });

    console.log(`\n✅ Done! Total Personio jobs saved: ${totalJobs}`);
}

// Graceful shutdown
process.on('SIGINT', async () => {
    console.log('\n⏹️ Shutting down gracefully...');
    await closePersonioBrowser();
    process.exit(0);
});

run().catch(console.error);
