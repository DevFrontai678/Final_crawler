require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const { processPersonioCompany, closePersonioBrowser } = require('../src/ats-adapters/personio-adapter');

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

async function run() {
    // Fetch Personio companies
    const { data: companies, error } = await supabase
        .from('companies')
        .select('"Id", "Name", detected_career_url')
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

    console.log(`📋 Processing ${companies.length} Personio companies...\n`);

    let totalJobs = 0;
    for (const company of companies) {
        const result = await processPersonioCompany(company);

        if (result.error) {
            console.log(`   ❌ ${result.error}`);
            await supabase.from('crawl_logs').insert({
                company_id: company.Id,
                status: 'failed',
                error_message: result.error,
                created_at: new Date()
            });
            await supabase.from('companies')
                .update({ crawl_status: 'failed' })
                .eq('Id', company.Id);
            continue;
        }

        if (result.jobs.length === 0) {
            console.log(`   ⚠️ No jobs found`);
            await supabase.from('crawl_logs').insert({
                company_id: company.Id,
                status: 'failed',
                error_message: 'No jobs found',
                created_at: new Date()
            });
            await supabase.from('companies')
                .update({ crawl_status: 'failed' })
                .eq('Id', company.Id);
            continue;
        }

        let missingDescCount = 0;
        let missingLocationCount = 0;

        for (const job of result.jobs) {
            if (!job.raw_description) missingDescCount++;
            if (!job.location) missingLocationCount++;

            const { error: insertError } = await supabase
                .from('jobs')
                .upsert({
                    company_id: company.Id,
                    company_name: job.company_name || company.Name || null, // ✅ ab genuinely save ho raha hai
                    external_job_id: job.external_job_id,
                    title: job.title || null,
                    location: job.location || null,          // ✅ empty string ki jagah explicit null
                    employment_type: job.employment_type || null,
                    raw_description: job.raw_description || null, // ✅ clean text, ya null agar na mile
                    apply_url: job.apply_url,
                    ats_source: job.ats_source || 'personio',
                    is_active: true,
                    first_seen_at: new Date(),
                    last_seen_at: new Date()
                }, { onConflict: 'company_id,external_job_id' });

            if (insertError) {
                console.error(`   ❌ Save error for job ${job.title}: ${insertError.message}`);
            }
        }

        totalJobs += result.jobs.length;
        console.log(`   💾 Saved ${result.jobs.length} jobs for ${company.Name} (missing desc: ${missingDescCount}, missing location: ${missingLocationCount})`);

        // Save to crawl_logs
        await supabase.from('crawl_logs').insert({
            company_id: company.Id,
            status: 'success',
            jobs_found: result.jobs.length,
            created_at: new Date()
        });

        await supabase.from('companies')
            .update({ crawl_status: 'ats_detected' })
            .eq('Id', company.Id);

        await new Promise(r => setTimeout(r, 500));
    }

    console.log(`\n✅ Done! Total Personio jobs saved: ${totalJobs}`);
}

// Graceful shutdown
process.on('SIGINT', async () => {
    console.log('\n⏹️ Shutting down gracefully...');
    await closePersonioBrowser();
    process.exit(0);
});

run().catch(console.error);
