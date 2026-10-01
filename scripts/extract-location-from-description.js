/**
 * Classify missing job locations with the shared authoritative LLM classifier.
 * 
 * Usage:
 *   node scripts/extract-location-from-description.js
 *   node scripts/extract-location-from-description.js --dry-run
 */

const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const { classifyJobWithLLM } = require('../src/ai/job-classifier');
require('dotenv').config();

const DRY_RUN = process.argv.includes('--dry-run');

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

// ─── Paginated fetch ────────────────────────────────────────────────────
async function fetchJobsWithoutLocation(page, pageSize) {
    const start = page * pageSize;
    const end = start + pageSize - 1;
    const { data, error } = await supabase
        .from('jobs')
        .select('id, title, raw_description, company_name, apply_url')
        .or('location.is.null,location.eq.')  // NULL or empty string
        .not('raw_description', 'is', null)
        .order('id', { ascending: true })
        .range(start, end);
    if (error) throw error;
    return data || [];
}

// ─── Main ──────────────────────────────────────────────────────────────────
async function run() {
    console.log('🔍 Extracting location from descriptions (dynamic)');
    console.log(`   Dry run: ${DRY_RUN}\n`);

    let processed = 0;
    let updated = 0;
    let found = 0;
    let page = 0;
    const PAGE_SIZE = 1000;

    while (true) {
        const jobs = await fetchJobsWithoutLocation(page, PAGE_SIZE);
        if (!jobs || jobs.length === 0) break;

        console.log(`📄 Page ${page + 1}: Processing ${jobs.length} jobs...`);

        for (const job of jobs) {
            processed++;
            const classification = await classifyJobWithLLM({
                company_name: job.company_name,
                external_job_id: job.id,
                title: job.title,
                raw_description: job.raw_description,
                source_url: job.apply_url
            });
            const place = classification.ok ? classification.data.job_location : null;
            if (!place) continue;

            found++;
            if (!DRY_RUN) {
                const { error: updateErr } = await supabase
                    .from('jobs')
                    .update({ location: place })
                    .eq('id', job.id);
                if (updateErr) {
                    console.error(`   ❌ Update error for ${job.id}: ${updateErr.message}`);
                } else {
                    updated++;
                }
            } else {
                console.log(`   🔍 Found place "${place}" in job ${job.id} (title: ${job.title})`);
            }
        }

        if (jobs.length < PAGE_SIZE) break;
        page++;
    }

    console.log(`\n✅ Done! Processed ${processed} jobs.`);
    console.log(`   Found place in ${found} jobs.`);
    console.log(`   Updated ${updated} jobs.`);
    console.log('📊 Now run geocoding to get lat/lng.');
}

run().catch(console.error);
