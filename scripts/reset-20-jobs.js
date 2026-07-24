/**
 * scripts/reset-20-jobs.js
 * 
 * Resets structured_skills to NULL for 20 random jobs.
 * Usage: node scripts/reset-20-jobs.js [--specific] [--ids=1,2,3]
 * 
 * --specific : reset jobs from a specific source (e.g., 'custom')
 * --ids=...  : reset only these job IDs (comma‑separated)
 */

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

const LIMIT = 20;
const args = process.argv.slice(2);
const specificIds = args.find(a => a.startsWith('--ids='))?.split('=')[1]?.split(',');

async function main() {
    let query = supabase
        .from('jobs')
        .select('id, title, structured_skills')
        .not('structured_skills', 'is', null)
        .limit(LIMIT);

    if (specificIds && specificIds.length > 0) {
        query = supabase
            .from('jobs')
            .select('id, title, structured_skills')
            .in('id', specificIds);
    }

    const { data: jobs, error } = await query;

    if (error) {
        console.error('❌ Supabase error:', error.message);
        process.exit(1);
    }

    if (!jobs || jobs.length === 0) {
        console.log('✅ No jobs with structured_skills found to reset.');
        return;
    }

    console.log(`📋 Found ${jobs.length} jobs to reset:\n`);
    jobs.forEach(j => console.log(`   - ${j.title} (${j.id})`));

    // Confirm before resetting (optional)
    console.log('\n⚠️  About to reset these jobs. Press Ctrl+C to cancel, or Enter to continue.');
    await new Promise(r => process.stdin.once('data', r));

    const ids = jobs.map(j => j.id);
    const { error: updateError } = await supabase
        .from('jobs')
        .update({ structured_skills: null })
        .in('id', ids);

    if (updateError) {
        console.error('❌ Reset error:', updateError.message);
        process.exit(1);
    }

    console.log(`✅ Reset ${jobs.length} jobs. They will be re‑structured by the next worker run.`);
    console.log('   Run the structurer worker to process them:');
    console.log('   node scripts/run-job-structuring-worker.js');
}

main().catch(console.error);
