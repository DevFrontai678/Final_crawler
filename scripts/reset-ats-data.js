/**
 * Reset ATS Detection Data — Supabase
 * 
 * Clears ATS detection fields on all companies so a fresh scan can run.
 * Jobs table is NOT touched — only the detection metadata on companies.
 * 
 * Usage:
 *   node scripts/reset-ats-data.js             → preview (dry run)
 *   node scripts/reset-ats-data.js --confirm   → actually reset
 */

'use strict';

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY,
);

const DRY_RUN = !process.argv.includes('--confirm');

async function main() {
  console.log('\n🗑  ATS Data Reset');
  console.log(DRY_RUN
    ? '  Mode: DRY RUN (run with --confirm to actually reset)\n'
    : '  Mode: LIVE — data will be cleared!\n'
  );

  // Count current state
  const { data: current } = await supabase
    .from('companies')
    .select('ats_type, crawl_status');

  if (!current) {
    console.error('Could not fetch companies');
    return;
  }

  const dist = {};
  current.forEach(r => {
    const k = r.ats_type || 'null';
    dist[k] = (dist[k] || 0) + 1;
  });

  console.log(`  Total companies : ${current.length}`);
  console.log('\n  Current ATS distribution:');
  Object.entries(dist)
    .sort((a, b) => b[1] - a[1])
    .forEach(([ats, count]) => {
      console.log(`    ${ats.padEnd(18)} ${count}`);
    });

  if (DRY_RUN) {
    console.log('\n  👆 Run with --confirm to reset all the above.\n');
    return;
  }

  // Reset all companies
  console.log('\n  Resetting...');
  const { error, count } = await supabase
    .from('companies')
    .update({
      ats_type:            null,
      ats_confidence:      null,
      detected_career_url: null,
      ats_api_url:         null,
      crawl_status:        'pending',
      last_crawled_at:     null,
      updated_at:          new Date().toISOString(),
    })
    .not('Id', 'is', null);   // matches all rows

  if (error) {
    console.error(`  ❌ Reset failed: ${error.message}`);
  } else {
    console.log(`  ✅ Reset complete — all companies set to pending`);
    console.log(`  ✅ Run: node scripts/run-ats-detection.js\n`);
  }
}

main().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});