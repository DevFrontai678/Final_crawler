/**
 * run-job-structuring.js
 * SIMPLE VERSION (Redis nahi chahiye): Directly Supabase se le ke structure karta hai.
 * Chota scale ke liye theek hai, lekin queue version zyada reliable hai.
 * 
 * Usage: node run-job-structuring.js
 *        BATCH_SIZE=200 node run-job-structuring.js   ← optional override
 */

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const { structureJob } = require('../src/ai/job-structurer');
const ws = require('ws');

// ─── Config ────────────────────────────────────────────────────────────────
const BATCH_SIZE      = Number(process.env.BATCH_SIZE)   || 100;
const TOTAL_LIMIT     = Number(process.env.TOTAL_LIMIT)  || 25000;
const DELAY_MS        = Number(process.env.DELAY_MS)     || 300;  // rate limiting ke liye

// ─── Client ────────────────────────────────────────────────────────────────
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY,
  { realtime: { transport: ws } }
);

// ─── Stats ─────────────────────────────────────────────────────────────────
const stats = { success: 0, failed: 0, skipped: 0, startTime: Date.now() };

function printProgress(current, total) {
  const pct     = ((current / total) * 100).toFixed(1);
  const elapsed = (Date.now() - stats.startTime) / 1000;
  const rate    = current > 0 ? (current / elapsed).toFixed(2) : 0;
  const eta     = rate > 0 ? Math.round((total - current) / rate) : '?';
  process.stdout.write(
    `\r⏳  ${current}/${total} (${pct}%) | ✅ ${stats.success} ❌ ${stats.failed} | ${rate} jobs/s | ETA: ${eta}s   `
  );
}

// ─── Main ──────────────────────────────────────────────────────────────────
async function run() {
  console.log('\n🤖  Job Structuring — SIMPLE MODE\n');

  let offset      = 0;
  let totalDone   = 0;
  let hasMore     = true;

  // Pehle total count nikaalo
  const { count } = await supabase
    .from('jobs')
    .select('id', { count: 'exact', head: true })
    .or('structured_skills.is.null,structured_skills.eq.{}');

  const totalToProcess = Math.min(count || 0, TOTAL_LIMIT);
  console.log(`📋  Jobs to structure: ${totalToProcess}\n`);

  if (totalToProcess === 0) {
    console.log('✅  Sab jobs already structured hain!');
    return;
  }

  while (hasMore && totalDone < TOTAL_LIMIT) {
    const fetchLimit = Math.min(BATCH_SIZE, TOTAL_LIMIT - totalDone);

    const { data: jobs, error } = await supabase
      .from('jobs')
      .select('*')
      .or('structured_skills.is.null,structured_skills.eq.{}')
      .range(offset, offset + fetchLimit - 1);

    if (error) {
      console.error('\n❌  Supabase error:', error.message);
      break;
    }

    if (!jobs || jobs.length === 0) {
      hasMore = false;
      break;
    }

    for (const job of jobs) {
      printProgress(totalDone, totalToProcess);

      try {
        const structured = await structureJob(job);

        if (!structured) {
          stats.skipped++;
          totalDone++;
          continue;
        }

        const { error: updateError } = await supabase
          .from('jobs')
          .update({
            structured_skills: structured.skills          || [],
            seniority_level:   structured.seniority_level || null,
            remote_type:       structured.remote_type     || null,
            employment_type:   structured.employment_type || job.employment_type || null,
            location:          structured.location_city   || job.location        || null,
            last_seen_at:      new Date().toISOString(),
          })
          .eq('id', job.id);

        if (updateError) {
          throw new Error(updateError.message);
        }

        stats.success++;
      } catch (err) {
        stats.failed++;
        console.error(`\n   ❌  "${job.title}": ${err.message}`);
      }

      totalDone++;

      // Rate limit — Claude API ko overwhelm mat karo
      if (DELAY_MS > 0) {
        await new Promise(r => setTimeout(r, DELAY_MS));
      }
    }

    offset  += jobs.length;
    hasMore  = jobs.length === fetchLimit;
  }

  // Final summary
  const elapsed = ((Date.now() - stats.startTime) / 1000 / 60).toFixed(1);
  console.log(`\n\n✅  Complete!`);
  console.log(`   Total processed : ${totalDone}`);
  console.log(`   Success         : ${stats.success}`);
  console.log(`   Failed          : ${stats.failed}`);
  console.log(`   Skipped         : ${stats.skipped}`);
  console.log(`   Time taken      : ${elapsed} minutes\n`);

  // Sample check
  const { data: sample } = await supabase
    .from('jobs')
    .select('title, structured_skills, seniority_level, remote_type')
    .not('structured_skills', 'is', null)
    .limit(5);

  if (sample?.length) {
    console.log('📊  Sample structured jobs:');
    sample.forEach(j => {
      console.log(`\n   ${j.title}`);
      console.log(`   Skills : ${j.structured_skills?.join(', ') || 'none'}`);
      console.log(`   Level  : ${j.seniority_level || 'N/A'} | Remote: ${j.remote_type || 'N/A'}`);
    });
  }
}

run().catch(err => {
  console.error('❌  Fatal error:', err);
  process.exit(1);
});
