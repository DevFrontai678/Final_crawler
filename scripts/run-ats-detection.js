/**
 * ATS Detection Runner — Production Grade
 * 
 * Features:
 *   - Concurrent processing (default: 5 workers)
 *   - Exponential backoff retry (up to 3 attempts)
 *   - Progress tracking with ETA
 *   - Resume support (skips already-detected companies)
 *   - Detailed summary at the end
 * 
 * Usage:
 *   node scripts/run-ats-detection.js              → process all pending
 *   node scripts/run-ats-detection.js --retry      → retry errors/unknowns too
 *   node scripts/run-ats-detection.js --all        → re-run everything (fresh scan)
 *   node scripts/run-ats-detection.js --limit 50   → process only 50 companies
 *   node scripts/run-ats-detection.js --concurrency 10
 */

'use strict';

require('dotenv').config();
const ws = require('ws');
const { createClient } = require('@supabase/supabase-js');
const { detectATS }    = require('../src/ats-adapters/ats-detector');

// ─── CONFIG ───────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);

const CONFIG = {
  concurrency: parseInt(getArg('--concurrency') || '5'),
  limit:       parseInt(getArg('--limit')       || '0'),   // 0 = no limit
  retryErrors: args.includes('--retry') || args.includes('--all'),
  rerunAll:    args.includes('--all'),
  delayMs:     1200,    // ms between each request (per worker)
  maxRetries:  3,       // attempts per company
  retryDelay:  3000,    // base delay for exponential backoff (ms)
};

function getArg(name) {
  const idx = args.indexOf(name);
  return idx !== -1 ? args[idx + 1] : null;
}

// ─── SUPABASE ─────────────────────────────────────────────────────────────────

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY,
  { global: { fetch }, realtime: { transport: ws } }
);

// ─── HELPERS ──────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

/** Exponential backoff: attempt 0→3s, 1→6s, 2→12s */
function backoffDelay(attempt) {
  return CONFIG.retryDelay * Math.pow(2, attempt);
}

function formatDuration(ms) {
  const s = Math.round(ms / 1000);
  if (s < 60)   return `${s}s`;
  if (s < 3600) return `${Math.floor(s/60)}m ${s%60}s`;
  return `${Math.floor(s/3600)}h ${Math.floor((s%3600)/60)}m`;
}

function printProgress(done, total, startTime, counters) {
  const pct  = ((done / total) * 100).toFixed(1);
  const elapsed = Date.now() - startTime;
  const rate    = done / (elapsed / 1000);          // companies/sec
  const remaining = total - done;
  const etaMs  = rate > 0 ? (remaining / rate) * 1000 : 0;

  process.stdout.write(
    `\r  Progress: ${done}/${total} (${pct}%) | ` +
    `✅ ${counters.success}  ❌ ${counters.error}  ⏭ ${counters.skipped} | ` +
    `ETA: ${formatDuration(etaMs)}    `
  );
}

// ─── SAVE RESULT TO SUPABASE ──────────────────────────────────────────────────

async function saveResult(company, result) {
  // Map confidence → crawl_status
  let crawlStatus;
  if (result.ats_type === 'error')  crawlStatus = 'failed';
  else if (result.ats_type === 'no_url') crawlStatus = 'no_url';
  else crawlStatus = 'ats_detected';

  const { error } = await supabase
    .from('companies')
    .update({
      ats_type:            result.ats_type,
      ats_confidence:      result.ats_confidence,
      detected_career_url: result.career_page_url,
      ats_api_url:         result.ats_api_url,
      crawl_status:        crawlStatus,
      last_crawled_at:     new Date().toISOString(),
      updated_at:          new Date().toISOString(),
    })
    .eq('Id', company['Id']);

  if (error) {
    console.error(`\n  ⚠  Supabase save error for ${company.Name}: ${error.message}`);
  }
}

// ─── SINGLE COMPANY WITH RETRY ────────────────────────────────────────────────

async function processCompany(company, workerIndex) {
  const url = company.detected_career_url || company.Website;

  if (!url) {
    await saveResult(company, {
      ats_type: 'no_url', ats_confidence: 0,
      career_page_url: null, ats_api_url: null,
    });
    return { status: 'skipped', reason: 'no_url' };
  }

  for (let attempt = 0; attempt < CONFIG.maxRetries; attempt++) {
    try {
      if (attempt > 0) {
        const delay = backoffDelay(attempt);
        console.log(`\n  ↻ Retry ${attempt}/${CONFIG.maxRetries-1} for ${company.Name} (wait ${delay/1000}s)`);
        await sleep(delay);
      }

      console.log(`\n  [W${workerIndex}] ${company.Name}`);
      console.log(`         URL: ${url}`);

      const result = await detectATS(company['Id'], url);
      await saveResult(company, result);

      console.log(`         → ${result.ats_type} (${result.detection_method}, conf: ${result.ats_confidence})`);
      return { status: result.ats_type === 'error' ? 'error' : 'success', result };

    } catch (err) {
      if (attempt === CONFIG.maxRetries - 1) {
        // Final attempt failed
        await saveResult(company, {
          ats_type: 'error', ats_confidence: 0,
          career_page_url: url, ats_api_url: null,
          error: err.message,
        });
        console.log(`\n  ✗ Failed after ${CONFIG.maxRetries} attempts: ${company.Name} — ${err.message}`);
        return { status: 'error', error: err.message };
      }
    }
  }
}

// ─── CONCURRENT WORKER POOL ───────────────────────────────────────────────────

async function runWithPool(companies, concurrency) {
  const total    = companies.length;
  const queue    = [...companies];
  const counters = { success: 0, error: 0, skipped: 0 };
  let   done     = 0;
  const startTime = Date.now();

  async function worker(id) {
    while (queue.length > 0) {
      const company = queue.shift();
      if (!company) break;

      const r = await processCompany(company, id);

      if (r.status === 'success')  counters.success++;
      else if (r.status === 'error')  counters.error++;
      else counters.skipped++;

      done++;
      printProgress(done, total, startTime, counters);

      // Rate limiting between requests
      await sleep(CONFIG.delayMs);
    }
  }

  // Launch workers in parallel
  const workers = Array.from({ length: concurrency }, (_, i) => worker(i + 1));
  await Promise.all(workers);

  console.log(''); // newline after progress line
  return { counters, elapsed: Date.now() - startTime };
}

// ─── FETCH COMPANIES FROM SUPABASE ────────────────────────────────────────────

async function fetchCompanies() {
  let query = supabase
    .from('companies')
    .select('Id, Name, Website, detected_career_url, ats_type, crawl_status')
    .not('Website', 'is', null);

  if (CONFIG.rerunAll) {
    // Process everything
    console.log('  Mode: ALL companies (fresh re-scan)');
  } else if (CONFIG.retryErrors) {
    // Process pending + errors + unknowns
    query = query.in('crawl_status', ['pending', 'failed', 'no_url'])
                 .or('ats_type.eq.unknown,ats_type.eq.error,ats_type.is.null');
    console.log('  Mode: pending + errors + unknowns');
  } else {
    // Only pending (default)
    query = query.or('crawl_status.eq.pending,crawl_status.is.null');
    console.log('  Mode: pending only (use --retry to include errors)');
  }

  if (CONFIG.limit > 0) {
    query = query.limit(CONFIG.limit);
    console.log(`  Limit: ${CONFIG.limit} companies`);
  }

  const { data, error } = await query;

  if (error) throw new Error(`Supabase fetch error: ${error.message}`);
  return data || [];
}

// ─── SUMMARY REPORT ───────────────────────────────────────────────────────────

async function printSummary(counters, elapsed) {
  console.log('\n\n══════════════════════════════════════════');
  console.log('  ATS DETECTION COMPLETE');
  console.log('══════════════════════════════════════════');
  console.log(`  Duration  : ${formatDuration(elapsed)}`);
  console.log(`  Success   : ${counters.success}`);
  console.log(`  Errors    : ${counters.error}`);
  console.log(`  Skipped   : ${counters.skipped}`);

  // Distribution from Supabase
  const { data } = await supabase
    .from('companies')
    .select('ats_type');

  if (data) {
    const dist = {};
    data.forEach(r => {
      const k = r.ats_type || 'null';
      dist[k] = (dist[k] || 0) + 1;
    });

    console.log('\n  ATS Distribution:');
    Object.entries(dist)
      .sort((a, b) => b[1] - a[1])
      .forEach(([ats, count]) => {
        const bar = '█'.repeat(Math.min(Math.round(count / 3), 30));
        console.log(`    ${ats.padEnd(18)} ${String(count).padStart(4)}  ${bar}`);
      });
  }

  console.log('══════════════════════════════════════════\n');
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log('\n🔍 ATS Detection Runner');
  console.log(`   Concurrency : ${CONFIG.concurrency} workers`);
  console.log(`   Delay       : ${CONFIG.delayMs}ms per worker\n`);

  const companies = await fetchCompanies();

  if (companies.length === 0) {
    console.log('  ✅ No companies to process. Use --retry or --all to re-scan.');
    return;
  }

  console.log(`  Found ${companies.length} companies to process\n`);

  const { counters, elapsed } = await runWithPool(companies, CONFIG.concurrency);
  await printSummary(counters, elapsed);
}

main().catch(err => {
  console.error('\n❌ Fatal error:', err.message);
  process.exit(1);
});