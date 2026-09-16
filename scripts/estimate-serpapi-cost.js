/**
 * scripts/estimate-serpapi-cost.js
 *
 * Estimates SerpAPI cost for Google Jobs search.
 * Counts companies that would trigger a search, multiplies by maxPages.
 * 
 * Usage: node scripts/estimate-serpapi-cost.js [--pages=3]
 */

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');

// ─── CONFIG ──────────────────────────────────────────────────────────────
const MAX_PAGES = parseInt(process.argv.find(a => a.startsWith('--pages='))?.split('=')[1] || 3);
const COST_PER_SEARCH = 0.03; // USD per SerpAPI request (adjust as needed)

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

// ─── MAIN ──────────────────────────────────────────────────────────────────
async function main() {
    console.log(`📋 Counting companies that would trigger Google Jobs search...`);

    // These are the same conditions used in run-google-jobs.js
    const { data: companies, error } = await supabase
        .from('companies')
        .select('Id, Name')
        .in('crawl_status', ['failed', 'error'])
        .or('ats_type.eq.error,ats_type.eq.unknown')
        .not('Name', 'is', null);

    if (error) {
        console.error('❌ Supabase error:', error.message);
        process.exit(1);
    }

    if (!companies || companies.length === 0) {
        console.log('⚠️ No companies found that need Google Jobs search.');
        return;
    }

    console.log(`✅ Found ${companies.length} companies.`);

    // Each company may have multiple pages (maxPages)
    const totalSearches = companies.length * MAX_PAGES;
    const totalCost = totalSearches * COST_PER_SEARCH;

    const report = `
═══════════════════════════════════════════════════════
📊 SerpAPI Cost Estimate for Google Jobs Search
═══════════════════════════════════════════════════════
  Companies to search    : ${companies.length}
  Max pages per company  : ${MAX_PAGES}
  Total SerpAPI calls    : ${totalSearches}
  Cost per search        : $${COST_PER_SEARCH.toFixed(4)}
  Estimated total cost   : $${totalCost.toFixed(4)}
  ─────────────────────────────────────────────────────
  Cost per company (max) : $${(COST_PER_SEARCH * MAX_PAGES).toFixed(4)}
═══════════════════════════════════════════════════════
`;

    console.log(report);
    fs.writeFileSync('serpapi-cost-estimate.log', report);
    console.log('✅ Report saved to serpapi-cost-estimate.log');
}

main().catch(console.error);
