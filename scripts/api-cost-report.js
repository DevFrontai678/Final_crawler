/**
 * API Cost Report – Calculate token usage per 48-hour pipeline run
 * 
 * Usage: node scripts/api-cost-report.js
 */

const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const fs = require('fs');
require('dotenv').config();

// ─── CONFIG ──────────────────────────────────────────────────────────────────
const REPORT_INTERVAL_HOURS = 48;
const CLAUDE_INPUT_COST_PER_1M = 0.25;
const CLAUDE_OUTPUT_COST_PER_1M = 1.25;
const VOYAGE_COST_PER_1M = 0.10;
const SCRAPERAPI_COST_PER_REQUEST = 0.005;

// Average tokens per job structuring (Haiku)
const CLAUDE_INPUT_TOKENS_PER_JOB = 650;
const CLAUDE_OUTPUT_TOKENS_PER_JOB = 150;

// Average tokens per embedding (Voyage)
const VOYAGE_TOKENS_PER_JOB = 650;
const VOYAGE_TOKENS_PER_CANDIDATE = 260;

// ─── SUPABASE ──────────────────────────────────────────────────────────────
const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

// ─── HELPERS ──────────────────────────────────────────────────────────────
function formatCurrency(amount) {
    return '$' + amount.toFixed(4);
}

function formatNumber(num) {
    return num.toLocaleString();
}

// ─── FETCH JOBS (last 48 hours) ──────────────────────────────────────────
async function fetchJobCounts() {
    const cutoff = new Date(Date.now() - REPORT_INTERVAL_HOURS * 60 * 60 * 1000).toISOString();

    // 🔥 FIX: Use last_seen_at instead of created_at
    const { data: jobs, error } = await supabase
        .from('jobs')
        .select('id, structured_skills, skill_embedding, last_seen_at')
        .gte('last_seen_at', cutoff);

    if (error) {
        console.error('❌ Supabase error:', error.message);
        return null;
    }

    const structuredJobs = jobs.filter(j => j.structured_skills && j.structured_skills.length > 0);
    const embeddedJobs = jobs.filter(j => j.skill_embedding !== null);

    return {
        totalJobs: jobs.length,
        structuredJobs: structuredJobs.length,
        embeddedJobs: embeddedJobs.length,
        jobs: jobs
    };
}

// ─── FETCH CANDIDATES (all with embeddings) ──────────────────────────────
async function fetchCandidateCounts() {
    // Candidates don't have a reliable timestamp, just count all with embeddings
    const { data: candidates, error } = await supabase
        .from('candidates')
        .select('id, skill_embedding');

    if (error) {
        console.error('❌ Supabase error:', error.message);
        return null;
    }

    const embeddedCandidates = candidates.filter(c => c.skill_embedding !== null);

    return {
        totalCandidates: candidates.length,
        embeddedCandidates: embeddedCandidates.length
    };
}

// ─── FETCH SCRAPERAPI CALLS (last 48 hours) ──────────────────────────────
async function fetchScraperAPICalls() {
    const cutoff = new Date(Date.now() - REPORT_INTERVAL_HOURS * 60 * 60 * 1000).toISOString();

    const { data: logs, error } = await supabase
        .from('crawl_logs')
        .select('error_message, created_at')
        .gte('created_at', cutoff)
        .ilike('error_message', '%ScraperAPI%');

    if (error) {
        console.error('❌ Supabase error:', error.message);
        return { scraperAPICalls: 0 };
    }

    return {
        scraperAPICalls: logs.length
    };
}

// ─── READ TOKEN USAGE LOG (if available) ──────────────────────────────────
function readTokenLogs() {
    const logFile = './token-usage.log';
    if (!fs.existsSync(logFile)) {
        return null;
    }

    try {
        const data = fs.readFileSync(logFile, 'utf8');
        const lines = data.split('\n').filter(l => l.trim());
        const stats = {
            claudeCalls: 0,
            claudeInputTokens: 0,
            claudeOutputTokens: 0,
            claudeTotalTokens: 0,
            voyageCalls: 0,
            voyageTokens: 0,
            scraperCalls: 0
        };

        for (const line of lines) {
            try {
                const entry = JSON.parse(line);
                if (entry.model && entry.model.includes('claude')) {
                    stats.claudeCalls++;
                    stats.claudeInputTokens += entry.input_tokens || 0;
                    stats.claudeOutputTokens += entry.output_tokens || 0;
                    stats.claudeTotalTokens += entry.total_tokens || 0;
                } else if (entry.model && entry.model.includes('voyage')) {
                    stats.voyageCalls++;
                    stats.voyageTokens += entry.total_tokens || entry.input_tokens || 0;
                } else if (entry.type === 'scraperapi') {
                    stats.scraperCalls++;
                }
            } catch (e) {}
        }

        return stats;
    } catch (err) {
        console.warn('⚠️ Could not read token log:', err.message);
        return null;
    }
}

// ─── CALCULATE COSTS ──────────────────────────────────────────────────────
function calculateCosts(jobCounts, candidateCounts, scraperCalls, tokenLogs) {
    let claudeCost = 0;
    let voyageCost = 0;
    let scraperCost = 0;

    // ─── Claude ────────────────────────────────────────────────────────────
    if (tokenLogs && tokenLogs.claudeTotalTokens > 0) {
        const inputCost = (tokenLogs.claudeInputTokens / 1000000) * CLAUDE_INPUT_COST_PER_1M;
        const outputCost = (tokenLogs.claudeOutputTokens / 1000000) * CLAUDE_OUTPUT_COST_PER_1M;
        claudeCost = inputCost + outputCost;
    } else {
        const inputTokens = jobCounts.structuredJobs * CLAUDE_INPUT_TOKENS_PER_JOB;
        const outputTokens = jobCounts.structuredJobs * CLAUDE_OUTPUT_TOKENS_PER_JOB;
        claudeCost = (inputTokens / 1000000) * CLAUDE_INPUT_COST_PER_1M +
                     (outputTokens / 1000000) * CLAUDE_OUTPUT_COST_PER_1M;
    }

    // ─── Voyage AI ────────────────────────────────────────────────────────
    if (tokenLogs && tokenLogs.voyageTokens > 0) {
        voyageCost = (tokenLogs.voyageTokens / 1000000) * VOYAGE_COST_PER_1M;
    } else {
        const jobTokens = jobCounts.embeddedJobs * VOYAGE_TOKENS_PER_JOB;
        const candidateTokens = candidateCounts.embeddedCandidates * VOYAGE_TOKENS_PER_CANDIDATE;
        voyageCost = ((jobTokens + candidateTokens) / 1000000) * VOYAGE_COST_PER_1M;
    }

    // ─── ScraperAPI ────────────────────────────────────────────────────────
    if (tokenLogs && tokenLogs.scraperCalls > 0) {
        scraperCost = tokenLogs.scraperCalls * SCRAPERAPI_COST_PER_REQUEST;
    } else {
        scraperCost = (scraperCalls || 0) * SCRAPERAPI_COST_PER_REQUEST;
    }

    return {
        claude: {
            cost: claudeCost,
            inputTokens: tokenLogs?.claudeInputTokens || (jobCounts.structuredJobs * CLAUDE_INPUT_TOKENS_PER_JOB),
            outputTokens: tokenLogs?.claudeOutputTokens || (jobCounts.structuredJobs * CLAUDE_OUTPUT_TOKENS_PER_JOB),
            totalTokens: tokenLogs?.claudeTotalTokens || (jobCounts.structuredJobs * (CLAUDE_INPUT_TOKENS_PER_JOB + CLAUDE_OUTPUT_TOKENS_PER_JOB)),
            calls: tokenLogs?.claudeCalls || jobCounts.structuredJobs
        },
        voyage: {
            cost: voyageCost,
            tokens: tokenLogs?.voyageTokens || (jobCounts.embeddedJobs * VOYAGE_TOKENS_PER_JOB + candidateCounts.embeddedCandidates * VOYAGE_TOKENS_PER_CANDIDATE),
            calls: tokenLogs?.voyageCalls || (jobCounts.embeddedJobs + candidateCounts.embeddedCandidates)
        },
        scraperapi: {
            cost: scraperCost,
            calls: tokenLogs?.scraperCalls || (scraperCalls || 0)
        },
        totalCost: claudeCost + voyageCost + scraperCost
    };
}

// ─── PRINT REPORT ──────────────────────────────────────────────────────────
function printReport(jobCounts, candidateCounts, scraperCalls, costs, tokenLogs) {
    console.log('\n' + '═'.repeat(70));
    console.log('📊 API COST REPORT – 48-HOUR PIPELINE');
    console.log('═'.repeat(70));
    console.log(`   Report generated: ${new Date().toISOString()}`);
    console.log(`   Interval:         ${REPORT_INTERVAL_HOURS} hours`);
    console.log(`   Token logs:       ${tokenLogs ? '✅ Available' : '❌ Not available (using estimates)'}`);
    console.log('─'.repeat(70));

    // ─── Usage Summary ────────────────────────────────────────────────────
    console.log('\n📈 USAGE SUMMARY');
    console.log(`   Jobs processed         : ${formatNumber(jobCounts.totalJobs)}`);
    console.log(`   Jobs structured (Claude): ${formatNumber(jobCounts.structuredJobs)}`);
    console.log(`   Jobs embedded (Voyage) : ${formatNumber(jobCounts.embeddedJobs)}`);
    console.log(`   Candidates embedded    : ${formatNumber(candidateCounts.embeddedCandidates)}`);
    console.log(`   ScraperAPI calls       : ${formatNumber(scraperCalls)}`);

    // ─── Claude ────────────────────────────────────────────────────────────
    console.log('\n🧠 CLAUDE (Anthropic)');
    console.log(`   Calls:          ${formatNumber(costs.claude.calls)}`);
    console.log(`   Input tokens:   ${formatNumber(costs.claude.inputTokens)}`);
    console.log(`   Output tokens:  ${formatNumber(costs.claude.outputTokens)}`);
    console.log(`   Total tokens:   ${formatNumber(costs.claude.totalTokens)}`);
    console.log(`   ──────────────────────────────────────`);
    console.log(`   Input cost:     ${formatCurrency((costs.claude.inputTokens / 1000000) * CLAUDE_INPUT_COST_PER_1M)}`);
    console.log(`   Output cost:    ${formatCurrency((costs.claude.outputTokens / 1000000) * CLAUDE_OUTPUT_COST_PER_1M)}`);
    console.log(`   Total Claude:   ${formatCurrency(costs.claude.cost)}`);

    // ─── Voyage AI ──────────────────────────────────────────────────────────
    console.log('\n🔢 VOYAGE AI');
    console.log(`   Calls:          ${formatNumber(costs.voyage.calls)}`);
    console.log(`   Tokens:         ${formatNumber(costs.voyage.tokens)}`);
    console.log(`   ──────────────────────────────────────`);
    console.log(`   Total Voyage:   ${formatCurrency(costs.voyage.cost)}`);

    // ─── ScraperAPI ──────────────────────────────────────────────────────────
    console.log('\n🌐 SCRAPERAPI');
    console.log(`   Calls:          ${formatNumber(costs.scraperapi.calls)}`);
    console.log(`   ──────────────────────────────────────`);
    console.log(`   Total ScraperAPI: ${formatCurrency(costs.scraperapi.cost)}`);

    // ─── Grand Total ──────────────────────────────────────────────────────
    console.log('\n' + '─'.repeat(70));
    console.log(`💰 GRAND TOTAL COST: ${formatCurrency(costs.totalCost)}`);
    console.log('═'.repeat(70) + '\n');

    // ─── Per-Day Estimate ──────────────────────────────────────────────────
    const perDayCost = costs.totalCost / (REPORT_INTERVAL_HOURS / 24);
    console.log(`📆 Estimated daily cost: ${formatCurrency(perDayCost)}`);
    console.log(`📆 Estimated monthly cost (30 days): ${formatCurrency(perDayCost * 30)}`);
    console.log('═'.repeat(70) + '\n');
}

// ─── MAIN ──────────────────────────────────────────────────────────────────
async function run() {
    console.log('⏳ Fetching data from Supabase...');

    const jobCounts = await fetchJobCounts();
    if (!jobCounts) {
        console.error('❌ Failed to fetch job data.');
        return;
    }

    const candidateCounts = await fetchCandidateCounts();
    if (!candidateCounts) {
        console.error('❌ Failed to fetch candidate data.');
        return;
    }

    const scraperData = await fetchScraperAPICalls();
    const scraperCalls = scraperData.scraperAPICalls || 0;

    const tokenLogs = readTokenLogs();
    const costs = calculateCosts(jobCounts, candidateCounts, scraperCalls, tokenLogs);

    printReport(jobCounts, candidateCounts, scraperCalls, costs, tokenLogs);
}

run().catch(err => {
    console.error('❌ Fatal error:', err.message);
    process.exit(1);
});
