/**
 * API Cost Report – Calculate token usage per 48‑hour pipeline run
 * 
 * Usage: node scripts/api-cost-report.js
 * 
 * Updated:
 *   - Sonnet 4.6 pricing (input: $3.00/1M, output: $15.00/1M)
 *   - Component breakdown: Job Structuring Worker, Google Jobs (Company),
 *     Google Jobs (Candidate), ATS Detection.
 *   - Estimates based on job counts per source (if real token logs not available).
 */

const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const fs = require('fs');
require('dotenv').config();

// ─── CONFIG ──────────────────────────────────────────────────────────────────
const REPORT_INTERVAL_HOURS = 48;

// 🔥 Updated to Sonnet 4.6 pricing
const CLAUDE_INPUT_COST_PER_1M = 3.00;    // Sonnet 4.6 input
const CLAUDE_OUTPUT_COST_PER_1M = 15.00;  // Sonnet 4.6 output
const VOYAGE_COST_PER_1M = 0.10;
const SCRAPERAPI_COST_PER_REQUEST = 0.005;

// Average tokens per job (used for estimates when logs are missing)
const CLAUDE_INPUT_TOKENS_PER_JOB = 650;   // input prompt + description
const CLAUDE_OUTPUT_TOKENS_PER_JOB = 150;  // structured JSON output

// Average tokens per embedding
const VOYAGE_TOKENS_PER_JOB = 650;
const VOYAGE_TOKENS_PER_CANDIDATE = 260;

// Estimate for ATS detection (per company, fallback Claude calls)
const ATS_COMPANIES = 4615;               // total companies in the system
const ATS_INPUT_TOKENS_PER_COMPANY = 800; // average fallback tokens per company
const ATS_OUTPUT_TOKENS_PER_COMPANY = 200;

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

    const { data: jobs, error } = await supabase
        .from('jobs')
        .select('id, structured_skills, skill_embedding, ats_source, last_seen_at')
        .gte('last_seen_at', cutoff);

    if (error) {
        console.error('❌ Supabase error:', error.message);
        return null;
    }

    const structuredJobs = jobs.filter(j => j.structured_skills && j.structured_skills.length > 0);
    const embeddedJobs = jobs.filter(j => j.skill_embedding !== null);

    // Separate Google Jobs sources
    const googleJobsCompany = jobs.filter(j => j.ats_source === 'google_jobs');
    const googleJobsCandidate = jobs.filter(j => j.ats_source === 'google_jobs_candidate');

    return {
        totalJobs: jobs.length,
        structuredJobs: structuredJobs.length,
        embeddedJobs: embeddedJobs.length,
        googleJobsCompany: googleJobsCompany.length,
        googleJobsCandidate: googleJobsCandidate.length,
        jobs: jobs
    };
}

// ─── FETCH CANDIDATES (all with embeddings) ──────────────────────────────
async function fetchCandidateCounts() {
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

    return { scraperAPICalls: logs.length };
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
    // ─── Claude ────────────────────────────────────────────────────────────
    // If real logs exist, use them; otherwise estimate from job counts.
    let claudeInputTokens, claudeOutputTokens, claudeCalls;
    if (tokenLogs && tokenLogs.claudeTotalTokens > 0) {
        claudeInputTokens = tokenLogs.claudeInputTokens;
        claudeOutputTokens = tokenLogs.claudeOutputTokens;
        claudeCalls = tokenLogs.claudeCalls;
    } else {
        // Estimate total tokens based on all structured jobs
        const totalStructured = jobCounts.structuredJobs;
        claudeInputTokens = totalStructured * CLAUDE_INPUT_TOKENS_PER_JOB;
        claudeOutputTokens = totalStructured * CLAUDE_OUTPUT_TOKENS_PER_JOB;
        claudeCalls = totalStructured;
    }

    // ─── Component breakdown ──────────────────────────────────────────────
    // Allocate Claude tokens to components based on job sources
    // 1. Job Structuring Worker: processes all structured jobs (including Google Jobs)
    //    But Google Jobs scripts also call Claude. We'll subtract Google Jobs jobs from worker count.
    const workerJobs = jobCounts.structuredJobs - jobCounts.googleJobsCompany - jobCounts.googleJobsCandidate;
    const googleCompanyJobs = jobCounts.googleJobsCompany;
    const googleCandidateJobs = jobCounts.googleJobsCandidate;

    // Tokens per component (estimated)
    const workerInputTokens = Math.max(0, workerJobs * CLAUDE_INPUT_TOKENS_PER_JOB);
    const workerOutputTokens = Math.max(0, workerJobs * CLAUDE_OUTPUT_TOKENS_PER_JOB);
    const googleCompanyInputTokens = googleCompanyJobs * CLAUDE_INPUT_TOKENS_PER_JOB;
    const googleCompanyOutputTokens = googleCompanyJobs * CLAUDE_OUTPUT_TOKENS_PER_JOB;
    const googleCandidateInputTokens = googleCandidateJobs * CLAUDE_INPUT_TOKENS_PER_JOB;
    const googleCandidateOutputTokens = googleCandidateJobs * CLAUDE_OUTPUT_TOKENS_PER_JOB;

    // ATS Detection (fallback Claude calls)
    const atsInputTokens = ATS_COMPANIES * ATS_INPUT_TOKENS_PER_COMPANY;
    const atsOutputTokens = ATS_COMPANIES * ATS_OUTPUT_TOKENS_PER_COMPANY;

    // Costs per component
    const calcCost = (inTokens, outTokens) => (
        (inTokens / 1000000) * CLAUDE_INPUT_COST_PER_1M +
        (outTokens / 1000000) * CLAUDE_OUTPUT_COST_PER_1M
    );

    const claudeCost = calcCost(claudeInputTokens, claudeOutputTokens);
    const workerCost = calcCost(workerInputTokens, workerOutputTokens);
    const googleCompanyCost = calcCost(googleCompanyInputTokens, googleCompanyOutputTokens);
    const googleCandidateCost = calcCost(googleCandidateInputTokens, googleCandidateOutputTokens);
    const atsCost = calcCost(atsInputTokens, atsOutputTokens);

    // ─── Voyage AI ────────────────────────────────────────────────────────
    let voyageTokens, voyageCalls;
    if (tokenLogs && tokenLogs.voyageTokens > 0) {
        voyageTokens = tokenLogs.voyageTokens;
        voyageCalls = tokenLogs.voyageCalls;
    } else {
        const jobTokens = jobCounts.embeddedJobs * VOYAGE_TOKENS_PER_JOB;
        const candidateTokens = candidateCounts.embeddedCandidates * VOYAGE_TOKENS_PER_CANDIDATE;
        voyageTokens = jobTokens + candidateTokens;
        voyageCalls = jobCounts.embeddedJobs + candidateCounts.embeddedCandidates;
    }
    const voyageCost = (voyageTokens / 1000000) * VOYAGE_COST_PER_1M;

    // ─── ScraperAPI ────────────────────────────────────────────────────────
    const scraperCallsTotal = tokenLogs?.scraperCalls || scraperCalls || 0;
    const scraperCost = scraperCallsTotal * SCRAPERAPI_COST_PER_REQUEST;

    // ─── Component breakdown (tokens and calls) ──────────────────────────
    const componentBreakdown = {
        'Job Structuring Worker': {
            calls: workerJobs,
            inputTokens: workerInputTokens,
            outputTokens: workerOutputTokens,
            cost: workerCost
        },
        'Google Jobs (Company)': {
            calls: googleCompanyJobs,
            inputTokens: googleCompanyInputTokens,
            outputTokens: googleCompanyOutputTokens,
            cost: googleCompanyCost
        },
        'Google Jobs (Candidate)': {
            calls: googleCandidateJobs,
            inputTokens: googleCandidateInputTokens,
            outputTokens: googleCandidateOutputTokens,
            cost: googleCandidateCost
        },
        'ATS Detection (fallback)': {
            calls: ATS_COMPANIES,
            inputTokens: atsInputTokens,
            outputTokens: atsOutputTokens,
            cost: atsCost
        }
    };

    const totalClaudeCost = workerCost + googleCompanyCost + googleCandidateCost + atsCost;
    const totalCost = totalClaudeCost + voyageCost + scraperCost;

    return {
        claude: {
            inputTokens: claudeInputTokens,
            outputTokens: claudeOutputTokens,
            totalTokens: claudeInputTokens + claudeOutputTokens,
            calls: claudeCalls,
            cost: claudeCost
        },
        voyage: {
            tokens: voyageTokens,
            calls: voyageCalls,
            cost: voyageCost
        },
        scraperapi: {
            calls: scraperCallsTotal,
            cost: scraperCost
        },
        components: componentBreakdown,
        totalCost: totalCost
    };
}

// ─── PRINT REPORT ──────────────────────────────────────────────────────────
function printReport(jobCounts, candidateCounts, scraperCalls, costs, tokenLogs) {
    console.log('\n' + '═'.repeat(80));
    console.log('📊 API COST REPORT – 48-HOUR PIPELINE');
    console.log('═'.repeat(80));
    console.log(`   Report generated: ${new Date().toISOString()}`);
    console.log(`   Interval:         ${REPORT_INTERVAL_HOURS} hours`);
    console.log(`   Token logs:       ${tokenLogs ? '✅ Available' : '❌ Not available (using estimates)'}`);
    console.log(`   Claude model:     Sonnet 4.6 (input: $3.00/1M, output: $15.00/1M)`);
    console.log('─'.repeat(80));

    // ─── Usage Summary ────────────────────────────────────────────────────
    console.log('\n📈 USAGE SUMMARY');
    console.log(`   Jobs processed         : ${formatNumber(jobCounts.totalJobs)}`);
    console.log(`   Jobs structured (Claude): ${formatNumber(jobCounts.structuredJobs)}`);
    console.log(`   Jobs embedded (Voyage) : ${formatNumber(jobCounts.embeddedJobs)}`);
    console.log(`   Candidates embedded    : ${formatNumber(candidateCounts.embeddedCandidates)}`);
    console.log(`   ScraperAPI calls       : ${formatNumber(scraperCalls)}`);

    // ─── Claude ────────────────────────────────────────────────────────────
    console.log('\n🧠 CLAUDE (Anthropic) – Total');
    console.log(`   Calls:          ${formatNumber(costs.claude.calls)}`);
    console.log(`   Input tokens:   ${formatNumber(costs.claude.inputTokens)}`);
    console.log(`   Output tokens:  ${formatNumber(costs.claude.outputTokens)}`);
    console.log(`   Total tokens:   ${formatNumber(costs.claude.totalTokens)}`);
    console.log(`   ──────────────────────────────────────`);
    console.log(`   Total cost:     ${formatCurrency(costs.claude.cost)}`);

    // ─── Component Breakdown ──────────────────────────────────────────────
    console.log('\n📋 CLAUDE USAGE BY COMPONENT:');
    const comps = costs.components;
    const compNames = Object.keys(comps);
    const maxNameLen = Math.max(...compNames.map(n => n.length));
    for (const name of compNames) {
        const c = comps[name];
        if (c.calls === 0 && c.cost === 0) continue;
        const pad = ' '.repeat(maxNameLen - name.length + 2);
        console.log(`   ${name}${pad} Calls: ${formatNumber(c.calls)} | Tokens: ${formatNumber(c.inputTokens + c.outputTokens)} | Cost: ${formatCurrency(c.cost)}`);
    }

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
    console.log('\n' + '─'.repeat(80));
    console.log(`💰 GRAND TOTAL COST: ${formatCurrency(costs.totalCost)}`);
    console.log('═'.repeat(80) + '\n');

    // ─── Per-Day Estimate ──────────────────────────────────────────────────
    const perDayCost = costs.totalCost / (REPORT_INTERVAL_HOURS / 24);
    console.log(`📆 Estimated daily cost: ${formatCurrency(perDayCost)}`);
    console.log(`📆 Estimated monthly cost (30 days): ${formatCurrency(perDayCost * 30)}`);
    console.log('═'.repeat(80) + '\n');
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
