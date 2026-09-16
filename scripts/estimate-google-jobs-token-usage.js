/**
 * scripts/estimate-google-jobs-token-usage.js
 *
 * Estimates token consumption of Claude for Google Jobs structuring.
 * Runs on a sample of jobs – does NOT modify production code.
 * 
 * Usage: node scripts/estimate-google-jobs-token-usage.js [--limit=20]
 */

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const Anthropic = require('@anthropic-ai/sdk');
const fs = require('fs');

// ─── CONFIG ──────────────────────────────────────────────────────────────
const LIMIT = parseInt(process.argv.find(a => a.startsWith('--limit='))?.split('=')[1] || 20);
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const CLAUDE_MODEL = 'claude-sonnet-4-6'; // Same as used in run-google-jobs.js

if (!ANTHROPIC_API_KEY) {
    console.error('❌ ANTHROPIC_API_KEY is not set in .env');
    process.exit(1);
}

const client = new Anthropic({ apiKey: ANTHROPIC_API_KEY });
const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

// ─── PROMPT (same as structureJobWithClaude in run-google-jobs.js) ──────
function buildPrompt(title, company, location, description) {
    const cleanDescription = (description || '')
        .replace(/[\u0000-\u001F\u007F-\u009F]/g, '')
        .replace(/\\/g, '\\\\')
        .replace(/"/g, '\\"')
        .slice(0, 3000);

    return `You are a job data extractor. Extract structured data from this job posting.

TITLE: ${title || 'Unknown'}
COMPANY: ${company || 'Unknown'}
LOCATION: ${location || 'Unknown'}
DESCRIPTION:
${cleanDescription}

---

Return ONLY valid JSON with NO preamble, NO markdown, NO extra text:

{
  "title": "extracted job title",
  "description": "full cleaned description",
  "location": "city or remote",
  "employment_type": "Full-time|Part-time|Contract|Unknown",
  "seniority_level": "Senior|Mid|Junior|Entry|Lead|Unknown",
  "skills": ["skill1", "skill2", "skill3"]
}

START JSON RESPONSE:`;
}

// ─── CALL CLAUDE ──────────────────────────────────────────────────────────
async function callClaude(title, company, location, description, jobId) {
    const prompt = buildPrompt(title, company, location, description);
    try {
        const response = await client.messages.create({
            model: CLAUDE_MODEL,
            max_tokens: 800,
            messages: [{ role: 'user', content: prompt }]
        });

        const usage = response.usage;
        return {
            success: true,
            job_id: jobId,
            input_tokens: usage?.input_tokens || 0,
            output_tokens: usage?.output_tokens || 0,
            total_tokens: usage?.total_tokens || 0
        };
    } catch (err) {
        return { success: false, job_id: jobId, error: err.message };
    }
}

// ─── MAIN ──────────────────────────────────────────────────────────────────
async function main() {
    console.log(`📋 Fetching ${LIMIT} jobs from Supabase...`);

    // Fetch jobs with enough data for the prompt
    const { data: jobs, error } = await supabase
        .from('jobs')
        .select('id, title, company_name, location, raw_description')
        .not('raw_description', 'is', null)
        .limit(LIMIT);

    if (error) {
        console.error('❌ Supabase error:', error.message);
        process.exit(1);
    }

    if (!jobs || jobs.length === 0) {
        console.log('⚠️ No jobs found.');
        return;
    }

    console.log(`✅ Fetched ${jobs.length} jobs.\n`);

    const results = [];
    let totalInput = 0;
    let totalOutput = 0;
    let totalTokens = 0;
    let successCount = 0;

    for (const job of jobs) {
        console.log(`🔄 Processing job ${job.id}...`);
        const result = await callClaude(
            job.title,
            job.company_name,
            job.location,
            job.raw_description,
            job.id
        );

        if (result.success) {
            successCount++;
            totalInput += result.input_tokens;
            totalOutput += result.output_tokens;
            totalTokens += result.total_tokens;
            console.log(`   ✅ Tokens: in=${result.input_tokens}, out=${result.output_tokens}, total=${result.total_tokens}`);
        } else {
            console.log(`   ❌ Claude error: ${result.error}`);
        }

        // Small delay to avoid rate limits
        await new Promise(r => setTimeout(r, 500));
    }

    // ─── SUMMARY ────────────────────────────────────────────────────────────
    const avgInput = successCount > 0 ? totalInput / successCount : 0;
    const avgOutput = successCount > 0 ? totalOutput / successCount : 0;
    const avgTotal = successCount > 0 ? totalTokens / successCount : 0;

    // Sonnet 4.6 pricing
    const inputCost = (totalInput / 1_000_000) * 3.00;
    const outputCost = (totalOutput / 1_000_000) * 15.00;
    const totalCost = inputCost + outputCost;

    const report = `
═══════════════════════════════════════════════════════
📊 Google Jobs Claude Token Usage Estimate (${successCount} successful calls)
═══════════════════════════════════════════════════════
  Total input tokens   : ${totalInput.toLocaleString()}
  Total output tokens  : ${totalOutput.toLocaleString()}
  Total tokens         : ${totalTokens.toLocaleString()}
  ─────────────────────────────────────────────────────
  Average per call:
    Input tokens       : ${avgInput.toFixed(0)}
    Output tokens      : ${avgOutput.toFixed(0)}
    Total tokens       : ${avgTotal.toFixed(0)}
  ─────────────────────────────────────────────────────
  Estimated cost:
    Input  ($${3.00}/1M)  : $${inputCost.toFixed(4)}
    Output ($${15.00}/1M) : $${outputCost.toFixed(4)}
    Total                 : $${totalCost.toFixed(4)}
  ─────────────────────────────────────────────────────
  Average cost per call  : $${(totalCost / successCount).toFixed(6)}
═══════════════════════════════════════════════════════
`;

    console.log(report);
    fs.writeFileSync('google-jobs-token-estimate.log', report);
    console.log('✅ Report saved to google-jobs-token-estimate.log');
}

main().catch(console.error);
