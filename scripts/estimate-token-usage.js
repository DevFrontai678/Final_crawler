/**
 * scripts/estimate-token-usage.js
 *
 * Estimates token consumption of GPT-4.1 Mini for job structuring.
 * Runs on a sample of 20 jobs – does NOT modify production code.
 * 
 * Usage: node scripts/estimate-token-usage.js [--limit=20]
 */

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const OpenAI = require('openai');
const fs = require('fs');

// ─── CONFIG ──────────────────────────────────────────────────────────────
const LIMIT = parseInt(process.argv.find(a => a.startsWith('--limit='))?.split('=')[1] || 20);
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_MODEL = 'gpt-4.1-mini';

if (!OPENAI_API_KEY) {
    console.error('❌ OPENAI_API_KEY is not set in .env');
    process.exit(1);
}

const client = new OpenAI({ apiKey: OPENAI_API_KEY });
const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

// ─── PROMPT (same as in gpt-structurer.js) ──────────────────────────────
function buildPrompt(title, description) {
    return `
You are an expert HR data analyst. Extract structured information from this job posting.

**Job Title:** ${title || 'Not provided'}
**Job Description:**
${description || 'No description provided.'}

Return ONLY valid JSON:
{
  "cleaned_title": "standardised job title (e.g., Senior Backend Engineer)",
  "skills": ["skill1", "skill2", ...],
  "seniority_level": "junior|mid|senior|lead|executive",
  "employment_type": "fulltime|parttime|contract|internship",
  "remote_type": "remote|hybrid|onsite",
  "location_city": "city or null"
}

Rules:
- cleaned_title: remove location, company name, "m/w/d", fluff – just the role.
- skills: Extract real skills. If description is short, infer from title. ALWAYS include at least 3 skills.
- If seniority unclear → "mid". If remote unclear → "onsite". If employment unclear → "fulltime".
- Return ONLY JSON. No extra text.
`;
}

// ─── ESTIMATE A SINGLE JOB ──────────────────────────────────────────────
async function estimateJob(job) {
    const title = job.title || '';
    const description = job.raw_description || '';
    const prompt = buildPrompt(title, description);

    try {
        const response = await client.chat.completions.create({
            model: OPENAI_MODEL,
            messages: [
                { role: 'system', content: 'You are a precise job data extractor. Return only valid JSON.' },
                { role: 'user', content: prompt }
            ],
            temperature: 0,
            max_tokens: 500,
            response_format: { type: 'json_object' }
        });

        const usage = response.usage;
        return {
            job_id: job.id,
            title: title,
            input_tokens: usage.prompt_tokens || 0,
            output_tokens: usage.completion_tokens || 0,
            total_tokens: usage.total_tokens || 0,
            success: true
        };
    } catch (err) {
        console.warn(`⚠️ Job ${job.id} failed: ${err.message}`);
        return { job_id: job.id, title, success: false, error: err.message };
    }
}

// ─── MAIN ──────────────────────────────────────────────────────────────────
async function main() {
    console.log(`📋 Fetching ${LIMIT} jobs from Supabase...`);

    const { data: jobs, error } = await supabase
        .from('jobs')
        .select('id, title, raw_description')
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
        const result = await estimateJob(job);
        if (result.success) {
            successCount++;
            totalInput += result.input_tokens;
            totalOutput += result.output_tokens;
            totalTokens += result.total_tokens;
            console.log(`   ✅ Tokens: in=${result.input_tokens}, out=${result.output_tokens}, total=${result.total_tokens}`);
        } else {
            console.log(`   ❌ Failed: ${result.error}`);
        }
        // Small delay to avoid rate limits
        await new Promise(r => setTimeout(r, 200));
    }

    // ─── SUMMARY ────────────────────────────────────────────────────────────
    const avgInput = successCount > 0 ? totalInput / successCount : 0;
    const avgOutput = successCount > 0 ? totalOutput / successCount : 0;
    const avgTotal = successCount > 0 ? totalTokens / successCount : 0;

    const inputCost = (totalInput / 1_000_000) * 0.15;   // $0.15 per 1M input tokens
    const outputCost = (totalOutput / 1_000_000) * 0.60; // $0.60 per 1M output tokens
    const totalCost = inputCost + outputCost;

    const report = `
═══════════════════════════════════════════════════════
📊 GPT-4.1 Mini Token Usage Estimate (${successCount} successful jobs)
═══════════════════════════════════════════════════════
  Total input tokens   : ${totalInput.toLocaleString()}
  Total output tokens  : ${totalOutput.toLocaleString()}
  Total tokens         : ${totalTokens.toLocaleString()}
  ─────────────────────────────────────────────────────
  Average per job:
    Input tokens       : ${avgInput.toFixed(0)}
    Output tokens      : ${avgOutput.toFixed(0)}
    Total tokens       : ${avgTotal.toFixed(0)}
  ─────────────────────────────────────────────────────
  Estimated cost:
    Input  ($${0.15}/1M)  : $${inputCost.toFixed(4)}
    Output ($${0.60}/1M)  : $${outputCost.toFixed(4)}
    Total                 : $${totalCost.toFixed(4)}
  ─────────────────────────────────────────────────────
  Average cost per job   : $${(totalCost / successCount).toFixed(6)}
═══════════════════════════════════════════════════════
`;

    console.log(report);

    // Save to file
    fs.writeFileSync('token-estimate.log', report);
    console.log('✅ Report saved to token-estimate.log');
}

main().catch(console.error);
