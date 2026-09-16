/**
 * scripts/estimate-voyage-cost.js
 *
 * Estimates token consumption and cost of Voyage AI embeddings.
 * Runs on a sample of jobs – does NOT modify production code.
 * 
 * Usage: node scripts/estimate-voyage-cost.js [--limit=20]
 */

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const axios = require('axios');
const fs = require('fs');

// ─── CONFIG ──────────────────────────────────────────────────────────────
const LIMIT = parseInt(process.argv.find(a => a.startsWith('--limit='))?.split('=')[1] || 20);
const VOYAGE_API_KEY = process.env.VOYAGE_API_KEY;
const VOYAGE_URL = 'https://api.voyageai.com/v1/embeddings';
const VOYAGE_MODEL = 'voyage-3-large';

if (!VOYAGE_API_KEY) {
    console.error('❌ VOYAGE_API_KEY is not set in .env');
    process.exit(1);
}

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

// ─── JOB TO TEXT (same as run-embeddings-queue.js) ──────────────────────
function jobToText(job) {
    let skillsText = '';
    if (Array.isArray(job.structured_skills) && job.structured_skills.length > 0) {
        skillsText = job.structured_skills.join(', ');
    } else if (job.raw_description) {
        // Fallback to description (first 1000 chars)
        skillsText = job.raw_description.slice(0, 1000);
    }

    const level = job.seniority_level || '';
    const remote = job.remote_type || '';
    const location = job.location || '';
    return `${job.title}. Skills: ${skillsText}. Level: ${level}. Remote: ${remote}. Location: ${location}`;
}

// ─── CALL VOYAGE AI ──────────────────────────────────────────────────────
async function embedText(text, jobId) {
    try {
        const response = await axios.post(VOYAGE_URL, {
            model: VOYAGE_MODEL,
            input: [text]
        }, {
            headers: {
                'Authorization': `Bearer ${VOYAGE_API_KEY}`,
                'Content-Type': 'application/json'
            },
            timeout: 60000
        });

        // Voyage AI returns tokens used (if available)
        const usage = response.data.usage || {};
        const totalTokens = usage.total_tokens || 0;

        return {
            success: true,
            job_id: jobId,
            input_tokens: totalTokens,
            total_tokens: totalTokens,
            embedding_length: response.data.data?.[0]?.embedding?.length || 0
        };
    } catch (err) {
        return { success: false, job_id: jobId, error: err.message };
    }
}

// ─── MAIN ──────────────────────────────────────────────────────────────────
async function main() {
    console.log(`📋 Fetching ${LIMIT} jobs from Supabase...`);

    // Fetch jobs that have structured_skills OR raw_description
    const { data: jobs, error } = await supabase
        .from('jobs')
        .select('id, title, structured_skills, seniority_level, remote_type, location, raw_description')
        .not('structured_skills', 'is', null)
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
    let totalTokens = 0;
    let successCount = 0;
    let embeddingLengths = [];

    for (const job of jobs) {
        console.log(`🔄 Processing job ${job.id}...`);
        const text = jobToText(job);
        console.log(`   📝 Text length: ${text.length} chars`);

        const result = await embedText(text, job.id);
        if (result.success) {
            successCount++;
            totalTokens += result.total_tokens;
            embeddingLengths.push(result.embedding_length);
            console.log(`   ✅ Tokens: ${result.total_tokens}, embedding length: ${result.embedding_length}`);
        } else {
            console.log(`   ❌ Failed: ${result.error}`);
        }

        // Small delay to avoid rate limits
        await new Promise(r => setTimeout(r, 500));
    }

    // ─── SUMMARY ────────────────────────────────────────────────────────────
    const avgTokens = successCount > 0 ? totalTokens / successCount : 0;
    const avgEmbeddingLength = successCount > 0 ? embeddingLengths.reduce((a, b) => a + b, 0) / successCount : 0;

    // Voyage AI pricing: $0.10 per 1M tokens
    const costPer1M = 0.10;
    const totalCost = (totalTokens / 1_000_000) * costPer1M;
    const avgCost = successCount > 0 ? totalCost / successCount : 0;

    const report = `
═══════════════════════════════════════════════════════
📊 Voyage AI Embedding Cost Estimate (${successCount} successful jobs)
═══════════════════════════════════════════════════════
  Total tokens          : ${totalTokens.toLocaleString()}
  ─────────────────────────────────────────────────────
  Average per job:
    Tokens              : ${avgTokens.toFixed(0)}
    Embedding length    : ${avgEmbeddingLength.toFixed(0)}
  ─────────────────────────────────────────────────────
  Estimated cost:
    Price per 1M tokens : $${costPer1M}
    Total               : $${totalCost.toFixed(6)}
    Average per job     : $${avgCost.toFixed(8)}
  ─────────────────────────────────────────────────────
  Estimated cost for 16,000 jobs: $${(avgCost * 16000).toFixed(4)}
═══════════════════════════════════════════════════════
`;

    console.log(report);
    fs.writeFileSync('voyage-cost-estimate.log', report);
    console.log('✅ Report saved to voyage-cost-estimate.log');
}

main().catch(console.error);
