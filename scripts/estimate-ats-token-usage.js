/**
 * scripts/estimate-ats-token-usage.js
 *
 * Estimates token consumption of Claude for ATS detection fallback.
 * Runs on a sample of companies – does NOT modify production code.
 * 
 * Usage: node scripts/estimate-ats-token-usage.js [--limit=20]
 */

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const axios = require('axios');
const cheerio = require('cheerio');
const Anthropic = require('@anthropic-ai/sdk');
const fs = require('fs');

// ─── CONFIG ──────────────────────────────────────────────────────────────
const LIMIT = parseInt(process.argv.find(a => a.startsWith('--limit='))?.split('=')[1] || 20);
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const CLAUDE_MODEL = 'claude-sonnet-4-6';

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

// ─── CONSTANTS (same as ats-detector.js) ──────────────────────────────────
const VALID_ATS = [
    'personio', 'softgarden', 'rexx', 'onlyfy', 'hr4you', 'umantis', 'talentsoft',
    'erecruiter', 'connectoor', 'onapply', 'jobware', 'join', 'haufe', 'prescreen',
    'talention', 'pinpoint', 'pidelta', 'concludis', 'workwise', 'viasto',
    'greenhouse', 'workday', 'lever', 'successfactors', 'teamtailor',
    'smartrecruiters', 'recruitee', 'taleo', 'icims', 'bamboohr', 'jobvite',
    'ashby', 'workable', 'breezyhr', 'custom', 'unknown'
];

// ─── FETCH HTML (simplified) ──────────────────────────────────────────────
async function fetchHtml(url) {
    try {
        const response = await axios.get(url, {
            timeout: 10000,
            headers: { 'User-Agent': 'Mozilla/5.0' },
            maxRedirects: 5
        });
        return response.data;
    } catch (err) {
        return null;
    }
}

// ─── BUILD CLAUDE CONTEXT (same as ats-detector.js) ──────────────────────
function buildClaudeContext(html, pageUrl) {
    const $ = cheerio.load(html);
    const scriptSrcs = [], iframeSrcs = [], careerLinks = [];
    $('script[src]').each((_, el) => {
        const s = $(el).attr('src') || '';
        if (s) scriptSrcs.push(s);
    });
    $('iframe[src]').each((_, el) => {
        const s = $(el).attr('src') || '';
        if (s) iframeSrcs.push(s);
    });
    $('a[href]').each((_, el) => {
        const h = $(el).attr('href') || '';
        if (h && /job|career|karriere|stellen|apply|bewerb/i.test(h)) {
            careerLinks.push(h);
        }
    });

    return [
        `PAGE URL: ${pageUrl || 'unknown'}`,
        `SCRIPT SRCS:\n${scriptSrcs.slice(0, 15).join('\n') || 'none'}`,
        `IFRAMES:\n${iframeSrcs.join('\n') || 'none'}`,
        `CAREER/JOB LINKS:\n${careerLinks.slice(0, 15).join('\n') || 'none'}`,
        `HTML SNIPPET:\n${html.substring(0, 1500)}`
    ].join('\n\n');
}

// ─── CALL CLAUDE ──────────────────────────────────────────────────────────
async function callClaude(html, pageUrl) {
    const context = buildClaudeContext(html, pageUrl);
    
    const prompt = `You are an expert at detecting Applicant Tracking Systems (ATS).\nAnalyze this career page data and identify which ATS is used.\n\n${context}\n\nReply with ONLY ONE WORD from this exact list:\n${VALID_ATS.join(', ')}\n\nUse "custom" if the company built their own job listing system.\nUse "unknown" if there are no jobs or no ATS detectable.`;

    try {
        const response = await client.messages.create({
            model: CLAUDE_MODEL,
            max_tokens: 50,
            messages: [{ role: 'user', content: prompt }]
        });

        return {
            success: true,
            input_tokens: response.usage?.input_tokens || 0,
            output_tokens: response.usage?.output_tokens || 0,
            total_tokens: response.usage?.total_tokens || 0,
            answer: response.content[0].text.trim()
        };
    } catch (err) {
        return { success: false, error: err.message };
    }
}

// ─── MAIN ──────────────────────────────────────────────────────────────────
async function main() {
    console.log(`📋 Fetching ${LIMIT} companies with detected_career_url...`);

    // Fetch companies that might need Claude fallback (custom/unknown)
    const { data: companies, error } = await supabase
        .from('companies')
        .select('Id, Name, detected_career_url')
        .in('ats_type', ['custom', 'unknown'])
        .not('detected_career_url', 'is', null)
        .limit(LIMIT);

    if (error) {
        console.error('❌ Supabase error:', error.message);
        process.exit(1);
    }

    if (!companies || companies.length === 0) {
        console.log('⚠️ No companies found.');
        return;
    }

    console.log(`✅ Fetched ${companies.length} companies.\n`);

    const results = [];
    let totalInput = 0;
    let totalOutput = 0;
    let totalTokens = 0;
    let successCount = 0;

    for (const company of companies) {
        console.log(`🔄 Processing: ${company.Name}`);
        console.log(`   URL: ${company.detected_career_url}`);

        const html = await fetchHtml(company.detected_career_url);
        if (!html) {
            console.log(`   ❌ Could not fetch page`);
            continue;
        }

        const result = await callClaude(html, company.detected_career_url);
        if (result.success) {
            successCount++;
            totalInput += result.input_tokens;
            totalOutput += result.output_tokens;
            totalTokens += result.total_tokens;
            console.log(`   ✅ Tokens: in=${result.input_tokens}, out=${result.output_tokens}, total=${result.total_tokens}`);
            console.log(`   📝 Claude said: "${result.answer}"`);
        } else {
            console.log(`   ❌ Claude error: ${result.error}`);
        }

        // Small delay to avoid rate limits
        await new Promise(r => setTimeout(r, 1000));
    }

    // ─── SUMMARY ────────────────────────────────────────────────────────────
    const avgInput = successCount > 0 ? totalInput / successCount : 0;
    const avgOutput = successCount > 0 ? totalOutput / successCount : 0;
    const avgTotal = successCount > 0 ? totalTokens / successCount : 0;

    const inputCost = (totalInput / 1_000_000) * 3.00;   // Sonnet 4.6 input
    const outputCost = (totalOutput / 1_000_000) * 15.00; // Sonnet 4.6 output
    const totalCost = inputCost + outputCost;

    const report = `
═══════════════════════════════════════════════════════
📊 Claude ATS Detection Token Usage Estimate (${successCount} successful calls)
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
    fs.writeFileSync('ats-token-estimate.log', report);
    console.log('✅ Report saved to ats-token-estimate.log');
}

main().catch(console.error);
