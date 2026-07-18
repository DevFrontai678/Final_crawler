/**
 * Show token usage from log file
 * 
 * Usage: node scripts/show-token-usage.js [log-file-path]
 */

const fs = require('fs');
const path = require('path');

const logFile = process.argv[2] || './token-usage.log';

if (!fs.existsSync(logFile)) {
    console.error(`❌ Log file not found: ${logFile}`);
    process.exit(1);
}

const data = JSON.parse(fs.readFileSync(logFile, 'utf8'));
const stats = data.stats;

console.log('\n' + '═'.repeat(70));
console.log(`📊 TOKEN USAGE SUMMARY (from ${logFile})`);
console.log(`   ${data.timestamp}`);
console.log('═'.repeat(70));

// ─── Claude ────────────────────────────────────────────────────────────
if (stats.claude.calls > 0) {
    const inputCost = (stats.claude.inputTokens / 1000000) * 3.00;   // Sonnet 4.6 input
    const outputCost = (stats.claude.outputTokens / 1000000) * 15.00; // Sonnet 4.6 output
    console.log(`\n🧠 CLAUDE (${stats.claude.model || 'unknown'})`);
    console.log(`   Calls:          ${stats.claude.calls}`);
    console.log(`   Input Tokens:   ${stats.claude.inputTokens.toLocaleString()}`);
    console.log(`   Output Tokens:  ${stats.claude.outputTokens.toLocaleString()}`);
    console.log(`   Total Tokens:   ${stats.claude.totalTokens.toLocaleString()}`);
    console.log(`   ──────────────────────────────────────`);
    console.log(`   Cost:           $${(inputCost + outputCost).toFixed(4)}`);
}

// ─── Voyage AI ──────────────────────────────────────────────────────────
if (stats.voyage.calls > 0) {
    const cost = (stats.voyage.totalTokens / 1000000) * 0.10;
    console.log(`\n🔢 VOYAGE AI (${stats.voyage.model || 'voyage-3-large'})`);
    console.log(`   Calls:          ${stats.voyage.calls}`);
    console.log(`   Total Tokens:   ${stats.voyage.totalTokens.toLocaleString()}`);
    console.log(`   ──────────────────────────────────────`);
    console.log(`   Cost:           $${cost.toFixed(4)}`);
}

// ─── ScraperAPI ─────────────────────────────────────────────────────────
if (stats.scraperapi.calls > 0) {
    const cost = stats.scraperapi.calls * 0.005;
    console.log(`\n🌐 SCRAPERAPI`);
    console.log(`   Calls:          ${stats.scraperapi.calls}`);
    console.log(`   ──────────────────────────────────────`);
    console.log(`   Cost:           $${cost.toFixed(4)}`);
}

// ─── Grand Total ────────────────────────────────────────────────────────
const totalCost = ((stats.claude.totalTokens / 1000000) * 3.00) +   // Sonnet 4.6 input
                  ((stats.claude.outputTokens / 1000000) * 15.00) +
                  (stats.voyage.totalTokens / 1000000) * 0.10 +
                  (stats.scraperapi.calls * 0.005);

console.log('\n' + '─'.repeat(70));
console.log(`💰 GRAND TOTAL COST: $${totalCost.toFixed(4)}`);
console.log('═'.repeat(70) + '\n');
