/**
 * scripts/build-skill-dictionary.js
 * Generates a domain‑agnostic skill dictionary from your job descriptions.
 * Run once: node scripts/build-skill-dictionary.js
 */

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const fs = require('fs');
const path = require('path');

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

// ─── Stopwords ──────────────────────────────────────────────────────────────
const STOPWORDS = new Set([
    'a', 'an', 'the', 'of', 'for', 'on', 'with', 'at', 'by', 'in', 'to',
    'from', 'into', 'through', 'during', 'including', 'without', 'per',
    'und', 'der', 'die', 'das', 'den', 'dem', 'des', 'ein', 'eine', 'einen',
    'einer', 'eines', 'für', 'mit', 'auf', 'bei', 'zur', 'zum', 'durch',
    'und', 'oder', 'von', 'mit', 'als', 'wie', 'ist', 'sind', 'werden',
    'wurde', 'wird', 'haben', 'hat', 'hatte', 'sein', 'war', 'waren',
    // Add more if needed
]);

// ─── Fetch all job descriptions ──────────────────────────────────────────
async function fetchAllDescriptions() {
    const all = [];
    let page = 0;
    const limit = 1000;
    let hasMore = true;

    while (hasMore) {
        const { data, error } = await supabase
            .from('jobs')
            .select('raw_description')
            .not('raw_description', 'is', null)
            .range(page * limit, (page + 1) * limit - 1);

        if (error) throw error;
        if (!data || data.length === 0) break;
        all.push(...data);
        if (data.length < limit) hasMore = false;
        page++;
    }
    return all.map(j => j.raw_description);
}

// ─── Extract 2‑ and 3‑word phrases ──────────────────────────────────────
function extractPhrases(text) {
    const sentences = text.split(/[.!?;:]/);
    const phrases = [];
    for (const sent of sentences) {
        const words = sent.toLowerCase().split(/[\s,;()"']+/).filter(w => w.length > 2 && !STOPWORDS.has(w));
        for (let i = 0; i < words.length - 1; i++) {
            const pair = words[i] + ' ' + words[i+1];
            phrases.push(pair);
            if (i < words.length - 2) {
                const triple = words[i] + ' ' + words[i+1] + ' ' + words[i+2];
                phrases.push(triple);
            }
        }
    }
    return phrases;
}

// ─── Main ──────────────────────────────────────────────────────────────────
async function main() {
    console.log('📥 Fetching job descriptions...');
    const texts = await fetchAllDescriptions();
    console.log(`✅ Fetched ${texts.length} descriptions.`);

    const freq = new Map();
    for (const text of texts) {
        const phrases = extractPhrases(text);
        for (const p of phrases) {
            freq.set(p, (freq.get(p) || 0) + 1);
        }
    }

    // Sort by frequency (descending)
    const sorted = [...freq.entries()].sort((a, b) => b[1] - a[1]);

    // Keep only phrases that appear at least 3 times
    const minFreq = 3;
    const filtered = sorted.filter(([_, count]) => count >= minFreq);

    console.log(`📊 Extracted ${freq.size} unique phrases. Filtered to ${filtered.length} (min freq ${minFreq}).`);

    // Write to a JSON file
    const outputPath = path.join(__dirname, '..', 'src', 'ai', 'skills-dictionary.json');
    fs.writeFileSync(outputPath, JSON.stringify(Object.fromEntries(filtered), null, 2));
    console.log(`✅ Skills dictionary saved to ${outputPath}`);
}

main().catch(console.error);
