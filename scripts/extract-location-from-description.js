/**
 * Extract location from job title/description (dynamic, no hardcoded list)
 * 
 * Usage:
 *   node scripts/extract-location-from-description.js
 *   node scripts/extract-location-from-description.js --dry-run
 */

const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
require('dotenv').config();

const DRY_RUN = process.argv.includes('--dry-run');

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

// ─── Stop words (to filter out non-place phrases) ──────────────────────
const STOP_WORDS = new Set([
    'javascript', 'react', 'angular', 'vue', 'node', 'python', 'java',
    'linux', 'windows', 'server', 'database', 'sql', 'cloud', 'docker',
    'kubernetes', 'devops', 'ci', 'cd', 'agile', 'scrum', 'kanban',
    'analysenformular', 'geolocation', 'button', 'standort', 'anfahrt',
    'anzeigen', 'technischer', 'support', 'service', 'karriere',
    'stellenangebote', 'ausbildung', 'homepage', 'bewerbungsformular',
    'email', 'telefon', 'öffnungszeiten', 'werkstatt', 'ersatzteil',
    'qualität', 'umwelt', 'sicherheit', 'finanzierung', 'schalung',
    'baumaschinen', 'lagermaschinen', 'gebrauchtmaschinen', 'baugeräte',
    'partner', 'eigenmarke', 'preis', 'bestseller', 'sortierung',
    'konfigurieren', 'voip', 'erstgespräch', 'ansprechpartner',
    'verarbeitung', 'aufbewahrungsdauer', 'mietsortiment', 'mietshop',
    'kaufen', 'ratgeber', 'magazin', 'blog', 'faq', 'hilfe', 'kontakt',
    'preference', 'centre', 'center', 'multiple', 'locations', 'eingeben',
    'gmbh', 'ag', 'kg', 'se', 'e.v.', 'registrieren', 'downloads',
    'tutorial', 'desk', 'meine auswahl', 'odeon', 'javascript!'
]);

function isStopWord(phrase) {
    const lower = phrase.toLowerCase();
    return STOP_WORDS.has(lower) || STOP_WORDS.has(lower.split(' ')[0]);
}

// ─── Extract plausible place name ───────────────────────────────────────
function extractPlaceName(text) {
    if (!text) return null;

    // 1. Patterns: "Standort: Berlin", "Arbeitsort: München"
    const patternMatch = text.match(/(?:standort|arbeitsort|ort|location)\s*[:]\s*([A-Za-zÄÖÜäöüß][\sA-Za-zÄÖÜäöüß-]{2,40})/i);
    if (patternMatch) {
        const candidate = patternMatch[1].trim();
        if (!isStopWord(candidate)) return candidate;
    }

    // 2. "in Berlin", "bei München"
    const inMatch = text.match(/\b(?:in|bei|near|nahe)\s+([A-Za-zÄÖÜäöüß][\sA-Za-zÄÖÜäöüß-]{2,40})\b/);
    if (inMatch) {
        const candidate = inMatch[1].trim();
        if (!isStopWord(candidate)) return candidate;
    }

    // 3. Postal code + word: "12345 Berlin"
    const postalMatch = text.match(/\b\d{4,5}\s+([A-Za-zÄÖÜäöüß][\sA-Za-zÄÖÜäöüß-]{2,40})\b/);
    if (postalMatch) {
        const candidate = postalMatch[1].trim();
        if (!isStopWord(candidate)) return candidate;
    }

    // 4. Any capitalized phrase of 1-3 words (candidate place)
    const phraseMatches = text.match(/\b([A-Z][a-zäöüß]+(?:\s+[A-Z][a-zäöüß]+){0,2})\b/g);
    if (phraseMatches) {
        for (const phrase of phraseMatches) {
            if (phrase.length > 2 && !isStopWord(phrase)) {
                // Filter out common non‑place words
                const lower = phrase.toLowerCase();
                if (lower === 'gmbh' || lower === 'ag' || lower === 'kg' || lower === 'se') continue;
                return phrase;
            }
        }
    }

    return null;
}

// ─── Paginated fetch ────────────────────────────────────────────────────
async function fetchJobsWithoutLocation(page, pageSize) {
    const start = page * pageSize;
    const end = start + pageSize - 1;
    const { data, error } = await supabase
        .from('jobs')
        .select('id, title, raw_description')
        .or('location.is.null,location.eq.')  // NULL or empty string
        .not('raw_description', 'is', null)
        .order('id', { ascending: true })
        .range(start, end);
    if (error) throw error;
    return data || [];
}

// ─── Main ──────────────────────────────────────────────────────────────────
async function run() {
    console.log('🔍 Extracting location from descriptions (dynamic)');
    console.log(`   Dry run: ${DRY_RUN}\n`);

    let processed = 0;
    let updated = 0;
    let found = 0;
    let page = 0;
    const PAGE_SIZE = 1000;

    while (true) {
        const jobs = await fetchJobsWithoutLocation(page, PAGE_SIZE);
        if (!jobs || jobs.length === 0) break;

        console.log(`📄 Page ${page + 1}: Processing ${jobs.length} jobs...`);

        for (const job of jobs) {
            processed++;
            const text = (job.title || '') + ' ' + (job.raw_description || '');
            const place = extractPlaceName(text);
            if (!place) continue;

            found++;
            if (!DRY_RUN) {
                const { error: updateErr } = await supabase
                    .from('jobs')
                    .update({ location: place })
                    .eq('id', job.id);
                if (updateErr) {
                    console.error(`   ❌ Update error for ${job.id}: ${updateErr.message}`);
                } else {
                    updated++;
                }
            } else {
                console.log(`   🔍 Found place "${place}" in job ${job.id} (title: ${job.title})`);
            }
        }

        if (jobs.length < PAGE_SIZE) break;
        page++;
    }

    console.log(`\n✅ Done! Processed ${processed} jobs.`);
    console.log(`   Found place in ${found} jobs.`);
    console.log(`   Updated ${updated} jobs.`);
    console.log('📊 Now run geocoding to get lat/lng.');
}

run().catch(console.error);
