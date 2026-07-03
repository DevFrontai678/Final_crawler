/**
 * Geocode jobs — smart location extraction (no hardcoded city list)
 * 
 * Usage:
 *   node scripts/geocode-jobs.js               # Full run
 *   node scripts/geocode-jobs.js --dry-run     # Test only
 *   node scripts/geocode-jobs.js --limit=100   # Process only 100 jobs
 */

const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const axios = require('axios');
require('dotenv').config();

// ─── Config ────────────────────────────────────────────────────────────────
const DRY_RUN = process.argv.includes('--dry-run');
const LIMIT = parseInt(process.argv.find(a => a.startsWith('--limit='))?.split('=')[1] || 0);
const PAGE_SIZE = 500;

// ─── Supabase ─────────────────────────────────────────────────────────────
const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

// ─── Cache ──────────────────────────────────────────────────────────────
const geocodeCache = new Map();
const negativeCache = new Set();

// ─── Stopwords ──────────────────────────────────────────────────────────
const STOP_WORDS = new Set([
    // Navigation / UI words (no change)
    'standort', 'anfahrt', 'anzeigen', 'technischer', 'support', 'service',
    'karriere', 'stellenangebote', 'ausbildung', 'homepage', 'bewerbungsformular',
    'email', 'telefon', 'öffnungszeiten', 'werkstatt', 'ersatzteil', 'qualität',
    'umwelt', 'sicherheit', 'finanzierung', 'schalung', 'baumaschinen',
    'lagermaschinen', 'gebrauchtmaschinen', 'baugeräte', 'partner', 'eigenmarke',
    'preis', 'bestseller', 'sortierung', 'konfigurieren', 'voip', 'erstgespräch',
    'ansprechpartner', 'verarbeitung', 'aufbewahrungsdauer', 'mietsortiment',
    'mietshop', 'kaufen', 'ratgeber', 'magazin', 'blog', 'faq', 'hilfe', 'kontakt',
    'preference', 'centre', 'center', 'multiple', 'locations', 'eingeben',
    'gmbh', 'ag', 'kg', 'se', 'e.v.', 'javascript', 'react', 'angular',
    'analysenformular', 'geolocation', 'button', 'odeon', 'registrieren',
    'downloads', 'tutorial', 'desk', 'meine auswahl',
    // ─── Job titles (common) ──────────────────────────────────────────
    'facharzt', 'entwickler', 'manager', 'engineer', 'consultant', 'architekt',
    'ingenieur', 'produktmanager', 'projektmanager', 'vertrieb', 'einkauf',
    'controller', 'buchhalter', 'personal', 'marketing', 'sales', 'support',
    'administrator', 'analyst', 'designer', 'berater', 'leiter', 'koordinator',
    'spezialist', 'expert', 'assistent', 'referent', 'sachbearbeiter',
    'teamleiter', 'abteilungsleiter', 'geschäftsführer', 'vorstand',
    // ─── Common words that appear as capitalized but are not cities ──
    'der', 'die', 'das', 'und', 'für', 'von', 'mit', 'auf', 'als', 'ist',
    'im', 'am', 'zum', 'zur', 'bei', 'durch', 'gegen', 'ohne', 'nach',
    'seit', 'über', 'unter', 'vor', 'zu', 'zwischen', 'oder', 'aber'
]);

// ─── Junk detector ──────────────────────────────────────────────────────
function looksLikeJunk(text) {
    if (!text) return true;
    if (text.length > 150) return true;
    if (/[{}<>;]/.test(text)) return true;
    const junkPatterns = [
        /var\s+\w+\s*=/i,
        /function\s*\(/i,
        /ajax_url/i,
        /wp-admin/i,
        /wp-includes/i,
        /rocket_beacon/i,
        /tinymce/i,
        /wpforms/i,
        /cookie[-_]?einstellungen/i,
        /["']val_required["']/i,
        /dns-prefetch/i,
        /border[-_]radius/i,
        /iframe_styles/i,
        /network\s+error/i,
        /please\s+enter/i,
        /\\\//,
        /https?:\\\//,
    ];
    if (junkPatterns.some(re => re.test(text))) return true;
    const specialCharCount = (text.match(/["'\\:,\[\]]/g) || []).length;
    if (specialCharCount > 3) return true;
    return false;
}

// ─── Smart city extraction ─────────────────────────────────────────────
function extractCity(text) {
    if (!text) return null;

    // 1. Postal code pattern: "12345 Berlin" → extract "Berlin"
    const postalMatch = text.match(/\b\d{4,5}\s+([A-ZÄÖÜ][a-zäöüß]+(?:[\s-][A-ZÄÖÜ][a-zäöüß]+){0,2})\b/);
    if (postalMatch) {
        const candidate = postalMatch[1];
        if (!isStopWord(candidate)) return candidate;
    }

    // 2. "in X" / "bei X" pattern
    const matchIn = text.match(/\b(?:in|bei|near|nahe)\s+([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+){0,2})\b/);
    if (matchIn) {
        const candidate = matchIn[1];
        if (!isStopWord(candidate)) return candidate;
    }

    // 3. Any capitalized word/phrase (multi‑word allowed)
    const matches = text.match(/\b([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+){0,2})\b/g);
    if (matches) {
        for (const word of matches) {
            // Skip if it's a job title or stopword
            if (!isStopWord(word)) {
                return word;
            }
        }
    }

    return null;
}

function isStopWord(candidate) {
    if (!candidate) return true;
    const lower = candidate.toLowerCase();
    const tokens = lower.split(/\s+/);
    return STOP_WORDS.has(lower) || tokens.some(t => STOP_WORDS.has(t));
}

// ─── Geocode with retry ─────────────────────────────────────────────────
async function geocodeWithRetry(locationStr, retries = 2, delay = 1000) {
    if (!locationStr) return null;
    const key = locationStr.toLowerCase();
    if (geocodeCache.has(key)) return geocodeCache.get(key);
    if (negativeCache.has(key)) return null;

    // Try both with and without country
    const urls = [
        `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(locationStr)}&format=json&limit=1`,
        `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(locationStr + ', Germany')}&format=json&limit=1`
    ];

    for (let attempt = 1; attempt <= retries; attempt++) {
        for (const url of urls) {
            try {
                const response = await axios.get(url, {
                    headers: { 'User-Agent': 'CustomerMatchingCrawler/1.0' },
                    timeout: 10000
                });
                if (response.data && response.data.length > 0) {
                    const coords = { lat: parseFloat(response.data[0].lat), lng: parseFloat(response.data[0].lon) };
                    geocodeCache.set(key, coords);
                    return coords;
                }
            } catch (err) {
                if (err.response?.status === 429) {
                    const wait = delay * Math.pow(2, attempt - 1);
                    console.log(`  ⏳ Rate limit (429) — waiting ${wait}ms...`);
                    await new Promise(r => setTimeout(r, wait));
                    break;
                }
            }
        }
        if (attempt < retries) {
            await new Promise(r => setTimeout(r, delay * Math.pow(2, attempt - 1)));
        }
    }

    negativeCache.add(key);
    return null;
}

// ─── Pagination ──────────────────────────────────────────────────────────
async function fetchPage(lastId, pageSize) {
    let query = supabase
        .from('jobs')
        .select('id, location')
        .not('location', 'is', null)
        .is('location_lat', null)
        .order('id', { ascending: true })
        .limit(pageSize);
    if (lastId !== null) {
        query = query.gt('id', lastId);
    }
    const { data, error } = await query;
    if (error) throw error;
    return data || [];
}

// ─── Process a single job ────────────────────────────────────────────────
async function processJob(job, index, totalLabel, stats) {
    if (looksLikeJunk(job.location)) {
        console.log(`  🗑️  ${index}${totalLabel} — Junk, skipping (id: ${job.id})`);
        return 'junk';
    }

    const city = extractCity(job.location);
    if (!city) {
        console.log(`  ⏭️ ${index}${totalLabel} — No city: "${job.location}"`);
        return 'no_city';
    }

    console.log(`  🔍 ${index}${totalLabel} — "${job.location}" → city: "${city}"`);

    if (DRY_RUN) return 'updated';

    const coords = await geocodeWithRetry(city);
    if (coords) {
        const { error: updateErr } = await supabase
            .from('jobs')
            .update({ location_lat: coords.lat, location_lng: coords.lng })
            .eq('id', job.id);
        if (updateErr) {
            console.error(`     ❌ Update failed: ${updateErr.message}`);
            return 'failed';
        }
        console.log(`     ✅ → ${coords.lat}, ${coords.lng}`);
        await new Promise(r => setTimeout(r, 1200));
        return 'updated';
    }

    console.log(`     ⚠️ No coordinates found for "${city}"`);
    await new Promise(r => setTimeout(r, 1200));
    return 'failed';
}

// ─── Main ──────────────────────────────────────────────────────────────────
async function run() {
    console.log('\n🗺️  Geocoding Jobs (smart extraction)');
    console.log('─────────────────────────────────────');
    console.log(`Dry run:    ${DRY_RUN}`);
    console.log(`Limit:      ${LIMIT || 'All'}`);
    console.log(`Page size:  ${PAGE_SIZE}\n`);

    const stats = { processed: 0, updated: 0, skipped: 0, junk: 0 };
    let lastId = null;
    let pageNum = 0;

    while (true) {
        let page;
        try {
            page = await fetchPage(lastId, PAGE_SIZE);
        } catch (err) {
            console.error(`❌ Supabase error: ${err.message}`);
            await new Promise(r => setTimeout(r, 5000));
            try {
                page = await fetchPage(lastId, PAGE_SIZE);
            } catch (err2) {
                console.error('❌ Retry failed. Stopping.', err2.message);
                break;
            }
        }

        if (!page || page.length === 0) {
            console.log('✅ No more jobs.');
            break;
        }

        pageNum++;
        console.log(`\n📄 Page ${pageNum} — ${page.length} jobs\n`);

        for (const job of page) {
            stats.processed++;
            const result = await processJob(job, stats.processed, LIMIT ? `/${LIMIT}` : '', stats);
            if (result === 'junk') stats.junk++;
            else if (result === 'no_city') stats.skipped++;
            else if (result === 'updated') stats.updated++;
            else stats.skipped++; // failed

            lastId = job.id;
            if (LIMIT > 0 && stats.processed >= LIMIT) {
                console.log(`\n🛑 Reached limit.`);
                printSummary(stats);
                return;
            }
        }
        if (page.length < PAGE_SIZE) break;
    }

    printSummary(stats);
}

function printSummary(stats) {
    console.log('\n📊 Summary');
    console.log(`   Processed:  ${stats.processed}`);
    console.log(`   ✅ Updated:  ${stats.updated}`);
    console.log(`   ⏭️ Skipped:  ${stats.skipped}`);
    console.log(`   🗑️ Junk:     ${stats.junk}`);
}

run().catch(err => {
    console.error('❌ Fatal error:', err.message);
    process.exit(1);
});
