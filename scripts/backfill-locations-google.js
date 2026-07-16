/**
 * Backfill locations using Google SERP API – v3
 * 
 * Usage: node scripts/backfill-locations-google.js
 */

const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const axios = require('axios');
const fs = require('fs');
require('dotenv').config();

// ─── CONFIG ──────────────────────────────────────────────────────────────────
const DRY_RUN = process.argv.includes('--dry-run');
const LIMIT = parseInt(process.argv.find(a => a.startsWith('--limit='))?.split('=')[1] || 0);
const CACHE_FILE = './location-cache.json';

// ─── SUPABASE ──────────────────────────────────────────────────────────────
const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

// ─── CACHE ─────────────────────────────────────────────────────────────────
let cache = {};
if (fs.existsSync(CACHE_FILE)) {
    cache = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf8'));
    console.log(`📦 Loaded ${Object.keys(cache).length} cached locations`);
}

function saveCache() {
    fs.writeFileSync(CACHE_FILE, JSON.stringify(cache, null, 2), 'utf8');
}

// ─── NORMALIZE COMPANY NAME ──────────────────────────────────────────────
function normalizeCompanyName(name) {
    if (!name) return '';
    return name
        .replace(/\s*(?:GmbH|AG|KG|SE|e\.V\.|UG|GbR|OHG|& Co\.|& Co|GmbH & Co\. KG|GmbH & Co. KG)\s*/gi, '')
        .replace(/\s+/g, ' ')
        .trim();
}

// ─── EXTRACT CITY ──────────────────────────────────────────────────────────
function extractCityFromText(text) {
    if (!text) return null;

    // Postal code + city: "12345 Berlin"
    const postalMatch = text.match(/\b(\d{4,5})\s+([A-ZÄÖÜ][a-zäöüß]+(?:[\s\-][A-ZÄÖÜ][a-zäöüß]+)?)\b/);
    if (postalMatch) return postalMatch[2];

    // City, postal code: "Berlin, 12345"
    const cityCommaMatch = text.match(/\b([A-ZÄÖÜ][a-zäöüß]+(?:[\s\-][A-ZÄÖÜ][a-zäöüß]+)?)\s*,\s*\d{4,5}\b/);
    if (cityCommaMatch) return cityCommaMatch[1];

    // City before Germany
    const germanyMatch = text.match(/\b([A-ZÄÖÜ][a-zäöüß]+(?:[\s\-][A-ZÄÖÜ][a-zäöüß]+)?)\s*(?:Germany|Deutschland)\b/);
    if (germanyMatch) return germanyMatch[1];

    // Common German cities
    const commonCities = ['Berlin','Hamburg','Munich','Cologne','Frankfurt','Stuttgart','Düsseldorf',
        'Dortmund','Essen','Leipzig','Dresden','Hanover','Nuremberg','Duisburg','Bochum','Wuppertal',
        'Bielefeld','Bonn','Mannheim','Karlsruhe','Wiesbaden','Mönchengladbach','Gelsenkirchen','Aachen',
        'Kiel','Magdeburg','Braunschweig','Chemnitz','Göttingen','Oberhausen','Hagen','Rostock','Kassel',
        'Saarbrücken','Augsburg','Ulm','Oldenburg','Potsdam','Halle','Erfurt','Jena','Ludwigshafen','Trier',
        'Freiburg','Heidelberg','Aalen','Offenburg','Reutlingen','Passau','Regensburg','Ingolstadt'];

    const words = text.split(/[\s,;]+/);
    for (const word of words) {
        if (commonCities.some(city => word.toLowerCase().includes(city.toLowerCase()))) {
            return word;
        }
    }

    // Any capitalized word (city-like)
    const cityMatch = text.match(/\b([A-ZÄÖÜ][a-zäöüß]{2,19}(?:\s[A-ZÄÖÜ][a-zäöüß]{2,19})?)\b/);
    if (cityMatch) {
        const candidate = cityMatch[1];
        const stopwords = ['Standort','Anfahrt','Service','Support','Produkte','Navigation','Home','Start'];
        if (!stopwords.includes(candidate) && candidate.length > 2) {
            return candidate;
        }
    }

    return null;
}

// ─── SEARCH LOCATION ──────────────────────────────────────────────────────
async function searchLocation(companyName) {
    const cacheKey = companyName.toLowerCase().trim();
    if (cache[cacheKey]) {
        console.log(`   📦 Cache hit: ${cache[cacheKey]}`);
        return cache[cacheKey];
    }

    const cleanName = normalizeCompanyName(companyName);
    const searchQueries = [
        `${cleanName} Adresse`,
        `${cleanName} Firmensitz`,
        `${cleanName} Standort`,
        `${cleanName} GmbH Adresse`
    ];

    for (const q of searchQueries) {
        console.log(`   🔍 Searching: "${q}"`);

        try {
            const response = await axios.get('https://serpapi.com/search', {
                params: {
                    api_key: process.env.SERPAPI_API_KEY,
                    q: q,
                    location: 'Germany',
                    hl: 'de',
                    gl: 'de',
                    engine: 'google',
                    num: 5
                },
                timeout: 15000
            });

            const data = response.data;
            let address = null;

            // Knowledge Graph
            if (data.knowledge_graph) {
                if (data.knowledge_graph.address) address = data.knowledge_graph.address;
                else if (data.knowledge_graph.mentioned) {
                    const arr = Array.isArray(data.knowledge_graph.mentioned) 
                        ? data.knowledge_graph.mentioned 
                        : [data.knowledge_graph.mentioned];
                    for (const item of arr) {
                        if (typeof item === 'string' && (item.includes('straße') || item.includes('strasse') || item.match(/\d{4,5}/))) {
                            address = item;
                            break;
                        }
                    }
                }
            }

            // Local Results
            if (!address && data.local_results && data.local_results.length > 0) {
                const local = data.local_results[0];
                if (local.address) address = local.address;
                if (local.snippet && local.snippet.includes('straße')) address = local.snippet;
            }

            // Organic Results
            if (!address && data.organic_results && data.organic_results.length > 0) {
                for (const result of data.organic_results) {
                    if (result.snippet && result.snippet.match(/\d{4,5}/)) {
                        address = result.snippet;
                        break;
                    }
                    if (result.rich_snippet && result.rich_snippet.address) {
                        address = result.rich_snippet.address;
                        break;
                    }
                }
            }

            // Answer Box
            if (!address && data.answer_box) {
                if (data.answer_box.address) address = data.answer_box.address;
                if (data.answer_box.snippet && data.answer_box.snippet.includes('straße')) address = data.answer_box.snippet;
            }

            if (address) {
                const city = extractCityFromText(address);
                if (city) {
                    cache[cacheKey] = city;
                    saveCache();
                    console.log(`   ✅ Found: ${city} (from: ${address.slice(0, 60)}...)`);
                    return city;
                }
            }

        } catch (err) {
            console.error(`   ❌ Search error: ${err.message}`);
        }

        await new Promise(r => setTimeout(r, 1000));
    }

    cache[cacheKey] = null;
    saveCache();
    console.log(`   ❌ No location found`);
    return null;
}

// ─── MAIN ──────────────────────────────────────────────────────────────────
async function run() {
    console.log('═'.repeat(70));
    console.log('📍 LOCATION BACKFILL – GOOGLE SERP API v3');
    console.log('═'.repeat(70));
    console.log(`   Dry run: ${DRY_RUN}`);
    console.log(`   Limit:  ${LIMIT || 'All'}`);
    console.log(`   Cache:  ${Object.keys(cache).length} entries\n`);

    let query = supabase
        .from('jobs')
        .select('id, title, company_name, location')
        .is('location', null);

    if (LIMIT > 0) query = query.limit(LIMIT);

    const { data: jobs, error } = await query;

    if (error) {
        console.error('❌ Supabase error:', error.message);
        return;
    }

    if (!jobs || jobs.length === 0) {
        console.log('✅ No jobs without location!');
        return;
    }

    console.log(`📋 Found ${jobs.length} jobs without location.\n`);

    const companies = {};
    for (const job of jobs) {
        if (!job.company_name) continue;
        const key = job.company_name.toLowerCase().trim();
        if (!companies[key]) {
            companies[key] = { company_name: job.company_name, job_ids: [] };
        }
        companies[key].job_ids.push(job.id);
    }

    console.log(`📊 Unique companies: ${Object.keys(companies).length}\n`);

    let updated = 0, skipped = 0, count = 0;

    for (const [key, data] of Object.entries(companies)) {
        count++;
        console.log(`[${count}/${Object.keys(companies).length}] ${data.company_name}`);

        if (DRY_RUN) {
            updated += data.job_ids.length;
            continue;
        }

        const location = await searchLocation(data.company_name);

        if (location && location !== 'null' && location.length > 1) {
            const { error: updateErr } = await supabase
                .from('jobs')
                .update({ location: location })
                .in('id', data.job_ids);

            if (updateErr) {
                console.error(`   ❌ Update error: ${updateErr.message}`);
                skipped += data.job_ids.length;
            } else {
                updated += data.job_ids.length;
                console.log(`   ✅ Updated ${data.job_ids.length} jobs with location: ${location}`);
            }
        } else {
            skipped += data.job_ids.length;
        }

        await new Promise(r => setTimeout(r, 1500));
    }

    console.log(`\n📊 SUMMARY`);
    console.log(`   Jobs processed:    ${jobs.length}`);
    console.log(`   Companies:         ${Object.keys(companies).length}`);
    console.log(`   ✅ Updated:        ${updated}`);
    console.log(`   ⏭️  Skipped:       ${skipped}`);
    console.log(`   📦 Cache entries:  ${Object.keys(cache).length}`);

    if (DRY_RUN) console.log(`\n⚠️ DRY RUN – No changes made.`);

    saveCache();
}

process.on('SIGINT', () => { saveCache(); process.exit(0); });

run().catch(console.error);
