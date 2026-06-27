const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');   // 🔥 WebSocket fix
const axios = require('axios');
require('dotenv').config();

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }   // 🔥 Transport option
);

// OpenStreetMap Nominatim (Free, 1 request per second)
async function geocode(location) {
    if (!location) return null;
    try {
        const response = await axios.get('https://nominatim.openstreetmap.org/search', {
            params: {
                q: location + ', Germany',
                format: 'json',
                limit: 1,
            },
            headers: { 'User-Agent': 'CustomerMatchingCrawler/1.0' }
        });
        if (response.data && response.data.length > 0) {
            return {
                lat: parseFloat(response.data[0].lat),
                lng: parseFloat(response.data[0].lon)
            };
        }
    } catch (err) {
        console.log(`  ⚠️ Geocode failed for "${location}": ${err.message}`);
    }
    return null;
}

async function run() {
    // Fetch jobs with location but no coordinates
    const { data: jobs, error } = await supabase
        .from('jobs')
        .select('id, location')
        .not('location', 'is', null)
        .is('location_lat', null);

    if (error) {
        console.error('❌ Error:', error.message);
        return;
    }

    console.log(`📍 Geocoding ${jobs.length} jobs...\n`);

    let updated = 0;
    let failed = 0;

    for (const job of jobs) {
        const coords = await geocode(job.location);
        if (coords) {
            const { error: updateErr } = await supabase
                .from('jobs')
                .update({
                    location_lat: coords.lat,
                    location_lng: coords.lng
                })
                .eq('id', job.id);

            if (!updateErr) {
                updated++;
                console.log(`  ✅ ${job.location} → ${coords.lat}, ${coords.lng}`);
            } else {
                failed++;
                console.log(`  ❌ Update failed for ${job.location}: ${updateErr.message}`);
            }
        } else {
            failed++;
            console.log(`  ⚠️ No coords for ${job.location}`);
        }
        // Rate limit (1 request per second)
        await new Promise(r => setTimeout(r, 1000));
    }

    console.log(`\n✅ Updated ${updated}/${jobs.length} jobs with coordinates.`);
    console.log(`❌ Failed: ${failed}`);
}

run().catch(console.error);
