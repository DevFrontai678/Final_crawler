require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');   // WebSocket fix

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

const axios = require('axios');
const cheerio = require('cheerio');

// ─── Improved Personio Adapter ──────────────────────────────────────────────

async function processPersonioCompany(company) {
    console.log(`\n🔍 Processing: ${company.Name}`);
    console.log(`   URL: ${company.detected_career_url}`);

    let slug = null;
    let personioUrl = company.detected_career_url;

    // 1. If the URL already contains 'personio', extract slug directly
    if (company.detected_career_url && company.detected_career_url.includes('personio')) {
        const match = company.detected_career_url.match(/https?:\/\/([^.]+)\.(?:career\.)?personio\.(?:de|com)/);
        if (match) {
            slug = match[1];
            personioUrl = `https://${slug}.jobs.personio.de`;
        }
    }

    // 2. If not found, fetch the page and search for Personio links
    if (!slug) {
        try {
            const response = await axios.get(company.detected_career_url, {
                timeout: 15000,
                headers: { 'User-Agent': 'Mozilla/5.0' }
            });
            const html = response.data;
            const $ = cheerio.load(html);

            // Search in iframes, a tags, script tags
            let foundUrl = null;
            $('iframe[src*="personio"], a[href*="personio"], script[src*="personio"]').each((_, el) => {
                const src = $(el).attr('src') || $(el).attr('href') || '';
                if (src.includes('personio')) {
                    foundUrl = src;
                }
            });

            if (foundUrl) {
                const match = foundUrl.match(/https?:\/\/([^.]+)\.(?:career\.)?personio\.(?:de|com)/);
                if (match) {
                    slug = match[1];
                    personioUrl = `https://${slug}.jobs.personio.de`;
                }
            }

            // Also try to find a link with "jobs.personio.de"
            if (!slug) {
                $('a').each((_, el) => {
                    const href = $(el).attr('href') || '';
                    if (href.includes('jobs.personio.de')) {
                        const match = href.match(/https?:\/\/([^.]+)\.jobs\.personio\.de/);
                        if (match) {
                            slug = match[1];
                            personioUrl = `https://${slug}.jobs.personio.de`;
                        }
                    }
                });
            }

            // Check if the page itself is a Personio page (e.g., it has a Personio widget)
            if (!slug && html.includes('personio')) {
                // Try to extract from script tags
                const scriptMatches = html.match(/https?:\/\/([^.]+)\.(?:career\.)?personio\.(?:de|com)/g);
                if (scriptMatches && scriptMatches.length > 0) {
                    const m = scriptMatches[0].match(/https?:\/\/([^.]+)\.(?:career\.)?personio\.(?:de|com)/);
                    if (m) {
                        slug = m[1];
                        personioUrl = `https://${slug}.jobs.personio.de`;
                    }
                }
            }
        } catch (err) {
            console.log(`   ⚠️ Could not fetch page: ${err.message}`);
        }
    }

    if (!slug) {
        console.log(`   ❌ Could not find Personio slug for ${company.Name}`);
        return { company, jobs: [], error: 'No slug found' };
    }

    console.log(`   ✅ Slug: ${slug}`);
    console.log(`   ✅ Personio URL: ${personioUrl}`);

    // ─── Fetch jobs ──────────────────────────────────────────────────────────

    const jobs = [];
    let apiCalled = false;

    // Method 1: XML API
    try {
        const xmlUrl = `https://${slug}.jobs.personio.de/xml`;
        const response = await axios.get(xmlUrl, {
            timeout: 15000,
            headers: { 'User-Agent': 'Mozilla/5.0' }
        });
        apiCalled = true;
        const xml = response.data;
        const positions = xml.match(/<position[^>]*>([\s\S]*?)<\/position>/g) || [];
        for (const pos of positions) {
            const getId = (tag) => {
                const match = pos.match(new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, 'i'));
                return match ? match[1].replace(/<!\[CDATA\[|\]\]>/g, '').trim() : null;
            };
            const id = getId('id') || String(Math.random());
            const title = getId('name') || getId('title') || 'Untitled';
            const location = getId('office') || getId('location') || null;
            const employmentType = getId('schedule') || null;
            const description = getId('jobDescriptions') || getId('description') || '';
            jobs.push({
                external_job_id: id,
                title: title,
                location: location,
                employment_type: employmentType,
                raw_description: description.slice(0, 5000),
                apply_url: `https://${slug}.jobs.personio.de/job/${id}`,
                ats_source: 'personio'
            });
        }
        console.log(`   ✅ Found ${jobs.length} jobs via XML API`);
    } catch (err) {
        if (err.response?.status === 429) {
            console.log(`   ⚠️ XML API rate limited (429) – waiting 10s...`);
            await new Promise(r => setTimeout(r, 10000));
            // Retry once
            try {
                const xmlUrl = `https://${slug}.jobs.personio.de/xml`;
                const response = await axios.get(xmlUrl, {
                    timeout: 15000,
                    headers: { 'User-Agent': 'Mozilla/5.0' }
                });
                apiCalled = true;
                const xml = response.data;
                const positions = xml.match(/<position[^>]*>([\s\S]*?)<\/position>/g) || [];
                for (const pos of positions) {
                    const getId = (tag) => {
                        const match = pos.match(new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, 'i'));
                        return match ? match[1].replace(/<!\[CDATA\[|\]\]>/g, '').trim() : null;
                    };
                    const id = getId('id') || String(Math.random());
                    const title = getId('name') || getId('title') || 'Untitled';
                    const location = getId('office') || getId('location') || null;
                    const employmentType = getId('schedule') || null;
                    const description = getId('jobDescriptions') || getId('description') || '';
                    jobs.push({
                        external_job_id: id,
                        title: title,
                        location: location,
                        employment_type: employmentType,
                        raw_description: description.slice(0, 5000),
                        apply_url: `https://${slug}.jobs.personio.de/job/${id}`,
                        ats_source: 'personio'
                    });
                }
                console.log(`   ✅ Found ${jobs.length} jobs via XML API (retry)`);
            } catch (e) {
                console.log(`   ❌ XML API retry also failed: ${e.message}`);
            }
        } else {
            console.log(`   ⚠️ XML API failed: ${err.message}`);
        }
    }

    // Method 2: JSON API (fallback)
    if (jobs.length === 0) {
        try {
            const jsonUrl = `https://${slug}.jobs.personio.de/api/v1/positions`;
            const response = await axios.get(jsonUrl, {
                timeout: 15000,
                headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0' }
            });
            apiCalled = true;
            const data = response.data;
            const items = data.data || [];
            for (const item of items) {
                const attrs = item.attributes || {};
                jobs.push({
                    external_job_id: String(item.id),
                    title: attrs.name || 'Untitled',
                    location: attrs.office?.attributes?.name || null,
                    employment_type: attrs.schedule || null,
                    raw_description: (attrs.jobDescriptions || []).map(d => d.value).join('\n').slice(0, 5000),
                    apply_url: `https://${slug}.jobs.personio.de/job/${item.id}`,
                    ats_source: 'personio'
                });
            }
            console.log(`   ✅ Found ${jobs.length} jobs via JSON API`);
        } catch (err) {
            if (err.response?.status === 429) {
                console.log(`   ⚠️ JSON API rate limited (429) – skipping`);
            } else {
                console.log(`   ❌ JSON API also failed: ${err.message}`);
            }
        }
    }

    if (jobs.length === 0 && !apiCalled) {
        console.log(`   ⚠️ No jobs found for ${company.Name}`);
    }

    return { company, slug, jobs, error: jobs.length === 0 ? 'No jobs found' : null };
}

async function run() {
    // Fetch Personio companies
    const { data: companies, error } = await supabase
        .from('companies')
        .select('"Id", "Name", detected_career_url')
        .eq('ats_type', 'personio');

    if (error) {
        console.error('❌ Supabase error:', error.message);
        return;
    }

    if (!companies || companies.length === 0) {
        console.log('No Personio companies found.');
        return;
    }

    console.log(`📋 Processing ${companies.length} Personio companies...\n`);

    let totalJobs = 0;
    for (const company of companies) {
        const result = await processPersonioCompany(company);
        if (result.jobs.length === 0) continue;

        for (const job of result.jobs) {
            const { error: insertError } = await supabase
                .from('jobs')
                .upsert({
                    company_id: company.Id,
                    external_job_id: job.external_job_id,
                    title: job.title,
                    location: job.location,
                    employment_type: job.employment_type,
                    raw_description: job.raw_description,
                    apply_url: job.apply_url,
                    is_active: true,
                    first_seen_at: new Date(),
                    last_seen_at: new Date()
                }, { onConflict: 'company_id,external_job_id' });

            if (insertError) {
                console.error(`   ❌ Save error for job ${job.title}: ${insertError.message}`);
            }
        }
        totalJobs += result.jobs.length;
        console.log(`   💾 Saved ${result.jobs.length} jobs for ${company.Name}`);
        await new Promise(r => setTimeout(r, 500));
    }

    console.log(`\n✅ Done! Total Personio jobs saved: ${totalJobs}`);
}

run().catch(console.error);
