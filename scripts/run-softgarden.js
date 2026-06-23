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

// ─── Softgarden Adapter ────────────────────────────────────────────────────

async function processSoftgardenCompany(company) {
    console.log(`\n🔍 Processing: ${company.Name}`);
    console.log(`   URL: ${company.detected_career_url}`);

    let userId = null;
    let projectId = null;

    try {
        const response = await axios.get(company.detected_career_url, {
            timeout: 15000,
            headers: { 'User-Agent': 'Mozilla/5.0' }
        });
        const html = response.data;
        const $ = cheerio.load(html);

        // 1. Look for softgarden subdomain in links/iframes/scripts
        let softgardenUrl = null;
        $('a, iframe, script').each((_, el) => {
            const src = $(el).attr('src') || $(el).attr('href') || '';
            if (src.includes('softgarden') || src.includes('career.softgarden')) {
                softgardenUrl = src;
            }
        });

        // 2. If found, extract slug and fetch IDs
        if (softgardenUrl) {
            const match = softgardenUrl.match(/https?:\/\/([^.]+)\.(?:career\.)?softgarden\.(?:de|io)/);
            if (match) {
                const slug = match[1];
                try {
                    const sgResponse = await axios.get(`https://${slug}.career.softgarden.de`, {
                        timeout: 15000,
                        headers: { 'User-Agent': 'Mozilla/5.0' }
                    });
                    const sgHtml = sgResponse.data;
                    const uMatch = sgHtml.match(/"userId"\s*:\s*"([a-f0-9-]{36})"/);
                    const pMatch = sgHtml.match(/"projectId"\s*:\s*"([a-f0-9-]{36})"/);
                    if (uMatch) userId = uMatch[1];
                    if (pMatch) projectId = pMatch[1];
                } catch (e) {
                    console.log(`   ⚠️ Could not fetch subdomain page: ${e.message}`);
                }
            }
        }

        // 3. If not found, scan HTML directly for userId/projectId
        if (!userId || !projectId) {
            const uMatch = html.match(/"userId"\s*:\s*"([a-f0-9-]{36})"/);
            const pMatch = html.match(/"projectId"\s*:\s*"([a-f0-9-]{36})"/);
            if (uMatch) userId = uMatch[1];
            if (pMatch) projectId = pMatch[1];
        }

        // 4. If still not found, try apiKey approach
        if (!userId || !projectId) {
            const apiKeyMatch = html.match(/apiKey=([a-f0-9-]+)/);
            if (apiKeyMatch) {
                const apiKey = apiKeyMatch[1];
                try {
                    const widgetUrl = `https://pcw-api.softgarden.de/widgets/widget?apiKey=${apiKey}`;
                    const widgetResponse = await axios.get(widgetUrl, { timeout: 10000 });
                    const widgetData = JSON.stringify(widgetResponse.data);
                    const uM = widgetData.match(/"userId":"([a-f0-9-]{36})"/);
                    const pM = widgetData.match(/"projectId":"([a-f0-9-]{36})"/);
                    if (uM) userId = uM[1];
                    if (pM) projectId = pM[1];
                } catch (e) {
                    console.log(`   ⚠️ Widget API failed: ${e.message}`);
                }
            }
        }

        // 5. Try consent URL method
        if (!userId || !projectId) {
            const consentMatch = html.match(/softgarden\.(?:de|io)\/consent\/([a-f0-9-]{36})/);
            if (consentMatch) {
                userId = consentMatch[1];
                projectId = consentMatch[1];
            }
        }

    } catch (err) {
        console.log(`   ⚠️ Error extracting IDs: ${err.message}`);
    }

    if (!userId || !projectId) {
        console.log(`   ❌ Could not find Softgarden IDs for ${company.Name}`);
        return { company, jobs: [], error: 'No IDs found' };
    }

    console.log(`   ✅ userId: ${userId}`);
    console.log(`   ✅ projectId: ${projectId}`);

    // ─── Fetch jobs ──────────────────────────────────────────────────────────

    const jobs = [];
    try {
        const apiUrl = 'https://pcw-api.softgarden.de/widgets/job-list/job-ads';
        const response = await axios.post(apiUrl, {
            userId,
            projectId,
            locale: 'de',
            numberOfJobsOnPage: 9999999,
            pageNumber: '1',
            isGetFilters: true,
            isActiveCustomJobPages: true,
            isForCurrentLocale: false,
            isUseLayoutsOfSubsidiaries: false,
            listState: {
                search: '',
                disableSearchInDescription: false,
                location: { osmLocation: '', range: 25, coords: [] }
            },
            filterStatus: {
                careerLevel: false,
                category: false,
                partnership: false,
                region: false,
                location: false
            }
        }, {
            headers: { 'Content-Type': 'application/json', 'User-Agent': 'Mozilla/5.0' },
            timeout: 15000
        });

        const data = response.data;
        const ads = data.jobAds || data.jobs || data.data || [];
        for (const ad of ads) {
            jobs.push({
                external_job_id: String(ad.id || ad.jobAdId || ad.externalId),
                title: ad.jobTitle || ad.title || ad.name || 'Untitled',
                location: ad.location?.city || ad.city || ad.locationName || null,
                employment_type: ad.workTime || ad.employmentType || null,
                raw_description: ad.jobDescription || ad.description || '',
                apply_url: ad.applyUrl || ad.applicationUrl || null,
                ats_source: 'softgarden'
            });
        }
        console.log(`   ✅ Found ${jobs.length} jobs via API`);
    } catch (err) {
        console.log(`   ❌ API error: ${err.message}`);
    }

    return { company, userId, projectId, jobs, error: jobs.length === 0 ? 'No jobs found' : null };
}

async function run() {
    // Fetch Softgarden companies
    const { data: companies, error } = await supabase
        .from('companies')
        .select('"Id", "Name", detected_career_url')
        .eq('ats_type', 'softgarden');

    if (error) {
        console.error('❌ Supabase error:', error.message);
        return;
    }

    if (!companies || companies.length === 0) {
        console.log('No Softgarden companies found.');
        return;
    }

    console.log(`📋 Processing ${companies.length} Softgarden companies...\n`);

    let totalJobs = 0;
    for (const company of companies) {
        const result = await processSoftgardenCompany(company);
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

    console.log(`\n✅ Done! Total Softgarden jobs saved: ${totalJobs}`);
}

run().catch(console.error);
