require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

const axios = require('axios');
const cheerio = require('cheerio');

function detectRemoteType(description) {
    const text = (description || '').toLowerCase();
    if (text.includes('remote') || text.includes('homeoffice') || text.includes('100% remote') || text.includes('full remote')) {
        return 'remote';
    }
    if (text.includes('hybrid') || text.includes('teilweise remote') || text.includes('mobile work') || text.includes('flexibles arbeiten')) {
        return 'hybrid';
    }
    return 'onsite';
}

async function processUmantisCompany(company) {
    console.log(`\n🔍 Processing: ${company.Name}`);
    console.log(`   URL: ${company.detected_career_url}`);

    let umantisDomain = null;
    let companySlug = null;

    if (company.detected_career_url && (
        company.detected_career_url.includes('umantis.com') ||
        company.detected_career_url.includes('lumesse.com')
    )) {
        let match = company.detected_career_url.match(/https?:\/\/([^.]+)\.umantis\.com/);
        if (match) {
            companySlug = match[1];
            umantisDomain = `${companySlug}.umantis.com`;
        } else {
            match = company.detected_career_url.match(/umantis\.com\/([^\/?]+)/);
            if (match) {
                companySlug = match[1];
                umantisDomain = `recruiting.umantis.com/${companySlug}`;
            }
        }
        if (!companySlug) {
            match = company.detected_career_url.match(/https?:\/\/([^.]+)\.lumesse\.com/);
            if (match) {
                companySlug = match[1];
                umantisDomain = `${companySlug}.lumesse.com`;
            }
        }
    }

    if (!companySlug) {
        try {
            const response = await axios.get(company.detected_career_url, {
                timeout: 15000,
                headers: { 'User-Agent': 'Mozilla/5.0' }
            });
            const html = response.data;
            const $ = cheerio.load(html);

            let foundUrl = null;
            $('iframe[src*="umantis.com"], a[href*="umantis.com"], script[src*="umantis.com"], iframe[src*="lumesse.com"], a[href*="lumesse.com"], script[src*="lumesse.com"]').each((_, el) => {
                const src = $(el).attr('src') || $(el).attr('href') || '';
                if (src.includes('umantis.com') || src.includes('lumesse.com')) {
                    foundUrl = src;
                }
            });

            if (foundUrl) {
                let match = foundUrl.match(/https?:\/\/([^.]+)\.umantis\.com/);
                if (match) {
                    companySlug = match[1];
                    umantisDomain = `${companySlug}.umantis.com`;
                } else {
                    match = foundUrl.match(/umantis\.com\/([^\/?]+)/);
                    if (match) {
                        companySlug = match[1];
                        umantisDomain = `recruiting.umantis.com/${companySlug}`;
                    }
                }
                if (!companySlug) {
                    match = foundUrl.match(/https?:\/\/([^.]+)\.lumesse\.com/);
                    if (match) {
                        companySlug = match[1];
                        umantisDomain = `${companySlug}.lumesse.com`;
                    }
                }
            }

            if (!companySlug && html.includes('umantis.com')) {
                let match = html.match(/https?:\/\/([^.]+)\.umantis\.com/);
                if (match) {
                    companySlug = match[1];
                    umantisDomain = `${companySlug}.umantis.com`;
                } else {
                    match = html.match(/umantis\.com\/([^\/?]+)/);
                    if (match) {
                        companySlug = match[1];
                        umantisDomain = `recruiting.umantis.com/${companySlug}`;
                    }
                }
            }
            if (!companySlug && html.includes('lumesse.com')) {
                const match = html.match(/https?:\/\/([^.]+)\.lumesse\.com/);
                if (match) {
                    companySlug = match[1];
                    umantisDomain = `${companySlug}.lumesse.com`;
                }
            }
        } catch (err) {
            console.log(`   ⚠️ Could not fetch page: ${err.message}`);
        }
    }

    if (!companySlug) {
        let candidate = company.Name
            .toLowerCase()
            .replace(/[^a-z0-9]/g, '')
            .replace(/gmbh|ag|kg|co|e\.k\./g, '')
            .trim();
        if (candidate.length > 3) {
            companySlug = candidate;
            umantisDomain = `${companySlug}.umantis.com`;
            console.log(`   ⚠️ Using guessed slug: ${companySlug}`);
        } else {
            console.log(`   ❌ Could not find Umantis slug for ${company.Name}`);
            return { company, jobs: [], error: 'No slug found' };
        }
    }

    console.log(`   ✅ Slug: ${companySlug}`);
    console.log(`   ✅ Umantis Domain: ${umantisDomain}`);

    const jobs = [];

    try {
        const apiUrls = [
            `https://${companySlug}.umantis.com/api/jobs`,
            `https://${companySlug}.umantis.com/api/v1/jobs`,
            `https://${companySlug}.umantis.com/rest/jobs`,
            `https://recruiting.umantis.com/${companySlug}/api/jobs`,
            `https://${companySlug}.lumesse.com/api/jobs`,
        ];

        let apiSuccess = false;
        for (const apiUrl of apiUrls) {
            try {
                const response = await axios.get(apiUrl, {
                    timeout: 10000,
                    headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0' }
                });
                const data = response.data;
                let items = data.jobs || data.data || data;
                if (!Array.isArray(items)) items = [];

                for (const item of items) {
                    const description = (item.description || item.jobDescription || '');
                    jobs.push({
                        external_job_id: String(item.id || item.jobId || Math.random()),
                        title: item.title || item.name || item.jobTitle || 'Untitled',
                        location: item.location || item.office || item.city || null,
                        employment_type: item.employmentType || item.schedule || null,
                        remote_type: detectRemoteType(description),
                        raw_description: description.slice(0, 5000),
                        apply_url: `https://${companySlug}.umantis.com/jobs/${item.id || item.jobId}`,
                        ats_source: 'umantis'
                    });
                }
                if (jobs.length > 0) {
                    apiSuccess = true;
                    console.log(`   ✅ Found ${jobs.length} jobs via Umantis API (${apiUrl})`);
                    break;
                }
            } catch (e) {}
        }

        if (!apiSuccess && jobs.length === 0) {
            try {
                const xmlUrl = `https://${companySlug}.umantis.com/xml/jobs`;
                const response = await axios.get(xmlUrl, {
                    timeout: 15000,
                    headers: { 'User-Agent': 'Mozilla/5.0' }
                });
                const xml = response.data;
                const positions = xml.match(/<job[^>]*>([\s\S]*?)<\/job>/g) || [];
                for (const pos of positions) {
                    const getId = (tag) => {
                        const match = pos.match(new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, 'i'));
                        return match ? match[1].replace(/<!\[CDATA\[|\]\]>/g, '').trim() : null;
                    };
                    const id = getId('id') || String(Math.random());
                    const title = getId('title') || getId('name') || 'Untitled';
                    const location = getId('location') || getId('office') || null;
                    const description = getId('description') || getId('jobDescription') || '';
                    jobs.push({
                        external_job_id: id,
                        title: title,
                        location: location,
                        employment_type: null,
                        remote_type: detectRemoteType(description),
                        raw_description: description.slice(0, 5000),
                        apply_url: `https://${companySlug}.umantis.com/jobs/${id}`,
                        ats_source: 'umantis'
                    });
                }
                if (jobs.length > 0) {
                    console.log(`   ✅ Found ${jobs.length} jobs via Umantis XML feed`);
                }
            } catch (err) {
                console.log(`   ⚠️ XML feed failed: ${err.message}`);
            }
        }
    } catch (err) {
        if (err.response?.status === 429) {
            console.log(`   ⚠️ API rate limited (429) – waiting 10s...`);
            await new Promise(r => setTimeout(r, 10000));
            try {
                const apiUrl = `https://${companySlug}.umantis.com/api/jobs`;
                const response = await axios.get(apiUrl, {
                    timeout: 15000,
                    headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0' }
                });
                const data = response.data;
                let items = data.jobs || data.data || data;
                if (!Array.isArray(items)) items = [];
                for (const item of items) {
                    const description = (item.description || item.jobDescription || '');
                    jobs.push({
                        external_job_id: String(item.id || item.jobId || Math.random()),
                        title: item.title || item.name || item.jobTitle || 'Untitled',
                        location: item.location || item.office || item.city || null,
                        employment_type: item.employmentType || item.schedule || null,
                        remote_type: detectRemoteType(description),
                        raw_description: description.slice(0, 5000),
                        apply_url: `https://${companySlug}.umantis.com/jobs/${item.id || item.jobId}`,
                        ats_source: 'umantis'
                    });
                }
                console.log(`   ✅ Found ${jobs.length} jobs via Umantis API (retry)`);
            } catch (e) {
                console.log(`   ❌ API retry also failed: ${e.message}`);
            }
        } else {
            console.log(`   ⚠️ Umantis API failed: ${err.message}`);
        }
    }

    if (jobs.length === 0) {
        console.log(`   ⚠️ No jobs via API – falling back to HTML scraping...`);
        try {
            const pageUrl = umantisDomain.startsWith('http') ? umantisDomain : `https://${umantisDomain}`;
            const response = await axios.get(pageUrl, {
                timeout: 15000,
                headers: { 'User-Agent': 'Mozilla/5.0' }
            });
            const html = response.data;
            const $ = cheerio.load(html);
            const jobElements = $('.job, .position, .job-item, .job-listing, [data-job-id], .job-card, .job-posting, .vacancy, .job-offer');
            if (jobElements.length > 0) {
                jobElements.each((_, el) => {
                    const title = $(el).find('.title, .job-title, h2, h3, .job-name, .vacancy-title').first().text().trim() || 'Untitled';
                    const link = $(el).find('a').first().attr('href') || '';
                    const location = $(el).find('.location, .office, .city, .job-location').first().text().trim() || null;
                    const description = $(el).find('.description, .job-description, .job-text, .vacancy-description').first().text().trim() || '';
                    const id = $(el).attr('data-job-id') || $(el).attr('data-id') || $(el).attr('data-position-id') || String(Math.random());
                    let fullUrl = link;
                    if (link && !link.startsWith('http')) {
                        fullUrl = new URL(link, pageUrl).href;
                    } else if (!link) {
                        fullUrl = `https://${companySlug}.umantis.com/jobs/${id}`;
                    }
                    jobs.push({
                        external_job_id: id,
                        title: title,
                        location: location,
                        employment_type: null,
                        remote_type: detectRemoteType(description),
                        raw_description: description.slice(0, 5000),
                        apply_url: fullUrl,
                        ats_source: 'umantis'
                    });
                });
                console.log(`   ✅ Found ${jobs.length} jobs via HTML scraping`);
            } else {
                $('script').each((_, el) => {
                    const content = $(el).html() || '';
                    if (content.includes('jobs') && content.includes('"id"')) {
                        try {
                            const jsonMatch = content.match(/\{.*"jobs".*\}/);
                            if (jsonMatch) {
                                const data = JSON.parse(jsonMatch[0]);
                                const items = data.jobs || data.data || [];
                                if (Array.isArray(items)) {
                                    for (const item of items) {
                                        const description = (item.description || '');
                                        jobs.push({
                                            external_job_id: String(item.id || Math.random()),
                                            title: item.title || 'Untitled',
                                            location: item.location || null,
                                            employment_type: null,
                                            remote_type: detectRemoteType(description),
                                            raw_description: description.slice(0, 5000),
                                            apply_url: `https://${companySlug}.umantis.com/jobs/${item.id}`,
                                            ats_source: 'umantis'
                                        });
                                    }
                                }
                            }
                        } catch (e) {}
                    }
                });
                if (jobs.length > 0) {
                    console.log(`   ✅ Found ${jobs.length} jobs via script JSON parsing`);
                } else {
                    console.log(`   ⚠️ No job listings found via HTML scraping`);
                }
            }
        } catch (err) {
            console.log(`   ❌ HTML scraping failed: ${err.message}`);
        }
    }

    return { company, slug: companySlug, jobs, error: jobs.length === 0 ? 'No jobs found' : null };
}

async function run() {
    const { data: companies, error } = await supabase
        .from('companies')
        .select('"Id", "Name", detected_career_url')
        .eq('ats_type', 'umantis');

    if (error) {
        console.error('❌ Supabase error:', error.message);
        return;
    }

    if (!companies || companies.length === 0) {
        console.log('No Umantis companies found.');
        return;
    }

    console.log(`📋 Processing ${companies.length} Umantis companies...\n`);

    let totalJobs = 0;
    for (const company of companies) {
        const result = await processUmantisCompany(company);
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
                    remote_type: job.remote_type,
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

    console.log(`\n✅ Done! Total Umantis jobs saved: ${totalJobs}`);
}

run().catch(console.error);
