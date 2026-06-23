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

async function processSmartRecruitersCompany(company) {
    console.log(`\n🔍 Processing: ${company.Name}`);
    console.log(`   URL: ${company.detected_career_url}`);

    let smartDomain = null;
    let companySlug = null;

    if (company.detected_career_url && company.detected_career_url.includes('smartrecruiters.com')) {
        let match = company.detected_career_url.match(/https?:\/\/([^.]+)\.careers\.smartrecruiters\.com/);
        if (match) {
            companySlug = match[1];
            smartDomain = `${companySlug}.careers.smartrecruiters.com`;
        } else {
            match = company.detected_career_url.match(/smartrecruiters\.com\/([^\/?]+)/);
            if (match) {
                companySlug = match[1];
                smartDomain = `careers.smartrecruiters.com/${companySlug}`;
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
            $('iframe[src*="smartrecruiters.com"], a[href*="smartrecruiters.com"], script[src*="smartrecruiters.com"]').each((_, el) => {
                const src = $(el).attr('src') || $(el).attr('href') || '';
                if (src.includes('smartrecruiters.com')) {
                    foundUrl = src;
                }
            });

            if (foundUrl) {
                let match = foundUrl.match(/https?:\/\/([^.]+)\.careers\.smartrecruiters\.com/);
                if (match) {
                    companySlug = match[1];
                    smartDomain = `${companySlug}.careers.smartrecruiters.com`;
                } else {
                    match = foundUrl.match(/smartrecruiters\.com\/([^\/?]+)/);
                    if (match) {
                        companySlug = match[1];
                        smartDomain = `careers.smartrecruiters.com/${companySlug}`;
                    }
                }
            }

            if (!companySlug && html.includes('smartrecruiters.com')) {
                let match = html.match(/https?:\/\/([^.]+)\.careers\.smartrecruiters\.com/);
                if (match) {
                    companySlug = match[1];
                    smartDomain = `${companySlug}.careers.smartrecruiters.com`;
                } else {
                    match = html.match(/smartrecruiters\.com\/([^\/?]+)/);
                    if (match) {
                        companySlug = match[1];
                        smartDomain = `careers.smartrecruiters.com/${companySlug}`;
                    }
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
            smartDomain = `${companySlug}.careers.smartrecruiters.com`;
            console.log(`   ⚠️ Using guessed slug: ${companySlug}`);
        } else {
            console.log(`   ❌ Could not find SmartRecruiters slug for ${company.Name}`);
            return { company, jobs: [], error: 'No slug found' };
        }
    }

    console.log(`   ✅ Slug: ${companySlug}`);
    console.log(`   ✅ SmartRecruiters Domain: ${smartDomain}`);

    const jobs = [];

    try {
        const apiUrl = `https://api.smartrecruiters.com/v1/companies/${companySlug}/jobs`;
        const response = await axios.get(apiUrl, {
            timeout: 15000,
            headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0' }
        });
        const data = response.data;
        let items = data.jobs || data.data || [];
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
                apply_url: `https://${companySlug}.careers.smartrecruiters.com/jobs/${item.id || item.jobId}`,
                ats_source: 'smartrecruiters'
            });
        }
        console.log(`   ✅ Found ${jobs.length} jobs via SmartRecruiters API`);
    } catch (err) {
        if (err.response?.status === 429) {
            console.log(`   ⚠️ API rate limited (429) – waiting 10s...`);
            await new Promise(r => setTimeout(r, 10000));
            try {
                const apiUrl = `https://api.smartrecruiters.com/v1/companies/${companySlug}/jobs`;
                const response = await axios.get(apiUrl, {
                    timeout: 15000,
                    headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0' }
                });
                const data = response.data;
                let items = data.jobs || data.data || [];
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
                        apply_url: `https://${companySlug}.careers.smartrecruiters.com/jobs/${item.id || item.jobId}`,
                        ats_source: 'smartrecruiters'
                    });
                }
                console.log(`   ✅ Found ${jobs.length} jobs via SmartRecruiters API (retry)`);
            } catch (e) {
                console.log(`   ❌ API retry also failed: ${e.message}`);
            }
        } else if (err.response?.status === 404) {
            console.log(`   ⚠️ SmartRecruiters API returned 404 – trying alternative endpoint...`);
            try {
                const altApiUrl = `https://api.smartrecruiters.com/jobs?company=${companySlug}`;
                const response = await axios.get(altApiUrl, {
                    timeout: 15000,
                    headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0' }
                });
                const data = response.data;
                let items = data.jobs || data.data || [];
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
                        apply_url: `https://${companySlug}.careers.smartrecruiters.com/jobs/${item.id || item.jobId}`,
                        ats_source: 'smartrecruiters'
                    });
                }
                console.log(`   ✅ Found ${jobs.length} jobs via alternative SmartRecruiters API`);
            } catch (e) {
                console.log(`   ❌ Alternative API also failed: ${e.message}`);
            }
        } else {
            console.log(`   ⚠️ SmartRecruiters API failed: ${err.message}`);
        }
    }

    if (jobs.length === 0) {
        console.log(`   ⚠️ No jobs via API – falling back to HTML scraping...`);
        try {
            const pageUrl = smartDomain.startsWith('http') ? smartDomain : `https://${smartDomain}`;
            const response = await axios.get(pageUrl, {
                timeout: 15000,
                headers: { 'User-Agent': 'Mozilla/5.0' }
            });
            const html = response.data;
            const $ = cheerio.load(html);
            const jobElements = $('.job, .position, .job-item, .job-listing, [data-job-id], .job-card, .job-posting, .job-offer');
            if (jobElements.length > 0) {
                jobElements.each((_, el) => {
                    const title = $(el).find('.title, .job-title, h2, h3, .job-name, .job-title').first().text().trim() || 'Untitled';
                    const link = $(el).find('a').first().attr('href') || '';
                    const location = $(el).find('.location, .office, .city, .job-location').first().text().trim() || null;
                    const description = $(el).find('.description, .job-description, .job-text, .job-summary').first().text().trim() || '';
                    const id = $(el).attr('data-job-id') || $(el).attr('data-id') || $(el).attr('data-position-id') || String(Math.random());
                    let fullUrl = link;
                    if (link && !link.startsWith('http')) {
                        fullUrl = new URL(link, pageUrl).href;
                    } else if (!link) {
                        fullUrl = `https://${companySlug}.careers.smartrecruiters.com/jobs/${id}`;
                    }
                    jobs.push({
                        external_job_id: id,
                        title: title,
                        location: location,
                        employment_type: null,
                        remote_type: detectRemoteType(description),
                        raw_description: description.slice(0, 5000),
                        apply_url: fullUrl,
                        ats_source: 'smartrecruiters'
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
                                            apply_url: `https://${companySlug}.careers.smartrecruiters.com/jobs/${item.id}`,
                                            ats_source: 'smartrecruiters'
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
        .eq('ats_type', 'smartrecruiters');

    if (error) {
        console.error('❌ Supabase error:', error.message);
        return;
    }

    if (!companies || companies.length === 0) {
        console.log('No SmartRecruiters companies found.');
        return;
    }

    console.log(`📋 Processing ${companies.length} SmartRecruiters companies...\n`);

    let totalJobs = 0;
    for (const company of companies) {
        const result = await processSmartRecruitersCompany(company);
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

    console.log(`\n✅ Done! Total SmartRecruiters jobs saved: ${totalJobs}`);
}

run().catch(console.error);
