require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const { chromium } = require('playwright');
const axios = require('axios');
const cheerio = require('cheerio');
const crypto = require('crypto');
const { CRAWLER_TIMEOUTS } = require('../src/utils/crawler-timeouts');
const { enrichJobForStorage } = require('../src/utils/job-enrichment');
const { runCompaniesInBatches } = require('../src/utils/company-batch-runner');

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

// ─── HELPER: generate external_hash ──────────────────────────────────────
function generateExternalHash(companyId, externalJobId) {
    if (!companyId || !externalJobId) return null;
    return crypto.createHash('sha256')
        .update(`${companyId}:${externalJobId}`)
        .digest('hex')
        .slice(0, 64);
}

// ─── Remote Type Detection ──────────────────────────────────────────────
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

// ─── CUSTOM CRAWLER HELPERS ──────────────────────────────────────────────
async function customCrawlerFetchPage(url, retries = 2) {
    for (let attempt = 0; attempt <= retries; attempt++) {
        let browser;
        try {
            browser = await chromium.launch({ headless: true });
            const page = await browser.newPage();
            await page.setExtraHTTPHeaders({
                'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8'
            });
            await page.goto(url, { waitUntil: 'networkidle', timeout: CRAWLER_TIMEOUTS.NAVIGATION_TIMEOUT_MS });
            const html = await page.content();
            await browser.close();
            return html;
        } catch (err) {
            console.log(`   ⚠️ Playwright attempt ${attempt + 1} failed: ${err.message}`);
            if (browser) {
                try { await browser.close(); } catch (_) {}
            }
            if (attempt === retries) {
                try {
                    const response = await axios.get(url, {
                        timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
                        headers: { 'User-Agent': 'Mozilla/5.0' }
                    });
                    return response.data;
                } catch (axiosErr) {
                    console.log(`   ❌ Axios also failed: ${axiosErr.message}`);
                    return null;
                }
            }
            await new Promise(r => setTimeout(r, 3000));
        }
    }
    return null;
}

function customCrawlerExtractLinks(html, baseUrl) {
    const $ = cheerio.load(html);
    const links = [];

    const jobKeywords = [
        'job', 'jobs', 'karriere', 'karrier', 'career', 'careers',
        'stelle', 'stellen', 'stellenangebote', 'offene-stellen',
        'vakanz', 'position', 'positionen', 'ausbildung',
        'praktikum', 'bewerbung', 'vacancies', 'vacancy',
        'mitarbeiter', 'fachkraft', 'führungskraft', 'leitung',
        'entwickler', 'engineer', 'manager', 'consultant'
    ];

    $('a').each((_, el) => {
        const href = $(el).attr('href');
        const text = $(el).text().toLowerCase().trim();
        if (!href || href.includes('#') || href.includes('mailto:') || href.includes('tel:')) return;
        const hrefLower = href.toLowerCase();
        if (jobKeywords.some(kw => hrefLower.includes(kw) || text.includes(kw))) {
            let fullUrl = href;
            if (!href.startsWith('http')) {
                try {
                    fullUrl = new URL(href, baseUrl).href;
                } catch (e) { return; }
            }
            links.push(fullUrl);
        }
    });

    const unique = [...new Set(links)];
    const filtered = unique.filter(href =>
        !/impressum|datenschutz|agb|cookie|kontakt|about|team|news|blog|unternehmen|über-uns|karriere-übersicht/i.test(href)
    );
    return filtered.length > 0 ? filtered.slice(0, 30) : unique.slice(0, 30);
}

async function customCrawlerScrapeJob(url) {
    const html = await customCrawlerFetchPage(url);
    if (!html) return null;
    const $ = cheerio.load(html);
    const title = $('title').text().trim() || 'Untitled';

    const selectors = [
        '.job-description', '.job-details', '.description', '.content',
        '#job-description', '.job-content', '[class*="job-description"]',
        '[class*="job-detail"]', '[class*="description"]', 'article',
        '.main-content', '#content', '.text-content', '.post-content',
        '.entry-content', '.job__description', '.job-listing__description',
        '[itemprop="description"]', '[itemprop="jobDescription"]',
        '[class*="stellenanzeige"]', '[class*="stelle"]', '[class*="anzeige"]',
        '[class*="aufgaben"]', '[class*="profil"]', '[class*="anforderung"]'
    ];
    let description = '';
    for (const selector of selectors) {
        const text = $(selector).text().trim();
        if (text && text.length > 200) {
            description = text;
            break;
        }
    }
    if (!description) {
        description = $('body').text()
            .split('\n')
            .map(l => l.trim())
            .filter(l => l.length > 20)
            .filter(l => !/impressum|datenschutz|agb|cookie|footer|menu|navigation|copyright|©/.test(l))
            .join('\n')
            .slice(0, 5000);
    }
    const location = $('.location, .office, .city, .job-location').first().text().trim() || null;
    return { title, description, location };
}

// ─── SmartRecruiters Processing ──────────────────────────────────────────
async function processSmartRecruitersCompany(company) {
    console.log(`\n🔍 Processing: ${company.Name}`);
    console.log(`   URL: ${company.detected_career_url}`);

    let smartDomain = null;
    let companySlug = null;

    if (company.detected_career_url && company.detected_career_url.includes('smartrecruiters.com')) {
        let match = company.detected_career_url.match(/https?:\/\/([^.]+)\.careers\.smartrecruiters\.com/);
        if (match && match[1] !== 'www') {
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
                timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
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
                if (match && match[1] !== 'www') {
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
                if (match && match[1] !== 'www') {
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
        const candidate = company.Name
            .toLowerCase()
            .replace(/\s+/g, '')
            .replace(/[^a-z0-9]/g, '')
            .replace(/gmbh|ag|kg|co|ek|gmbhcokg/g, '')
            .trim();

        if (candidate.length >= 4) {
            companySlug = candidate;
            smartDomain = `${companySlug}.careers.smartrecruiters.com`;
            console.log(`   ⚠️ Using guessed slug: ${companySlug}`);
        } else {
            console.log(`   ❌ Could not find valid SmartRecruiters slug for ${company.Name} (candidate too short) – using custom crawler fallback`);
            return await fallbackToCustomCrawler(company);
        }
    }

    console.log(`   ✅ Slug: ${companySlug}`);
    console.log(`   ✅ SmartRecruiters Domain: ${smartDomain}`);

    const jobs = [];

    // ─── LAYER 1: API ─────────────────────────────────────────────────────
    try {
        const apiUrl = `https://api.smartrecruiters.com/v1/companies/${companySlug}/jobs`;
        const response = await axios.get(apiUrl, {
            timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
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
        console.log(`   ✅ Layer 1 (API): Found ${jobs.length} jobs`);
    } catch (err) {
        if (err.response?.status === 429) {
            console.log(`   ⚠️ API rate limited (429) – waiting 10s...`);
            await new Promise(r => setTimeout(r, 10000));
            try {
                const apiUrl = `https://api.smartrecruiters.com/v1/companies/${companySlug}/jobs`;
                const response = await axios.get(apiUrl, {
                    timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
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
                console.log(`   ✅ Layer 1 (API) retry: Found ${jobs.length} jobs`);
            } catch (e) {
                console.log(`   ❌ API retry also failed: ${e.message}`);
            }
        } else if (err.response?.status === 404) {
            console.log(`   ⚠️ API returned 404 – trying alternative endpoint...`);
            try {
                const altApiUrl = `https://api.smartrecruiters.com/jobs?company=${companySlug}`;
                const response = await axios.get(altApiUrl, {
                    timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
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
                console.log(`   ✅ Layer 1 (alt API): Found ${jobs.length} jobs`);
            } catch (e) {
                console.log(`   ❌ Alternative API also failed: ${e.message}`);
            }
        } else {
            console.log(`   ⚠️ API failed: ${err.message}`);
        }
    }

    // ─── LAYER 2: HTML Scraping ──────────────────────────────────────────
    if (jobs.length === 0) {
        console.log(`   🔄 Layer 2: HTML scraping...`);
        try {
            const pageUrl = smartDomain.startsWith('http') ? smartDomain : `https://${smartDomain}`;
            const response = await axios.get(pageUrl, {
                timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
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
                        try {
                            fullUrl = new URL(link, pageUrl).href;
                        } catch (e) { fullUrl = `https://${companySlug}.careers.smartrecruiters.com/jobs/${id}`; }
                    } else if (!link) {
                        fullUrl = `https://${companySlug}.careers.smartrecruiters.com/jobs/${id}`;
                    }
                    jobs.push({
                        external_job_id: id,
                        title,
                        location,
                        employment_type: null,
                        remote_type: detectRemoteType(description),
                        raw_description: description.slice(0, 5000),
                        apply_url: fullUrl,
                        ats_source: 'smartrecruiters'
                    });
                });
                console.log(`   ✅ Layer 2: Found ${jobs.length} jobs via HTML scraping`);
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
                    console.log(`   ✅ Layer 2: Found ${jobs.length} jobs via script JSON parsing`);
                } else {
                    console.log(`   ⚠️ Layer 2: No job listings found`);
                }
            }
        } catch (err) {
            console.log(`   ❌ Layer 2 failed: ${err.message}`);
        }
    }

    // ─── LAYER 3: CUSTOM CRAWLER FALLBACK ──────────────────────────────
    if (jobs.length === 0) {
        return await fallbackToCustomCrawler(company, jobs);
    }

    // ─── Enrich jobs with company_name and external_hash ────────────────
    const enrichedJobs = jobs.map(job => ({
        ...job,
        company_name: company.Name,
        external_hash: generateExternalHash(company.Id, job.external_job_id) || job.external_job_id
    }));

    return { company, jobs: enrichedJobs, error: jobs.length === 0 ? 'No jobs found' : null };
}

// ─── Custom crawler fallback ──────────────────────────────────────────────
async function fallbackToCustomCrawler(company, existingJobs = []) {
    const jobs = [...existingJobs];
    console.log(`   🔄 Layer 3: Custom crawler fallback...`);

    const html = await customCrawlerFetchPage(company.detected_career_url);
    if (html) {
        const links = customCrawlerExtractLinks(html, company.detected_career_url);
        if (links.length > 0) {
            let saved = 0;
            for (const link of links) {
                if (jobs.some(j => j.apply_url === link)) continue;
                const jobData = await customCrawlerScrapeJob(link);
                if (jobData) {
                    const id = Buffer.from(link).toString('base64').slice(0, 50);
                    jobs.push({
                        external_job_id: id,
                        title: jobData.title,
                        location: jobData.location,
                        employment_type: null,
                        remote_type: detectRemoteType(jobData.description),
                        raw_description: jobData.description.slice(0, 5000),
                        apply_url: link,
                        ats_source: 'smartrecruiters'   // ← FIXED: use the adapter's name
                    });
                    saved++;
                }
            }
            if (saved > 0) {
                console.log(`   ✅ Layer 3: Found ${saved} jobs via custom crawler`);
            } else {
                console.log(`   ⚠️ Layer 3: No jobs found`);
            }
        } else {
            console.log(`   ⚠️ Layer 3: No job links found`);
        }
    } else {
        console.log(`   ❌ Layer 3: Failed to fetch page`);
    }

    // ─── Enrich jobs with company_name and external_hash ────────────────
    const enrichedJobs = jobs.map(job => ({
        ...job,
        company_name: company.Name,
        external_hash: generateExternalHash(company.Id, job.external_job_id) || job.external_job_id
    }));

    return { company, jobs: enrichedJobs, error: jobs.length === 0 ? 'No jobs found' : null };
}

// ─── MAIN RUNNER ──────────────────────────────────────────────────────────
async function run() {
    const { data: companies, error } = await supabase
        .from('companies')
        .select('"Id", "Name", "Website", detected_career_url')
        .eq('ats_type', 'smartrecruiters')
        .eq('crawl_status', 'pending');

    if (error) {
        console.error('❌ Supabase error:', error.message);
        return;
    }

    if (!companies || companies.length === 0) {
        console.log('No SmartRecruiters companies found.');
        return;
    }

    console.log(`📋 Processing ${companies.length} SmartRecruiters companies in batches of 10...\n`);

    const { jobs: totalJobs } = await runCompaniesInBatches(companies, {
        batchSize: parseInt(process.env.CRAWLER_COMPANY_BATCH_SIZE || '10', 10),
        label: 'SMARTRECRUITERS',
        handler: async (company, meta) => {
            try {
                const result = await processSmartRecruitersCompany(company);
                if (result.jobs.length === 0) {
                    console.log(`   ⚠️ [${meta.companyIndex}/${meta.companyTotal}] ${company.Name} | no jobs found`);
                    await supabase.from('companies')
                        .update({ crawl_status: 'failed' })
                        .eq('Id', company.Id);
                    return { status: 'no_jobs', jobs: [] };
                }

                for (const job of result.jobs) {
                    const storageJob = await enrichJobForStorage({
                        company_id: company.Id,
                        company_name: job.company_name || company.Name || null,
                        company_website: company.Website || null,
                        external_job_id: job.external_job_id,
                        external_hash: job.external_hash,
                        title: job.title,
                        location: job.location,
                        employment_type: job.employment_type,
                        remote_type: job.remote_type,
                        raw_description: job.raw_description,
                        apply_url: job.apply_url,
                        ats_source: 'smartrecruiters',
                        is_active: true
                    });

                    const { error: insertError } = await supabase
                        .from('jobs')
                        .upsert({
                            ...storageJob,
                            first_seen_at: new Date(),
                            last_seen_at: new Date()
                        }, { onConflict: 'company_id,external_job_id' });

                    if (insertError) {
                        console.error(`   ❌ Save error for job ${job.title}: ${insertError.message}`);
                    }
                }
                console.log(`   💾 [${meta.companyIndex}/${meta.companyTotal}] ${company.Name} | saved=${result.jobs.length}`);
                await supabase.from('companies')
                    .update({ crawl_status: 'completed' })
                    .eq('Id', company.Id);
                return { status: 'completed', jobs: result.jobs };
            } catch (err) {
                console.error(`   ⚠️ [${meta.companyIndex}/${meta.companyTotal}] ${company.Name} failed: ${err.message}`);
                await supabase.from('companies')
                    .update({ crawl_status: 'failed' })
                    .eq('Id', company.Id);
                return { status: 'failed', jobs: [] };
            }
        }
    });

    console.log(`\n✅ Done! Total SmartRecruiters jobs saved: ${totalJobs}`);
}

run().catch(console.error);
