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

// ─── CUSTOM CRAWLER HELPERS ─────────────────────────────────────────────
// These act as a fallback for Rexx companies

async function customCrawlerFetchPage(url) {
    let browser;
    try {
        browser = await chromium.launch({ headless: true });
        const page = await browser.newPage();
        await page.setExtraHTTPHeaders({
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8'
        });
        await page.goto(url, { waitUntil: 'networkidle', timeout: CRAWLER_TIMEOUTS.NAVIGATION_TIMEOUT_MS });
        const html = await page.content();
        return html;
    } catch (err) {
        console.log(`   ⚠️ Custom crawler fetch error: ${err.message}`);
        return null;
    } finally {
        if (browser) await browser.close();
    }
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

// ─── Rexx Company Processing (with fallback layers) ──────────────────
async function processRexxCompany(company) {
    console.log(`\n🔍 Processing: ${company.Name}`);
    console.log(`   URL: ${company.detected_career_url}`);

    const careerUrl = company.detected_career_url;
    const jobs = [];

    // ─── LAYER 1: Try Rexx API ──────────────────────────────────────────
    let rexxApiUrl = null;
    try {
        const html = await axios.get(careerUrl, { timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS, headers: { 'User-Agent': 'Mozilla/5.0' } })
            .then(res => res.data)
            .catch(() => null);
        if (html) {
            const $ = cheerio.load(html);
            let foundUrl = null;
            $('iframe[src*="rexx"], a[href*="rexx"], script[src*="rexx"]').each((_, el) => {
                const src = $(el).attr('src') || $(el).attr('href') || '';
                if (src.includes('rexx')) {
                    foundUrl = src;
                }
            });
            if (foundUrl) {
                const match = foundUrl.match(/https?:\/\/([^/]+)/);
                if (match && (match[1].includes('rexx-recruitment') || match[1].includes('rexx-systems'))) {
                    rexxApiUrl = `https://${match[1]}`;
                    console.log(`   ✅ Found Rexx API URL: ${rexxApiUrl}`);
                }
            }
        }
    } catch (err) {
        // Ignore
    }

    if (rexxApiUrl) {
        try {
            const apiUrl = `${rexxApiUrl}/api/v1/jobs`;
            const response = await axios.get(apiUrl, {
                timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
                headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0' }
            });
            const data = response.data;
            let items = data.jobs || data.data || data;
            if (!Array.isArray(items)) items = [];
            for (const item of items) {
                const desc = (item.description || item.jobDescription || '');
                jobs.push({
                    external_job_id: String(item.id || item.jobId || Math.random()),
                    title: item.title || item.name || item.jobTitle || 'Untitled',
                    location: item.location || item.office || null,
                    employment_type: item.employmentType || item.schedule || null,
                    remote_type: detectRemoteType(desc),
                    raw_description: desc.slice(0, 5000),
                    apply_url: `${rexxApiUrl}/job/${item.id || item.jobId}`,
                    ats_source: 'rexx'
                });
            }
            if (jobs.length > 0) {
                console.log(`   ✅ Layer 1 (API): Found ${jobs.length} jobs`);
            }
        } catch (err) {
            console.log(`   ⚠️ Layer 1 (API) failed: ${err.message}`);
        }
    }

    // ─── LAYER 2: Rexx-specific scraping (if no API jobs) ──────────────
    if (jobs.length === 0) {
        console.log(`   🔄 Layer 2: Rexx-specific scraping...`);
        try {
        const html = await axios.get(careerUrl, {
            timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
                headers: { 'User-Agent': 'Mozilla/5.0' }
            }).then(res => res.data).catch(() => null);
            if (html) {
                const $ = cheerio.load(html);
                // Look for job links that might be specific to Rexx
                const links = [];
                $('a[href*="job"], a[href*="stelle"], a[href*="karriere"], a[href*="vakanz"], a[href*="position"]').each((_, el) => {
                    const href = $(el).attr('href');
                    if (href && !href.includes('#') && !href.includes('mailto:')) {
                        let fullUrl = href;
                        if (!href.startsWith('http')) {
                            try {
                                fullUrl = new URL(href, careerUrl).href;
                            } catch (e) { return; }
                        }
                        links.push(fullUrl);
                    }
                });
                const uniqueLinks = [...new Set(links)].slice(0, 20);
                let scraped = 0;
                for (const link of uniqueLinks) {
                    const jobHtml = await axios.get(link, {
                        timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
                        headers: { 'User-Agent': 'Mozilla/5.0' }
                    }).then(res => res.data).catch(() => null);
                    if (!jobHtml) continue;
                    const $j = cheerio.load(jobHtml);
                    const title = $j('title').text().trim() || 'Untitled';
                    const desc = $j('.job-description, .description, .content, article').text().trim();
                    if (desc.length < 100) continue;
                    const id = Buffer.from(link).toString('base64').slice(0, 50);
                    jobs.push({
                        external_job_id: id,
                        title: title,
                        location: null,
                        employment_type: null,
                        remote_type: detectRemoteType(desc),
                        raw_description: desc.slice(0, 5000),
                        apply_url: link,
                        ats_source: 'rexx'
                    });
                    scraped++;
                }
                if (scraped > 0) {
                    console.log(`   ✅ Layer 2: Found ${scraped} jobs via Rexx-specific scraping`);
                }
            }
        } catch (err) {
            console.log(`   ⚠️ Layer 2 failed: ${err.message}`);
        }
    }

    // ─── LAYER 3: CUSTOM CRAWLER FALLBACK (if still no jobs) ──────────
    if (jobs.length === 0) {
        console.log(`   🔄 Layer 3: Custom crawler fallback...`);
        const html = await customCrawlerFetchPage(careerUrl);
        if (html) {
            const links = customCrawlerExtractLinks(html, careerUrl);
            if (links.length > 0) {
                let saved = 0;
                for (const link of links) {
                    // Avoid duplicates
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
                            ats_source: 'rexx'
                        });
                        saved++;
                    }
                }
                console.log(`   ✅ Layer 3 (Custom Crawler): Found ${saved} jobs`);
            } else {
                console.log(`   ⚠️ Layer 3: No job links found`);
            }
        } else {
            console.log(`   ❌ Layer 3: Failed to fetch page`);
        }
    }

    // ─── Enrich jobs with company_name ────────────────────────────────
    const enrichedJobs = jobs.map(job => ({
        ...job,
        company_name: company.Name,
        external_hash: generateExternalHash(company.Id, job.external_job_id) || job.external_job_id
    }));

    return { company, jobs: enrichedJobs, error: jobs.length === 0 ? 'No jobs found' : null };
}

// ─── Main Runner ──────────────────────────────────────────────────────────
async function run() {
    const { data: companies, error } = await supabase
        .from('companies')
        .select('"Id", "Name", "Website", detected_career_url')
        .eq('ats_type', 'rexx')
        .eq('crawl_status', 'pending');

    if (error) {
        console.error('❌ Supabase error:', error.message);
        return;
    }

    if (!companies || companies.length === 0) {
        console.log('No Rexx companies found.');
        return;
    }

    console.log(`📋 Processing ${companies.length} Rexx companies...\n`);

    const { jobs: totalJobs } = await runCompaniesInBatches(companies, {
        batchSize: parseInt(process.env.CRAWLER_COMPANY_BATCH_SIZE || '10', 10),
        label: 'REXX',
        handler: async (company, meta) => {
            try {
                const result = await processRexxCompany(company);
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
                        ats_source: 'rexx',
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

    console.log(`\n✅ Done! Total Rexx jobs saved: ${totalJobs}`);
}

run().catch(console.error);
