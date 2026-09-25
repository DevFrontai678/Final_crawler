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

// ─── CUSTOM CRAWLER HELPERS (IMPROVED) ────────────────────────────────

function getPotentialCareerUrls(baseUrl) {
    const paths = [
        '',
        '/careers',
        '/jobs',
        '/karriere',
        '/stellenangebote',
        '/en/careers',
        '/en/jobs',
        '/europe/jobs',
        '/europe/careers',
        '/de/karriere',
        '/de/stellen',
        '/company/careers',
        '/career',
        '/jobs/list',
        '/open-positions',
        '/vacancies',
    ];
    const urlSet = new Set();
    const result = [];
    for (const path of paths) {
        try {
            const url = new URL(path, baseUrl).href;
            if (!urlSet.has(url)) {
                urlSet.add(url);
                result.push(url);
            }
        } catch (_) {}
    }
    return result;
}

async function customCrawlerFetchPage(url, retries = 2) {
    let browser;
    for (let attempt = 0; attempt <= retries; attempt++) {
        try {
            browser = await chromium.launch({ headless: true });
            const page = await browser.newPage();
            await page.setExtraHTTPHeaders({
                'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            });
            await page.goto(url, { waitUntil: 'domcontentloaded', timeout: CRAWLER_TIMEOUTS.NAVIGATION_TIMEOUT_MS });
            await page.waitForSelector('a[href*="job"], a[href*="career"]', { timeout: CRAWLER_TIMEOUTS.SELECTOR_TIMEOUT_MS }).catch(() => {});
            const html = await page.content();
            return html;
        } catch (err) {
            console.log(`   ⚠️ Attempt ${attempt+1} failed for ${url}: ${err.message}`);
            if (attempt === retries) break;
            await new Promise(r => setTimeout(r, 2000 * (attempt + 1)));
        } finally {
            if (browser) await browser.close();
        }
    }
    return null;
}

function customCrawlerExtractLinks(html, baseUrl) {
    const $ = cheerio.load(html);
    const links = new Set();

    const jobKeywords = [
        'job', 'jobs', 'karriere', 'career', 'careers',
        'stelle', 'stellen', 'stellenangebote', 'offene-stellen',
        'vakanz', 'position', 'positionen',
        'mitarbeiter', 'fachkraft', 'leitung',
        'entwickler', 'engineer', 'manager', 'consultant',
        'offer', 'offers', 'apply', 'bewerben'
    ];

    const containers = [
        '.jobs', '.job-list', '.career-list', '.positions',
        '.vacancies', '.open-positions', '[class*="job"]',
        '[class*="career"]', '[class*="position"]', 'ul li a'
    ];

    containers.forEach(selector => {
        $(selector).find('a').each((_, el) => {
            const href = $(el).attr('href');
            const text = $(el).text().toLowerCase().trim();
            if (href && !href.startsWith('#') && !href.includes('mailto:')) {
                try {
                    const full = new URL(href, baseUrl).href;
                    if (jobKeywords.some(kw => href.includes(kw) || text.includes(kw))) {
                        links.add(full);
                    }
                } catch (_) {}
            }
        });
    });

    $('a').each((_, el) => {
        const href = $(el).attr('href');
        const text = $(el).text().toLowerCase().trim();
        if (!href || href.startsWith('#') || href.includes('mailto:') || href.includes('tel:')) return;
        const hrefLower = href.toLowerCase();
        if (jobKeywords.some(kw => hrefLower.includes(kw) || text.includes(kw))) {
            try {
                const full = new URL(href, baseUrl).href;
                links.add(full);
            } catch (_) {}
        }
    });

    const filtered = [...links].filter(href =>
        !/impressum|datenschutz|agb|cookie|kontakt|about|team|news|blog|unternehmen|über-uns/i.test(href)
    );

    return filtered.length ? filtered.slice(0, 50) : [...links].slice(0, 50);
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

// ─── Recruitee Processing ──────────────────────────────────────────────
async function processRecruiteeCompany(company) {
    console.log(`\n🔍 Processing: ${company.Name}`);
    console.log(`   URL: ${company.detected_career_url}`);

    let recruiteeDomain = null;
    let companySlug = null;

    if (company.detected_career_url && company.detected_career_url.includes('recruitee.com')) {
        let match = company.detected_career_url.match(/https?:\/\/([^.]+)\.recruitee\.com/);
        if (match && match[1] !== 'www') {
            companySlug = match[1];
            recruiteeDomain = `${companySlug}.recruitee.com`;
        } else {
            match = company.detected_career_url.match(/recruitee\.com\/companies\/([^\/?]+)/);
            if (match) {
                companySlug = match[1];
                recruiteeDomain = `recruitee.com/companies/${companySlug}`;
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
            $('iframe[src*="recruitee.com"], a[href*="recruitee.com"], script[src*="recruitee.com"]').each((_, el) => {
                const src = $(el).attr('src') || $(el).attr('href') || '';
                if (src.includes('recruitee.com')) {
                    foundUrl = src;
                }
            });

            if (foundUrl) {
                let match = foundUrl.match(/https?:\/\/([^.]+)\.recruitee\.com/);
                if (match && match[1] !== 'www') {
                    companySlug = match[1];
                    recruiteeDomain = `${companySlug}.recruitee.com`;
                } else {
                    match = foundUrl.match(/recruitee\.com\/companies\/([^\/?]+)/);
                    if (match) {
                        companySlug = match[1];
                        recruiteeDomain = `recruitee.com/companies/${companySlug}`;
                    }
                }
            }

            if (!companySlug && html.includes('recruitee.com')) {
                let match = html.match(/https?:\/\/([^.]+)\.recruitee\.com/);
                if (match && match[1] !== 'www') {
                    companySlug = match[1];
                    recruiteeDomain = `${companySlug}.recruitee.com`;
                } else {
                    match = html.match(/recruitee\.com\/companies\/([^\/?]+)/);
                    if (match) {
                        companySlug = match[1];
                        recruiteeDomain = `recruitee.com/companies/${companySlug}`;
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
            recruiteeDomain = `${companySlug}.recruitee.com`;
            console.log(`   ⚠️ Using guessed slug: ${companySlug}`);
        } else {
            console.log(`   ❌ Could not find Recruitee slug for ${company.Name}`);
            return { company, jobs: [], error: 'No slug found' };
        }
    }

    console.log(`   ✅ Slug: ${companySlug}`);
    console.log(`   ✅ Recruitee Domain: ${recruiteeDomain}`);

    const jobs = [];

    // ─── LAYER 1: API ─────────────────────────────────────────────────────
    const apiUrls = [
        `https://${companySlug}.recruitee.com/api/offers`,
        `https://api.recruitee.com/api/offers?company_id=${companySlug}`,
        `https://recruitee.com/api/offers?company=${companySlug}`,
        `https://${companySlug}.recruitee.com/jobs.json`
    ];

    for (const apiUrl of apiUrls) {
        try {
            const response = await axios.get(apiUrl, {
                timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
                headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0' }
            });
            const data = response.data;
            let items = data.offers || data.jobs || data.data || data;
            if (!Array.isArray(items)) items = [];

            for (const item of items) {
                const description = (item.description || item.jobDescription || '');
                jobs.push({
                    external_job_id: String(item.id || item.offerId || Math.random()),
                    title: item.title || item.name || item.jobTitle || 'Untitled',
                    location: item.location || item.office || item.city || null,
                    employment_type: item.employmentType || item.schedule || item.contract_type || null,
                    remote_type: detectRemoteType(description),
                    raw_description: description.slice(0, 5000),
                    apply_url: `https://${companySlug}.recruitee.com/offers/${item.id || item.offerId}`,
                    ats_source: 'recruitee'
                });
            }
            if (jobs.length > 0) {
                console.log(`   ✅ Layer 1 (API): Found ${jobs.length} jobs`);
                break;
            }
        } catch (err) {
            // try next endpoint
        }
    }

    // ─── LAYER 2: HTML Scraping ──────────────────────────────────────────
    if (jobs.length === 0) {
        console.log(`   🔄 Layer 2: HTML scraping...`);
        try {
            const pageUrl = recruiteeDomain.startsWith('http') ? recruiteeDomain : `https://${recruiteeDomain}`;
            const response = await axios.get(pageUrl, {
                timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
                headers: { 'User-Agent': 'Mozilla/5.0' }
            });
            const html = response.data;
            const $ = cheerio.load(html);

            const jobElements = $('.offer, .job, .position, .job-item, .job-listing, [data-offer-id], .job-card, .job-posting');
            if (jobElements.length > 0) {
                jobElements.each((_, el) => {
                    const title = $(el).find('.title, .job-title, h2, h3, .offer-title').first().text().trim() || 'Untitled';
                    const link = $(el).find('a').first().attr('href') || '';
                    const location = $(el).find('.location, .office, .city, .job-location').first().text().trim() || null;
                    const description = $(el).find('.description, .job-description, .offer-description').first().text().trim() || '';
                    const id = $(el).attr('data-offer-id') || $(el).attr('data-id') || $(el).attr('data-position-id') || String(Math.random());
                    let fullUrl = link;
                    if (link && !link.startsWith('http')) {
                        try {
                            fullUrl = new URL(link, pageUrl).href;
                        } catch (e) { fullUrl = `https://${companySlug}.recruitee.com/offers/${id}`; }
                    } else if (!link) {
                        fullUrl = `https://${companySlug}.recruitee.com/offers/${id}`;
                    }
                    jobs.push({
                        external_job_id: id,
                        title: title,
                        location: location,
                        employment_type: null,
                        remote_type: detectRemoteType(description),
                        raw_description: description.slice(0, 5000),
                        apply_url: fullUrl,
                        ats_source: 'recruitee'
                    });
                });
                console.log(`   ✅ Layer 2: Found ${jobs.length} jobs via HTML scraping`);
            } else {
                // Try script JSON parsing
                $('script').each((_, el) => {
                    const content = $(el).html() || '';
                    if (content.includes('offers') || content.includes('jobs')) {
                        try {
                            const jsonMatch = content.match(/\{.*"offers".*\}/);
                            if (jsonMatch) {
                                const data = JSON.parse(jsonMatch[0]);
                                const items = data.offers || data.jobs || data.data || [];
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
                                            apply_url: `https://${companySlug}.recruitee.com/offers/${item.id}`,
                                            ats_source: 'recruitee'
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

    // ─── LAYER 3: IMPROVED CUSTOM CRAWLER FALLBACK ──────────────────────
    if (jobs.length === 0) {
        console.log(`   🔄 Layer 3: Custom crawler fallback...`);

        let baseUrl;
        try {
            const parsed = new URL(company.detected_career_url);
            baseUrl = parsed.origin;
        } catch (_) {
            baseUrl = company.detected_career_url;
        }

        const candidateUrls = getPotentialCareerUrls(baseUrl);
        let found = false;

        for (const url of candidateUrls) {
            if (found) break;
            console.log(`   🔄 Trying: ${url}`);
            const html = await customCrawlerFetchPage(url);
            if (!html) continue;

            const links = customCrawlerExtractLinks(html, url);
            if (links.length === 0) {
                console.log(`   ⚠️ No job links found at ${url}`);
                continue;
            }

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
                        ats_source: 'recruitee'
                    });
                    saved++;
                }
            }
            if (saved > 0) {
                console.log(`   ✅ Layer 3: Found ${saved} jobs via custom crawler from ${url}`);
                found = true;
            } else {
                console.log(`   ⚠️ Layer 3: No jobs found at ${url}`);
            }
        }

        if (!found) {
            console.log(`   ❌ Layer 3: No jobs found after trying all candidate URLs`);
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

// ─── MAIN ─────────────────────────────────────────────────────────────────
async function run() {
    const { data: companies, error } = await supabase
        .from('companies')
        .select('"Id", "Name", "Website", detected_career_url')
        .eq('ats_type', 'recruitee')
        .eq('crawl_status', 'pending');

    if (error) {
        console.error('❌ Supabase error:', error.message);
        return;
    }

    if (!companies || companies.length === 0) {
        console.log('No Recruitee companies found.');
        return;
    }

    console.log(`📋 Processing ${companies.length} Recruitee companies...\n`);

    const { jobs: totalJobs } = await runCompaniesInBatches(companies, {
        batchSize: parseInt(process.env.CRAWLER_COMPANY_BATCH_SIZE || '10', 10),
        label: 'RECRUITEE',
        handler: async (company, meta) => {
            try {
                const result = await processRecruiteeCompany(company);
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
                        ats_source: 'recruitee',
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

    console.log(`\n✅ Done! Total Recruitee jobs saved: ${totalJobs}`);
}

run().catch(console.error);
