const axios = require('axios');
const cheerio = require('cheerio');
require('dotenv').config();

// ─── HELPER: ACCEPT COOKIES ON PAGE ──────────────────────────────────────
async function acceptCookies(page) {
  const cookieSelectors = [
    'button[aria-label*="cookie"]',
    'button[aria-label*="Cookie"]',
    'button[id*="cookie"]',
    'button[class*="cookie"]',
    'button:has-text("Accept")',
    'button:has-text("Accept all")',
    'button:has-text("Zustimmen")',
    'button:has-text("Alle akzeptieren")',
    'button:has-text("OK")',
    'a:has-text("Accept")',
    'a:has-text("Zustimmen")',
    '#cookie-consent-accept',
    '.cookie-accept-button',
    '.cookie-consent-accept',
    '[data-cy="cookie-accept"]',
  ];

  for (const selector of cookieSelectors) {
    try {
      const acceptBtn = await page.locator(selector).first();
      if (await acceptBtn.isVisible({ timeout: 1500 })) {
        await acceptBtn.click();
        console.log('    🍪 Accepted cookies');
        return true;
      }
    } catch (e) {}
  }
  return false;
}

// ─── HTML SE CONFIG PARSE KARO — STATIC ───────────────────────────────────
function parseConfigFromHtml(html) {
  const $ = cheerio.load(html);

  const uMatch = html.match(/"userId"\s*:\s*"([a-f0-9-]{36})"/);
  const pMatch = html.match(/"projectId"\s*:\s*"([a-f0-9-]{36})"/);
  const pgMatch = html.match(/"pageId"\s*:\s*"([a-f0-9-]{36})"/);

  if (uMatch && pMatch) {
    console.log(`    ✅ Method 1: JSON pattern`);
    return {
      userId: uMatch[1],
      projectId: pMatch[1],
      pageId: pgMatch ? pgMatch[1] : null
    };
  }

  const jsU = html.match(/userId['":\s]+['"]([a-f0-9-]{36})['"]/);
  const jsP = html.match(/projectId['":\s]+['"]([a-f0-9-]{36})['"]/);
  const jsPg = html.match(/pageId['":\s]+['"]([a-f0-9-]{36})['"]/);

  if (jsU && jsP) {
    console.log(`    ✅ Method 2: JS variable`);
    return {
      userId: jsU[1],
      projectId: jsP[1],
      pageId: jsPg ? jsPg[1] : null
    };
  }

  let scriptConfig = null;
  $('script').each((_, el) => {
    const content = $(el).html() || '';
    if (!content.includes('userId')) return;

    const su = content.match(/"userId"\s*:\s*"([a-f0-9-]{36})"/);
    const sp = content.match(/"projectId"\s*:\s*"([a-f0-9-]{36})"/);
    const spg = content.match(/"pageId"\s*:\s*"([a-f0-9-]{36})"/);

    if (su && sp) {
      scriptConfig = {
        userId: su[1],
        projectId: sp[1],
        pageId: spg ? spg[1] : null
      };
    }
  });

  if (scriptConfig) {
    console.log(`    ✅ Method 3: Script tag`);
    return scriptConfig;
  }

  const cronMatch = html.match(
    /cron\?apiKey=[^&"']+&userId=([a-f0-9-]{36})(?:[^"']*?)projectId=([a-f0-9-]{36})/
  );
  if (cronMatch) {
    console.log(`    ✅ Method 4: Cron URL`);
    return {
      userId: cronMatch[1],
      projectId: cronMatch[2],
      pageId: null
    };
  }

  return null;
}

// ─── GLOBAL BROWSER ──────────────────────────────────────────────────────────
let globalBrowser = null;

async function getBrowser() {
  if (!globalBrowser || !globalBrowser.isConnected()) {
    const { chromium } = require('playwright');
    console.log(`    🔄 Launching Softgarden browser instance...`);
    globalBrowser = await chromium.launch({ headless: true });
  }
  return globalBrowser;
}

async function closeSoftgardenBrowser() {
  if (globalBrowser) {
    await globalBrowser.close().catch(() => {});
    globalBrowser = null;
  }
}

// ─── FETCH HTML WITH CONTENT VALIDATION + SCRAPERAPI FALLBACK ──────────────
async function fetchHtmlWithFallback(url) {
  // 1. Try static axios first
  let html = null;
  let staticSuccess = false;

  try {
    const response = await axios.get(url, {
      timeout: 15000,
      headers: { 'User-Agent': 'Mozilla/5.0' },
      maxRedirects: 5
    });
    html = response.data;
    staticSuccess = true;
  } catch (staticErr) {
    console.log(`    ⚠️ Static failed: ${staticErr.message}`);
  }

  // Check if static HTML contains meaningful job content
  if (staticSuccess && html) {
    const $ = cheerio.load(html);
    const hasJobContent = 
      html.includes('softgarden') ||
      html.includes('vacancies') ||
      html.includes('job') ||
      html.includes('karriere') ||
      html.includes('stelle') ||
      html.includes('bewerbung') ||
      $('a[href*="job"]').length > 0 ||
      $('a[href*="vacancies"]').length > 0 ||
      $('.job, .job-item, .job-card').length > 0;

    if (hasJobContent) {
      console.log(`    ✅ Static fetch returned valid job content`);
      return html;
    } else {
      console.log(`    ⚠️ Static fetch returned HTML but no job content — falling through`);
    }
  }

  // 2. Try Playwright with cookie handling
  let context = null;
  let page = null;
  try {
    const browser = await getBrowser();
    context = await browser.newContext({
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      locale: 'de-DE',
    });
    page = await context.newPage();
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
    await acceptCookies(page);
    await page.waitForTimeout(2000);
    const playwrightHtml = await page.content();

    // Validate content again
    const $ = cheerio.load(playwrightHtml);
    const hasJobContent = 
      playwrightHtml.includes('softgarden') ||
      playwrightHtml.includes('vacancies') ||
      playwrightHtml.includes('job') ||
      playwrightHtml.includes('karriere') ||
      playwrightHtml.includes('stelle') ||
      playwrightHtml.includes('bewerbung') ||
      $('a[href*="job"]').length > 0 ||
      $('a[href*="vacancies"]').length > 0 ||
      $('.job, .job-item, .job-card').length > 0;

    if (hasJobContent) {
      console.log(`    ✅ Playwright returned valid job content`);
      return playwrightHtml;
    } else {
      console.log(`    ⚠️ Playwright HTML also has no job content — falling through`);
    }
  } catch (pwErr) {
    console.log(`    ⚠️ Playwright failed: ${pwErr.message}`);
  } finally {
    if (page) await page.close().catch(() => {});
    if (context) await context.close().catch(() => {});
  }

  // 3. 🔥 Last Resort: ScraperAPI
  try {
    console.log(`    🔄 Trying ScraperAPI fallback...`);
    const apiKey = process.env.SCRAPERAPI_API_KEY;
    if (!apiKey) {
      console.log(`    ⚠️ SCRAPERAPI_API_KEY not set in .env`);
      return null;
    }

    const encodedUrl = encodeURIComponent(url);
    const apiUrl = `https://api.scraperapi.com/?api_key=${apiKey}&url=${encodedUrl}&render=true&country_code=de&premium=true`;
    
    const response = await axios.get(apiUrl, {
      timeout: 30000,
      headers: { 'Accept': 'text/html' }
    });

    if (response.status === 200 && response.data) {
      // Validate content (ScraperAPI should render properly, but still check)
      const $ = cheerio.load(response.data);
      const hasJobContent = 
        response.data.includes('softgarden') ||
        response.data.includes('vacancies') ||
        response.data.includes('job') ||
        response.data.includes('karriere') ||
        response.data.includes('stelle') ||
        response.data.includes('bewerbung') ||
        $('a[href*="job"]').length > 0 ||
        $('a[href*="vacancies"]').length > 0 ||
        $('.job, .job-item, .job-card').length > 0;

      if (hasJobContent) {
        console.log(`    ✅ ScraperAPI returned valid job content`);
        return response.data;
      } else {
        console.log(`    ⚠️ ScraperAPI returned HTML but no job content`);
        return null;
      }
    } else {
      console.log(`    ⚠️ ScraperAPI returned status: ${response.status}`);
    }
  } catch (saErr) {
    console.log(`    ❌ ScraperAPI failed: ${saErr.message}`);
  }

  return null;
}

// ─── PLAYWRIGHT SE CONFIG NIKALO ──────────────────────────────────────────
async function extractConfigWithPlaywright(url) {
  let context = null;
  let page = null;

  try {
    console.log(`    Playwright launching...`);
    const browser = await getBrowser();
    context = await browser.newContext({
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      locale: 'de-DE',
    });
    page = await context.newPage();

    let capturedConfig = null;

    page.on('request', request => {
      const reqUrl = request.url();
      const postData = request.postData();

      if (reqUrl.includes('job-list/job-ads') && postData) {
        try {
          const body = JSON.parse(postData);
          if (body.userId && body.projectId) {
            console.log(`    ✅ Captured from network!`);
            capturedConfig = {
              userId: body.userId,
              projectId: body.projectId,
              pageId: body.pageId || null
            };
          }
        } catch (e) {}
      }
    });

    await page.goto(url, {
      waitUntil: 'networkidle',
      timeout: 60000
    });

    await acceptCookies(page);
    await page.waitForTimeout(2000);

    if (capturedConfig) return capturedConfig;

    const html = await page.content();
    const htmlConfig = parseConfigFromHtml(html);
    if (htmlConfig) {
      console.log(`    ✅ Found in rendered HTML!`);
      return htmlConfig;
    }

    const jsConfig = await page.evaluate(() => {
      const result = {};

      if (window.__NEXT_DATA__) {
        const data = JSON.stringify(window.__NEXT_DATA__);
        const uMatch = data.match(/"userId":"([a-f0-9-]{36})"/);
        const pMatch = data.match(/"projectId":"([a-f0-9-]{36})"/);
        const pgMatch = data.match(/"pageId":"([a-f0-9-]{36})"/);

        if (uMatch) result.userId = uMatch[1];
        if (pMatch) result.projectId = pMatch[1];
        if (pgMatch) result.pageId = pgMatch[1];
      }

      if (window.userId) result.userId = window.userId;
      if (window.projectId) result.projectId = window.projectId;
      if (window.pageId) result.pageId = window.pageId;

      if (window.__store__) {
        const state = JSON.stringify(window.__store__.getState());
        const uMatch = state.match(/"userId":"([a-f0-9-]{36})"/);
        const pMatch = state.match(/"projectId":"([a-f0-9-]{36})"/);
        if (uMatch) result.userId = uMatch[1];
        if (pMatch) result.projectId = pMatch[1];
      }

      return result.userId && result.projectId ? result : null;
    });

    if (jsConfig) {
      console.log(`    ✅ Found in JS window!`);
      return jsConfig;
    }

    console.log(`    ❌ Playwright bhi fail hua`);
    return null;

  } catch (err) {
    console.log(`    Playwright error: ${err.message}`);
    return null;
  } finally {
    if (page) await page.close().catch(() => {});
    if (context) await context.close().catch(() => {});
  }
}

// ─── URL SE CONFIG NIKALO ──────────────────────────────────────────────────
async function extractSoftgardenConfig(url) {
  try {
    console.log(`    Static fetch: ${url}`);
    const response = await axios.get(url, {
      timeout: 15000,
      headers: { 'User-Agent': 'Mozilla/5.0' },
      maxRedirects: 5
    });

    const config = parseConfigFromHtml(response.data);
    if (config) return config;

  } catch (err) {
    console.log(`    Static failed: ${err.message}`);
  }

  return await extractConfigWithPlaywright(url);
}

// ─── KNOWN FALSE-POSITIVE SUBDOMAINS ──────────────────────────────────────
const EXCLUDED_SUBDOMAINS = [
  'certificate',
  'datagroup',
  'commerzdirektservice',
  'hegemann-gruppe',
  'next'
];

function isExcludedSlug(slug) {
  return EXCLUDED_SUBDOMAINS.some(ex => slug.toLowerCase().includes(ex));
}

// ─── MAIN ID EXTRACTION ────────────────────────────────────────────────────
async function extractSoftgardenIds(careerPageUrl) {
  try {
    console.log(`    Extracting from: ${careerPageUrl}`);

    console.log(`    Trying direct page first...`);
    const directConfig = await extractSoftgardenConfig(careerPageUrl);
    if (directConfig) {
      console.log(`    ✅ Found directly on career page!`);
      return directConfig;
    }

    const response = await axios.get(careerPageUrl, {
      timeout: 15000,
      headers: { 'User-Agent': 'Mozilla/5.0' },
      maxRedirects: 5
    });

    const html = response.data;
    const $ = cheerio.load(html);

    const allLinks = [];
    $('a, iframe, script').each((_, el) => {
      const href = $(el).attr('href') ||
                   $(el).attr('src') ||
                   $(el).attr('data-src') || '';
      if (href) allLinks.push(href);
    });

    const softgardenLinks = allLinks.filter(l => l.includes('softgarden'));
    console.log(`    Softgarden links: ${softgardenLinks.length}`);
    softgardenLinks.forEach(l => console.log(`      → ${l}`));

    for (const link of allLinks) {
      const match = link.match(/https?:\/\/([^.]+)\.career\.softgarden\.de/);
      if (match && !isExcludedSlug(match[1])) {
        console.log(`    Found .de subdomain: ${match[1]}`);
        const config = await extractSoftgardenConfig(
          `https://${match[1]}.career.softgarden.de`
        );
        if (config) return config;
      } else if (match) {
        console.log(`    ⏭️ Skipping known junk subdomain: ${match[1]}`);
      }
    }

    for (const link of allLinks) {
      const match = link.match(/https?:\/\/([^./]+)\.softgarden\.io/);
      if (match && !isExcludedSlug(match[1])) {
        console.log(`    Found .io subdomain: ${match[1]}`);
        const config = await extractSoftgardenConfig(
          `https://${match[1]}.softgarden.io`
        );
        if (config) return config;
      } else if (match) {
        console.log(`    ⏭️ Skipping known junk subdomain: ${match[1]}`);
      }
    }

    console.log(`    ❌ No valid Softgarden config found`);
    return null;

  } catch (err) {
    console.log(`    Error: ${err.message}`);
    return null;
  }
}

// ─── NAYA: PUBLIC jobs.feed.json ──────────────────────────────────────────
async function fetchJobsFeedJson(baseUrl) {
  try {
    const feedUrl = `${baseUrl.replace(/\/$/, '')}/jobs.feed.json`;
    const response = await axios.get(feedUrl, {
      timeout: 10000,
      headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json' }
    });

    const data = response.data;
    let items = [];

    if (data.dataFeedElement) items = data.dataFeedElement;
    else if (data.jobs) items = data.jobs;
    else if (data.data) items = data.data;

    const map = new Map();

    for (const entry of items) {
      const job = entry.item || entry;
      if (!job) continue;

      const id = job.identifier?.value || job.id || job.jobId || job.jobPostingId;
      if (!id) continue;

      map.set(String(id), {
        title: job.title || job.jobTitle || null,
        raw_description: job.description || job.jobDescription || null,
        apply_url: job.url || job.applicationUrl || job.applyUrl || null,
        employment_type: job.employmentType || job.workTime || null,
        location: job.location || job.jobLocation?.address?.addressLocality || job.city || null,
        datePosted: job.datePosted || job.validThrough || job.dateCreated || job.publicationDate || null
      });
    }

    if (map.size > 0) {
      console.log(`    ✅ jobs.feed.json mila: ${map.size} jobs full data ke saath`);
    }
    return map;
  } catch (err) {
    return new Map();
  }
}

// ─── DISCOVER SOFTGARDEN FEED MAP ──────────────────────────────────────────
async function discoverSoftgardenFeedMap(careerPageUrl) {
  let feedMap = await fetchJobsFeedJson(careerPageUrl);
  if (feedMap.size > 0) return feedMap;

  try {
    const response = await axios.get(careerPageUrl, {
      timeout: 15000,
      headers: { 'User-Agent': 'Mozilla/5.0' },
      maxRedirects: 5
    });

    const html = response.data;
    const $ = cheerio.load(html);
    const allLinks = [];
    $('a, iframe, script').each((_, el) => {
      const href = $(el).attr('href') || $(el).attr('src') || $(el).attr('data-src') || '';
      if (href) allLinks.push(href);
    });

    for (const link of allLinks) {
      const deMatch = link.match(/https?:\/\/([^.]+)\.career\.softgarden\.de/);
      if (deMatch && !isExcludedSlug(deMatch[1])) {
        const map = await fetchJobsFeedJson(`https://${deMatch[1]}.career.softgarden.de`);
        if (map.size > 0) return map;
      }
      const ioMatch = link.match(/https?:\/\/([^./]+)\.softgarden\.io/);
      if (ioMatch && !isExcludedSlug(ioMatch[1])) {
        const map = await fetchJobsFeedJson(`https://${ioMatch[1]}.softgarden.io`);
        if (map.size > 0) return map;
      }
    }

    return new Map();

  } catch (err) {
    return new Map();
  }
}

// ─── 🔥 CUSTOM SOFTGARDEN SCRAPER ──────────────────────────────────────────
async function customSoftgardenScraper(careerPageUrl) {
  console.log(`    🔄 Custom Softgarden scraper fallback...`);

  try {
    const html = await fetchHtmlWithFallback(careerPageUrl);
    if (!html) {
      console.log(`    ❌ Could not fetch page`);
      return [];
    }

    const $ = cheerio.load(html);
    const jobs = [];

    $('a[href*="vacancies"], a[href*="job"], a[href*="stelle"], a[href*="karriere"], a[href*="bewerbung"]').each((_, el) => {
      let href = $(el).attr('href');
      if (!href) return;

      if (!href.startsWith('http')) {
        const baseUrl = new URL(careerPageUrl);
        href = new URL(href, baseUrl).href;
      }

      if (!href.includes('softgarden')) return;
      if (href.includes('login') || href.includes('register') || href.includes('imprint') || href.includes('data-security')) return;

      const title = $(el).text().trim();
      if (!title || title.length < 3) return;

      const titleLower = title.toLowerCase();
      const linkLower = href.toLowerCase();
      if (titleLower === 'stellenangebote' || 
          titleLower === 'bereits mitarbeiter?' || 
          titleLower.includes('abonnieren') ||
          linkLower.includes('?1.-.jobsearch') ||
          linkLower.includes('internallink') ||
          linkLower.includes('jobabOlink')) {
        return;
      }

      const idMatch = href.match(/\/job\/(\d+)/);
      if (!idMatch) return;

      jobs.push({
        external_job_id: idMatch[1],
        title: title,
        raw_description: null,
        apply_url: href,
        location: null,
        employment_type: null,
        department: null,
        ats_source: 'custom_softgarden_scraper'
      });
    });

    if (jobs.length === 0) {
      $('.job, .job-item, .job-card, [class*="job"], [class*="vacancy"]').each((_, el) => {
        const link = $(el).find('a').first();
        let href = link.attr('href');
        if (!href) return;

        if (!href.startsWith('http')) {
          const baseUrl = new URL(careerPageUrl);
          href = new URL(href, baseUrl).href;
        }

        if (!href.includes('softgarden')) return;
        if (href.includes('login') || href.includes('register')) return;

        const idMatch = href.match(/\/job\/(\d+)/);
        if (!idMatch) return;

        const title = link.text().trim() || $(el).text().trim().split('\n')[0];
        if (!title || title.length < 3) return;

        jobs.push({
          external_job_id: idMatch[1],
          title: title,
          raw_description: null,
          apply_url: href,
          location: null,
          employment_type: null,
          department: null,
          ats_source: 'custom_softgarden_scraper'
        });
      });
    }

    console.log(`    ✅ Custom scraper found ${jobs.length} jobs`);
    return jobs;

  } catch (err) {
    console.log(`    ❌ Custom scraper error: ${err.message}`);
    return [];
  }
}

// ─── GENERIC FALLBACK CRAWLER ──────────────────────────────────────────────
function extractJobLinksGeneric(html, baseUrl) {
  const $ = cheerio.load(html);
  const links = [];

  $('a[href*="job"], a[href*="stelle"], a[href*="karriere"], a[href*="bewerbung"], a[href*="vakanz"], a[href*="offene"], a[href*="position"], a[href*="ausbildung"], a[href*="praktikum"], a[href*="vacancies"]').each((_, el) => {
    let href = $(el).attr('href');
    if (href && !href.includes('#') && !href.includes('mailto:') && !href.includes('tel:')) {
      if (!href.startsWith('http')) {
        href = new URL(href, baseUrl).href;
      }
      links.push(href);
    }
  });

  const filtered = links.filter(href =>
    !/karriere|jobs|stellenangebote|offene-stellen|jobboerse|careers|career|bewerbung|bewerben|vacancies/i.test(href) ||
    /\/job\//i.test(href) ||
    /\/stelle\//i.test(href) ||
    /\/position\//i.test(href) ||
    /\/vakanz\//i.test(href) ||
    /\/ausschreibung\//i.test(href) ||
    /\/detail\?/i.test(href) ||
    /\/job-\d+/i.test(href) ||
    /\/vacancies\//i.test(href)
  );

  return [...new Set(filtered)].slice(0, 20);
}

function extractDescriptionGeneric(html) {
  const $ = cheerio.load(html);

  const selectors = [
    '.job-description', '.job-details', '.description', '.content',
    '#job-description', '.job-content', '[class*="job-description"]',
    '[class*="job-detail"]', '[class*="description"]', 'article',
    '.main-content', '#content', '.text-content', '.post-content',
    '.entry-content', '[itemprop="description"]', '[itemprop="jobDescription"]'
  ];

  for (const selector of selectors) {
    const text = $(selector).text().trim();
    if (text && text.length > 200) return text;
  }

  let text = $('body').text();
  text = text.split('\n')
    .map(l => l.trim())
    .filter(l => l.length > 20)
    .filter(l => !/impressum|datenschutz|agb|cookie|footer|menu|navigation|copyright|©/.test(l))
    .join('\n');
  return text.length > 200 ? text : '';
}

async function genericFallbackCrawl(careerPageUrl) {
  console.log(`    🔄 Last resort: generic job-link scraping...`);

  const html = await fetchHtmlWithFallback(careerPageUrl);
  if (!html) {
    console.log(`    ❌ Fallback: could not fetch career page`);
    return [];
  }

  const jobLinks = extractJobLinksGeneric(html, careerPageUrl);
  console.log(`    Fallback found ${jobLinks.length} job detail links`);

  const jobs = [];

  for (const link of jobLinks) {
    const jobHtml = await fetchHtmlWithFallback(link);
    if (!jobHtml) continue;

    const $ = cheerio.load(jobHtml);
    const title = $('title').text().trim() || 'Untitled Job';
    const description = extractDescriptionGeneric(jobHtml);

    if (!description || description.length < 100) continue;

    const externalId = Buffer.from(link).toString('base64').slice(0, 50);
    jobs.push({
      external_job_id: externalId,
      title,
      raw_description: description.slice(0, 5000),
      apply_url: link,
      location: null,
      employment_type: null,
      department: null,
      ats_source: 'custom_fallback'
    });
  }

  console.log(`    Fallback extracted ${jobs.length} jobs`);
  return jobs;
}

// ─── SIMPLE CONCURRENCY-LIMITED MAPPER ─────────────────────────────────────
async function mapWithConcurrency(items, limit, fn) {
  const results = new Array(items.length);
  let index = 0;

  async function worker() {
    while (index < items.length) {
      const i = index++;
      results[i] = await fn(items[i], i);
    }
  }

  const workers = Array.from({ length: Math.min(limit, items.length) }, worker);
  await Promise.all(workers);
  return results;
}

// ─── DESCRIPTION ENRICHMENT ────────────────────────────────────────────────
async function enrichJobDescription(job) {
  if (job.raw_description && job.raw_description.length > 100) {
    return job;
  }

  if (!job.apply_url) return job;

  try {
    const html = await fetchHtmlWithFallback(job.apply_url);
    if (!html) return job;

    const description = extractDescriptionGeneric(html);
    if (description && description.length > 100) {
      job.raw_description = description.slice(0, 5000);
    }
  } catch (err) {
    console.log(`      ⚠️ Could not enrich description: ${err.message}`);
  }

  return job;
}

// ─── PARSE JOBS FROM FEED — WITH DATE FILTER ──────────────────────────────
function parseJobsFromFeed(data, subdomain) {
  const MAX_DAYS = parseInt(process.env.SOFTGARDEN_MAX_DAYS_BACK) || 30;
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - MAX_DAYS);

  let jobs = [];

  if (data.dataFeedElement && Array.isArray(data.dataFeedElement)) {
    jobs = data.dataFeedElement
      .map(item => item.item || item)
      .filter(job => job && (job.title || job.name || job.jobTitle));
  } else if (data.jobs && Array.isArray(data.jobs)) {
    jobs = data.jobs;
  } else if (data.data && Array.isArray(data.data)) {
    jobs = data.data;
  } else if (data.jobAds && Array.isArray(data.jobAds)) {
    jobs = data.jobAds;
  } else if (data.jobPostings && Array.isArray(data.jobPostings)) {
    jobs = data.jobPostings;
  } else if (typeof data === 'object' && !Array.isArray(data)) {
    const values = Object.values(data);
    if (values.some(v => v && typeof v === 'object' && (v.title || v.jobTitle))) {
      jobs = values.filter(v => v && typeof v === 'object' && (v.title || v.jobTitle));
    }
  }

  const filtered = jobs
    .map(job => {
      const datePosted = job.datePosted || job.validThrough || job.dateCreated || job.publicationDate ||
                         job.publishedDate || job.startDate || job.publishDate || job.validFrom || job.availableDate;
      if (datePosted) {
        const jobDate = new Date(datePosted);
        if (jobDate < cutoffDate) {
          console.log(`    ⏭️ Skipping old job: ${job.title || job.jobTitle} (${datePosted})`);
          return null;
        }
      }

      const id = job.identifier?.value || job.id || job.jobId || job.jobPostingId || Math.random().toString();
      const title = job.title || job.jobTitle || job.name || 'Unknown';
      const description = job.description || job.jobDescription || job.rawDescription || '';
      const location = job.location || job.jobLocation?.address?.addressLocality || job.city || null;
      const employmentType = job.employmentType || job.workTime || job.employment_type || null;
      const applyUrl = job.url || job.applicationUrl || job.applyUrl || job.link ||
        (subdomain ? `https://${subdomain}.career.softgarden.de/jobs/${id}` : null);

      return {
        external_job_id: String(id),
        title: title,
        location: location,
        employment_type: employmentType,
        raw_description: description,
        apply_url: applyUrl,
        department: job.category || job.department || null,
        ats_source: 'softgarden'
      };
    })
    .filter(job => job !== null);

  console.log(`    ✅ ${filtered.length} recent jobs (last ${MAX_DAYS} days)`);
  return filtered;
}

// ─── SOFTGARDEN API SE JOBS FETCH KARO ────────────────────────────────────
async function fetchSoftgardenJobs(userId, projectId, pageId, feedMap = new Map()) {
  try {
    console.log(`    Fetching jobs...`);
    console.log(`    userId:    ${userId}`);
    console.log(`    projectId: ${projectId}`);
    console.log(`    pageId:    ${pageId || 'none'}`);

    const payload = {
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
        location: {
          osmLocation: '',
          range: 25,
          coords: []
        },
        filters: {
          careerLevel: [],
          category: [],
          location: [],
          company: [],
          partnership: [],
          region: [],
          country: []
        }
      },
      filterStatus: {
        careerLevel: false,
        category: false,
        partnership: false,
        region: false,
        location: false
      }
    };

    if (pageId) payload.pageId = pageId;

    const response = await axios.post(
      'https://pcw-api.softgarden.de/widgets/job-list/job-ads',
      payload,
      {
        headers: {
          'Content-Type': 'application/json',
          'User-Agent': 'Mozilla/5.0',
          'Accept': 'application/json'
        },
        timeout: 15000
      }
    );

    const data = response.data;
    const rawJobs = data?.jobs || data?.jobAds || data?.data || [];

    console.log(`    ✅ ${rawJobs.length} jobs found!`);

    const jobs = rawJobs.map(job => {
      const externalId = String(job.jobPostingId || job.id || job.jobAdId || Math.random());
      const feedEntry = feedMap.get(externalId);

      return {
        external_job_id: externalId,
        title: (feedEntry && feedEntry.title) || job.jobTitle || job.title || job.name || 'Unknown',
        location: (feedEntry && feedEntry.location) ||
                  job.location?.city || job.city || job.locationName || job.location || null,
        employment_type: (feedEntry && feedEntry.employment_type) || job.workTime || job.employmentType || null,
        raw_description: (feedEntry && feedEntry.raw_description) || job.jobDescription || job.description || null,
        apply_url: (feedEntry && feedEntry.apply_url) ||
                   job.applyUrl || job.applicationUrl ||
                   `https://pcw-api.softgarden.de/job/${job.jobPostingId}` || null,
        department: job.category || job.department || null,
        ats_source: 'softgarden'
      };
    });

    console.log(`    📄 Enriching ${jobs.length} jobs with full descriptions...`);
    await mapWithConcurrency(jobs, 5, job => enrichJobDescription(job));

    const withDescription = jobs.filter(j => j.raw_description && j.raw_description.length > 100).length;
    console.log(`    📄 ${withDescription}/${jobs.length} jobs have usable descriptions`);

    return jobs;

  } catch (err) {
    console.log(`    API error: ${err.message}`);
    if (err.response) {
      console.log(`    Status: ${err.response.status}`);
      console.log(`    Data: ${JSON.stringify(err.response.data).substring(0, 300)}`);
    }
    return [];
  }
}

// ─── HELPER: GET SUBDOMAIN ─────────────────────────────────────────────────
async function getSoftgardenSubdomain(careerPageUrl) {
  if (!careerPageUrl) return null;

  let match = careerPageUrl.match(/https?:\/\/([^.]+)\.career\.softgarden\.(?:de|io)/);
  if (match && !isExcludedSlug(match[1])) {
    return match[1];
  }

  try {
    const response = await axios.get(careerPageUrl, {
      timeout: 15000,
      headers: { 'User-Agent': 'Mozilla/5.0' }
    });
    const html = response.data;
    const $ = cheerio.load(html);
    const links = [];

    $('a, iframe, script, link').each((_, el) => {
      const href = $(el).attr('href') || $(el).attr('src') || '';
      if (href) links.push(href);
    });

    for (const link of links) {
      const m = link.match(/https?:\/\/([^.]+)\.career\.softgarden\.(?:de|io)/);
      if (m && !isExcludedSlug(m[1])) {
        return m[1];
      }
      const m2 = link.match(/https?:\/\/([^.]+)\.softgarden\.io/);
      if (m2 && !isExcludedSlug(m2[1])) {
        return m2[1];
      }
    }

    const domainMatch = careerPageUrl.match(/https?:\/\/(?:www\.)?([^.]+)\./);
    if (domainMatch && domainMatch[1].toLowerCase() === 'she') {
      return 'shejobs';
    }

  } catch (err) {
    console.log(`   ⚠️ Subdomain scan failed: ${err.message}`);
  }

  return null;
}

// ─── MAIN FUNCTION ──────────────────────────────────────────────────────────
async function processSoftgardenCompany(company) {
  console.log(`\n🔍 Processing: ${company.Name}`);
  console.log(`   Career URL: ${company.detected_career_url}`);

  const ids = await extractSoftgardenIds(company.detected_career_url);
  const feedMap = await discoverSoftgardenFeedMap(company.detected_career_url);

  if (!ids) {
    console.log(`   ⚠️ Softgarden IDs nahi milay — trying custom Softgarden scraper...`);
    const customJobs = await customSoftgardenScraper(company.detected_career_url);
    if (customJobs.length > 0) {
      console.log(`   ✅ Custom scraper successful: ${customJobs.length} jobs`);
      return { company, jobs: customJobs, error: null, usedFallback: true };
    }

    console.log(`   ⚠️ Custom scraper failed — generic fallback try karte hain...`);
    const fallbackJobs = await genericFallbackCrawl(company.detected_career_url);
    if (fallbackJobs.length > 0) {
      console.log(`   ✅ Fallback successful: ${fallbackJobs.length} jobs`);
      return { company, jobs: fallbackJobs, error: null, usedFallback: true };
    }

    console.log(`   ❌ All fallbacks failed — koi job nahi mila`);
    return { company, jobs: [], error: 'No Softgarden IDs, all fallbacks returned 0 jobs' };
  }

  console.log(`   ✅ userId:    ${ids.userId}`);
  console.log(`   ✅ projectId: ${ids.projectId}`);
  console.log(`   ✅ pageId:    ${ids.pageId || 'not found'}`);

  let jobs = [];
  if (feedMap.size > 0) {
    try {
      const subdomain = await getSoftgardenSubdomain(company.detected_career_url);
      if (subdomain) {
        const feedUrl = `https://${subdomain}.career.softgarden.de/jobs.feed.json`;
        const response = await axios.get(feedUrl, {
          timeout: 10000,
          headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json' }
        });
        const parsedJobs = parseJobsFromFeed(response.data, subdomain);
        if (parsedJobs.length > 0) {
          jobs = parsedJobs;
          console.log(`   📋 ${jobs.length} recent jobs from feed`);
        } else {
          console.log(`   ⚠️ No recent jobs in feed, falling back to API...`);
        }
      }
    } catch (err) {
      console.log(`   ⚠️ Feed parsing error: ${err.message}, using API...`);
    }
  }

  if (jobs.length === 0) {
    jobs = await fetchSoftgardenJobs(ids.userId, ids.projectId, ids.pageId, feedMap);
  }

  console.log(`   📋 Total jobs: ${jobs.length}`);

  if (jobs.length === 0) {
    console.log(`   ⚠️ API returned 0 jobs — trying custom Softgarden scraper...`);
    const customJobs = await customSoftgardenScraper(company.detected_career_url);
    if (customJobs.length > 0) {
      console.log(`   ✅ Custom scraper successful: ${customJobs.length} jobs`);
      return { company, jobs: customJobs, error: null, usedFallback: true };
    }

    console.log(`   ⚠️ Custom scraper failed — generic fallback try karte hain...`);
    const fallbackJobs = await genericFallbackCrawl(company.detected_career_url);
    if (fallbackJobs.length > 0) {
      console.log(`   ✅ Fallback successful: ${fallbackJobs.length} jobs`);
      return { company, jobs: fallbackJobs, error: null, usedFallback: true };
    }

    console.log(`   ❌ All fallbacks failed — koi job nahi mila`);
    return { company, jobs: [], error: 'All methods returned 0 jobs' };
  }

  return { company, ids, jobs, error: null };
}

module.exports = {
  processSoftgardenCompany,
  fetchSoftgardenJobs,
  extractSoftgardenIds,
  closeSoftgardenBrowser,
  customSoftgardenScraper
};
