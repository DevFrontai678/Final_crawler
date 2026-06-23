const axios = require('axios');
const cheerio = require('cheerio');
require('dotenv').config();

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

// ─── GLOBAL BROWSER (reused across all requests — no leak, no repeated cold-starts) ──
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

// ─── PLAYWRIGHT SE CONFIG NIKALO — JS RENDER KE BAAD ──────────────────────
async function extractConfigWithPlaywright(url) {
  let context = null;
  let page = null;

  try {
    console.log(`    Playwright launching...`);
    const browser = await getBrowser();
    context = await browser.newContext({
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
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
      timeout: 30000
    });

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

// ─── URL SE CONFIG NIKALO — STATIC + PLAYWRIGHT ───────────────────────────
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
// Softgarden career pages embed a "related jobs" widget showing OTHER
// unrelated client companies. Add to this list as more false positives
// get discovered (verify by opening the subdomain manually in a browser).
const EXCLUDED_SUBDOMAINS = ['certificate', 'datagroup'];

function isExcludedSlug(slug) {
  return EXCLUDED_SUBDOMAINS.some(ex => slug.toLowerCase().includes(ex));
}

// ─── MAIN ID EXTRACTION — DIRECT PAGE FIRST, WIDGET-LINK FALLBACK LAST ────
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

// ─── GENERIC FALLBACK CRAWLER (last resort — sirf jab Softgarden IDs na milein) ──
function extractJobLinksGeneric(html, baseUrl) {
  const $ = cheerio.load(html);
  const links = [];

  $('a[href*="job"], a[href*="stelle"], a[href*="karriere"], a[href*="bewerbung"], a[href*="vakanz"], a[href*="offene"], a[href*="position"], a[href*="ausbildung"], a[href*="praktikum"]').each((_, el) => {
    let href = $(el).attr('href');
    if (href && !href.includes('#') && !href.includes('mailto:') && !href.includes('tel:')) {
      if (!href.startsWith('http')) {
        href = new URL(href, baseUrl).href;
      }
      links.push(href);
    }
  });

  const filtered = links.filter(href =>
    !/karriere|jobs|stellenangebote|offene-stellen|jobboerse|careers|career|bewerbung|bewerben/i.test(href) ||
    /\/job\//i.test(href) ||
    /\/stelle\//i.test(href) ||
    /\/position\//i.test(href) ||
    /\/vakanz\//i.test(href) ||
    /\/ausschreibung\//i.test(href) ||
    /\/detail\?/i.test(href) ||
    /\/job-\d+/i.test(href)
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

async function fetchHtmlForFallback(url) {
  try {
    const response = await axios.get(url, {
      timeout: 15000,
      headers: { 'User-Agent': 'Mozilla/5.0' },
      maxRedirects: 5
    });
    return response.data;
  } catch (err) {
    let context = null;
    let page = null;
    try {
      const browser = await getBrowser();
      context = await browser.newContext();
      page = await context.newPage();
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
      const html = await page.content();
      return html;
    } catch (playwrightErr) {
      return null;
    } finally {
      if (page) await page.close().catch(() => {});
      if (context) await context.close().catch(() => {});
    }
  }
}

async function genericFallbackCrawl(careerPageUrl) {
  console.log(`    🔄 Last resort: generic job-link scraping...`);

  const html = await fetchHtmlForFallback(careerPageUrl);
  if (!html) {
    console.log(`    ❌ Fallback: could not fetch career page`);
    return [];
  }

  const jobLinks = extractJobLinksGeneric(html, careerPageUrl);
  console.log(`    Fallback found ${jobLinks.length} job detail links`);

  const jobs = [];

  for (const link of jobLinks) {
    const jobHtml = await fetchHtmlForFallback(link);
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

// ─── SOFTGARDEN API SE JOBS FETCH KARO ────────────────────────────────────
async function fetchSoftgardenJobs(userId, projectId, pageId) {
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
    const jobs = data?.jobs || data?.jobAds || data?.data || [];

    console.log(`    ✅ ${jobs.length} jobs found!`);

    return jobs.map(job => ({
      external_job_id: String(
        job.jobPostingId || job.id || job.jobAdId || Math.random()
      ),
      title: job.jobTitle || job.title || job.name || 'Unknown',
      location: job.location?.city ||
                job.city ||
                job.locationName ||
                job.location || null,
      employment_type: job.workTime || job.employmentType || null,
      raw_description: job.jobDescription || job.description || null,
      apply_url: job.applyUrl ||
                 job.applicationUrl ||
                 `https://pcw-api.softgarden.de/job/${job.jobPostingId}` || null,
      department: job.category || job.department || null,
      ats_source: 'softgarden'
    }));

  } catch (err) {
    console.log(`    API error: ${err.message}`);
    if (err.response) {
      console.log(`    Status: ${err.response.status}`);
      console.log(`    Data: ${JSON.stringify(err.response.data).substring(0, 300)}`);
    }
    return [];
  }
}

// ─── MAIN FUNCTION — SOFTGARDEN FIRST, GENERIC FALLBACK LAST ──────────────
async function processSoftgardenCompany(company) {
  console.log(`\n🔍 Processing: ${company.Name}`);
  console.log(`   Career URL: ${company.detected_career_url}`);

  const ids = await extractSoftgardenIds(company.detected_career_url);

  if (!ids) {
    console.log(`   ⚠️ Softgarden IDs nahi milay — generic fallback try karte hain...`);
    const fallbackJobs = await genericFallbackCrawl(company.detected_career_url);

    if (fallbackJobs.length > 0) {
      console.log(`   ✅ Fallback successful: ${fallbackJobs.length} jobs`);
      return { company, jobs: fallbackJobs, error: null, usedFallback: true };
    }

    console.log(`   ❌ Fallback bhi fail — koi job nahi mila`);
    return { company, jobs: [], error: 'No Softgarden IDs, fallback also returned 0 jobs' };
  }

  console.log(`   ✅ userId:    ${ids.userId}`);
  console.log(`   ✅ projectId: ${ids.projectId}`);
  console.log(`   ✅ pageId:    ${ids.pageId || 'not found'}`);

  const jobs = await fetchSoftgardenJobs(ids.userId, ids.projectId, ids.pageId);
  console.log(`   📋 Total jobs: ${jobs.length}`);

  return { company, ids, jobs, error: null };
}

module.exports = {
  processSoftgardenCompany,
  fetchSoftgardenJobs,
  extractSoftgardenIds,
  closeSoftgardenBrowser
};
