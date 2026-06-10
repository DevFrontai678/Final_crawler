const axios = require('axios');
const cheerio = require('cheerio');
require('dotenv').config();

// HTML se config parse karo — static
function parseConfigFromHtml(html) {
  const $ = cheerio.load(html);

  // Method 1: JSON named patterns
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

  // Method 2: JS variable patterns
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

  // Method 3: Script tags
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

  // Method 4: Cron URL pattern
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

// Playwright se config nikalo — JS render ke baad
async function extractConfigWithPlaywright(url) {
  const { chromium } = require('playwright');
  let browser = null;

  try {
    console.log(`    Playwright launching...`);
    browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    });
    const page = await context.newPage();

    // Network requests intercept karo
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

    // Page kholو
    await page.goto(url, {
      waitUntil: 'networkidle',
      timeout: 30000
    });

    // 2 second wait — JS load hone do
    await page.waitForTimeout(2000);

    // Network se capture hua?
    if (capturedConfig) return capturedConfig;

    // Rendered HTML se try karo
    const html = await page.content();
    const htmlConfig = parseConfigFromHtml(html);
    if (htmlConfig) {
      console.log(`    ✅ Found in rendered HTML!`);
      return htmlConfig;
    }

    // JavaScript window object se try karo
    const jsConfig = await page.evaluate(() => {
      const result = {};

      // __NEXT_DATA__ check karo (Next.js)
      if (window.__NEXT_DATA__) {
        const data = JSON.stringify(window.__NEXT_DATA__);
        const uMatch = data.match(/"userId":"([a-f0-9-]{36})"/);
        const pMatch = data.match(/"projectId":"([a-f0-9-]{36})"/);
        const pgMatch = data.match(/"pageId":"([a-f0-9-]{36})"/);

        if (uMatch) result.userId = uMatch[1];
        if (pMatch) result.projectId = pMatch[1];
        if (pgMatch) result.pageId = pgMatch[1];
      }

      // Global window variables
      if (window.userId) result.userId = window.userId;
      if (window.projectId) result.projectId = window.projectId;
      if (window.pageId) result.pageId = window.pageId;

      // Redux/Zustand store
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
    if (browser) await browser.close();
  }
}

// URL se config nikalo — static + playwright
async function extractSoftgardenConfig(url) {
  // Step 1: Static HTML try karo (fast)
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

  // Step 2: Playwright fallback
  return await extractConfigWithPlaywright(url);
}

// Main ID extraction
async function extractSoftgardenIds(careerPageUrl) {
  try {
    console.log(`    Extracting from: ${careerPageUrl}`);

    const response = await axios.get(careerPageUrl, {
      timeout: 15000,
      headers: { 'User-Agent': 'Mozilla/5.0' },
      maxRedirects: 5
    });

    const html = response.data;
    const $ = cheerio.load(html);

    // Sare links collect karo
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

    // Method 1: {slug}.career.softgarden.de
    for (const link of allLinks) {
      const match = link.match(/https?:\/\/([^.]+)\.career\.softgarden\.de/);
      if (match) {
        console.log(`    Found .de subdomain: ${match[1]}`);
        const config = await extractSoftgardenConfig(
          `https://${match[1]}.career.softgarden.de`
        );
        if (config) return config;
      }
    }

    // Method 2: Page itself .de subdomain
    if (careerPageUrl.includes('.career.softgarden.de')) {
      console.log(`    Page is .de subdomain`);
      const config = await extractSoftgardenConfig(careerPageUrl);
      if (config) return config;
    }

    // Method 3: {slug}.softgarden.io
    for (const link of allLinks) {
      const match = link.match(/https?:\/\/([^./]+)\.softgarden\.io/);
      if (match && !link.includes('certificate')) {
        console.log(`    Found .io subdomain: ${match[1]}`);
        const config = await extractSoftgardenConfig(
          `https://${match[1]}.softgarden.io`
        );
        if (config) return config;
      }
    }

    // Method 4: Page itself .io subdomain
    if (careerPageUrl.includes('.softgarden.io')) {
      console.log(`    Page is .io subdomain`);
      const config = await extractSoftgardenConfig(careerPageUrl);
      if (config) return config;
    }

    // Method 5: Direct on career page
    console.log(`    Trying direct page...`);
    const config = await extractSoftgardenConfig(careerPageUrl);
    if (config) return config;

    return null;

  } catch (err) {
    console.log(`    Error: ${err.message}`);
    return null;
  }
}

// Softgarden API se jobs fetch karo
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

// Main function
async function processSoftgardenCompany(company) {
  console.log(`\n🔍 Processing: ${company.Name}`);
  console.log(`   Career URL: ${company.detected_career_url}`);

  const ids = await extractSoftgardenIds(company.detected_career_url);

  if (!ids) {
    console.log(`   ❌ Could not find Softgarden IDs`);
    return { company, jobs: [], error: 'No IDs found' };
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
  extractSoftgardenIds
};