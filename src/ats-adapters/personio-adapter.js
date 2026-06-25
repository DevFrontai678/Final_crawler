const axios = require('axios');
const cheerio = require('cheerio');
require('dotenv').config();

// ─── GLOBAL BROWSER (reused across all requests) ──────────────────────────
let globalBrowser = null;

async function getBrowser() {
  if (!globalBrowser || !globalBrowser.isConnected()) {
    const { chromium } = require('playwright');
    console.log(`    🔄 Launching Personio browser instance...`);
    globalBrowser = await chromium.launch({ headless: true });
  }
  return globalBrowser;
}

async function closePersonioBrowser() {
  if (globalBrowser) {
    await globalBrowser.close().catch(() => {});
    globalBrowser = null;
  }
}

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

// ─── FETCH HTML WITH STRICT CONTENT VALIDATION + SCRAPERAPI ──────────────
async function fetchHtmlWithFallback(url) {
  // Helper function to check if HTML has Personio job content
  function hasPersonioJobContent(html) {
    const $ = cheerio.load(html);
    
    // Check for Personio job links
    const hasPersonioLinks = 
      html.includes('personio') || 
      html.includes('jobs.personio.de') ||
      $('a[href*="personio"]').length > 0 ||
      $('iframe[src*="personio"]').length > 0;
    
    // Check for job cards/items
    const hasJobItems = 
      $('[data-position-id]').length > 0 ||
      $('.job-position, .position-item, [class*="job-"], [class*="position"]').length > 2;
    
    // Check for job keywords
    const hasJobKeywords = 
      html.includes('Stellenangebote') ||
      html.includes('Job offers') ||
      html.includes('Karriere') ||
      html.includes('Bewerbung');
    
    return hasPersonioLinks && (hasJobItems || hasJobKeywords);
  }

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

  if (staticSuccess && html && hasPersonioJobContent(html)) {
    console.log(`    ✅ Static fetch returned valid job content`);
    return html;
  } else if (staticSuccess && html) {
    console.log(`    ⚠️ Static HTML but no real job content — trying Playwright...`);
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
    await page.waitForTimeout(3000);
    const playwrightHtml = await page.content();

    if (hasPersonioJobContent(playwrightHtml)) {
      console.log(`    ✅ Playwright returned valid job content`);
      return playwrightHtml;
    } else {
      console.log(`    ⚠️ Playwright HTML but no real job content — trying ScraperAPI...`);
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
      if (hasPersonioJobContent(response.data)) {
        console.log(`    ✅ ScraperAPI returned valid job content`);
        return response.data;
      } else {
        console.log(`    ⚠️ ScraperAPI returned HTML but no real job content`);
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

// ─── EXTRACT PERSONIO SLUG (IMPROVED) ─────────────────────────────────────
async function extractPersonioSlug(careerPageUrl) {
  console.log(`    🔍 Extracting Personio slug...`);

  // 1. Direct URL pattern check
  if (careerPageUrl.includes('personio')) {
    const match = careerPageUrl.match(/https?:\/\/([^.]+)\.(?:career\.)?personio\.(?:de|com)/);
    if (match) {
      console.log(`    ✅ Slug from URL: ${match[1]}`);
      return { type: 'subdomain', slug: match[1] };
    }
    // Also check for jobs.personio.de
    const jobMatch = careerPageUrl.match(/https?:\/\/([^.]+)\.jobs\.personio\.(?:de|com)/);
    if (jobMatch) {
      console.log(`    ✅ Slug from jobs URL: ${jobMatch[1]}`);
      return { type: 'subdomain', slug: jobMatch[1] };
    }
  }

  // 2. Fetch page and search for Personio links (with cookie handling)
  try {
    const html = await fetchHtmlWithFallback(careerPageUrl);
    if (!html) {
      console.log(`    ❌ Could not fetch page`);
      return null;
    }

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
        console.log(`    ✅ Slug from link: ${match[1]}`);
        return { type: 'subdomain', slug: match[1] };
      }
      const jobMatch = foundUrl.match(/https?:\/\/([^.]+)\.jobs\.personio\.(?:de|com)/);
      if (jobMatch) {
        console.log(`    ✅ Slug from jobs link: ${jobMatch[1]}`);
        return { type: 'subdomain', slug: jobMatch[1] };
      }
    }

    // Also try to find a link with "jobs.personio.de"
    $('a').each((_, el) => {
      const href = $(el).attr('href') || '';
      if (href.includes('jobs.personio.de')) {
        const match = href.match(/https?:\/\/([^.]+)\.jobs\.personio\.de/);
        if (match) {
          console.log(`    ✅ Slug from jobs link: ${match[1]}`);
          return { type: 'subdomain', slug: match[1] };
        }
      }
    });

    // Check if the page itself is a Personio page
    if (html.includes('personio')) {
      // Try to extract from script tags
      const scriptMatches = html.match(/https?:\/\/([^.]+)\.(?:career\.)?personio\.(?:de|com)/g);
      if (scriptMatches && scriptMatches.length > 0) {
        const m = scriptMatches[0].match(/https?:\/\/([^.]+)\.(?:career\.)?personio\.(?:de|com)/);
        if (m) {
          console.log(`    ✅ Slug from script: ${m[1]}`);
          return { type: 'subdomain', slug: m[1] };
        }
      }
    }

    // Try to find Personio widget in data attributes
    const dataSlug = $('[data-personio-url]').attr('data-personio-url') ||
                     $('[data-company]').attr('data-company');
    if (dataSlug) {
      console.log(`    ✅ Slug from data attr: ${dataSlug}`);
      return { type: 'subdomain', slug: dataSlug };
    }

    // Try to find from div with class containing personio
    const personioContainer = $('[class*="personio"]').first();
    if (personioContainer.length) {
      const containerText = personioContainer.text();
      const match = containerText.match(/([a-z0-9-]+)\.personio/i);
      if (match) {
        console.log(`    ✅ Slug from container: ${match[1]}`);
        return { type: 'subdomain', slug: match[1] };
      }
    }

  } catch (err) {
    console.log(`    ❌ Fetch error: ${err.message}`);
  }

  console.log(`    ❌ Could not find Personio slug`);
  return null;
}

// ─── FETCH PERSONIO JOBS (with API fallbacks) ─────────────────────────────
async function fetchPersonioJobs(slug) {
  const results = [];

  // Method 1: XML API
  try {
    const xmlUrl = `https://${slug}.jobs.personio.de/xml`;
    console.log(`    📡 Trying XML API: ${xmlUrl}`);
    
    const response = await axios.get(xmlUrl, {
      timeout: 15000,
      headers: { 'User-Agent': 'Mozilla/5.0' }
    });

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
      
      results.push({
        external_job_id: id,
        title: title,
        location: location,
        employment_type: employmentType,
        raw_description: description.slice(0, 5000),
        apply_url: `https://${slug}.jobs.personio.de/job/${id}`,
        ats_source: 'personio'
      });
    }

    if (results.length > 0) {
      console.log(`    ✅ Found ${results.length} jobs via XML API`);
      return results;
    }
  } catch (err) {
    if (err.response?.status === 429) {
      console.log(`    ⚠️ XML API rate limited (429) – waiting 5s...`);
      await new Promise(r => setTimeout(r, 5000));
      // Retry once
      try {
        const xmlUrl = `https://${slug}.jobs.personio.de/xml`;
        const response = await axios.get(xmlUrl, {
          timeout: 15000,
          headers: { 'User-Agent': 'Mozilla/5.0' }
        });
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
          results.push({
            external_job_id: id,
            title: title,
            location: location,
            employment_type: employmentType,
            raw_description: description.slice(0, 5000),
            apply_url: `https://${slug}.jobs.personio.de/job/${id}`,
            ats_source: 'personio'
          });
        }
        if (results.length > 0) {
          console.log(`    ✅ Found ${results.length} jobs via XML API (retry)`);
          return results;
        }
      } catch (e) {
        console.log(`    ❌ XML API retry also failed: ${e.message}`);
      }
    } else {
      console.log(`    ⚠️ XML API failed: ${err.message}`);
    }
  }

  // Method 2: JSON API
  try {
    const jsonUrl = `https://${slug}.jobs.personio.de/api/v1/positions`;
    console.log(`    📡 Trying JSON API: ${jsonUrl}`);

    const response = await axios.get(jsonUrl, {
      timeout: 15000,
      headers: {
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0'
      }
    });

    const data = response.data;
    const items = data.data || [];

    if (items.length > 0) {
      for (const item of items) {
        const attrs = item.attributes || {};
        results.push({
          external_job_id: String(item.id),
          title: attrs.name || 'Untitled',
          location: attrs.office?.attributes?.name || null,
          employment_type: attrs.schedule || null,
          raw_description: (attrs.jobDescriptions || []).map(d => d.value).join('\n').slice(0, 5000),
          apply_url: `https://${slug}.jobs.personio.de/job/${item.id}`,
          ats_source: 'personio'
        });
      }
      console.log(`    ✅ Found ${results.length} jobs via JSON API`);
      return results;
    }
  } catch (err) {
    if (err.response?.status === 429) {
      console.log(`    ⚠️ JSON API rate limited (429) – skipping`);
    } else {
      console.log(`    ⚠️ JSON API failed: ${err.message}`);
    }
  }

  // Method 3: HTML scraping (last resort)
  if (results.length === 0) {
    try {
      const pageUrl = `https://${slug}.jobs.personio.de`;
      console.log(`    📡 Trying HTML scrape: ${pageUrl}`);

      const html = await fetchHtmlWithFallback(pageUrl);
      if (!html) {
        console.log(`    ❌ Could not fetch page`);
        return results;
      }

      const $ = cheerio.load(html);

      // Job listings parse karo
      $('[data-position-id], .job-position, .position-item, [class*="job-"], [class*="position"]').each((_, el) => {
        const id = $(el).attr('data-position-id') || 
                   $(el).attr('data-id') || 
                   $(el).find('a[href*="job/"]').attr('href')?.match(/job\/(\d+)/)?.[1] ||
                   String(Math.random());
        
        const title = $(el).find('h2, h3, .title, [class*="title"]').first().text().trim();
        const location = $(el).find('[class*="location"], [class*="office"]').first().text().trim();

        if (title) {
          results.push({
            external_job_id: id,
            title,
            location: location || null,
            raw_description: null,
            apply_url: `https://${slug}.jobs.personio.de/job/${id}`,
            ats_source: 'personio'
          });
        }
      });

      // Also check regular links
      if (results.length === 0) {
        $('a[href*="job/"]').each((_, el) => {
          const href = $(el).attr('href');
          const id = href?.match(/job\/(\d+)/)?.[1];
          if (id) {
            const title = $(el).text().trim();
            results.push({
              external_job_id: id,
              title: title || 'Untitled',
              location: null,
              raw_description: null,
              apply_url: href.startsWith('http') ? href : `https://${slug}.jobs.personio.de${href}`,
              ats_source: 'personio'
            });
          }
        });
      }

      if (results.length > 0) {
        console.log(`    ✅ Found ${results.length} jobs via HTML scrape`);
        return results;
      }
    } catch (err) {
      console.log(`    ⚠️ HTML scrape failed: ${err.message}`);
    }
  }

  if (results.length === 0) {
    console.log(`    ⚠️ No jobs found for ${slug}`);
  }

  return results;
}

// ─── MAIN FUNCTION ──────────────────────────────────────────────────────────
async function processPersonioCompany(company) {
  console.log(`\n🔍 Processing: ${company.Name}`);
  console.log(`   Career URL: ${company.detected_career_url}`);

  const slugData = await extractPersonioSlug(company.detected_career_url);

  if (!slugData) {
    console.log(`   ❌ Could not find Personio slug`);
    return { company, jobs: [], error: 'No slug found' };
  }

  console.log(`   ✅ Slug: ${slugData.slug}`);
  const jobs = await fetchPersonioJobs(slugData.slug);
  console.log(`   📋 Total jobs: ${jobs.length}`);

  return { company, slug: slugData.slug, jobs, error: null };
}

module.exports = {
  processPersonioCompany,
  fetchPersonioJobs,
  extractPersonioSlug,
  closePersonioBrowser
};
