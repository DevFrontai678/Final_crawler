const axios = require('axios');
const cheerio = require('cheerio');
require('dotenv').config();
const { SCRAPERAPI_CONFIG } = require('../utils/scraperapi-config');
const { CRAWLER_TIMEOUTS } = require('../utils/crawler-timeouts');

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

// ─── HELPER: STRIP HTML → CLEAN TEXT ──────────────────────────────────────
function stripHtmlToText(html) {
  if (!html) return '';
  try {
    const $ = cheerio.load(`<div>${html}</div>`);
    return $('div')
      .text()
      .replace(/\r\n/g, '\n')
      .replace(/[ \t]+/g, ' ')
      .replace(/\n{3,}/g, '\n\n')
      .trim();
  } catch (e) {
    return html.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
  }
}

function decodeCdata(str) {
  if (!str) return '';
  return str.replace(/<!\[CDATA\[/g, '').replace(/\]\]>/g, '').trim();
}

function cleanOrNull(str) {
  if (!str) return null;
  const t = String(str).trim();
  return t.length > 0 ? t : null;
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
      if (await acceptBtn.isVisible({ timeout: CRAWLER_TIMEOUTS.VISIBILITY_TIMEOUT_MS })) {
        await acceptBtn.click();
        console.log('    🍪 Accepted cookies');
        return true;
      }
    } catch (e) {}
  }
  return false;
}

// ─── FETCH HTML WITH STRICT CONTENT VALIDATION + SCRAPERAPI ──────────────
// 🔥 FIX: Added retry limit to prevent infinite loops
async function fetchHtmlWithFallback(url, retryCount = 0) {
  const MAX_RETRIES = 2;

  function hasPersonioJobContent(html) {
    const $ = cheerio.load(html);
    const hasPersonioLinks =
      html.includes('personio') ||
      html.includes('jobs.personio.de') ||
      $('a[href*="personio"]').length > 0 ||
      $('iframe[src*="personio"]').length > 0;
    const hasJobItems =
      $('[data-position-id]').length > 0 ||
      $('.job-position, .position-item, [class*="job-"], [class*="position"]').length > 2;
    const hasJobKeywords =
      html.includes('Stellenangebote') ||
      html.includes('Job offers') ||
      html.includes('Karriere') ||
      html.includes('Bewerbung');
    return hasPersonioLinks && (hasJobItems || hasJobKeywords);
  }

  let html = null;
  let staticSuccess = false;

  try {
    const response = await axios.get(url, {
      timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
      headers: { 'User-Agent': 'Mozilla/5.0' },
      maxRedirects: 5
    });
    html = response.data;
    staticSuccess = true;
  } catch (staticErr) {
    if (staticErr.response?.status === 404) {
      console.log(`    ⚠️ Static 404 for ${url} – skipping further attempts`);
      return null;
    }
    console.log(`    ⚠️ Static failed: ${staticErr.message}`);
  }

  if (staticSuccess && html && hasPersonioJobContent(html)) {
    console.log(`    ✅ Static fetch returned valid job content`);
    return html;
  } else if (staticSuccess && html) {
    console.log(`    ⚠️ Static HTML but no real job content — trying Playwright...`);
  }

  if (retryCount >= MAX_RETRIES) {
    console.log(`    ⚠️ Max retries reached for ${url}`);
    return null;
  }

  let context = null;
  let page = null;
  try {
    const browser = await getBrowser();
    context = await browser.newContext({
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      locale: 'de-DE',
    });
    page = await context.newPage();
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: CRAWLER_TIMEOUTS.NAVIGATION_TIMEOUT_MS });
    await acceptCookies(page);
    await page.waitForTimeout(CRAWLER_TIMEOUTS.WAIT_TIMEOUT_MS + 1000);
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
      timeout: SCRAPERAPI_CONFIG.requestTimeoutMs,
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

// ─── EXTRACT PERSONIO SLUG ────────────────────────────────────────────────
async function extractPersonioSlug(careerPageUrl) {
  console.log(`    🔍 Extracting Personio slug...`);

  if (careerPageUrl.includes('personio')) {
    const match = careerPageUrl.match(/https?:\/\/([^.]+)\.(?:career\.)?personio\.(?:de|com)/);
    if (match) {
      console.log(`    ✅ Slug from URL: ${match[1]}`);
      return { type: 'subdomain', slug: match[1] };
    }
    const jobMatch = careerPageUrl.match(/https?:\/\/([^.]+)\.jobs\.personio\.(?:de|com)/);
    if (jobMatch) {
      console.log(`    ✅ Slug from jobs URL: ${jobMatch[1]}`);
      return { type: 'subdomain', slug: jobMatch[1] };
    }
  }

  try {
    const html = await fetchHtmlWithFallback(careerPageUrl);
    if (!html) {
      console.log(`    ❌ Could not fetch page`);
      return null;
    }

    const $ = cheerio.load(html);

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

    let slugFromJobsLink = null;
    $('a').each((_, el) => {
      const href = $(el).attr('href') || '';
      if (href.includes('jobs.personio.de')) {
        const match = href.match(/https?:\/\/([^.]+)\.jobs\.personio\.de/);
        if (match) slugFromJobsLink = match[1];
      }
    });
    if (slugFromJobsLink) {
      console.log(`    ✅ Slug from jobs link: ${slugFromJobsLink}`);
      return { type: 'subdomain', slug: slugFromJobsLink };
    }

    if (html.includes('personio')) {
      const scriptMatches = html.match(/https?:\/\/([^.]+)\.(?:career\.)?personio\.(?:de|com)/g);
      if (scriptMatches && scriptMatches.length > 0) {
        const m = scriptMatches[0].match(/https?:\/\/([^.]+)\.(?:career\.)?personio\.(?:de|com)/);
        if (m) {
          console.log(`    ✅ Slug from script: ${m[1]}`);
          return { type: 'subdomain', slug: m[1] };
        }
      }
    }

    const dataSlug = $('[data-personio-url]').attr('data-personio-url') ||
                     $('[data-company]').attr('data-company');
    if (dataSlug) {
      console.log(`    ✅ Slug from data attr: ${dataSlug}`);
      return { type: 'subdomain', slug: dataSlug };
    }

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

// ─── PARSE jobDescriptions BLOCK FROM RAW XML ────────────────────────────
function parseXmlJobDescriptions(positionXml) {
  const block = positionXml.match(/<jobDescriptions>([\s\S]*?)<\/jobDescriptions>/i);
  if (!block) return '';

  const descItems = block[1].match(/<jobDescription>([\s\S]*?)<\/jobDescription>/gi) || [];
  const parts = [];

  for (const item of descItems) {
    const nameMatch = item.match(/<name>([\s\S]*?)<\/name>/i);
    const valueMatch = item.match(/<value>([\s\S]*?)<\/value>/i);
    const heading = nameMatch ? decodeCdata(nameMatch[1]) : '';
    const rawValue = valueMatch ? decodeCdata(valueMatch[1]) : '';
    const cleanValue = stripHtmlToText(rawValue);

    if (cleanValue) {
      parts.push(heading ? `${heading}:\n${cleanValue}` : cleanValue);
    }
  }

  if (parts.length === 0) {
    return stripHtmlToText(decodeCdata(block[1]));
  }

  return parts.join('\n\n');
}

// ─── FETCH PERSONIO JOBS (with API fallbacks) ─────────────────────────────
async function fetchPersonioJobs(slug) {
  const results = [];

  // Method 1: XML API
  try {
    const xmlUrl = `https://${slug}.jobs.personio.de/xml`;
    console.log(`    📡 Trying XML API: ${xmlUrl}`);

    const response = await axios.get(xmlUrl, {
      timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
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
      const title = cleanOrNull(getId('name') || getId('title')) || 'Untitled';
      const location = cleanOrNull(getId('office') || getId('location'));
      const employmentType = cleanOrNull(getId('schedule'));
      const description = cleanOrNull(parseXmlJobDescriptions(pos));

      results.push({
        external_job_id: id,
        title: title,
        location: location,
        employment_type: employmentType,
        raw_description: description ? description.slice(0, 8000) : null,
        apply_url: `https://${slug}.jobs.personio.de/job/${id}`,
        ats_source: 'personio'
      });
    }

    if (results.length > 0) {
      console.log(`    ✅ Found ${results.length} jobs via XML API`);
      return results;
    }
  } catch (err) {
    if (err.response?.status === 404) {
      console.log(`    ⚠️ XML API returned 404 – trying JSON API...`);
    } else if (err.response?.status === 429) {
      console.log(`    ⚠️ XML API rate limited (429) – waiting 5s...`);
      await new Promise(r => setTimeout(r, 5000));
      // Retry once
      try {
        const xmlUrl = `https://${slug}.jobs.personio.de/xml`;
        const response = await axios.get(xmlUrl, {
          timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
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
          const title = cleanOrNull(getId('name') || getId('title')) || 'Untitled';
          const location = cleanOrNull(getId('office') || getId('location'));
          const employmentType = cleanOrNull(getId('schedule'));
          const description = cleanOrNull(parseXmlJobDescriptions(pos));
          results.push({
            external_job_id: id,
            title: title,
            location: location,
            employment_type: employmentType,
            raw_description: description ? description.slice(0, 8000) : null,
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
      timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
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
        let location = null;
        if (attrs.office?.attributes?.name) {
          location = cleanOrNull(attrs.office.attributes.name);
        } else if (Array.isArray(attrs.offices) && attrs.offices.length > 0) {
          const names = attrs.offices
            .map(o => o.attributes?.name)
            .filter(Boolean);
          if (names.length > 0) location = cleanOrNull(names.join(', '));
        }

        const rawDescs = Array.isArray(attrs.jobDescriptions) ? attrs.jobDescriptions : [];
        const descParts = rawDescs
          .map(d => {
            const heading = cleanOrNull(d.name);
            const clean = stripHtmlToText(d.value);
            if (!clean) return null;
            return heading ? `${heading}:\n${clean}` : clean;
          })
          .filter(Boolean);
        const description = descParts.length > 0 ? descParts.join('\n\n') : null;

        results.push({
          external_job_id: String(item.id),
          title: cleanOrNull(attrs.name) || 'Untitled',
          location: location,
          employment_type: cleanOrNull(attrs.schedule),
          raw_description: description ? description.slice(0, 8000) : null,
          apply_url: `https://${slug}.jobs.personio.de/job/${item.id}`,
          ats_source: 'personio'
        });
      }
      console.log(`    ✅ Found ${results.length} jobs via JSON API`);
      return results;
    }
  } catch (err) {
    if (err.response?.status === 404) {
      console.log(`    ⚠️ JSON API returned 404 – trying HTML scrape...`);
    } else if (err.response?.status === 429) {
      console.log(`    ⚠️ JSON API rate limited (429) – skipping`);
    } else {
      console.log(`    ⚠️ JSON API failed: ${err.message}`);
    }
  }

  // ─── Method 3: HTML scraping (last resort) ─────────────────────────────
  // 🔥 FIX: Only scrape if we can find real job IDs. Skip fake IDs.
  if (results.length === 0) {
    try {
      const pageUrl = `https://${slug}.jobs.personio.de`;
      console.log(`    📡 Trying HTML scrape: ${pageUrl}`);

      // Single fetch attempt
      const html = await fetchHtmlWithFallback(pageUrl, 0);
      if (!html) {
        console.log(`    ❌ Could not fetch page – skipping HTML scrape`);
        return results;
      }

      const $ = cheerio.load(html);

      // First, check if there are any job elements with real IDs
      const hasJobElements = $('[data-position-id]').length > 0 || 
                             $('a[href*="job/"]').length > 0 ||
                             $('.job-position, .position-item').length > 0;

      if (!hasJobElements) {
        console.log(`    ⚠️ No job elements found on page – skipping HTML scrape`);
        return results;
      }

      // Extract jobs using data-position-id (Personio's pattern)
      $('[data-position-id]').each((_, el) => {
        const id = $(el).attr('data-position-id');
        // Only accept numeric IDs (not random strings)
        if (!id || !/^\d+$/.test(id)) return;

        const title = $(el).find('h2, h3, .title, [class*="title"]').first().text().trim();
        const location = $(el).find('[class*="location"], [class*="office"]').first().text().trim();

        if (title) {
          results.push({
            external_job_id: id,
            title: title || 'Untitled',
            location: cleanOrNull(location),
            raw_description: null,
            apply_url: `https://${slug}.jobs.personio.de/job/${id}`,
            ats_source: 'personio'
          });
        }
      });

      // If no jobs found via data-position-id, try looking for links with job IDs
      if (results.length === 0) {
        $('a[href*="job/"]').each((_, el) => {
          const href = $(el).attr('href');
          const idMatch = href?.match(/job\/(\d+)/);
          if (idMatch && idMatch[1]) {
            const id = idMatch[1];
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

      // If we found jobs with real IDs, try to enrich with detail pages (only if needed)
      if (results.length > 0) {
        const detailCache = new Map();

        for (const job of results) {
          // Skip if we already have both description and location
          if (job.raw_description && job.location) continue;

          // Check cache
          if (detailCache.has(job.apply_url)) {
            const cached = detailCache.get(job.apply_url);
            if (!job.raw_description) job.raw_description = cached.raw_description;
            if (!job.location) job.location = cached.location;
            continue;
          }

          try {
            console.log(`    📡 Fetching detail: ${job.apply_url}`);
            const detailHtml = await fetchHtmlWithFallback(job.apply_url, 1);
            if (detailHtml) {
              const $$ = cheerio.load(detailHtml);

              if (!job.raw_description) {
                const bodyText = $$('main, .job-description, [class*="description"], article').first().text();
                job.raw_description = cleanOrNull(
                  bodyText ? bodyText.replace(/\s+/g, ' ').trim().slice(0, 8000) : null
                );
              }

              if (!job.location) {
                const locText = $$('[class*="location"], [class*="office"]').first().text();
                job.location = cleanOrNull(locText);
              }

              detailCache.set(job.apply_url, {
                raw_description: job.raw_description,
                location: job.location
              });
            }
          } catch (detailErr) {
            console.log(`    ⚠️ Detail fetch failed for ${job.apply_url}: ${detailErr.message}`);
          }
          await new Promise(r => setTimeout(r, 300));
        }

        console.log(`    ✅ Found ${results.length} jobs via HTML scrape`);
        return results;
      } else {
        console.log(`    ⚠️ No valid job IDs found in HTML scrape`);
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

  // Enrich each job with the company name
  const enrichedJobs = jobs.map(job => ({
    ...job,
    company_name: cleanOrNull(company.Name)
  }));

  console.log(`   📋 Total jobs: ${enrichedJobs.length}`);

  return { company, slug: slugData.slug, jobs: enrichedJobs, error: null };
}

module.exports = {
  processPersonioCompany,
  fetchPersonioJobs,
  extractPersonioSlug,
  closePersonioBrowser
};
