const axios = require('axios');
const cheerio = require('cheerio');
require('dotenv').config();

const ATS_URL_PATTERNS = [
  { ats: 'greenhouse',      regex: /greenhouse\.io|grnh\.se/i },
  { ats: 'personio',        regex: /personio\.de|personio\.com/i },
  { ats: 'workday',         regex: /myworkdayjobs\.com|workday\.com/i },
  { ats: 'lever',           regex: /lever\.co/i },
  { ats: 'sap',             regex: /successfactors\.com/i },
  { ats: 'teamtailor',      regex: /teamtailor\.com/i },
  { ats: 'recruitee',       regex: /recruitee\.com/i },
  { ats: 'softgarden',      regex: /softgarden\.io|softgarden\.de|bewerbung\.softgarden/i },
  { ats: 'rexx',            regex: /rexx-recruitment\.com|rexx-systems\.com|rexx-enterprise/i },
  { ats: 'smartrecruiters', regex: /smartrecruiters\.com/i },
  { ats: 'successfactors',  regex: /successfactors\.eu|sapsf\.com/i },
  { ats: 'taleo',           regex: /taleo\.net|tbe\.taleo/i },
  { ats: 'icims',           regex: /icims\.com/i },
  { ats: 'bamboohr',        regex: /bamboohr\.com/i },
  { ats: 'jobvite',         regex: /jobvite\.com/i },
];

const ATS_HTML_SIGNATURES = [
  { ats: 'greenhouse',      patterns: ['greenhouse-job-board', 'boards.greenhouse.io', 'grnhse'] },
  { ats: 'personio',        patterns: ['personio.de/job', 'personio-job', 'data-personio', 'personio.de', 'api.personio'] },
  { ats: 'workday',         patterns: ['myworkdayjobs', 'workday.com', 'wd3.myworkday'] },
  { ats: 'lever',           patterns: ['jobs.lever.co', 'lever-job'] },
  { ats: 'teamtailor',      patterns: ['teamtailor', 'career.teamtailor'] },
  { ats: 'recruitee',       patterns: ['recruitee.com'] },
  { ats: 'softgarden',      patterns: ['softgarden.io', 'softgarden.de', 'bewerbung.softgarden', 'join.softgarden'] },
  { ats: 'rexx',            patterns: ['rexx-recruitment.com', 'rexx-systems.com', 'rexx-enterprise'] },
  { ats: 'smartrecruiters', patterns: ['smartrecruiters.com', 'careers.smartrecruiters'] },
  { ats: 'successfactors',  patterns: ['successfactors.com', 'successfactors.eu', 'sapsf.com'] },
  { ats: 'taleo',           patterns: ['taleo.net', 'tbe.taleo.net'] },
  { ats: 'icims',           patterns: ['icims.com', 'careers.icims'] },
  { ats: 'bamboohr',        patterns: ['bamboohr.com', 'app.bamboohr'] },
  { ats: 'jobvite',         patterns: ['jobvite.com', 'jobs.jobvite'] },
  { ats: 'jobware',         patterns: ['jobware.de', 'jobware.net'] },
];

// Career page URL dhundو
async function findCareerPageUrl(websiteUrl) {
  const careerPaths = [
    '/careers', '/jobs', '/karriere', '/stellenangebote',
    '/en/careers', '/de/karriere', '/about/careers',
    '/company/careers', '/jobs/all', '/open-positions',
    '/stellen', '/offene-stellen', '/job-angebote'
  ];

  let baseUrl = websiteUrl
    .replace(/\/impressum.*$/i, '')
    .replace(/\/footer.*$/i, '')
    .replace(/\/de\/impressum.*$/i, '')
    .replace(/\/en\/.*$/i, '')
    .replace(/\/$/, '');

  // Base URL pe pehle check karo
  try {
    const response = await axios.get(baseUrl, {
      timeout: 8000,
      headers: { 'User-Agent': 'Mozilla/5.0' },
      maxRedirects: 3
    });

    const html = response.data.toLowerCase();

    for (const sig of ATS_HTML_SIGNATURES) {
      const matched = sig.patterns.filter(p => html.includes(p.toLowerCase()));
      if (matched.length > 0) {
        console.log(`    ATS found on base URL: ${sig.ats}`);
        return baseUrl;
      }
    }
  } catch (e) {
    // Try career paths
  }

  // Career paths try karo
  for (const path of careerPaths) {
    try {
      const testUrl = baseUrl + path;
      const res = await axios.get(testUrl, {
        timeout: 8000,
        headers: { 'User-Agent': 'Mozilla/5.0' },
        maxRedirects: 3
      });
      if (res.status === 200) {
        console.log(`    Career page found: ${testUrl}`);
        return testUrl;
      }
    } catch (e) {
      // Try next
    }
  }

  return baseUrl;
}

// Claude fallback — zyada context ke saath
async function claudeFallback(html, pageUrl, $) {
  try {
    const Anthropic = require('@anthropic-ai/sdk');
    const client = new Anthropic();

    // Scripts collect karo
    const scripts = [];
    $('script').each((_, el) => {
      const src = $(el).attr('src') || '';
      if (src) scripts.push(src);
    });

    // Iframes collect karo
    const iframes = [];
    $('iframe').each((_, el) => {
      const src = $(el).attr('src') || '';
      if (src) iframes.push(src);
    });

    // Job related links collect karo
    const links = [];
    $('a').each((_, el) => {
      const href = $(el).attr('href') || '';
      if (href && (
        href.includes('job') ||
        href.includes('career') ||
        href.includes('karriere') ||
        href.includes('stellen') ||
        href.includes('apply') ||
        href.includes('bewerb')
      )) links.push(href);
    });

    // Form actions collect karo
    const forms = [];
    $('form').each((_, el) => {
      const action = $(el).attr('action') || '';
      if (action) forms.push(action);
    });

    const context = `
URL: ${pageUrl}

SCRIPT TAGS (external JS files loaded):
${scripts.slice(0, 20).join('\n') || 'none'}

IFRAMES:
${iframes.join('\n') || 'none'}

JOB/CAREER LINKS found on page:
${links.slice(0, 20).join('\n') || 'none'}

FORM ACTIONS:
${forms.join('\n') || 'none'}

HTML SNIPPET (first 2000 chars):
${html.substring(0, 2000)}
`;

    const response = await client.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 100,
      messages: [{
        role: 'user',
        content: `You are an expert at detecting Applicant Tracking Systems (ATS) used by companies.

Analyze this career page data carefully:

${context}

Common patterns to look for:
- personio: scripts/iframes from personio.de, personio.com
- softgarden: scripts from softgarden.io, softgarden.de
- teamtailor: URLs containing teamtailor.com
- greenhouse: boards.greenhouse.io, grnhse
- workday: myworkdayjobs.com
- rexx: rexx-recruitment.com, rexx-systems.com
- smartrecruiters: smartrecruiters.com
- successfactors: successfactors.com, successfactors.eu
- taleo: taleo.net
- bamboohr: bamboohr.com
- custom: company built their own job listing system
- unknown: no job system detected at all (no jobs on this page)

Reply with ONLY one word from this exact list:
greenhouse, personio, workday, lever, sap, teamtailor, softgarden, smartrecruiters, successfactors, taleo, bamboohr, jobvite, icims, recruitee, rexx, custom, unknown`
      }]
    });

    const answer = response.content[0].text.trim().toLowerCase();
    const validATS = [
      'greenhouse','personio','workday','lever','sap','teamtailor',
      'softgarden','smartrecruiters','successfactors','taleo',
      'bamboohr','jobvite','icims','recruitee','rexx','custom','unknown'
    ];

    console.log(`    Claude says: ${answer}`);
    return validATS.includes(answer) ? answer : 'unknown';

  } catch (err) {
    console.log(`    Claude error: ${err.message}`);
    return 'unknown';
  }
}

async function detectATS(companyId, careerPageUrl) {
  const result = {
    company_id: companyId,
    career_page_url: careerPageUrl,
    ats_type: 'unknown',
    ats_confidence: 0,
    ats_api_url: null,
    detection_method: null,
    error: null
  };

  if (!careerPageUrl) {
    result.ats_type = 'no_url';
    return result;
  }

  let url = careerPageUrl;
  if (!url.startsWith('http')) url = 'https://' + url;

  console.log(`    Finding career page...`);
  url = await findCareerPageUrl(url);
  result.career_page_url = url;
  console.log(`    Using: ${url}`);

  try {
    // LAYER 1: URL Pattern
    for (const pattern of ATS_URL_PATTERNS) {
      if (pattern.regex.test(url)) {
        result.ats_type = pattern.ats;
        result.ats_confidence = 0.95;
        result.detection_method = 'url_pattern';
        console.log(`    Layer 1: ${pattern.ats}`);
        return result;
      }
    }

    // LAYER 2: HTML Fetch
    const response = await axios.get(url, {
      timeout: 15000,
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' },
      maxRedirects: 5
    });

    const html = response.data.toLowerCase();
    const $ = cheerio.load(response.data);

    // HTML signatures check
    for (const sig of ATS_HTML_SIGNATURES) {
      const matched = sig.patterns.filter(p => html.includes(p.toLowerCase()));
      if (matched.length > 0) {
        result.ats_type = sig.ats;
        result.ats_confidence = matched.length >= 2 ? 0.90 : 0.75;
        result.detection_method = 'html_signature';
        console.log(`    Layer 2 HTML: ${sig.ats}`);
        return result;
      }
    }

    // Script tags check
    let scriptFound = false;
    $('script').each((_, el) => {
      const src = $(el).attr('src') || '';
      const content = $(el).html() || '';

      for (const pattern of ATS_URL_PATTERNS) {
        if (pattern.regex.test(src) || pattern.regex.test(content)) {
          result.ats_type = pattern.ats;
          result.ats_confidence = 0.88;
          result.detection_method = 'script_tag';
          console.log(`    Script tag: ${pattern.ats}`);
          scriptFound = true;
        }
      }

      for (const sig of ATS_HTML_SIGNATURES) {
        const matched = sig.patterns.filter(p =>
          src.toLowerCase().includes(p) ||
          content.toLowerCase().includes(p)
        );
        if (matched.length > 0 && !scriptFound) {
          result.ats_type = sig.ats;
          result.ats_confidence = 0.85;
          result.detection_method = 'script_content';
          console.log(`    Script content: ${sig.ats}`);
          scriptFound = true;
        }
      }
    });
    if (scriptFound) return result;

    // Meta tags check
    let metaFound = false;
    $('meta').each((_, el) => {
      const content = $(el).attr('content') || '';
      const name = $(el).attr('name') || '';

      for (const sig of ATS_HTML_SIGNATURES) {
        const matched = sig.patterns.filter(p =>
          content.toLowerCase().includes(p) ||
          name.toLowerCase().includes(p)
        );
        if (matched.length > 0) {
          result.ats_type = sig.ats;
          result.ats_confidence = 0.80;
          result.detection_method = 'meta_tag';
          console.log(`    Meta tag: ${sig.ats}`);
          metaFound = true;
        }
      }
    });
    if (metaFound) return result;

    // Iframes check
    $('iframe').each((_, el) => {
      const src = $(el).attr('src') || '';
      for (const pattern of ATS_URL_PATTERNS) {
        if (pattern.regex.test(src)) {
          result.ats_type = pattern.ats;
          result.ats_confidence = 0.92;
          result.ats_api_url = src;
          result.detection_method = 'iframe';
          console.log(`    Iframe: ${pattern.ats}`);
        }
      }
    });
    if (result.ats_type !== 'unknown') return result;

    // Links check
    $('a').each((_, el) => {
      const href = $(el).attr('href') || '';
      for (const pattern of ATS_URL_PATTERNS) {
        if (pattern.regex.test(href)) {
          result.ats_type = pattern.ats;
          result.ats_confidence = 0.80;
          result.ats_api_url = href;
          result.detection_method = 'link';
          console.log(`    Link: ${pattern.ats}`);
        }
      }
    });
    if (result.ats_type !== 'unknown') return result;

    // LAYER 3: Claude fallback — ab $ bhi pass ho raha hai
    console.log(`    Layer 3: Claude fallback...`);
    result.ats_type = await claudeFallback(html, url, $);
    result.ats_confidence = 0.60;
    result.detection_method = 'claude';

  } catch (err) {
    result.ats_type = 'error';
    result.error = err.message;
    console.log(`    Error: ${err.message}`);
  }

  return result;
}

module.exports = { detectATS };