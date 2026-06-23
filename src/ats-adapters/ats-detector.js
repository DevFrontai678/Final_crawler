/**
 * ATS Detector — Production Grade (Fixed for Accuracy + Hang Prevention)
 */

'use strict';

const axios = require('axios');
const cheerio = require('cheerio');
require('dotenv').config();

// ─── URL PATTERNS ────────────────────────────────────────────────────────────
const ATS_URL_PATTERNS = [
  { ats: 'personio',        regex: /(?:^|\.)personio\.(?:de|com)/i },
  { ats: 'softgarden',      regex: /(?:^|\.)softgarden\.(?:io|de|net|com)/i },
  { ats: 'rexx',            regex: /(?:^|\.)rexx-(?:recruitment|systems|enterprise)\.(?:com|de)/i },
  { ats: 'onlyfy',          regex: /(?:^|\.)onlyfy\.(?:io|com)/i },
  { ats: 'hr4you',          regex: /(?:^|\.)hr4you\.(?:de|com)/i },
  { ats: 'umantis',         regex: /(?:^|\.)umantis\.com|lumesse\.com/i },
  { ats: 'talentsoft',      regex: /(?:^|\.)talentsoft\.com/i },
  { ats: 'erecruiter',      regex: /(?:^|\.)erecruiter\.(?:net|de)/i },
  { ats: 'connectoor',      regex: /(?:^|\.)connectoor\.de/i },
  { ats: 'onapply',         regex: /(?:^|\.)onapply\.de/i },
  { ats: 'jobware',         regex: /(?:^|\.)jobware\.(?:de|net)/i },
  { ats: 'join',            regex: /(?:^|\.)join\.com\/companies|jobs\.join\.com/i },
  { ats: 'haufe',           regex: /(?:^|\.)haufe-talent\.de|haufe\.de\/karriere/i },
  { ats: 'prescreen',       regex: /(?:^|\.)prescreen\.io/i },
  { ats: 'pinpoint',        regex: /(?:^|\.)pinpoint\.world/i },
  { ats: 'workwise',        regex: /(?:^|\.)workwise\.io/i },
  { ats: 'viasto',          regex: /(?:^|\.)viasto\.com/i },
  { ats: 'greenhouse',      regex: /(?:^|\.)greenhouse\.io|boards\.greenhouse\.io/i },
  { ats: 'workday',         regex: /(?:^|\.)myworkdayjobs\.com|workday\.com\/.*jobs/i },
  { ats: 'lever',           regex: /(?:^|\.)jobs\.lever\.co|lever\.co/i },
  { ats: 'successfactors',  regex: /(?:^|\.)successfactors\.(?:com|eu)|sapsf\.com/i },
  { ats: 'teamtailor',      regex: /(?:^|\.)teamtailor\.com/i },
  { ats: 'smartrecruiters', regex: /(?:^|\.)smartrecruiters\.com|careers\.smartrecruiters/i },
  { ats: 'recruitee',       regex: /(?:^|\.)recruitee\.com/i },
  { ats: 'taleo',           regex: /(?:^|\.)taleo\.net|tbe\.taleo/i },
  { ats: 'icims',           regex: /(?:^|\.)icims\.com/i },
  { ats: 'bamboohr',        regex: /(?:^|\.)bamboohr\.com/i },
  { ats: 'jobvite',         regex: /(?:^|\.)jobvite\.com/i },
  { ats: 'ashby',           regex: /(?:^|\.)ashbyhq\.com/i },
  { ats: 'workable',        regex: /(?:^|\.)workable\.com|apply\.workable/i },
  { ats: 'breezyhr',        regex: /(?:^|\.)breezyhr\.com/i },
];

// ─── HTML SIGNATURES ─────────────────────────────────────────────────────────
const ATS_HTML_SIGNATURES = [
  { ats: 'personio', patterns: ['personio.de','personio.com','api.personio','data-personio','personio-job'], minMatches: 1 },
  { ats: 'softgarden', patterns: ['softgarden.io','softgarden.de','bewerbung.softgarden','join.softgarden','softgarden-widget','jobs.softgarden'], minMatches: 2 },
  { ats: 'rexx', patterns: ['rexx-recruitment.com','rexx-systems.com','rexx-enterprise'], minMatches: 1 },
  { ats: 'onlyfy', patterns: ['onlyfy.io','onlyfy.com','xing-jobs'], minMatches: 1 },
  { ats: 'hr4you', patterns: ['hr4you.de','hr4you.com','hr4you-widget'], minMatches: 1 },
  { ats: 'umantis', patterns: ['umantis.com','lumesse.com'], minMatches: 1 },
  { ats: 'talentsoft', patterns: ['talentsoft.com','talentsoft-widget'], minMatches: 1 },
  { ats: 'erecruiter', patterns: ['erecruiter.net','erecruiter.de'], minMatches: 1 },
  { ats: 'connectoor', patterns: ['connectoor.de'], minMatches: 1 },
  { ats: 'onapply', patterns: ['onapply.de','onapply-widget'], minMatches: 1 },
  { ats: 'jobware', patterns: ['jobware.de','jobware.net'], minMatches: 1 },
  { ats: 'join', patterns: ['join.com/companies','jobs.join.com','join-widget'], minMatches: 1 },
  { ats: 'haufe', patterns: ['haufe-talent.de','haufe.de/karriere','haufe-widget'], minMatches: 1 },
  { ats: 'prescreen', patterns: ['prescreen.io','prescreen-widget'], minMatches: 1 },
  { ats: 'talention', patterns: ['talention.com'], minMatches: 1 },
  { ats: 'pinpoint', patterns: ['pinpoint.world'], minMatches: 1 },
  { ats: 'pidelta', patterns: ['pidelta.de'], minMatches: 1 },
  { ats: 'concludis', patterns: ['concludis.de','concludis-widget'], minMatches: 1 },
  { ats: 'workwise', patterns: ['workwise.io'], minMatches: 1 },
  { ats: 'viasto', patterns: ['viasto.com'], minMatches: 1 },
  { ats: 'greenhouse', patterns: ['greenhouse-job-board','boards.greenhouse.io','grnhse','greenhouse.io'], minMatches: 1 },
  { ats: 'workday', patterns: ['myworkdayjobs','workday.com','wd3.myworkday','workdayjobs'], minMatches: 1 },
  { ats: 'lever', patterns: ['jobs.lever.co','lever-job','lever.co'], minMatches: 1 },
  { ats: 'successfactors', patterns: ['successfactors.com','successfactors.eu','sapsf.com','jobs.sap.com'], minMatches: 1 },
  { ats: 'teamtailor', patterns: ['teamtailor.com','career.teamtailor','teamtailor-widget'], minMatches: 1 },
  { ats: 'smartrecruiters', patterns: ['smartrecruiters.com','careers.smartrecruiters','smartrecruiters-widget'], minMatches: 1 },
  { ats: 'recruitee', patterns: ['recruitee.com','recruitee-widget'], minMatches: 1 },
  { ats: 'taleo', patterns: ['taleo.net','tbe.taleo.net'], minMatches: 1 },
  { ats: 'icims', patterns: ['icims.com','careers.icims'], minMatches: 1 },
  { ats: 'bamboohr', patterns: ['bamboohr.com','app.bamboohr'], minMatches: 1 },
  { ats: 'jobvite', patterns: ['jobvite.com','jobs.jobvite'], minMatches: 1 },
  { ats: 'ashby', patterns: ['ashbyhq.com','jobs.ashbyhq'], minMatches: 1 },
  { ats: 'workable', patterns: ['workable.com','apply.workable.com'], minMatches: 1 },
  { ats: 'breezyhr', patterns: ['breezyhr.com','app.breezyhr'], minMatches: 1 },
];

// ─── PATHS & KEYWORDS ──────────────────────────────────────────────────────
const CAREER_PATHS = [
  '/karriere','/jobs','/stellenangebote','/careers',
  '/offene-stellen','/stellen','/job-angebote','/vakanz',
  '/stellenausschreibungen','/en/careers','/de/karriere',
  '/about/careers','/company/careers','/jobs/all',
  '/open-positions','/vacancies','/jobangebote',
  '/arbeiten-bei-uns','/team/jobs','/en/jobs'
];

const JOB_CONTENT_KEYWORDS = [
  'stellenanzeige','stellenangebote','offene stellen','jetzt bewerben',
  'bewerbung','karriere','vollzeit','teilzeit','festanstellung',
  'job description','apply now','open positions','job opening',
  'we are hiring','join our team','view all jobs','current openings',
  'send application','upload cv'
];

// ─── HELPERS ──────────────────────────────────────────────────────────────────
function normalizeUrl(url) {
  if (!url) return '';
  let u = url.trim();
  if (!u.startsWith('http')) u = 'https://' + u;
  u = u.split('#')[0];
  u = u.replace(/\/$/, '');
  return u;
}

function getBaseUrl(url) {
  try { const { origin } = new URL(url); return origin; } catch { return url; }
}

function jobContentScore(html) {
  if (!html) return 0;
  const lower = html.toLowerCase();
  return JOB_CONTENT_KEYWORDS.filter(kw => lower.includes(kw)).length;
}

// ─── FETCH WITH TIMEOUT ──────────────────────────────────────────────────────
async function fetchWithTimeout(url, timeoutMs = 10000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await axios.get(url, {
      timeout: timeoutMs,
      signal: controller.signal,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
      },
      maxRedirects: 6,
      validateStatus: s => s < 500
    });
    clearTimeout(timeout);
    return response;
  } catch (error) {
    clearTimeout(timeout);
    throw error;
  }
}

// ─── CAREER PAGE DISCOVERY ──────────────────────────────────────────────────
async function fetchCareerPage(rawUrl) {
  if (!rawUrl) return null;
  const base = getBaseUrl(normalizeUrl(rawUrl));
  const rawNorm = normalizeUrl(rawUrl);

  // try exact URL
  if (rawNorm !== base) {
    try {
      const res = await fetchWithTimeout(rawNorm, 8000);
      if (res.status === 200 && res.data && typeof res.data === 'string') {
        return { url: rawNorm, html: res.data };
      }
    } catch {}
  }

  // try career paths
  for (const path of CAREER_PATHS) {
    try {
      const testUrl = base + path;
      const res = await fetchWithTimeout(testUrl, 8000);
      if (res.status === 200 && res.data && typeof res.data === 'string') {
        if (jobContentScore(res.data) >= 1) {
          return { url: testUrl, html: res.data };
        }
      }
    } catch {}
  }

  // fallback to base
  try {
    const res = await fetchWithTimeout(base, 8000);
    if (res.status === 200 && res.data && typeof res.data === 'string') {
      return { url: base, html: res.data };
    }
  } catch {}
  return null;
}

// ─── HTML DEEP SCAN ──────────────────────────────────────────────────────────
function deepScanHtml(html, pageUrl) {
  if (!html) return null;
  const $ = cheerio.load(html);
  const lower = html.toLowerCase();
  const matchCount = {};

  function countMatches(text) {
    if (!text) return;
    const t = text.toLowerCase();
    for (const sig of ATS_HTML_SIGNATURES) {
      let count = 0;
      for (const p of sig.patterns) {
        if (t.includes(p)) count++;
      }
      if (count > 0) {
        matchCount[sig.ats] = (matchCount[sig.ats] || 0) + count;
      }
    }
  }

  countMatches(lower);
  $('script[src]').each((_, el) => { const s = $(el).attr('src') || ''; if (s) countMatches(s); });
  $('iframe[src]').each((_, el) => { const s = $(el).attr('src') || ''; if (s) countMatches(s); });
  $('a[href]').each((_, el) => { const h = $(el).attr('href') || ''; if (h && !h.startsWith('mailto') && !h.startsWith('tel')) countMatches(h); });
  $('meta').each((_, el) => { const c = $(el).attr('content') || ''; if (c) countMatches(c); });

  for (const sig of ATS_HTML_SIGNATURES) {
    const count = matchCount[sig.ats] || 0;
    if (count >= (sig.minMatches || 1)) {
      let confidence = 0.85;
      if (count >= 3) confidence = 0.92;
      if (pageUrl && pageUrl.toLowerCase().includes(sig.ats)) confidence = 0.95;
      return { ats_type: sig.ats, ats_confidence: confidence, ats_api_url: null, detection_method: 'html_signatures' };
    }
  }

  if (jobContentScore(html) >= 2) {
    return { ats_type: 'custom', ats_confidence: 0.72, ats_api_url: null, detection_method: 'job_content_heuristic' };
  }
  return null;
}

// ─── CLAUDE FALLBACK ─────────────────────────────────────────────────────────
async function claudeFallback(html, pageUrl) {
  try {
    const Anthropic = require('@anthropic-ai/sdk');
    const client = new Anthropic();
    const $ = cheerio.load(html);
    const scriptSrcs = [], iframeSrcs = [], careerLinks = [];
    $('script[src]').each((_, el) => { const s = $(el).attr('src') || ''; if (s) scriptSrcs.push(s); });
    $('iframe[src]').each((_, el) => { const s = $(el).attr('src') || ''; if (s) iframeSrcs.push(s); });
    $('a[href]').each((_, el) => { const h = $(el).attr('href') || ''; if (h && /job|career|karriere|stellen|apply|bewerb/i.test(h)) careerLinks.push(h); });

    const context = [
      `PAGE URL: ${pageUrl || 'unknown'}`,
      `SCRIPT SRCS:\n${scriptSrcs.slice(0,15).join('\n') || 'none'}`,
      `IFRAMES:\n${iframeSrcs.join('\n') || 'none'}`,
      `CAREER/JOB LINKS:\n${careerLinks.slice(0,15).join('\n') || 'none'}`,
      `HTML SNIPPET:\n${html.substring(0,1500)}`
    ].join('\n\n');

    const VALID = ['personio','softgarden','rexx','onlyfy','hr4you','umantis','talentsoft',
      'erecruiter','connectoor','onapply','jobware','join','haufe','prescreen',
      'talention','pinpoint','pidelta','concludis','workwise','viasto',
      'greenhouse','workday','lever','successfactors','teamtailor',
      'smartrecruiters','recruitee','taleo','icims','bamboohr','jobvite',
      'ashby','workable','breezyhr','custom','unknown'
    ];

    const response = await client.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 50,
      messages: [{
        role: 'user',
        content: `You are an expert at detecting Applicant Tracking Systems (ATS).\nAnalyze this career page data and identify which ATS is used.\n\n${context}\n\nReply with ONLY ONE WORD from this exact list:\n${VALID.join(', ')}\n\nUse "custom" if the company built their own job listing system.\nUse "unknown" if there are no jobs or no ATS detectable.`
      }]
    });

    const answer = response.content[0].text.trim().toLowerCase().split(/\s/)[0];
    console.log(`      ↳ Claude says: "${answer}"`);
    return VALID.includes(answer) ? answer : 'unknown';
  } catch (err) {
    console.log(`      ↳ Claude error: ${err.message}`);
    return 'unknown';
  }
}

// ─── MAIN EXPORT ──────────────────────────────────────────────────────────────
async function detectATS(companyId, websiteUrl) {
  const result = {
    company_id: companyId,
    career_page_url: websiteUrl || '',
    ats_type: 'unknown',
    ats_confidence: 0,
    ats_api_url: null,
    detection_method: null,
    error: null,
  };

  if (!websiteUrl || websiteUrl.trim() === '') {
    result.ats_type = 'no_url';
    return result;
  }

  const normUrl = normalizeUrl(websiteUrl);

  // LAYER 1: URL pattern
  for (const p of ATS_URL_PATTERNS) {
    if (p.regex.test(normUrl)) {
      result.ats_type = p.ats;
      result.ats_confidence = 0.95;
      result.detection_method = 'url_pattern';
      result.career_page_url = normUrl;
      console.log(`    [L1] ${p.ats} (url_pattern)`);
      return result;
    }
  }

  // fetch career page
  let page = null;
  try {
    page = await fetchCareerPage(normUrl);
  } catch (err) {
    result.ats_type = 'error';
    result.error = err.message || 'Fetch failed';
    return result;
  }

  if (!page || !page.html) {
    result.ats_type = 'error';
    result.error = 'Could not fetch any page';
    return result;
  }

  result.career_page_url = page.url || normUrl;

  // LAYER 1 on discovered URL
  for (const p of ATS_URL_PATTERNS) {
    if (p.regex.test(page.url)) {
      result.ats_type = p.ats;
      result.ats_confidence = 0.95;
      result.detection_method = 'url_pattern_redirect';
      console.log(`    [L1r] ${p.ats} (redirect url)`);
      return result;
    }
  }

  // LAYER 2: HTML deep scan
  let l2 = null;
  try {
    l2 = deepScanHtml(page.html, page.url);
  } catch (e) {
    result.ats_type = 'error';
    result.error = e.message || String(e);
    return result;
  }

  if (l2) {
    result.ats_type = l2.ats_type;
    result.ats_confidence = l2.ats_confidence;
    result.ats_api_url = l2.ats_api_url;
    result.detection_method = l2.detection_method;
    console.log(`    [L2] ${l2.ats_type} (${l2.detection_method}, conf: ${l2.ats_confidence})`);
    return result;
  }

  // check job content → custom
  const score = jobContentScore(String(page.html));
  if (score >= 2) {
    result.ats_type = 'custom';
    result.ats_confidence = 0.72;
    result.detection_method = 'job_content_heuristic';
    console.log(`    [L2h] custom (job_content_heuristic, score=${score})`);
    return result;
  }

  // LAYER 3: Claude fallback
  console.log(`    [L3] Claude fallback...`);
  const claudeAts = await claudeFallback(String(page.html), page.url);
  result.ats_type = claudeAts;
  result.ats_confidence = 0.60;
  result.detection_method = 'claude_fallback';
  return result;
}

module.exports = { detectATS };
