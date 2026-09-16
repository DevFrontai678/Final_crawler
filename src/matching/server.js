#!/usr/bin/env node
'use strict';

/**
 * PRODUCTION CUSTOMER MATCHING ENGINE v10.2
 *
 * Main changes from v5.3
 *
 * 1. Uses the complete candidate payload for semantic matching.
 *    This includes MCG content, Frontsheet content, summaries, experience,
 *    education, certifications, industries, languages and any other
 *    professional fields supplied by the upstream workflow.
 *
 * 2. Recursively reads nested JSON objects and arrays, so Finance and Legal
 *    Frontsheets do not need to share one rigid schema.
 *
 * 3. Uses the complete stored candidate row as a fallback when fields already
 *    exist in Supabase.
 *
 * 4. Rebuilds the candidate embedding on every matching request so changes in
 *    any candidate content can affect the result, not only summary changes.
 *
 * 5. Splits long profiles into chunks, embeds every chunk, then creates one
 *    weighted profile embedding. This avoids silently dropping long MCG or
 *    Frontsheet content.
 *
 * 6. Separates IT Consulting, Construction, Business, Finance and Legal.
 *    Accounting, Audit and Tax are normalized into Finance.
 *
 * 7. Removes the old early gate that discarded jobs before semantic profile
 *    similarity could be considered.
 *
 * 8. Fixes overly broad garbage title regexes that rejected valid job titles.
 *
 * 9. Uses dynamic scoring. Missing skill or title data does not give a free
 *    score and does not block semantic matching. Its unused weight is
 *    redistributed across the signals that are actually available.
 *
 * 10. Always clears stale stored matches before writing a new result set.
 */

const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const axios = require('axios');
const fs = require('fs');
const https = require('https');
const { v4: uuidv4 } = require('uuid');
const crypto = require('crypto');
require('dotenv').config();

// Configuration
const PORT = Number.parseInt(process.env.PORT || '3000', 10);
const HOST = process.env.HOST || '0.0.0.0';
const MAX_CONCURRENT_MATCHES = Number.parseInt(process.env.MAX_CONCURRENT_MATCHES || '4', 10);
const JOB_CACHE_TTL_MS = Number.parseInt(process.env.JOB_CACHE_TTL_MS || '3600000', 10);
const TIMEOUT_MS = Number.parseInt(process.env.TIMEOUT_MS || '120000', 10);
const DB_MAX_RETRIES = Math.max(1, Number.parseInt(process.env.DB_MAX_RETRIES || '2', 10));
const DB_RETRY_BASE_MS = Math.max(250, Number.parseInt(process.env.DB_RETRY_BASE_MS || '500', 10));
const DB_REQUEST_TIMEOUT_MS = Math.max(8000, Number.parseInt(process.env.DB_REQUEST_TIMEOUT_MS || '20000', 10));
const DB_SLOW_QUERY_MS = Math.max(1000, Number.parseInt(process.env.DB_SLOW_QUERY_MS || '3000', 10));
const JOB_FETCH_PAGE_SIZE = Math.max(100, Number.parseInt(process.env.JOB_FETCH_PAGE_SIZE || '500', 10));
const COMPANY_FETCH_PAGE_SIZE = Math.max(50, Number.parseInt(process.env.COMPANY_FETCH_PAGE_SIZE || '500', 10));
const JSON_LIMIT = process.env.JSON_LIMIT || '25mb';
const MATCH_WEBHOOK_SECRET = process.env.MATCH_WEBHOOK_SECRET || '';
const BUILD_ID = '2026-08-19-v10.3-unique-top30-candidate-data';

// Semantic profile embedding configuration
const EMBED_CHUNK_CHARS = Math.max(2000, Number.parseInt(process.env.EMBED_CHUNK_CHARS || '8000', 10));
const EMBED_BATCH_SIZE = Math.max(1, Number.parseInt(process.env.EMBED_BATCH_SIZE || '8', 10));
const MAX_PROFILE_CHARS = Math.max(0, Number.parseInt(process.env.MAX_PROFILE_CHARS || '0', 10));

// Scoring weights
const SEMANTIC_WEIGHT = Number.parseFloat(process.env.SEMANTIC_WEIGHT || '0.16');
const SKILL_WEIGHT = Number.parseFloat(process.env.SKILL_WEIGHT || '0.18');
const TITLE_WEIGHT = Number.parseFloat(process.env.TITLE_WEIGHT || '0.34');
const DIVISION_WEIGHT = Number.parseFloat(process.env.DIVISION_WEIGHT || '0.03');
const ROLE_WEIGHT = Number.parseFloat(process.env.ROLE_WEIGHT || '0.21');
const SENIORITY_WEIGHT = Number.parseFloat(process.env.SENIORITY_WEIGHT || '0.08');
const TITLE_BONUS = Number.parseFloat(process.env.TITLE_BONUS || '0.05');

const MIN_SEMANTIC_SIMILARITY = Number.parseFloat(process.env.MIN_SEMANTIC_SIMILARITY || '0.15');
const DEFAULT_RADIUS_KM = Number.parseFloat(process.env.DEFAULT_RADIUS_KM || '50');
const DEFAULT_TOP_K = Number.parseInt(process.env.DEFAULT_TOP_K || '30', 10);
const MIN_RETURN_MATCHES = Math.max(1, Number.parseInt(process.env.MIN_RETURN_MATCHES || '30', 10));
const MAX_TOP_K = Math.max(MIN_RETURN_MATCHES, Number.parseInt(process.env.MAX_TOP_K || '100', 10));


// Quality controls for CV accurate matching.
// These defaults are deliberately conservative. It is better to return 12
// genuinely relevant vacancies than pad the result to 30 with weak or
// crawler generated garbage.
const MIN_FINAL_SCORE = Number.parseFloat(process.env.MIN_FINAL_SCORE || '0.60');
const MIN_TITLE_SIGNAL = Number.parseFloat(process.env.MIN_TITLE_SIGNAL || '0.12');
const MIN_SKILL_SIGNAL = Number.parseFloat(process.env.MIN_SKILL_SIGNAL || '0.10');
const MIN_ROLE_FAMILY_SCORE = Number.parseFloat(process.env.MIN_ROLE_FAMILY_SCORE || '0.65');
const STRONG_SKILL_OVERRIDE = Number.parseFloat(process.env.STRONG_SKILL_OVERRIDE || '0.65');
const STRONG_TITLE_OVERRIDE = Number.parseFloat(process.env.STRONG_TITLE_OVERRIDE || '0.70');
const MAX_SENIORITY_GAP = Number.parseInt(process.env.MAX_SENIORITY_GAP || '1', 10);
const STRICT_DIVISION_MATCH = !/^(0|false|no)$/i.test(String(process.env.STRICT_DIVISION_MATCH || 'true'));
const ALLOW_REMOTE_OUTSIDE_RADIUS = /^(1|true|yes)$/i.test(String(process.env.ALLOW_REMOTE_OUTSIDE_RADIUS || 'false'));
const REJECT_FOREIGN_SCRIPT_TITLES = !/^(0|false|no)$/i.test(String(process.env.REJECT_FOREIGN_SCRIPT_TITLES || 'true'));
const STRICT_JOB_TITLE_VALIDATION = !/^(0|false|no)$/i.test(String(process.env.STRICT_JOB_TITLE_VALIDATION || 'true'));
const REJECT_NON_JOB_URLS = !/^(0|false|no)$/i.test(String(process.env.REJECT_NON_JOB_URLS || 'true'));
const ALLOW_REMOTE_UNKNOWN_DISTANCE = /^(1|true|yes)$/i.test(String(process.env.ALLOW_REMOTE_UNKNOWN_DISTANCE || 'false'));
const SEMANTIC_SHORTLIST_SIZE = Math.max(100, Number.parseInt(process.env.SEMANTIC_SHORTLIST_SIZE || '250', 10));
const CANDIDATE_EMBED_CACHE_TTL_MS = Math.max(60000, Number.parseInt(process.env.CANDIDATE_EMBED_CACHE_TTL_MS || '21600000', 10));

const SEMANTIC_CALIBRATION_FLOOR = Number.parseFloat(process.env.SEMANTIC_CALIBRATION_FLOOR || '0.45');
const SEMANTIC_CALIBRATION_CEILING = Number.parseFloat(process.env.SEMANTIC_CALIBRATION_CEILING || '0.82');
const MAX_INDEXED_CANDIDATE_POOL = Math.max(500, Number.parseInt(process.env.MAX_INDEXED_CANDIDATE_POOL || '3000', 10));
const FINAL_GARBAGE_GUARD = !/^(0|false|no)$/i.test(String(process.env.FINAL_GARBAGE_GUARD || 'true'));

// Adaptive clean backfill. The engine keeps all hard safety gates such as
// garbage rejection, division, seniority and selected radius, but relaxes the
// soft relevance gates when the strict pass produces fewer than top_k rows.
// This lets Salesforce receive the requested top 30 clean vacancies without
// padding the response with crawler headings or unrelated divisions.
const GUARANTEE_TOP_K = !/^(0|false|no)$/i.test(String(process.env.GUARANTEE_TOP_K || 'true'));
const BACKFILL_MIN_ROLE_FAMILY_SCORE = Number.parseFloat(process.env.BACKFILL_MIN_ROLE_FAMILY_SCORE || '0.25');
const BACKFILL_MIN_TITLE_SIGNAL = Number.parseFloat(process.env.BACKFILL_MIN_TITLE_SIGNAL || '0.05');
const BACKFILL_MIN_SKILL_SIGNAL = Number.parseFloat(process.env.BACKFILL_MIN_SKILL_SIGNAL || '0.05');
const BACKFILL_MIN_SEMANTIC_SIMILARITY = Number.parseFloat(process.env.BACKFILL_MIN_SEMANTIC_SIMILARITY || '0.05');
const BACKFILL_SEMANTIC_LIMIT = Math.max(100, Number.parseInt(process.env.BACKFILL_SEMANTIC_LIMIT || '400', 10));

// Every returned vacancy must have a real company name. Missing or placeholder
// company values are rejected before indexing, matching, persistence and response.
const REQUIRE_COMPANY_NAME = !/^(0|false|no)$/i.test(String(process.env.REQUIRE_COMPANY_NAME || 'true'));
const EXACT_TOP_30 = !/^(0|false|no)$/i.test(String(process.env.EXACT_TOP_30 || 'true'));
const TOP30_ALLOW_OUTSIDE_RADIUS_FALLBACK =
  !/^(0|false|no)$/i.test(String(process.env.TOP30_ALLOW_OUTSIDE_RADIUS_FALLBACK || 'true'));
const TOP30_OUTSIDE_RADIUS_LIMIT = Math.max(
  1000,
  Number.parseInt(process.env.TOP30_OUTSIDE_RADIUS_LIMIT || '5000', 10)
);
const TOP30_OUTSIDE_RADIUS_MIN_ROLE_SCORE = Number.parseFloat(
  process.env.TOP30_OUTSIDE_RADIUS_MIN_ROLE_SCORE || '0.30'
);
const TOP30_OUTSIDE_RADIUS_MIN_SEMANTIC = Number.parseFloat(
  process.env.TOP30_OUTSIDE_RADIUS_MIN_SEMANTIC || '0.05'
);

// Selected radius is a hard constraint when enabled.
const STRICT_SELECTED_RADIUS = !/^(0|false|no)$/i.test(
  String(process.env.STRICT_SELECTED_RADIUS || 'true')
);

// Fully remote jobs may bypass the selected geographic radius only when the
// candidate's own remote_preference indicates that remote work is acceptable.
const CANDIDATE_REMOTE_RADIUS_EXCEPTION = !/^(0|false|no)$/i.test(
  String(process.env.CANDIDATE_REMOTE_RADIUS_EXCEPTION || 'true')
);

// Remote fallback can be used to complete Top 30, but it remains profession
// aware and never allows an outside-radius onsite/hybrid job.
const REMOTE_TOP30_MIN_ROLE_SIGNAL = Number.parseFloat(
  process.env.REMOTE_TOP30_MIN_ROLE_SIGNAL || '0.45'
);
const REMOTE_TOP30_MIN_TITLE_SIGNAL = Number.parseFloat(
  process.env.REMOTE_TOP30_MIN_TITLE_SIGNAL || '0.18'
);
const REMOTE_TOP30_MIN_SEMANTIC = Number.parseFloat(
  process.env.REMOTE_TOP30_MIN_SEMANTIC || '0.05'
);

// If remote preference is genuinely not specified, fully remote vacancies may
// be used only as the final Top-30 fallback. Explicit onsite/no-remote still
// blocks this.
const REMOTE_UNSPECIFIED_TOP30_FALLBACK = !/^(0|false|no)$/i.test(
  String(process.env.REMOTE_UNSPECIFIED_TOP30_FALLBACK || 'true')
);
const REMOTE_TOP30_MIN_FINAL_SCORE = Number.parseFloat(
  process.env.REMOTE_TOP30_MIN_FINAL_SCORE || '0.50'
);

// Collect more than 30 raw candidates so final deduplication can still return
// 30 unique vacancies whenever enough distinct eligible jobs exist.
const UNIQUE_TOP30_COLLECTION_MULTIPLIER = Math.max(
  2,
  Number.parseInt(process.env.UNIQUE_TOP30_COLLECTION_MULTIPLIER || '3', 10)
);

// Backfill may be slightly more flexible than the strict pass, but it still
// must clear a meaningful relevance score.
const CLEAN_BACKFILL_MIN_FINAL_SCORE = Number.parseFloat(
  process.env.CLEAN_BACKFILL_MIN_FINAL_SCORE || '0.55'
);
const BROAD_BACKFILL_MIN_FINAL_SCORE = Number.parseFloat(
  process.env.BROAD_BACKFILL_MIN_FINAL_SCORE || '0.55'
);

// Broad fill must retain at least one concrete profession signal.
const BROAD_MIN_ROLE_SIGNAL = Number.parseFloat(
  process.env.BROAD_MIN_ROLE_SIGNAL || '0.45'
);
const BROAD_MIN_TITLE_SIGNAL = Number.parseFloat(
  process.env.BROAD_MIN_TITLE_SIGNAL || '0.20'
);
const BROAD_MIN_SKILL_SIGNAL = Number.parseFloat(
  process.env.BROAD_MIN_SKILL_SIGNAL || '0.15'
);


// Final broad fill used only when the strict and normal backfill passes still
// have fewer than the required result count. It keeps hard data quality,
// division, seniority, location and company gates, while ranking a wider pool
// from the candidate division so Salesforce can receive 30 clean jobs whenever
// at least 30 eligible jobs exist in the dataset.
const BROAD_BACKFILL_SEMANTIC_LIMIT = Math.max(300, Number.parseInt(process.env.BROAD_BACKFILL_SEMANTIC_LIMIT || '1500', 10));
const BROAD_BACKFILL_MIN_SEMANTIC_SIMILARITY = Number.parseFloat(process.env.BROAD_BACKFILL_MIN_SEMANTIC_SIMILARITY || '0.05');


// Business-only matching fallback.
// These settings are used ONLY when resolvedDivision === 'Business' and the
// candidate has no structured skill_scores but does have a usable summary.
const BUSINESS_NO_SKILLS_SUMMARY_MIN_CHARS = Math.max(
  20,
  Number.parseInt(process.env.BUSINESS_NO_SKILLS_SUMMARY_MIN_CHARS || '40', 10)
);
const BUSINESS_NO_SKILLS_SUMMARY_OVERRIDE = Number.parseFloat(
  process.env.BUSINESS_NO_SKILLS_SUMMARY_OVERRIDE || '0.35'
);
const BUSINESS_NO_SKILLS_ROLE_BACKFILL_MIN = Number.parseFloat(
  process.env.BUSINESS_NO_SKILLS_ROLE_BACKFILL_MIN || '0.55'
);

const BUSINESS_PROFILE_SEMANTIC_FLOOR = Number.parseFloat(
  process.env.BUSINESS_PROFILE_SEMANTIC_FLOOR || '0.30'
);
const BUSINESS_PROFILE_SEMANTIC_CEILING = Number.parseFloat(
  process.env.BUSINESS_PROFILE_SEMANTIC_CEILING || '0.76'
);
const BUSINESS_PRIMARY_ROLE_MIN = Number.parseFloat(
  process.env.BUSINESS_PRIMARY_ROLE_MIN || '0.55'
);


const WEIGHT_SUM = SEMANTIC_WEIGHT + SKILL_WEIGHT + TITLE_WEIGHT + DIVISION_WEIGHT + ROLE_WEIGHT + SENIORITY_WEIGHT;
if (!Number.isFinite(WEIGHT_SUM) || WEIGHT_SUM <= 0) {
  console.error('Invalid matching weights. Check semantic, skill, title, division, role and seniority weights.');
  process.exit(1);
}

// Division configuration
const DIVISION_CONFIG = {
  'IT Consulting': {
    minSkillOverlap: 0,
    requireSkillMatch: false,
  },
  Construction: {
    minSkillOverlap: 0,
    requireSkillMatch: false,
  },
  Business: {
    minSkillOverlap: 0,
    requireSkillMatch: false,
  },
  Finance: {
    minSkillOverlap: 0,
    requireSkillMatch: false,
  },
  Legal: {
    minSkillOverlap: 0,
    requireSkillMatch: false,
  },
  default: {
    minSkillOverlap: 0,
    requireSkillMatch: false,
  },
};

// Garbage title and crawler page detection
//
// These rules are based on the real jobs export supplied by the client.
// The export contains valid vacancies mixed with career landing pages,
// 404 pages, cookie/privacy pages, social sharing URLs, contact pages,
// product pages, navigation labels and page section headings.
//
// Matching must never allow extracted page skills to rescue one of these
// records. A job must first look like a real vacancy title.
const GARBAGE_TITLE_PATTERNS = [
  // Career and vacancy landing pages
  /^karriere$/i,
  /^career$/i,
  /^careers$/i,
  /^jobs$/i,
  /^job$/i,
  /^job offers$/i,
  /^stellenangebote$/i,
  /^aktuelle stellenangebote$/i,
  /^offene stellen$/i,
  /^unsere stellenangebote$/i,
  /^unsere jobs$/i,
  /^aktuelle jobs$/i,
  /^open positions$/i,
  /^career opportunities$/i,
  /^join us$/i,
  /^join our team$/i,
  /^work with us$/i,
  /^jobs\s*&\s*karriere$/i,
  /^karriere\s*&\s*jobs$/i,
  /^jobs at .+$/i,
  /^karriere bei .+$/i,
  /^jobportal$/i,
  /^jobsportal$/i,
  /^job board$/i,
  /^job portal$/i,
  /^stellenmarkt$/i,
  /^jobmarkt$/i,

  // Generic application pages are not concrete vacancies
  /^initiativbewerbung(?:\s*\([^)]*\))?$/i,
  /^proactive application(?:\s*\([^)]*\))?$/i,
  /^proactive application\s*\/\s*referral$/i,
  /^online(?:-| )bewerbung$/i,
  /^bewerbung$/i,
  /^referral$/i,
  /^bewirb dich direkt online bei uns!?$/i,

  // Generic education landing pages. Specific occupations such as
  // "Ausbildung zum Fachinformatiker" remain valid.
  /^ausbildung$/i,
  /^studium$/i,
  /^duales studium$/i,
  /^ausbildung\s*(?:&|und|\/)\s*(?:duales\s+)?studium$/i,

  // Error and missing pages
  /^untitled$/i,
  /^404$/i,
  /^404[\s\-:|].*$/i,
  /^fehler\s*404\b.*$/i,
  /^error\s*404\b.*$/i,
  /^seite nicht gefunden\b.*$/i,
  /^page not found\b.*$/i,
  /^sorry[, ]+page not found\b.*$/i,
  /^oops[,! ]+.*(?:page )?could not be found\b.*$/i,

  // Legal, cookie, privacy and navigation pages
  /^impressum(?:\b.*)?$/i,
  /^datenschutz(?:\b.*)?$/i,
  /^datenschutzerkl[aä]rung(?:\b.*)?$/i,
  /^privacy(?: policy)?(?:\b.*)?$/i,
  /^cookie(?:s| hinweis| details)?(?:\b.*)?$/i,
  /^wir verwenden cookies\b.*$/i,
  /^wir nutzen diese cookie\b.*$/i,
  /^einwilligung zu cookies\b.*$/i,
  /^kontakt$/i,
  /^contact$/i,
  /^faq$/i,
  /^search$/i,
  /^suchen$/i,
  /^zur[uü]ck$/i,
  /^startseite$/i,
  /^home$/i,
  /^newsletter$/i,
  /^newsletter anmeldung$/i,
  /^newsletter abbestellen$/i,

  // Social and UI labels
  /^facebook$/i,
  /^instagram$/i,
  /^linkedin$/i,
  /^xing$/i,
  /^youtube$/i,
  /^twitter$/i,
  /^0 benachrichtigungen$/i,
  /^w[aä]hlen sie eine sprache$/i,
  /^events?$/i,
  /^marken$/i,
  /^wissen$/i,
  /^schule$/i,
  /^campus$/i,
  /^people$/i,
  /^pupils$/i,

  // Company/about/marketing page headings
  /^services?\s*:?$/i,
  /^managed services?\s*:?$/i,
  /^business services?\s*:?$/i,
  /^(?:u|ü)ber uns\.?\s*:?$/i,
  /^about us\s*:?$/i,
  /^wer wir sind\??$/i,
  /^wir (?:u|ü)ber uns\.?$/i,
  /^das besch[aä]ftigt die welt\.?$/i,
  /^heilen\.\s*pflegen\.\s*helfen\.?$/i,
  /^b[uü]robedarf$/i,

  // Page section headings accidentally stored as vacancy titles
  /^ihre aufgaben(?:\b.*)?$/i,
  /^deine aufgaben(?:\b.*)?$/i,
  /^ihre t[aä]tigkeiten(?:\b.*)?$/i,
  /^deine t[aä]tigkeiten(?:\b.*)?$/i,
  /^unsere benefits(?:\b.*)?$/i,
  /^deine benefits(?:\b.*)?$/i,
  /^ihre benefits(?:\b.*)?$/i,
  /^unsere vorteile(?:\b.*)?$/i,
  /^deine vorteile(?:\b.*)?$/i,
  /^ihre vorteile(?:\b.*)?$/i,
  /^anforderungen\s*:?$/i,
  /^ihr profil(?:\b.*)?$/i,
  /^dein profil(?:\b.*)?$/i,
  /^was sie erwartet(?:\b.*)?$/i,
  /^was dich erwartet(?:\b.*)?$/i,
  /^wir bieten(?:\b.*)?$/i,
  /^das bieten wir(?:\b.*)?$/i,
  /^was wir bieten(?:\b.*)?$/i,
  /^job description\s*:?$/i,
  /^stellenbeschreibung\s*:?$/i,
  /^responsibilities\s*:?$/i,
  /^requirements\s*:?$/i,

  // Other known crawler headings from the supplied export
  /^so gestaltet sich deine ausbildung bei uns\s*:?$/i,
  /^das sind (?:ihre|deine) aufgaben\s*:?$/i,
  /^das bringst du mit\b.*$/i,
  /^womit du uns beeindruckst\b.*$/i,
  /^sie bringen neben einem serviceorientierten denken und handeln mit\s*:?$/i,
  /^folgendes bringst du mit\s*:?$/i,
  /^hier finden sie eine [uü]bersicht unserer .*stellenangebote\s*:?$/i,
  /^aktuelle stellenausschreibungen\s*:?$/i,
  /^offene jobs in .*$/i,
  /^open positions at .*$/i,

  // Existing production noise rules
  /great to have you here/i,
  /super, dass du hier bist/i,
  /wir suchen dich/i,
  /are you looking for new challenges/i,
  /dein traumjob/i,
  /willkommen in ihrer zukunft/i,
  /bewerbungsprozess/i,
  /bewerben\.\s*begegnen\.\s*beginnen\./i,
  /see beyond\.\s*secure beyond\./i,
  /passioniert,\s*eigenverantwortlich/i,
  /wir freuen uns auf ihre anfrage/i,
  /menschlichkeit pragmatismus nachhaltigkeit/i,
  /^gr[uü]ne baustelle$/i,
  /^dein weg zu uns$/i,
  /^fachabteilungen\s*&\s*organisation$/i,
  /^regionaldirektionen$/i,
  /^gesch[aä]ftsstellen$/i,
  /^partner-?\/mitgliedschaften$/i,
  /^kontakte\s*&\s*standorte$/i,
  /^kommen sie ins team!?$/i,
  /^komm(?:en)? (?:sie|du) ins team!?$/i,
  /^.+\s*[—–\-|:]\s*karriere\s*$/i,
  /^.+\s*[—–\-|:]\s*career(?:s)?\s*$/i,
  /^the cloud\s*[—–-].*anbieter von professionellen.*wifi/i,
  /^homeoffice arbeitsplatz$/i,
  /^home office arbeitsplatz$/i,
  /^deine suche$/i,
  /^funktionale aufgaben(?:\b.*)?$/i,
  /^functional tasks(?:\b.*)?$/i,
  /^funktionalni zavdannja(?:\b.*)?$/i,
  /^what we do(?:\b.*)?$/i,
  /^our services(?:\b.*)?$/i,
  /^unsere services(?:\b.*)?$/i,
  /^unsere leistungen(?:\b.*)?$/i,
  /^leistungen(?:\b.*)?$/i,
  /^arbeiten bei(?:\b.*)?$/i,
  /^arbeiten bei uns(?:\b.*)?$/i,
  /^warum wir(?:\b.*)?$/i,
  /^warum zu uns(?:\b.*)?$/i,
  /^benefits(?:\b.*)?$/i,
];

// Markers that prove the crawler captured website chrome instead of a title.
const CRAWLER_UI_MARKERS = [
  'click to open the search input field',
  'usercentrics consent management',
  'consent management platform logo',
  'kategoriekategoriekategorie',
  'nach oben scrollen',
  'linkedin youtube instagram facebook',
  'facebook instagram github',
  'linkedin profile bluesky profile',
  'rss feeds',
  'suche instagram linkedin',
  'e mail usercentrics',
  'zuruckumschaltenzuruck',
  'schliessenbarcode scanner',
];

// These URL patterns can never be application URLs.
const SOCIAL_SHARE_URL_PATTERNS = [
  /^https?:\/\/(?:www\.)?google\.[^/]+\/search\?/i,
  /^https?:\/\/(?:www\.)?(?:twitter\.com|x\.com)\/(?:intent|share)/i,
  /^https?:\/\/(?:www\.)?linkedin\.com\/(?:share|sharing|sharearticle)/i,
  /^https?:\/\/(?:www\.)?facebook\.com\/(?:sharer|share\.php)/i,
];

// High confidence non vacancy pages. These are applied mainly to custom
// crawler records because structured ATS URLs may use their own route format.
const NON_JOB_CUSTOM_URL_PATTERNS = [
  /\/(?:impressum|datenschutz|privacy-policy|privacy|cookie-policy)(?:\/|$|\?)/i,
  /\/(?:kontakt|contact|newsletter)(?:\/|$|\?)/i,
  /\/(?:news|blog|magazin|presse|press|events?)(?:\/|$|\?)/i,
  /\/(?:produkte|products?|shop|downloads?)(?:\/|$|\?)/i,
  /\/(?:referenzen|references|success-stories)(?:\/|$|\?)/i,
];

function cleanJobTitle(title) {
  let value = String(title || '')
    .replace(/<[^>]*>/g, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;|&apos;/gi, "'")
    .replace(/[\u200B-\u200D\uFEFF]/g, '')
    .replace(/\s+/g, ' ')
    .trim();

  const uiCutMarkers = [
    'LupeGebäude', 'LupeGebaeude', 'Schneller-Timer', 'Standort Symbol',
    'Job merken Icon', 'Jobangebot ignorieren', 'Verantwortlichkeiten Symbol',
    'Vorteile Symbol', 'Unternehmen Symbol', 'Mitarbeiter Icon',
    'Häufige Fragen', 'Haufige Fragen', '"show"Pfeil', 'PfeilPfeilPfeil'
  ];

  let cutAt = value.length;
  for (const marker of uiCutMarkers) {
    const idx = value.toLowerCase().indexOf(marker.toLowerCase());
    if (idx >= 0) cutAt = Math.min(cutAt, idx);
  }
  value = value.slice(0, cutAt).trim();

  value = value.replace(
    /(\((?:m\/w\/d|w\/m\/d|d\/m\/w|all genders|gn)\))(?=[A-ZÄÖÜ][^\n]*(?:Hybrid|Vollzeit|Teilzeit|Festanstellung)).*$/i,
    '$1'
  );

  return value.replace(/\s+/g, ' ').trim();
}

function isGarbageTitle(title) {
  const raw = cleanJobTitle(title);
  if (!raw) return true;
  if (raw.length < 3 || raw.length > 220) return true;
  if (/^https?:\/\//i.test(raw) || /^www\./i.test(raw)) return true;

  // A colon at the very end is strongly associated with a page section
  // heading in the supplied export. Colons inside real titles remain valid.
  if (/[:：]\s*$/.test(raw)) return true;

  if (GARBAGE_TITLE_PATTERNS.some((pattern) => pattern.test(raw))) return true;

  const normalized = normalizeText(raw);
  if (!normalized) return true;

  if (CRAWLER_UI_MARKERS.some((marker) => normalized.includes(normalizeText(marker)))) {
    return true;
  }

  // Repeated social/navigation words are a strong sign that the page title
  // was concatenated with website chrome.
  const socialHits = ['linkedin', 'facebook', 'instagram', 'youtube', 'xing', 'twitter']
    .filter((term) => containsTerm(normalized, term)).length;
  if (socialHits >= 3) return true;

  return false;
}

function normalizedComparable(value) {
  return normalizeText(value)
    .replace(/\b(?:gmbh|ag|kg|se|ug|gbr|ohg|mbh|co|ltd|inc|group|holding)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function titleIsCompanyOnly(title, companyName) {
  if (!title || !companyName) return false;
  const titleNorm = normalizedComparable(title);
  const companyNorm = normalizedComparable(companyName);
  if (!titleNorm || !companyNorm) return false;
  return titleNorm === companyNorm;
}

function hasValidCompanyName(value) {
  const raw = String(value ?? '').trim();
  if (!raw) return false;

  const normalized = normalizeText(raw);
  if (!normalized) return false;

  const placeholders = new Set([
    'unknown',
    'n a',
    'na',
    'none',
    'null',
    'undefined',
    'not available',
    'not provided',
    'company',
    'employer',
    'unbekannt',
    'nicht bekannt',
    'keine angabe',
  ]);

  return !placeholders.has(normalized);
}

function getGarbageJobReason(job) {
  if (!job) return 'missing job';

  if (REQUIRE_COMPANY_NAME && !hasValidCompanyName(job.company_name)) {
    return 'missing company name';
  }

  const title = cleanJobTitle(job.title);
  if (isGarbageTitle(title)) return 'garbage title';

  if (titleIsCompanyOnly(title, job.company_name)) {
    return 'company name used as title';
  }

  const url = String(job.apply_url || '').trim();
  if (url && SOCIAL_SHARE_URL_PATTERNS.some((pattern) => pattern.test(url))) {
    return 'social share URL';
  }

  const source = normalizeText(job.ats_source || '');
  if (
    REJECT_NON_JOB_URLS &&
    source === 'custom' &&
    url &&
    NON_JOB_CUSTOM_URL_PATTERNS.some((pattern) => pattern.test(url))
  ) {
    return 'non job custom URL';
  }

  return null;
}

function isGarbageJob(job) {
  return getGarbageJobReason(job) !== null;
}

function hasITDomainAnchor(title) {
  const value = normalizeText(title);
  if (!value) return false;

  return /\b(it|ict|edv|informatik|information technology|system|systeme|systemtechnik|network|netzwerk|server|client|desktop|computer|software|hardware|windows|linux|microsoft|azure|m365|office 365|active directory|vmware|cloud|cisco|firewall|infrastruktur|infrastructure|helpdesk|service desk|cyber|security)\b/.test(
    value
  );
}

function isAmbiguousNonITServiceTechnicianTitle(title) {
  const value = normalizeText(title);
  if (!value) return false;

  const ambiguousServiceTitle =
    /\b(servicetechniker|service techniker|service technician|field service technician)\b/.test(
      value
    );

  return ambiguousServiceTitle && !hasITDomainAnchor(value);
}

function strictVacancyGuard(job, division, candidateScriptText = '') {
  if (!FINAL_GARBAGE_GUARD) return null;
  if (!job) return 'missing job';

  const title = cleanJobTitle(job.title);
  const base = { ...job, title };
  const basicReason = getGarbageJobReason(base);
  if (basicReason) return basicReason;

  if (!title || title.length > 170) return 'invalid title length';

  const normalized = normalizeText(title);
  const words = normalized.split(/\s+/).filter(Boolean);
  if (words.length > 24) return 'crawler concatenated title';

  if (foreignScriptMismatch(candidateScriptText, title)) {
    return 'foreign script title';
  }

  // Known website section and marketing language that can contain IT terms
  // without being an actual vacancy.
  const pageHeadingPatterns = [
    /^(?:ihre|deine|unsere)\s+(?:aufgaben|anforderungen|benefits|vorteile|leistungen)\b/i,
    /^(?:das|was)\s+(?:bieten|erwartet|bringst)\b/i,
    /^(?:kommen|komm)\s+(?:sie|du)\s+ins\s+team\b/i,
    /\b(?:karriere|career|careers)\s*$/i,
    /^homeoffice\s+arbeitsplatz\b/i,
    /^the\s+cloud\b.*\bwifi\b/i,
    /^(?:services?|managed services?)\s*$/i,
  ];

  if (pageHeadingPatterns.some((pattern) => pattern.test(normalized))) {
    return 'website heading';
  }

  if (
    division === 'IT Consulting' &&
    isAmbiguousNonITServiceTechnicianTitle(title)
  ) {
    return 'non IT service technician';
  }

  if (
    STRICT_JOB_TITLE_VALIDATION &&
    division &&
    division !== 'default' &&
    !hasRecognizableRoleSignal(title, division)
  ) {
    return 'no occupation signal';
  }

  return null;
}

const KNOWN_GARBAGE_SELF_TESTS = [
  'Services',
  'IHRE Aufgaben:',
  'KOmmen Sie ins Team!',
  'GOPAS Solutions – Karriere',
  'THE CLOUD – EUROPAS GRÖSSTER ANBIETER VON PROFESSIONELLEN WiFi-NETZWERKEN',
  'HomeOffice Arbeitsplatz',
  'Функціональні завдання:',
  'Servicetechniker (m/w/d) eMobility Fellbach',
];

function runGarbageSelfTest() {
  const failed = KNOWN_GARBAGE_SELF_TESTS.filter((title) => {
    const reason = strictVacancyGuard(
      {
        title,
        company_name: 'Example GmbH',
        apply_url: 'https://example.com/job/test',
        ats_source: 'custom',
      },
      'IT Consulting',
      'Angehende Fachinformatikerin Systemintegration'
    );
    return !reason;
  });

  if (failed.length > 0) {
    throw new Error(`Garbage filter self test failed for: ${failed.join(' | ')}`);
  }

  return true;
}

function scriptProfile(text) {
  const value = String(text || '');
  let latin = 0;
  let cyrillic = 0;
  let greek = 0;
  for (const ch of value) {
    if (/\p{Script=Latin}/u.test(ch)) latin += 1;
    else if (/\p{Script=Cyrillic}/u.test(ch)) cyrillic += 1;
    else if (/\p{Script=Greek}/u.test(ch)) greek += 1;
  }
  return { latin, cyrillic, greek, totalLetters: latin + cyrillic + greek };
}

function foreignScriptMismatch(candidateText, jobTitle) {
  if (!REJECT_FOREIGN_SCRIPT_TITLES) return false;
  const candidate = scriptProfile(candidateText);
  const job = scriptProfile(jobTitle);
  if (candidate.totalLetters < 3 || job.totalLetters < 3) return false;

  const candidateMostlyLatin = candidate.latin / candidate.totalLetters >= 0.80;
  const jobForeignShare = (job.cyrillic + job.greek) / job.totalLetters;
  return candidateMostlyLatin && jobForeignShare >= 0.35;
}

// Stopwords for title and lexical processing
const STOPWORDS = new Set([
  'der', 'die', 'das', 'und', 'oder', 'für', 'mit', 'von', 'zu', 'im', 'am',
  'als', 'auch', 'auf', 'bei', 'durch', 'in', 'nach', 'um', 'über', 'unter',
  'ohne', 'des', 'dem', 'den', 'ein', 'eine', 'eines', 'einer', 'einem', 'einen',
  'the', 'and', 'or', 'for', 'with', 'without', 'of', 'to', 'from', 'by',
  'at', 'on', 'into', 'through', 'during', 'including', 'per', 'via', 'a', 'an',
  'is', 'are', 'be', 'as', 'our', 'your', 'we', 'you', 'this', 'that', 'role',
  'position', 'job', 'candidate', 'profile', 'summary',
]);

function normalizeText(value) {
  return String(value ?? '')
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9äöüß+#.]+/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractKeywords(text) {
  const normalized = normalizeText(text);
  if (!normalized) return [];
  const words = normalized.split(/\s+/g);
  return [...new Set(words.filter((word) => word.length > 2 && !STOPWORDS.has(word)))];
}

function clamp01(value) {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(1, value));
}

function hasMeaningfulValue(value) {
  if (value === undefined || value === null) return false;
  if (typeof value === 'string') return value.trim().length > 0;
  if (Array.isArray(value)) return value.length > 0;
  if (typeof value === 'object') return Object.keys(value).length > 0;
  return true;
}

function preferIncoming(incoming, existing, fallback = null) {
  return hasMeaningfulValue(incoming) ? incoming : (hasMeaningfulValue(existing) ? existing : fallback);
}


function getFirstValue(object, keys, fallback = null) {
  if (!object || typeof object !== 'object') return fallback;

  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(object, key) && hasMeaningfulValue(object[key])) {
      return object[key];
    }
  }

  const lowerMap = new Map(
    Object.keys(object).map((key) => [String(key).toLowerCase().replace(/[\s_-]+/g, ''), key])
  );

  for (const key of keys) {
    const normalized = String(key).toLowerCase().replace(/[\s_-]+/g, '');
    const actualKey = lowerMap.get(normalized);
    if (actualKey && hasMeaningfulValue(object[actualKey])) return object[actualKey];
  }

  return fallback;
}

function toFiniteNumber(value) {
  if (value === null || value === undefined || value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

// Division handling
const DIVISION_TERMS = {
  'IT Consulting': [
    'software', 'developer', 'development', 'devops', 'cloud', 'cyber',
    'cybersecurity', 'security engineer', 'network', 'netzwerk', 'system engineer',
    'system administrator', 'administrator', 'infrastructure', 'server', 'linux',
    'windows', 'programming', 'coding', 'sap', 'data engineer', 'data scientist',
    'database', 'sql', 'azure', 'aws', 'gcp', 'kubernetes', 'docker', 'java',
    'javascript', 'typescript', 'python', '.net', 'c#', 'frontend', 'backend',
    'fullstack', 'full stack', 'it consultant', 'technical consultant',
  ],
  Construction: [
    'construction', 'bau', 'hochbau', 'tiefbau', 'architekt', 'architect',
    'architecture', 'civil engineer', 'building', 'baustelle', 'bauleiter',
    'projektleiter bau', 'project manager construction', 'planung', 'bim',
    'immobilie', 'real estate development', 'structural engineer',
  ],
  Finance: [
    'finance', 'financial', 'accounting', 'accountant', 'buchhaltung',
    'buchhalter', 'controller', 'controlling', 'audit', 'auditor', 'tax',
    'steuer', 'steuerberater', 'treasury', 'ifrs', 'gaap', 'payroll',
    'accounts payable', 'accounts receivable', 'fp&a', 'financial analyst',
    'investment', 'banking', 'credit', 'risk management',
  ],
  Legal: [
    'legal', 'lawyer', 'attorney', 'anwalt', 'rechtsanwalt', 'jurist',
    'juristin', 'counsel', 'general counsel', 'legal counsel', 'recht',
    'compliance', 'contract law', 'contracts', 'datenschutz', 'privacy law',
    'litigation', 'corporate law', 'arbeitsrecht', 'gesellschaftsrecht',
  ],
  Business: [
    'business development', 'sales', 'account manager', 'key account',
    'marketing', 'procurement', 'einkauf', 'operations', 'strategy',
    'human resources', 'hr manager', 'recruiter', 'recruitment', 'talent',
    'supply chain', 'logistics', 'customer success', 'customer service',
    'project manager', 'product manager', 'management consultant',
  ],
 };

// Role families are used as a hard relevance guard. Semantic similarity by
// itself is not enough because a CV containing Java, Azure and Windows can
// otherwise match a Java developer vacancy even when the person is actually
// a System Integration or 1st Level Support candidate.
const ROLE_FAMILIES = {
  'IT Consulting': {
    support: [
      'helpdesk', 'help desk', 'service desk', 'it support', 'technical support',
      'support engineer', 'support specialist', 'supporter', '1st level',
      '2nd level', '3rd level', 'first level', 'second level', 'third level',
      '1st line', '2nd line', '3rd line', 'first line', 'second line', 'third line',
      'it service techniker', 'it servicetechniker', 'it service specialist',
      'it support techniker', 'it support technician', 'desktop support',
      'it field support', 'support mitarbeiter', 'anwender support',
      'anwenderbetreuung',
    ],
    systems: [
      'systemadministrator', 'system administrator', 'it administrator',
      'administrator', 'system engineer', 'systems engineer', 'systemintegration',
      'fachinformatiker systemintegration', 'windows server', 'active directory',
      'microsoft 365', 'm365', 'infrastruktur', 'infrastructure', 'server administrator',
      'systemintegrator', 'system integrator', 'it systemintegrator',
      'windows administrator', 'microsoft administrator', 'client administrator',
      'systembetreuer', 'it betreuer', 'anwenderbetreuer',
      'it systemelektroniker', 'it-systemelektroniker', 'systemelektroniker',
      'infra support', 'infrastructure support', 'infrastruktur support',
      'infrastructure technician', 'infrastruktur techniker',
      'managed services specialist', 'it infrastructure specialist',
    ],
    network: [
      'network engineer', 'network administrator', 'network specialist', 'network',
      'netzwerk', 'netzwerkadministrator', 'netzwerktechniker', 'cisco', 'firewall',
      'lan', 'wan',
    ],
    cloud_devops: [
      'devops', 'cloud engineer', 'cloud architect', 'platform engineer',
      'site reliability', 'sre', 'kubernetes', 'terraform', 'azure engineer',
      'aws engineer',
    ],
    software: [
      'software engineer', 'software developer', 'softwareentwickler', 'entwickler',
      'developer', 'frontend developer', 'backend developer', 'fullstack', 'full stack',
      'java developer', '.net developer', 'application developer', 'anwendungsentwickler',
      'fachinformatiker anwendungsentwicklung',
    ],
    security: [
      'cybersecurity', 'cyber security', 'security engineer', 'security analyst',
      'soc analyst', 'penetration tester', 'information security', 'it security',
    ],
    data: [
      'data engineer', 'data scientist', 'data analyst', 'database administrator',
      'dba', 'business intelligence', 'bi developer', 'analytics engineer',
    ],
    sap_erp: [
      'sap consultant', 'sap berater', 'sap entwickler', 'sap developer',
      'sap basis', 'abap', 's 4hana', 's4hana', 'erp consultant',
    ],
    it_general: [
      'fachinformatiker', 'it specialist', 'it spezialist', 'it technician',
      'it techniker', 'informatiker', 'information technology specialist',
    ],
  },
  Construction: {
    site_management: ['bauleiter', 'site manager', 'construction manager', 'polier', 'bauleitung'],
    planning: ['bauplaner', 'planer', 'architect', 'architekt', 'bim', 'cad', 'planung', 'designer construction'],
    engineering: ['civil engineer', 'bauingenieur', 'structural engineer', 'tragwerksplaner', 'construction engineer'],
    project: ['projektleiter bau', 'project manager construction', 'projektmanager bau', 'construction project manager'],
    estimating: ['kalkulator', 'estimator', 'cost estimator', 'quantity surveyor', 'baukalkulation'],
    trades: ['bautechniker', 'construction technician', 'facharbeiter', 'monteur', 'meister bau'],
  },
  Finance: {
    accounting: ['accountant', 'buchhalter', 'buchhaltung', 'accounts payable', 'accounts receivable', 'financial accountant', 'bilanzbuchhalter'],
    controlling: ['controller', 'controlling', 'financial controller', 'business controller'],
    audit: ['auditor', 'audit', 'wirtschaftsprufer', 'wirtschaftsprüfer', 'revision'],
    tax: ['tax consultant', 'steuerberater', 'steuerfachangestellte', 'tax manager', 'steuer'],
    fpna: ['financial analyst', 'fp&a', 'fpna', 'finance analyst', 'planning analyst'],
    treasury: ['treasury', 'treasurer', 'cash management'],
    banking: ['banker', 'banking', 'credit analyst', 'kreditanalyst', 'investment analyst'],
    finance_management: ['finance manager', 'head of finance', 'cfo', 'finance director'],
  },
  Legal: {
    counsel: ['legal counsel', 'general counsel', 'counsel', 'syndikus', 'syndikusrechtsanwalt'],
    attorney: ['lawyer', 'attorney', 'rechtsanwalt', 'anwalt', 'jurist', 'juristin'],
    compliance: ['compliance officer', 'compliance manager', 'compliance specialist', 'compliance'],
    contracts: ['contract manager', 'contracts manager', 'vertragsmanager', 'contract specialist'],
    privacy: ['data protection officer', 'datenschutzbeauftragter', 'privacy counsel', 'privacy lawyer', 'datenschutz'],
    paralegal: ['paralegal', 'rechtsanwaltsfachangestellte', 'legal assistant', 'legal secretary'],
  },
  Business: {
    payroll: [
      'payroll', 'payroll specialist', 'payroll administrator', 'payroll assistant',
      'payroll accountant', 'payroll clerk', 'payroll officer', 'payroll coordinator',
      'payroll manager', 'hr payroll', 'lohn und gehaltsbuchhalter',
      'lohn gehaltsbuchhalter', 'lohnbuchhalter', 'lohnbuchhaltung',
      'gehaltsbuchhalter', 'gehaltsabrechnung', 'lohnabrechnung',
      'entgeltabrechnung', 'entgeltabrechner', 'personalabrechnung',
      'salary accounting', 'wage accounting'
    ],
    hr_administration: [
      'hr administrator', 'hr administration', 'personaladministration',
      'personalsachbearbeiter', 'personalreferent', 'hr specialist',
      'human resources specialist', 'hr generalist', 'hr operations',
      'people operations', 'personnel administration', 'employee administration',
      'onboarding', 'offboarding', 'vertragswesen', 'personalakten'
    ],
    recruiting: [
      'recruiter', 'recruitment', 'talent acquisition', 'talent recruiter',
      'hr recruiter', 'personalrecruiting'
    ],
    administration: [
      'administrative assistant', 'office administrator', 'business administrator',
      'commercial administrator', 'kaufmann', 'kauffrau',
      'kaufmännischer mitarbeiter', 'kaufmannischer mitarbeiter',
      'kaufmännischer sachbearbeiter', 'kaufmannischer sachbearbeiter',
      'sachbearbeiter administration'
    ],
    sales: [
      'sales manager', 'sales representative', 'vertrieb', 'account executive',
      'account manager', 'key account', 'business development'
    ],
    marketing: [
      'marketing manager', 'marketing specialist', 'content manager',
      'performance marketing', 'brand manager'
    ],
    procurement: ['procurement', 'einkaufer', 'einkäufer', 'buyer', 'purchasing'],
    operations: ['operations manager', 'operations specialist', 'betriebsleiter', 'business operations'],
    customer: ['customer success', 'customer service', 'kundenservice', 'customer support'],
    project: ['project manager', 'projektmanager', 'projektleiter', 'program manager'],
    product: ['product manager', 'produktmanager', 'product owner'],
    consulting: ['management consultant', 'business consultant', 'unternehmensberater', 'strategy consultant'],
    logistics: ['logistics', 'logistik', 'supply chain', 'disponent'],
  },
};

const GENERIC_ROLE_TERMS = [
  'manager', 'specialist', 'spezialist', 'consultant', 'berater', 'engineer',
  'ingenieur', 'administrator', 'entwickler', 'developer', 'architect', 'architekt',
  'technician', 'techniker', 'analyst', 'controller', 'accountant', 'buchhalter',
  'auditor', 'counsel', 'lawyer', 'attorney', 'anwalt', 'jurist', 'paralegal',
  'bauleiter', 'projektleiter', 'projektmanager', 'fachinformatiker', 'informatiker',
  'support', 'helpdesk', 'service desk', 'recruiter', 'einkaufer', 'einkäufer',
  'sachbearbeiter', 'mitarbeiter', 'fachkraft', 'assistant', 'assistent', 'trainee',
  'azubi', 'ausbildung', 'praktikant', 'intern', 'devops', 'administratorin',
  'systemintegrator', 'system integrator', 'systembetreuer', 'anwenderbetreuer',
  'beraterin', 'ingenieurin', 'entwicklerin', 'technikerin', 'juristin',
];

const ROLE_RELATIONSHIPS = {
  'IT Consulting': {
    support: { systems: 0.85, network: 0.65, it_general: 0.75 },
    systems: { support: 0.85, network: 0.75, cloud_devops: 0.60, security: 0.45, it_general: 0.80 },
    network: { systems: 0.75, support: 0.65, security: 0.55, cloud_devops: 0.45, it_general: 0.65 },
    cloud_devops: { systems: 0.60, software: 0.45, network: 0.45, security: 0.45, it_general: 0.55 },
    software: { cloud_devops: 0.45, data: 0.35, it_general: 0.50 },
    security: { network: 0.55, systems: 0.45, cloud_devops: 0.45, it_general: 0.50 },
    data: { software: 0.35, it_general: 0.40 },
    sap_erp: { it_general: 0.45 },
    it_general: { support: 0.75, systems: 0.80, network: 0.65, cloud_devops: 0.55, software: 0.50, security: 0.50, sap_erp: 0.45 },
  },
  Construction: {
    site_management: { project: 0.85, engineering: 0.55, trades: 0.65, estimating: 0.45 },
    planning: { engineering: 0.75, project: 0.55, estimating: 0.45 },
    engineering: { planning: 0.75, project: 0.65, site_management: 0.55, estimating: 0.50 },
    project: { site_management: 0.85, engineering: 0.65, planning: 0.55, estimating: 0.55 },
    estimating: { project: 0.55, engineering: 0.50, planning: 0.45, site_management: 0.45 },
    trades: { site_management: 0.65 },
  },
  Finance: {
    accounting: { controlling: 0.70, audit: 0.65, tax: 0.55, fpna: 0.45 },
    controlling: { accounting: 0.70, fpna: 0.80, finance_management: 0.60, audit: 0.45 },
    audit: { accounting: 0.65, tax: 0.55, controlling: 0.45 },
    tax: { accounting: 0.55, audit: 0.55 },
    fpna: { controlling: 0.80, finance_management: 0.60, accounting: 0.45, treasury: 0.45 },
    treasury: { banking: 0.55, fpna: 0.45, finance_management: 0.45 },
    banking: { treasury: 0.55, fpna: 0.45 },
    finance_management: { controlling: 0.60, fpna: 0.60, accounting: 0.50, treasury: 0.45 },
  },
  Legal: {
    counsel: { attorney: 0.90, contracts: 0.75, compliance: 0.60, privacy: 0.60 },
    attorney: { counsel: 0.90, contracts: 0.65, compliance: 0.50, privacy: 0.55, paralegal: 0.40 },
    compliance: { privacy: 0.75, counsel: 0.60, contracts: 0.55, attorney: 0.50 },
    contracts: { counsel: 0.75, attorney: 0.65, compliance: 0.55 },
    privacy: { compliance: 0.75, counsel: 0.60, attorney: 0.55 },
    paralegal: { attorney: 0.40 },
  },
  Business: {
    payroll: {
      hr_administration: 0.88,
      administration: 0.65,
      recruiting: 0.25,
      operations: 0.25
    },
    hr_administration: {
      payroll: 0.88,
      administration: 0.75,
      recruiting: 0.55,
      operations: 0.50
    },
    recruiting: {
      hr_administration: 0.55,
      payroll: 0.25
    },
    administration: {
      hr_administration: 0.75,
      payroll: 0.65,
      operations: 0.45
    },
    sales: { customer: 0.55, marketing: 0.45, consulting: 0.45 },
    marketing: { sales: 0.45, product: 0.45 },
    procurement: { logistics: 0.70, operations: 0.55 },
    operations: {
      project: 0.75,
      logistics: 0.65,
      procurement: 0.55,
      product: 0.45,
      administration: 0.45,
      hr_administration: 0.50
    },
    customer: { sales: 0.55, operations: 0.45 },
    project: { operations: 0.75, product: 0.70, consulting: 0.55 },
    product: { project: 0.70, marketing: 0.45, operations: 0.45 },
    consulting: { project: 0.55, sales: 0.45, hr_administration: 0.45 },
    logistics: { procurement: 0.70, operations: 0.65 },
  },
};

function scoreFamilyTerms(text, terms, multiplier = 1) {
  const normalized = normalizeText(text);
  if (!normalized) return 0;
  let score = 0;
  for (const term of terms) {
    if (containsTerm(normalized, term)) score += (term.includes(' ') ? 2 : 1) * multiplier;
  }
  return score;
}

function inferRoleFamilies(division, titleText, contextText = '', skills = []) {
  const families = ROLE_FAMILIES[division];
  if (!families) return [];

  const scored = [];
  const skillText = Array.isArray(skills) ? skills.join(' ') : String(skills || '');

  for (const [family, terms] of Object.entries(families)) {
    // Role title is the strongest source. Summary is supporting evidence.
    // Skills are useful but intentionally weakest so a single technology does
    // not turn a System Administrator into a Software Developer.
    const score =
      scoreFamilyTerms(titleText, terms, 4.0) +
      scoreFamilyTerms(contextText, terms, 1.25) +
      scoreFamilyTerms(skillText, terms, 0.45);
    if (score > 0) scored.push({ family, score });
  }

  if (scored.length === 0) return [];
  scored.sort((a, b) => b.score - a.score);
  const best = scored[0].score;

  return scored
    .filter((item) => item.score >= 2 && item.score >= best * 0.45)
    .slice(0, 3);
}

function calculateRoleFamilyCompatibility(division, candidateFamilies, jobFamilies) {
  if (!candidateFamilies.length || !jobFamilies.length) return null;

  let best = 0;
  for (const candidate of candidateFamilies) {
    for (const job of jobFamilies) {
      if (candidate.family === job.family) {
        best = Math.max(best, 1);
        continue;
      }
      const related = ROLE_RELATIONSHIPS[division]?.[candidate.family]?.[job.family] || 0;
      best = Math.max(best, related);
    }
  }
  return clamp01(best);
}

// Business-only dynamic profile matching.
//
// No candidate ID, company, location or one-off vacancy is hardcoded here.
// The profession is derived from the position, summary and complete Business CV.
function inferBusinessRoleFamiliesFromProfile(positionText, summaryText, fullSourceText = '') {
  const families = ROLE_FAMILIES.Business;
  const scored = [];

  for (const [family, terms] of Object.entries(families)) {
    const score =
      scoreFamilyTerms(positionText, terms, 5.0) +
      scoreFamilyTerms(summaryText, terms, 2.2) +
      scoreFamilyTerms(fullSourceText, terms, 0.55);

    if (score > 0) scored.push({ family, score });
  }

  if (scored.length === 0) return [];

  scored.sort((a, b) => b.score - a.score);
  const best = scored[0].score;

  return scored
    .filter((item) => item.score >= 2 && item.score >= best * 0.32)
    .slice(0, 4);
}

function calculateBusinessProfessionFit(candidateFamilies, jobFamilies) {
  if (!candidateFamilies.length || !jobFamilies.length) return null;

  const primaryCandidate = candidateFamilies[0];
  const primaryJob = jobFamilies[0];

  const relation = (candidateFamily, jobFamily) => {
    if (!candidateFamily || !jobFamily) return 0;
    if (candidateFamily === jobFamily) return 1;
    return ROLE_RELATIONSHIPS.Business?.[candidateFamily]?.[jobFamily] || 0;
  };

  let best = relation(primaryCandidate.family, primaryJob.family);

  for (let i = 1; i < jobFamilies.length; i += 1) {
    best = Math.max(
      best,
      relation(primaryCandidate.family, jobFamilies[i].family) * 0.82
    );
  }

  for (let i = 1; i < candidateFamilies.length; i += 1) {
    best = Math.max(
      best,
      relation(candidateFamilies[i].family, primaryJob.family) * 0.68
    );
  }

  return clamp01(best);
}

function calibrateBusinessProfileSemantic(rawSimilarity) {
  const raw = Number(rawSimilarity);
  if (!Number.isFinite(raw)) return 0;

  const floor = Math.min(
    BUSINESS_PROFILE_SEMANTIC_FLOOR,
    BUSINESS_PROFILE_SEMANTIC_CEILING - 0.01
  );
  const ceiling = Math.max(
    BUSINESS_PROFILE_SEMANTIC_CEILING,
    floor + 0.01
  );

  if (raw <= floor) return 0;
  if (raw >= ceiling) return 1;
  return clamp01((raw - floor) / (ceiling - floor));
}

function effectiveBusinessTitleFit(titleMatch, professionFit) {
  let score =
    titleMatch?.available && Number.isFinite(titleMatch.score)
      ? titleMatch.score
      : null;

  if (Number.isFinite(professionFit)) {
    let familyFloor = 0;

    if (professionFit >= 0.95) familyFloor = 0.92;
    else if (professionFit >= 0.85) familyFloor = 0.84;
    else if (professionFit >= 0.75) familyFloor = 0.76;
    else if (professionFit >= BUSINESS_PRIMARY_ROLE_MIN) familyFloor = 0.66;

    if (familyFloor > 0) {
      score = score === null ? familyFloor : Math.max(score, familyFloor);
    }
  }

  return score;
}

function buildBusinessKeywordProfile(positionText, summaryText, fullSourceText = '') {
  const weights = new Map();

  const add = (value, weight, limit = 160) => {
    const keywords = extractKeywords(value).slice(0, limit);
    for (const keyword of keywords) {
      if (keyword.length < 4) continue;
      weights.set(keyword, (weights.get(keyword) || 0) + weight);
    }
  };

  add(positionText, 4.0, 40);
  add(summaryText, 1.8, 140);
  add(fullSourceText, 0.35, 220);

  const ranked = [...weights.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 45);

  return {
    weights: new Map(ranked),
    totalWeight: ranked.reduce((sum, [, weight]) => sum + weight, 0),
  };
}

function calculateBusinessProfileEvidence(profile, jobTitle, jobSkills) {
  if (!profile || profile.weights.size === 0) return null;

  const jobText = normalizeText(
    [jobTitle, ...(Array.isArray(jobSkills) ? jobSkills : [])].join(' ')
  );
  if (!jobText) return null;

  let matchedWeight = 0;

  for (const [keyword, weight] of profile.weights.entries()) {
    const exact = containsTerm(jobText, keyword);
    const partial =
      !exact &&
      keyword.length >= 7 &&
      jobText.split(/\s+/).some(
        (token) =>
          token.length >= 7 &&
          (token.includes(keyword) || keyword.includes(token))
      );

    if (exact || partial) matchedWeight += weight;
  }

  const targetWeight = Math.max(4, profile.totalWeight * 0.25);
  return clamp01(matchedWeight / targetWeight);
}

function isBusinessCandidateNoiseTitle(title) {
  const value = normalizeText(cleanJobTitle(title));
  if (!value) return true;

  const patterns = [
    /^(?:careers?|karriere)(?:\s+(?:at|bei))?\b/,
    /^your\s+(?:career|future)\s+at\b/,
    /^(?:your|our)\s+tasks?$/,
    /^contact\s+persons?$/,
    /^working\s+from\s+home\b.*$/,
    /^(?:our|your)\s+(?:benefits|advantages|responsibilities)$/,
  ];

  return patterns.some((pattern) => pattern.test(value));
}

function calculateBusinessSummaryFinalScore({
  semantic,
  title,
  division,
  role,
  seniority,
  profileEvidence,
}) {
  const signals = [
    { score: role, weight: 0.36 },
    { score: semantic, weight: 0.25 },
    { score: title, weight: 0.25 },
    { score: profileEvidence, weight: 0.08 },
    { score: seniority, weight: 0.04 },
    { score: division, weight: 0.02 },
  ];

  let total = 0;
  let weight = 0;

  for (const signal of signals) {
    if (
      signal.score === null ||
      signal.score === undefined ||
      !Number.isFinite(signal.score)
    ) {
      continue;
    }

    total += clamp01(signal.score) * signal.weight;
    weight += signal.weight;
  }

  return weight > 0 ? clamp01(total / weight) : 0;
}

function getCandidateAwareJobDivision(job, candidateDivision, businessSummaryMode) {
  const inferred = job._jobDivision || inferDivisionFromText(job.title);

  if (
    businessSummaryMode &&
    candidateDivision === 'Business'
  ) {
    const businessFamilies = getPreparedRoleFamilies(job, 'Business');
    if (businessFamilies.length > 0) return 'Business';
  }

  return inferred;
}

function hasRecognizableRoleSignal(title, division) {
  const normalized = normalizeText(title);
  if (!normalized) return false;

  // For configured divisions, require an occupation or profession signal in
  // the title itself. Skills extracted from the page are supporting evidence
  // only and cannot turn a landing page into a vacancy.
  const families = ROLE_FAMILIES[division];
  if (families) {
    for (const terms of Object.values(families)) {
      if (terms.some((term) => containsTerm(normalized, term))) return true;
    }

    // Generic occupation words are accepted only when the title also contains
    // a division anchor. This blocks titles such as "Services", "Company Name"
    // and "Managed Services" from passing because of noisy page skills.
    const genericRole = GENERIC_ROLE_TERMS.some((term) => containsTerm(normalized, term));
    if (!genericRole) return false;

    const divisionTerms = DIVISION_TERMS[division] || [];
    return divisionTerms.some((term) => containsTerm(normalized, term));
  }

  return GENERIC_ROLE_TERMS.some((term) => containsTerm(normalized, term));
}

function inferExplicitSeniorityFromTitle(title = '') {
  const text = normalizeText(title);
  if (!text) return null;

  if (/\b(geschaftsleitung|c level|cxo|ceo|cfo|cto|cio|managing director|geschaftsfuhrer)\b/.test(text)) return 5;
  if (/\b(head of|director|team lead|teamleiter|principal|staff engineer|leiter|leitung)\b/.test(text)) return 4;
  if (/\b(senior|sr\.?|experte|expert|lead engineer|senior counsel)\b/.test(text)) return 3;
  if (/\b(junior|jr\.?|einsteiger|berufseinsteiger|entry level|trainee|graduate|absolvent|angehend(?:e|er|es|en)?|ausbildung|azubi|apprentice|praktikant|intern)\b/.test(text)) return 1;
  return null;
}

function inferSeniorityFromMetadata(value = '') {
  const text = normalizeText(value);
  if (!text) return null;
  if (/\b(executive|geschaftsleitung|c level|director|head)\b/.test(text)) return 4;
  if (/\b(senior|expert|experte|lead)\b/.test(text)) return 3;
  if (/\b(mid|middle|fachkraft|professional|specialist|spezialist|consultant|berater)\b/.test(text)) return 2;
  if (/\b(junior|entry|trainee|graduate|intern|apprentice)\b/.test(text)) return 1;
  return null;
}

function inferSeniorityLevel(value, title = '') {
  return inferExplicitSeniorityFromTitle(title) ?? inferSeniorityFromMetadata(value);
}

function calculateSeniorityCompatibility(candidateSeniority, candidateTitle, jobSeniority, jobTitle) {
  const candidateTitleLevel = inferExplicitSeniorityFromTitle(candidateTitle);
  const candidateLevel = candidateTitleLevel ?? inferSeniorityFromMetadata(candidateSeniority);

  const jobTitleLevel = inferExplicitSeniorityFromTitle(jobTitle);
  const jobMetadataLevel = inferSeniorityFromMetadata(jobSeniority);
  const jobLevel = jobTitleLevel ?? jobMetadataLevel;

  const normalizedJobTitle = normalizeText(jobTitle);
  const explicitTrainingVacancy =
    /\b(ausbildung|azubi|apprentice|praktikant|intern|trainee|graduate program|duales studium|werkstudent|working student|student assistant)\b/.test(
      normalizedJobTitle
    );

  if (
    candidateLevel !== null &&
    candidateLevel >= 2 &&
    explicitTrainingVacancy
  ) {
    return {
      available: true,
      score: 0,
      candidateLevel,
      jobLevel: jobLevel ?? 1,
      reject: true,
      jobTitleExplicit: true,
    };
  }

  if (candidateLevel === null || jobLevel === null) {
    return {
      available: false,
      score: null,
      candidateLevel,
      jobLevel,
      reject: false,
      jobTitleExplicit: jobTitleLevel !== null,
    };
  }

  const gap = jobLevel - candidateLevel;
  const reject = jobTitleLevel !== null && gap > MAX_SENIORITY_GAP;

  let score;
  if (gap <= 0) score = Math.max(0.82, 1 - Math.abs(gap) * 0.06);
  else if (jobTitleLevel !== null) score = Math.max(0, 1 - gap * 0.24);
  else score = Math.max(0.58, 1 - gap * 0.12);

  return {
    available: true,
    score: clamp01(score),
    candidateLevel,
    jobLevel,
    reject,
    jobTitleExplicit: jobTitleLevel !== null,
  };
}

function normalizeDivision(rawDivision, contextText = '') {
  const raw = normalizeText(rawDivision);
  const context = normalizeText(contextText);

  if (raw) {
    if (/^(it|it consulting|technology|tech|information technology)$/.test(raw)) return 'IT Consulting';
    if (/^(construction|bau|building|engineering construction)$/.test(raw)) return 'Construction';
    if (/^(finance|financial|accounting|audit|tax|business finance|finance business)$/.test(raw)) return 'Finance';
    if (/^(legal|law|business legal|legal business)$/.test(raw)) return 'Legal';

    // Business-only fix: when Salesforce explicitly sends Division=Business,
    // keep it as Business. This prevents a Business Payroll candidate from
    // being reclassified as Finance simply because "payroll" appears in the
    // candidate summary. Legacy umbrella values keep the original behaviour.
    if (raw === 'business') return 'Business';

    if (/^(business finance legal|finance legal)$/.test(raw)) {
      const inferred = inferDivisionFromText(context);
      if (inferred === 'Finance' || inferred === 'Legal') return inferred;
      return 'Business';
    }
  }

  return inferDivisionFromText(context);
}

function containsTerm(text, term) {
  const normalizedText = normalizeText(text);
  const normalizedTerm = normalizeText(term);
  if (!normalizedText || !normalizedTerm) return false;
  return ` ${normalizedText} `.includes(` ${normalizedTerm} `);
}

function inferDivisionFromText(text) {
  const normalized = normalizeText(text);
  if (!normalized) return 'default';

  const scores = new Map();
  for (const [division, terms] of Object.entries(DIVISION_TERMS)) {
    let score = 0;
    for (const term of terms) {
      if (containsTerm(normalized, term)) {
        score += term.includes(' ') ? 2 : 1;
      }
    }
    scores.set(division, score);
  }

  const ranked = [...scores.entries()].sort((a, b) => b[1] - a[1]);
  if (ranked.length === 0 || ranked[0][1] === 0) return 'default';

  // When Finance and Legal tie inside the Business umbrella, keep Business
  // rather than forcing an arbitrary specialization.
  if (ranked.length > 1 && ranked[0][1] === ranked[1][1] && ranked[0][1] > 0) {
    const tied = ranked.filter((entry) => entry[1] === ranked[0][1]).map((entry) => entry[0]);
    if (tied.includes('Finance') && tied.includes('Legal')) return 'Business';
  }

  return ranked[0][0];
}

function divisionCompatibility(candidateDivision, jobDivision) {
  const candidate = candidateDivision || 'default';
  const job = jobDivision || 'default';

  if (candidate === 'default' || job === 'default') return null;
  if (candidate === job) return 1;

  if (candidate === 'Business' && (job === 'Finance' || job === 'Legal')) return 0.75;
  if (job === 'Business' && (candidate === 'Finance' || candidate === 'Legal')) return 0.70;

  return 0.05;
}

// Express app
const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: JSON_LIMIT }));
app.use(express.urlencoded({ extended: true, limit: JSON_LIMIT }));

// Optional webhook authentication. It is backward compatible when unset.
function authenticateMatchWebhook(req, res, next) {
  if (!MATCH_WEBHOOK_SECRET) return next();

  const bearer = String(req.headers.authorization || '').replace(/^Bearer\s+/i, '').trim();
  const headerSecret = String(req.headers['x-match-secret'] || '').trim();
  if (bearer === MATCH_WEBHOOK_SECRET || headerSecret === MATCH_WEBHOOK_SECRET) return next();

  return res.status(401).json({
    success: false,
    error: 'Unauthorized',
    timestamp: new Date().toISOString(),
  });
}

// Supabase
const supabaseUrl = process.env.SUPABASE_URL;
const serviceKey = process.env.SUPABASE_SERVICE_KEY;
if (!supabaseUrl || !serviceKey) {
  console.error('SUPABASE_URL or SUPABASE_SERVICE_KEY missing in .env');
  process.exit(1);
}
const supabase = createClient(supabaseUrl, serviceKey, {
  realtime: { transport: ws },
  auth: { persistSession: false, autoRefreshToken: false },
});

// Voyage AI
const VOYAGE_API_KEY = process.env.VOYAGE_API_KEY;
const VOYAGE_URL = 'https://api.voyageai.com/v1/embeddings';
const VOYAGE_MODEL = process.env.VOYAGE_MODEL || 'voyage-3-large';
if (!VOYAGE_API_KEY) {
  console.error('VOYAGE_API_KEY missing in .env');
  process.exit(1);
}

// Semaphore
class Semaphore {
  constructor(limit) {
    this.limit = Math.max(1, limit);
    this.running = 0;
    this.queue = [];
  }

  async acquire() {
    if (this.running < this.limit) {
      this.running += 1;
      return;
    }
    await new Promise((resolve) => this.queue.push(resolve));
    this.running += 1;
  }

  release() {
    this.running = Math.max(0, this.running - 1);
    const next = this.queue.shift();
    if (next) next();
  }

  async run(fn) {
    await this.acquire();
    try {
      return await fn();
    } finally {
      this.release();
    }
  }
}

const matchSemaphore = new Semaphore(MAX_CONCURRENT_MATCHES);

// Database reliability helpers
//
// Important: calling set_config through a Supabase RPC does not reliably raise
// the statement timeout for later PostgREST requests because those requests can
// use different database sessions. Instead, keep database requests small,
// retry transient statement timeouts, and avoid expensive filtered scans.
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function formatDbError(error) {
  if (!error) return 'Unknown database error';
  return [error.message, error.details, error.hint, error.code]
    .filter(Boolean)
    .join(' | ');
}

function isRetryableDbError(error) {
  if (!error) return false;
  const code = String(error.code || '');
  const name = String(error.name || '');
  const message = formatDbError(error);

  return (
    code === '57014' ||
    /timeout|timed out|canceling statement due to statement timeout|statement timeout/i.test(message) ||
    /ECONNRESET|ETIMEDOUT|EAI_AGAIN|fetch failed|socket hang up/i.test(message) ||
    /AbortError|TimeoutError/i.test(name)
  );
}

async function runDbRawWithRetry(label, operation, maxAttempts = DB_MAX_RETRIES) {
  let lastError = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const started = Date.now();

    try {
      const result = await operation(attempt);
      const elapsed = Date.now() - started;
      const dbError = result && result.error ? result.error : null;

      if (!dbError) {
        if (elapsed >= DB_SLOW_QUERY_MS) {
          console.warn(`[DB] ${label} completed slowly in ${elapsed}ms on attempt ${attempt}.`);
        }
        return result;
      }

      lastError = dbError;

      if (!isRetryableDbError(dbError) || attempt >= maxAttempts) {
        return result;
      }

      const delay = Math.min(DB_RETRY_BASE_MS * (2 ** (attempt - 1)), 8000);
      console.warn(
        `[DB] ${label} hit a transient timeout on attempt ${attempt}/${maxAttempts}. ` +
        `Retrying in ${delay}ms. Error: ${formatDbError(dbError)}`
      );
      await sleep(delay);
    } catch (error) {
      lastError = error;

      if (!isRetryableDbError(error) || attempt >= maxAttempts) {
        throw error;
      }

      const delay = Math.min(DB_RETRY_BASE_MS * (2 ** (attempt - 1)), 8000);
      console.warn(
        `[DB] ${label} threw a transient timeout on attempt ${attempt}/${maxAttempts}. ` +
        `Retrying in ${delay}ms. Error: ${formatDbError(error)}`
      );
      await sleep(delay);
    }
  }

  throw lastError || new Error(`${label} failed after ${maxAttempts} attempts`);
}

async function runDbOperation(label, operation, maxAttempts = DB_MAX_RETRIES) {
  const result = await runDbRawWithRetry(label, operation, maxAttempts);
  if (result && result.error) {
    throw new Error(`${label}: ${formatDbError(result.error)}`);
  }
  return result;
}

// Job cache
let cachedJobs = null;
let jobsCacheTimestamp = 0;
let jobsFetchInFlight = null;

let cachedJobSearchIndex = null;
const MATCH_DIVISIONS = ['IT Consulting', 'Construction', 'Business', 'Finance', 'Legal'];

function buildJobSearchIndex(jobs) {
  const byDivision = new Map();
  const byDivisionFamily = new Map();

  const pushUnique = (map, key, job) => {
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(job);
  };

  for (const job of jobs || []) {
    for (const division of MATCH_DIVISIONS) {
      if (!getPreparedRoleSignal(job, division)) continue;

      pushUnique(byDivision, division, job);

      const families = getPreparedRoleFamilies(job, division);
      for (const family of families) {
        pushUnique(byDivisionFamily, `${division}:${family.family}`, job);
      }
    }
  }

  return { byDivision, byDivisionFamily };
}

function getIndexedCandidateJobPool(allJobs, division, candidateFamilies) {
  if (
    !cachedJobSearchIndex ||
    !division ||
    division === 'default' ||
    !Array.isArray(candidateFamilies) ||
    candidateFamilies.length === 0
  ) {
    return allJobs;
  }

  const allowedFamilies = new Set();

  for (const candidate of candidateFamilies) {
    if (!candidate?.family) continue;
    allowedFamilies.add(candidate.family);

    const related = ROLE_RELATIONSHIPS[division]?.[candidate.family] || {};
    for (const [family, relationScore] of Object.entries(related)) {
      if (relationScore >= MIN_ROLE_FAMILY_SCORE) {
        allowedFamilies.add(family);
      }
    }
  }

  const seen = new Set();
  const pool = [];

  const addJobs = (jobs) => {
    for (const job of jobs || []) {
      if (!job?.id || seen.has(job.id)) continue;
      seen.add(job.id);
      pool.push(job);
      if (pool.length >= MAX_INDEXED_CANDIDATE_POOL) return;
    }
  };

  for (const family of allowedFamilies) {
    addJobs(cachedJobSearchIndex.byDivisionFamily.get(`${division}:${family}`));
    if (pool.length >= MAX_INDEXED_CANDIDATE_POOL) break;
  }

  // If the family index is unexpectedly sparse, fall back to the whole
  // division rather than risking false negatives.
  if (pool.length < 100) {
    addJobs(cachedJobSearchIndex.byDivision.get(division));
  }

  return pool.length > 0 ? pool : allJobs;
}

async function fetchJobsFromSupabase() {
  const start = Date.now();
  console.log(`Fetching and preprocessing jobs. Page size=${JOB_FETCH_PAGE_SIZE}...`);

  const jobs = [];
  const seenCacheKeys = new Set();
  let lastId = null;
  let page = 0;
  let scannedRows = 0;
  let rejectedAtCache = 0;
  let duplicatesAtCache = 0;

  while (true) {
    const pageNumber = page + 1;

    const { data } = await runDbOperation(
      `jobs page ${pageNumber}`,
      () => {
        let query = supabase
          .from('jobs')
          .select(
            'id, title, company_id, company_name, apply_url, location, location_lat, location_lng, remote_type, seniority_level, structured_skills, skill_embedding, ats_source, is_active, employment_type'
          )
          .order('id', { ascending: true })
          .limit(JOB_FETCH_PAGE_SIZE)
          .abortSignal(AbortSignal.timeout(DB_REQUEST_TIMEOUT_MS));

        if (lastId !== null) query = query.gt('id', lastId);
        return query;
      }
    );

    if (!data || data.length === 0) break;

    scannedRows += data.length;

    for (const rawJob of data) {
      const job = prepareJobForCache(rawJob);
      if (!job) {
        rejectedAtCache += 1;
        continue;
      }

      const dedupKey =
        `${job.company_id || ''}|${normalizeText(job.title)}|${job._normalizedLocation}`;

      if (seenCacheKeys.has(dedupKey)) {
        duplicatesAtCache += 1;
        continue;
      }

      seenCacheKeys.add(dedupKey);
      jobs.push(job);
    }

    lastId = data[data.length - 1].id;
    page += 1;

    if (page % 10 === 0) {
      console.log(
        `Jobs cache progress: scanned=${scannedRows}, prepared=${jobs.length}, ` +
        `rejected=${rejectedAtCache}, duplicates=${duplicatesAtCache}, pages=${page}`
      );
    }

    if (data.length < JOB_FETCH_PAGE_SIZE) break;
  }

  console.log(
    `Prepared ${jobs.length} jobs after scanning ${scannedRows} rows in ${Date.now() - start}ms. ` +
    `Rejected=${rejectedAtCache}, deduplicated=${duplicatesAtCache}.`
  );

  return jobs;
}

function startJobsRefreshInBackground() {
  if (jobsFetchInFlight) return jobsFetchInFlight;

  jobsFetchInFlight = (async () => {
    try {
      const jobs = await fetchJobsFromSupabase();
      cachedJobs = jobs;
      cachedJobSearchIndex = buildJobSearchIndex(jobs);
      jobsCacheTimestamp = Date.now();
      console.log(
        `Background jobs cache refresh complete: ${jobs.length} jobs, indexed for fast role matching.`
      );
      return jobs;
    } catch (error) {
      console.warn(`Background jobs cache refresh failed: ${error.message}`);
      if (!cachedJobs || cachedJobs.length === 0) throw error;
      return cachedJobs;
    } finally {
      jobsFetchInFlight = null;
    }
  })();

  return jobsFetchInFlight;
}

async function getCachedJobs() {
  const now = Date.now();

  if (cachedJobs && cachedJobs.length > 0) {
    if (!cachedJobSearchIndex) {
      cachedJobSearchIndex = buildJobSearchIndex(cachedJobs);
    }
    if ((now - jobsCacheTimestamp) >= JOB_CACHE_TTL_MS && !jobsFetchInFlight) {
      startJobsRefreshInBackground();
    }
    return cachedJobs;
  }

  if (jobsFetchInFlight) return jobsFetchInFlight;
  return startJobsRefreshInBackground();
}

// Company cache
let companiesMap = null;
let companiesCacheTimestamp = 0;
let companiesFetchInFlight = null;

async function fetchCompaniesFromSupabase() {
  const start = Date.now();
  console.log(
    `Fetching companies with timeout safe pagination. Page size=${COMPANY_FETCH_PAGE_SIZE}...`
  );

  const byId = new Map();
  const byName = new Map();
  let lastId = null;
  let page = 0;
  let totalRows = 0;

  while (true) {
    const pageNumber = page + 1;

    const { data } = await runDbOperation(
      `companies page ${pageNumber}`,
      () => {
        let query = supabase
          .from('companies')
          .select('"Id", "Name", "detected_career_url", "last_crawled_at", "crawl_status"')
          .order('Id', { ascending: true })
          .limit(COMPANY_FETCH_PAGE_SIZE)
          .abortSignal(AbortSignal.timeout(DB_REQUEST_TIMEOUT_MS));

        if (lastId !== null) query = query.gt('Id', lastId);
        return query;
      }
    );

    if (!data || data.length === 0) break;

    for (const company of data) {
      const info = {
        Id: company.Id,
        Name: company.Name,
        career_page_url: company.detected_career_url || null,
        last_crawled_at: company.last_crawled_at || null,
        crawl_status: company.crawl_status || null,
      };

      byId.set(company.Id, info);
      if (company.Name) {
        const key = company.Name.toLowerCase().trim();
        if (!byName.has(key)) byName.set(key, info);
      }
    }

    totalRows += data.length;
    lastId = data[data.length - 1].Id;
    page += 1;

    if (data.length < COMPANY_FETCH_PAGE_SIZE) break;
  }

  console.log(`Loaded ${totalRows} companies in ${Date.now() - start}ms`);
  return { byId, byName };
}

function startCompaniesRefreshInBackground() {
  if (companiesFetchInFlight) return companiesFetchInFlight;

  companiesFetchInFlight = (async () => {
    try {
      const map = await fetchCompaniesFromSupabase();
      companiesMap = map;
      companiesCacheTimestamp = Date.now();
      console.log('Background companies cache refresh complete.');
      return map;
    } catch (error) {
      console.warn(`Background companies cache refresh failed: ${error.message}`);
      if (!companiesMap) throw error;
      return companiesMap;
    } finally {
      companiesFetchInFlight = null;
    }
  })();

  return companiesFetchInFlight;
}

async function getCompaniesMap() {
  const now = Date.now();

  if (companiesMap) {
    if ((now - companiesCacheTimestamp) >= JOB_CACHE_TTL_MS && !companiesFetchInFlight) {
      startCompaniesRefreshInBackground();
    }
    return companiesMap;
  }

  if (companiesFetchInFlight) return companiesFetchInFlight;
  return startCompaniesRefreshInBackground();
}

async function warmupCache(retries = 3) {
  console.log('Warming matching caches...');

  for (let attempt = 1; attempt <= retries; attempt += 1) {
    try {
      await getCachedJobs();
      console.log('Job cache warmup complete.');
      return true;
    } catch (error) {
      console.error(`Cache warmup attempt ${attempt} failed: ${error.message}`);
      if (attempt < retries) await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }

  return false;
}

// Embedding helpers
function parseEmbedding(embedding) {
  if (!embedding) return null;
  if (Array.isArray(embedding)) return embedding.map(Number);
  if (typeof embedding === 'string') {
    try {
      const parsed = JSON.parse(embedding);
      return Array.isArray(parsed) ? parsed.map(Number) : null;
    } catch {
      return null;
    }
  }
  return null;
}

function parseSkillScores(raw) {
  if (!raw) return {};

  let value = raw;
  if (typeof value === 'string') {
    try {
      value = JSON.parse(value);
    } catch {
      return {};
    }
  }

  if (typeof value !== 'object' || value === null || Array.isArray(value)) return {};

  const output = {};
  for (const [skill, rawScore] of Object.entries(value)) {
    if (!skill || !String(skill).trim()) continue;
    const score = Number(rawScore);
    output[String(skill).trim()] = Number.isFinite(score) ? score : 1;
  }
  return output;
}

function parseStructuredSkills(raw) {
  if (!raw) return [];

  let value = raw;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) return [];
    try {
      value = JSON.parse(trimmed);
    } catch {
      value = trimmed.split(/[,;|\n]/g).map((part) => part.trim()).filter(Boolean);
    }
  }

  if (Array.isArray(value)) {
    return value
      .flatMap((item) => {
        if (typeof item === 'string') return [item];
        if (item && typeof item === 'object') {
          return [item.name, item.skill, item.label, item.value].filter(Boolean);
        }
        return [];
      })
      .map((item) => String(item).trim())
      .filter(Boolean);
  }

  if (typeof value === 'object' && value !== null) {
    return Object.keys(value).map((item) => item.trim()).filter(Boolean);
  }

  return [];
}

function cosineSimilarity(vecA, vecB) {
  const a = parseEmbedding(vecA);
  const b = parseEmbedding(vecB);
  if (!a || !b || a.length === 0 || a.length !== b.length) return 0;

  let dot = 0;
  let magA = 0;
  let magB = 0;

  for (let i = 0; i < a.length; i += 1) {
    const av = Number(a[i]);
    const bv = Number(b[i]);
    if (!Number.isFinite(av) || !Number.isFinite(bv)) return 0;
    dot += av * bv;
    magA += av * av;
    magB += bv * bv;
  }

  if (magA === 0 || magB === 0) return 0;
  return clamp01(dot / (Math.sqrt(magA) * Math.sqrt(magB)));
}


function vectorNorm(vector) {
  if (!vector || vector.length === 0) return 0;
  let sum = 0;
  for (let i = 0; i < vector.length; i += 1) {
    const value = Number(vector[i]);
    if (!Number.isFinite(value)) return 0;
    sum += value * value;
  }
  return Math.sqrt(sum);
}

function cosineSimilarityPrepared(candidateVector, candidateNorm, jobVector, jobNorm) {
  if (!candidateVector || !jobVector || candidateVector.length !== jobVector.length) return 0;
  if (!candidateNorm || !jobNorm) return 0;

  let dot = 0;
  for (let i = 0; i < candidateVector.length; i += 1) {
    dot += candidateVector[i] * jobVector[i];
  }

  return clamp01(dot / (candidateNorm * jobNorm));
}

function canonicalSkillName(skill) {
  const value = normalizeSkillName(skill);
  const aliases = {
    'microsoft azure': 'azure',
    'ms azure': 'azure',
    'amazon web services': 'aws',
    'google cloud platform': 'gcp',
    'microsoft 365': 'm365',
    'office 365': 'm365',
    'powershell scripting': 'powershell',
    'nodejs': 'node js',
    'reactjs': 'react',
    'postgresql': 'postgres',
    'netzwerkadministration': 'network administration',
  };
  return aliases[value] || value;
}

function cleanJobSkills(raw) {
  const skills = parseStructuredSkills(raw);
  const output = [];
  const seen = new Set();

  for (const rawSkill of skills) {
    const value = String(rawSkill || '')
      .replace(/<[^>]*>/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();

    if (!value || value.length < 2 || value.length > 80) continue;
    if (/[{}<>]/.test(value)) continue;
    if (/elementor|usercentrics|symbolwas|pfeil|iconjob|cookie|aufgaben:/i.test(value)) continue;

    const canonical = canonicalSkillName(value);
    if (!canonical || canonical.length > 80 || seen.has(canonical)) continue;

    seen.add(canonical);
    output.push(value);
    if (output.length >= 50) break;
  }

  return output;
}

function buildCandidateSkillIndex(candidateSkillScores) {
  const weights = new Map();
  let totalWeight = 0;

  for (const [skill, rawScore] of Object.entries(candidateSkillScores || {})) {
    const key = canonicalSkillName(skill);
    if (!key) continue;

    const weight = Math.max(0.25, Number(rawScore) || 1);
    const existing = weights.get(key) || 0;
    if (weight > existing) weights.set(key, weight);
  }

  for (const weight of weights.values()) totalWeight += weight;
  return { weights, totalWeight };
}

function calculatePreparedSkillOverlap(candidateIndex, jobSkillKeys, displaySkills = []) {
  if (
    !candidateIndex ||
    candidateIndex.weights.size === 0 ||
    !jobSkillKeys ||
    jobSkillKeys.length === 0
  ) {
    return {
      available: false,
      score: null,
      matchedSkills: [],
      matchedJobSkills: [],
      candidateCoverage: null,
      jobCoverage: null,
    };
  }

  let matchedCandidateWeight = 0;
  const matchedSkills = [];
  const matchedJobSkills = [];
  const matchedCandidateKeys = new Set();

  for (let i = 0; i < jobSkillKeys.length; i += 1) {
    const jobKey = jobSkillKeys[i];
    let matchedKey = null;

    if (candidateIndex.weights.has(jobKey)) {
      matchedKey = jobKey;
    } else if (jobKey.length >= 6) {
      for (const candidateKey of candidateIndex.weights.keys()) {
        if (
          candidateKey.length >= 6 &&
          (candidateKey.includes(jobKey) || jobKey.includes(candidateKey))
        ) {
          matchedKey = candidateKey;
          break;
        }
      }
    }

    if (matchedKey) {
      if (!matchedCandidateKeys.has(matchedKey)) {
        matchedCandidateKeys.add(matchedKey);
        matchedCandidateWeight += candidateIndex.weights.get(matchedKey) || 0;
        matchedSkills.push(matchedKey);
      }
      matchedJobSkills.push(displaySkills[i] || jobKey);
    }
  }

  const candidateCoverage = candidateIndex.totalWeight > 0
    ? clamp01(matchedCandidateWeight / candidateIndex.totalWeight)
    : null;
  const jobCoverage = clamp01(matchedJobSkills.length / jobSkillKeys.length);

  return {
    available: true,
    score: clamp01((jobCoverage * 0.78) + ((candidateCoverage || 0) * 0.22)),
    matchedSkills,
    matchedJobSkills,
    candidateCoverage,
    jobCoverage,
  };
}

function buildCandidateTitleProfile(position) {
  let normalized = normalizeText(position)
    .replace(/\bangehend(?:e|er|es|en)?\b/g, ' ')
    .replace(/\bjunior\b/g, ' ')
    .replace(/\bausbildung\b/g, ' ')
    .replace(/\bazubi\b/g, ' ')
    .replace(/\btrainee\b/g, ' ')
    .replace(/\bm w d\b/g, ' ')
    .replace(/\bw m d\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  normalized = normalized
    .replace(/\bfachinformatikerin\b/g, 'fachinformatiker')
    .replace(/\bfachinformatiker in\b/g, 'fachinformatiker')
    .replace(/\bsystem administrator\b/g, 'systemadministrator')
    .replace(/\bit systemadministrator\b/g, 'systemadministrator');

  const keywords = extractKeywords(normalized);

  return {
    normalized,
    keywords,
    keywordSet: new Set(keywords),
  };
}

function calculatePreparedTitleMatch(candidateTitleProfile, jobTitle) {
  const jobNormalized = normalizeText(jobTitle)
    .replace(/\bfachinformatikerin\b/g, 'fachinformatiker')
    .replace(/\bfachinformatiker in\b/g, 'fachinformatiker')
    .replace(/\bsystem administrator\b/g, 'systemadministrator')
    .replace(/\bit systemadministrator\b/g, 'systemadministrator');

  const jobKeywords = extractKeywords(jobNormalized);

  if (
    !candidateTitleProfile ||
    candidateTitleProfile.keywords.length === 0 ||
    jobKeywords.length === 0
  ) {
    return { available: false, score: null, exactPhrase: false };
  }

  const jobSet = new Set(jobKeywords);
  let intersection = 0;

  for (const keyword of candidateTitleProfile.keywordSet) {
    if (jobSet.has(keyword)) intersection += 1;
  }

  const candidateCoverage = intersection / candidateTitleProfile.keywordSet.size;
  const jobCoverage = intersection / jobSet.size;
  let score = clamp01((candidateCoverage * 0.74) + (jobCoverage * 0.26));

  const exactPhrase = Boolean(
    candidateTitleProfile.normalized &&
    (
      jobNormalized.includes(candidateTitleProfile.normalized) ||
      candidateTitleProfile.normalized.includes(jobNormalized)
    )
  );

  if (
    candidateTitleProfile.normalized.includes('fachinformatiker') &&
    candidateTitleProfile.normalized.includes('systemintegration') &&
    jobNormalized.includes('fachinformatiker') &&
    jobNormalized.includes('systemintegration')
  ) {
    score = 1;
  }

  return {
    available: true,
    score: exactPhrase ? Math.max(score, 0.97) : score,
    exactPhrase,
  };
}

function getPreparedRoleFamilies(job, division) {
  return job._roleFamilies?.[division] || inferRoleFamilies(division, job.title, '', []);
}

function getPreparedRoleSignal(job, division) {
  if (
    job._roleSignal &&
    Object.prototype.hasOwnProperty.call(job._roleSignal, division)
  ) {
    return job._roleSignal[division];
  }
  return hasRecognizableRoleSignal(job.title, division);
}

function prepareJobForCache(job) {
  if (!job || job.is_active !== true || !job.skill_embedding) return null;

  const cleanTitle = cleanJobTitle(job.title);
  const baseForValidation = { ...job, title: cleanTitle };
  const garbageReason = getGarbageJobReason(baseForValidation);
  if (garbageReason) return null;

  const parsedEmbedding = parseEmbedding(job.skill_embedding);
  if (!parsedEmbedding || parsedEmbedding.length === 0) return null;

  const embedding = Float32Array.from(parsedEmbedding);
  const embeddingNorm = vectorNorm(embedding);
  if (!embeddingNorm) return null;

  const cleanSkills = cleanJobSkills(job.structured_skills);
  const skillKeys = cleanSkills.map(canonicalSkillName).filter(Boolean);

  const roleFamilies = {};
  const roleSignal = {};

  for (const division of ['IT Consulting', 'Construction', 'Business', 'Finance', 'Legal']) {
    roleFamilies[division] = inferRoleFamilies(division, cleanTitle, '', []);
    roleSignal[division] = hasRecognizableRoleSignal(cleanTitle, division);
  }

  return {
    id: job.id,
    title: cleanTitle,
    company_id: job.company_id,
    company_name: String(job.company_name || '').trim(),
    apply_url: job.apply_url,
    location: job.location,
    location_lat: job.location_lat,
    location_lng: job.location_lng,
    remote_type: job.remote_type,
    seniority_level: job.seniority_level,
    structured_skills: cleanSkills,
    ats_source: job.ats_source,
    is_active: true,
    employment_type: job.employment_type,
    _embedding: embedding,
    _embeddingNorm: embeddingNorm,
    _skillKeys: skillKeys,
    _jobDivision: inferDivisionFromText(cleanTitle),
    _roleFamilies: roleFamilies,
    _roleSignal: roleSignal,
    _jobLat: toFiniteNumber(job.location_lat),
    _jobLng: toFiniteNumber(job.location_lng),
    _remote: isRemoteJob(job.remote_type),
    _script: scriptProfile(cleanTitle),
    _normalizedLocation: normalizeText(job.location || ''),
  };
}

function weightedAverageEmbeddings(items) {
  if (!Array.isArray(items) || items.length === 0) return null;

  const valid = items.filter((item) => Array.isArray(item.embedding) && item.embedding.length > 0 && item.weight > 0);
  if (valid.length === 0) return null;

  const dimension = valid[0].embedding.length;
  if (!valid.every((item) => item.embedding.length === dimension)) {
    throw new Error('Voyage AI returned embeddings with inconsistent dimensions');
  }

  const output = new Array(dimension).fill(0);
  let totalWeight = 0;

  for (const item of valid) {
    const weight = Number(item.weight) || 1;
    totalWeight += weight;
    for (let i = 0; i < dimension; i += 1) {
      output[i] += Number(item.embedding[i]) * weight;
    }
  }

  if (totalWeight <= 0) return null;
  for (let i = 0; i < dimension; i += 1) output[i] /= totalWeight;

  // Normalize the average vector to unit length.
  const magnitude = Math.sqrt(output.reduce((sum, value) => sum + value * value, 0));
  if (magnitude > 0) {
    for (let i = 0; i < output.length; i += 1) output[i] /= magnitude;
  }

  return output;
}

function splitTextIntoChunks(text, maxChars = EMBED_CHUNK_CHARS) {
  const cleaned = String(text || '').replace(/\r\n/g, '\n').trim();
  if (!cleaned) return [];
  if (cleaned.length <= maxChars) return [cleaned];

  const chunks = [];
  let remaining = cleaned;

  while (remaining.length > maxChars) {
    let splitAt = remaining.lastIndexOf('\n', maxChars);
    if (splitAt < Math.floor(maxChars * 0.60)) splitAt = remaining.lastIndexOf('. ', maxChars);
    if (splitAt < Math.floor(maxChars * 0.60)) splitAt = remaining.lastIndexOf(' ', maxChars);
    if (splitAt < Math.floor(maxChars * 0.60)) splitAt = maxChars;

    chunks.push(remaining.slice(0, splitAt).trim());
    remaining = remaining.slice(splitAt).trim();
  }

  if (remaining) chunks.push(remaining);
  return chunks.filter(Boolean);
}

async function embedTexts(texts) {
  if (!Array.isArray(texts) || texts.length === 0) throw new Error('No text supplied for embedding');

  const allEmbeddings = [];

  for (let offset = 0; offset < texts.length; offset += EMBED_BATCH_SIZE) {
    const batch = texts.slice(offset, offset + EMBED_BATCH_SIZE);

    try {
      const response = await axios.post(
        VOYAGE_URL,
        {
          model: VOYAGE_MODEL,
          input: batch,
        },
        {
          headers: {
            Authorization: `Bearer ${VOYAGE_API_KEY}`,
            'Content-Type': 'application/json',
          },
          timeout: 60000,
        }
      );

      const data = response.data?.data;
      if (!Array.isArray(data) || data.length !== batch.length) {
        throw new Error(`Expected ${batch.length} embeddings but received ${Array.isArray(data) ? data.length : 0}`);
      }

      for (const row of data) {
        if (!Array.isArray(row.embedding) || row.embedding.length === 0) {
          throw new Error('Voyage AI returned an empty embedding');
        }
        allEmbeddings.push(row.embedding);
      }
    } catch (error) {
      const details = error.response?.data ? JSON.stringify(error.response.data).slice(0, 1000) : error.message;
      throw new Error(`Voyage AI error: ${details}`);
    }
  }

  return allEmbeddings;
}

// Full candidate profile extraction
const SEMANTIC_SKIP_KEYS = new Set([
  'id',
  'salesforce_contact_id',
  'skill_embedding',
  'embedding',
  'created_at',
  'updated_at',
  'deleted_at',
  'location_lat',
  'location_lng',
  'latitude',
  'longitude',
  'radius',
  'radius_km',
  'top_k',
  'topk',
  'request_id',
  'is_active',
]);

const CORE_PROFILE_KEYS = new Set([
  'position',
  'name',
  'current_employer',
  'salary_expectation',
  'language',
  'languages',
  'seniority_level',
  'remote_preference',
  'location',
  'summary',
  'skill_scores',
  'division',
]);

function isSensitiveKey(key) {
  const normalized = String(key || '').toLowerCase();
  return /(^|_)(password|passwd|secret|token|authorization|api_key|apikey|access_key|private_key)($|_)/i.test(normalized);
}

function looksLikeBase64Blob(value) {
  const text = String(value || '').trim();
  if (text.length < 1000) return false;
  if (/^data:[^;]+;base64,/i.test(text)) return true;
  const sample = text.slice(0, Math.min(4000, text.length));
  const compact = sample.replace(/\s+/g, '');
  if (compact.length < 1000) return false;
  return /^[A-Za-z0-9+/=]+$/.test(compact) && compact.length / sample.length > 0.95;
}

function humanizeKey(key) {
  return String(key || '')
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/[_\-.]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function maybeParseJsonString(value) {
  if (typeof value !== 'string') return value;
  const trimmed = value.trim();
  if (!trimmed || !((trimmed.startsWith('{') && trimmed.endsWith('}')) || (trimmed.startsWith('[') && trimmed.endsWith(']')))) {
    return value;
  }

  try {
    return JSON.parse(trimmed);
  } catch {
    return value;
  }
}

function flattenSemanticContent(value, path = [], output = [], seen = new WeakSet(), depth = 0) {
  if (depth > 12 || value === undefined || value === null) return output;

  if (typeof value === 'object' && value !== null) {
    if (seen.has(value)) return output;
    seen.add(value);
  }

  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index += 1) {
      flattenSemanticContent(value[index], path, output, seen, depth + 1);
    }
    return output;
  }

  if (typeof value === 'object' && value !== null) {
    for (const [rawKey, rawValue] of Object.entries(value)) {
      const key = String(rawKey).trim();
      const normalizedKey = key.toLowerCase();
      if (!key || SEMANTIC_SKIP_KEYS.has(normalizedKey) || isSensitiveKey(normalizedKey)) continue;

      const parsedValue = maybeParseJsonString(rawValue);
      flattenSemanticContent(parsedValue, [...path, key], output, seen, depth + 1);
    }
    return output;
  }

  const text = String(value).replace(/\s+/g, ' ').trim();
  if (!text || looksLikeBase64Blob(text)) return output;

  const label = path.map(humanizeKey).filter(Boolean).join(' > ');
  output.push(label ? `${label}: ${text}` : text);
  return output;
}

function buildCoreProfileText(profile) {
  const parts = [];

  const position = preferIncoming(profile.Position, profile.position, null);
  const name = preferIncoming(profile.name, profile.Name, null);
  const employer = preferIncoming(profile.current_employer, profile.currentEmployer, null);
  const salary = preferIncoming(profile.salary_expectation, profile.salaryExpectation, null);
  const languages = preferIncoming(profile.language, profile.languages, null);
  const seniority = preferIncoming(profile.seniority_level, profile.seniorityLevel, null);
  const remote = preferIncoming(profile.remote_preference, profile.remotePreference, null);
  const location = profile.location;
  const summary = preferIncoming(profile.summary, profile.candidate_summary, null);
  const division = preferIncoming(profile.Division, profile.division, null);
  const skillScores = parseSkillScores(profile.skill_scores);

  if (position) parts.push(`Current or target position: ${position}`);
  if (name) parts.push(`Candidate name: ${name}`);
  if (employer) parts.push(`Current employer: ${employer}`);
  if (seniority) parts.push(`Seniority: ${seniority}`);
  if (division) parts.push(`Division: ${division}`);
  if (location) parts.push(`Location: ${location}`);
  if (remote) parts.push(`Remote preference: ${remote}`);
  if (languages) parts.push(`Languages: ${typeof languages === 'string' ? languages : JSON.stringify(languages)}`);
  if (salary) parts.push(`Salary expectation: ${salary}`);

  if (Object.keys(skillScores).length > 0) {
    const skills = Object.entries(skillScores)
      .map(([skill, score]) => `${skill} (${score})`)
      .join(', ');
    parts.push(`Skills and proficiency: ${skills}`);
  }

  // The summary is intentionally prominent because the client specifically
  // requested it to influence matching across every division.
  if (summary) parts.push(`Candidate summary: ${summary}`);

  return parts.join('\n');
}

function buildFullSourceText(profile) {
  const filtered = {};

  for (const [key, value] of Object.entries(profile || {})) {
    const normalizedKey = key.toLowerCase();
    if (SEMANTIC_SKIP_KEYS.has(normalizedKey) || isSensitiveKey(normalizedKey)) continue;

    // Canonical fields are already represented in the high priority core
    // section. Other MCG and Frontsheet fields remain here in full.
    if (CORE_PROFILE_KEYS.has(normalizedKey)) continue;

    filtered[key] = value;
  }

  return flattenSemanticContent(filtered).join('\n');
}

function buildCandidateSemanticProfile(profile) {
  const coreText = buildCoreProfileText(profile);
  const fullSourceText = buildFullSourceText(profile);

  let totalText = [coreText, fullSourceText].filter(Boolean).join('\n\n');
  let truncated = false;

  if (MAX_PROFILE_CHARS > 0 && totalText.length > MAX_PROFILE_CHARS) {
    totalText = totalText.slice(0, MAX_PROFILE_CHARS);
    truncated = true;
  }

  // Recreate sections after optional global cap so no hidden text is embedded.
  const effectiveCore = coreText.slice(0, totalText.length);
  const remainingChars = Math.max(0, totalText.length - effectiveCore.length - (effectiveCore && fullSourceText ? 2 : 0));
  const effectiveFull = remainingChars > 0 ? fullSourceText.slice(0, remainingChars) : '';

  const sections = [];
  if (effectiveCore) sections.push({ name: 'core', text: effectiveCore, weight: 2.0 });
  if (effectiveFull) sections.push({ name: 'full_source', text: effectiveFull, weight: 1.0 });

  return {
    sections,
    totalText,
    totalCharacters: totalText.length,
    truncated,
    hasFullSourceContent: Boolean(effectiveFull),
  };
}

async function embedCandidateProfile(profile) {
  const built = buildCandidateSemanticProfile(profile);
  if (!built.totalText || built.totalText.length < 5) throw new Error('No valid candidate content to embed');

  const chunkDescriptors = [];

  for (const section of built.sections) {
    const chunks = splitTextIntoChunks(section.text, EMBED_CHUNK_CHARS);
    for (const chunk of chunks) {
      chunkDescriptors.push({
        text: chunk,
        weight: section.weight * Math.max(1, chunk.length / 1000),
        section: section.name,
      });
    }
  }

  if (chunkDescriptors.length === 0) throw new Error('Candidate semantic profile produced no embedding chunks');

  const embeddings = await embedTexts(chunkDescriptors.map((item) => item.text));
  const weighted = weightedAverageEmbeddings(
    embeddings.map((embedding, index) => ({
      embedding,
      weight: chunkDescriptors[index].weight,
    }))
  );

  if (!weighted) throw new Error('Unable to build candidate profile embedding');

  return {
    embedding: weighted,
    characters: built.totalCharacters,
    chunks: chunkDescriptors.length,
    truncated: built.truncated,
    hasFullSourceContent: built.hasFullSourceContent,
  };
}

function mergeProfileData(existingCandidate, incomingCandidate) {
  const merged = { ...(existingCandidate || {}) };

  for (const [key, value] of Object.entries(incomingCandidate || {})) {
    if (value !== undefined && value !== null) merged[key] = value;
  }

  return merged;
}

// Skill matching
function normalizeSkillName(skill) {
  return normalizeText(skill)
    .replace(/\bmicrosoft\b/g, 'ms')
    .replace(/\s+/g, ' ')
    .trim();
}

function skillNamesEquivalent(a, b) {
  const left = normalizeSkillName(a);
  const right = normalizeSkillName(b);
  if (!left || !right) return false;
  if (left === right) return true;

  const aliasPairs = [
    ['ms excel', 'excel'],
    ['microsoft excel', 'excel'],
    ['ms azure', 'azure'],
    ['amazon web services', 'aws'],
    ['google cloud platform', 'gcp'],
    ['javascript', 'js'],
    ['typescript', 'ts'],
    ['nodejs', 'node js'],
    ['reactjs', 'react'],
    ['postgresql', 'postgres'],
  ];

  for (const [x, y] of aliasPairs) {
    if ((left === x && right === y) || (left === y && right === x)) return true;
  }

  // Safe containment for longer multiword skills only.
  if (left.length >= 6 && right.length >= 6) {
    if (left.includes(right) || right.includes(left)) return true;
  }

  return false;
}

function calculateSkillOverlap(candidateSkillScores, jobStructuredSkills) {
  const candidateEntries = Object.entries(candidateSkillScores || {});
  const jobSkills = parseStructuredSkills(jobStructuredSkills);

  if (candidateEntries.length === 0 || jobSkills.length === 0) {
    return {
      available: false,
      score: null,
      matchedSkills: [],
      matchedJobSkills: [],
      candidateCoverage: null,
      jobCoverage: null,
    };
  }

  let totalCandidateWeight = 0;
  let matchedCandidateWeight = 0;
  const matchedSkills = [];
  const matchedJobSkills = [];

  for (const [candidateSkill, rawScore] of candidateEntries) {
    const weight = Math.max(0.25, Number(rawScore) || 1);
    totalCandidateWeight += weight;

    const matchingJobSkills = jobSkills.filter((jobSkill) => skillNamesEquivalent(candidateSkill, jobSkill));
    if (matchingJobSkills.length > 0) {
      matchedCandidateWeight += weight;
      matchedSkills.push(candidateSkill);
      for (const skill of matchingJobSkills) {
        if (!matchedJobSkills.some((existing) => skillNamesEquivalent(existing, skill))) {
          matchedJobSkills.push(skill);
        }
      }
    }
  }

  if (totalCandidateWeight <= 0) {
    return {
      available: false,
      score: null,
      matchedSkills: [],
      matchedJobSkills: [],
      candidateCoverage: null,
      jobCoverage: null,
    };
  }

  const candidateCoverage = clamp01(matchedCandidateWeight / totalCandidateWeight);
  const jobCoverage = clamp01(matchedJobSkills.length / jobSkills.length);

  // Job coverage is more important than coverage of the entire CV skill list.
  // A candidate may legitimately have 70 skills while a vacancy names only 5.
  const score = clamp01((jobCoverage * 0.72) + (candidateCoverage * 0.28));

  return {
    available: true,
    score,
    matchedSkills,
    matchedJobSkills,
    candidateCoverage,
    jobCoverage,
  };
}

// Title matching
function calculateTitleMatch(candidatePosition, jobTitle) {
  const candidateKeywords = extractKeywords(candidatePosition);
  const jobKeywords = extractKeywords(jobTitle);

  if (candidateKeywords.length === 0 || jobKeywords.length === 0) {
    return { available: false, score: null, exactPhrase: false };
  }

  const candidateSet = new Set(candidateKeywords);
  const jobSet = new Set(jobKeywords);

  let intersection = 0;
  for (const keyword of candidateSet) {
    if (jobSet.has(keyword)) intersection += 1;
  }

  const candidateCoverage = intersection / candidateSet.size;
  const jobCoverage = intersection / jobSet.size;
  const score = clamp01((candidateCoverage * 0.65) + (jobCoverage * 0.35));

  const normalizedPosition = normalizeText(candidatePosition);
  const normalizedTitle = normalizeText(jobTitle);
  const exactPhrase = Boolean(
    normalizedPosition && normalizedTitle &&
    (normalizedTitle.includes(normalizedPosition) || normalizedPosition.includes(normalizedTitle))
  );

  return {
    available: true,
    score: exactPhrase ? Math.max(score, 0.95) : score,
    exactPhrase,
  };
}

// Calibrate raw cosine similarity into a human readable fit signal.
// Voyage cosine values around 0.65 to 0.75 can represent a useful semantic
// relationship, so treating raw cosine as a percentage makes strong matches
// look artificially weak.
function calibrateSemanticSimilarity(rawSimilarity) {
  const raw = Number(rawSimilarity);
  if (!Number.isFinite(raw)) return 0;

  const floor = Math.min(SEMANTIC_CALIBRATION_FLOOR, SEMANTIC_CALIBRATION_CEILING - 0.01);
  const ceiling = Math.max(SEMANTIC_CALIBRATION_CEILING, floor + 0.01);

  if (raw <= floor) return 0;
  if (raw >= ceiling) return 1;
  return clamp01((raw - floor) / (ceiling - floor));
}

function effectiveTitleFit(titleMatch, roleFamilyScore) {
  let score =
    titleMatch && titleMatch.available && Number.isFinite(titleMatch.score)
      ? titleMatch.score
      : null;

  // When two titles belong to the same profession family, give the title
  // signal a sensible floor even when literal wording differs, e.g.
  // Fachinformatiker Systemintegration vs IT-Systemadministrator.
  if (Number.isFinite(roleFamilyScore) && roleFamilyScore >= MIN_ROLE_FAMILY_SCORE) {
    const familyFloor = clamp01(roleFamilyScore * 0.62);
    score = score === null ? familyFloor : Math.max(score, familyFloor);
  }

  return score;
}

function matchQualityLabel(score) {
  if (score >= 0.85) return 'excellent';
  if (score >= 0.75) return 'strong';
  if (score >= 0.65) return 'good';
  if (score >= MIN_FINAL_SCORE) return 'possible';
  return 'weak';
}

// Dynamic scoring
function calculateDynamicFinalScore({ semantic, skill, title, division, role, seniority }) {
  const signals = [
    { name: 'semantic', score: semantic, weight: SEMANTIC_WEIGHT },
    { name: 'skill', score: skill, weight: SKILL_WEIGHT },
    { name: 'title', score: title, weight: TITLE_WEIGHT },
    { name: 'division', score: division, weight: DIVISION_WEIGHT },
    { name: 'role', score: role, weight: ROLE_WEIGHT },
    { name: 'seniority', score: seniority, weight: SENIORITY_WEIGHT },
  ];

  let weightedScore = 0;
  let activeWeight = 0;

  for (const signal of signals) {
    if (signal.score === null || signal.score === undefined || !Number.isFinite(signal.score)) continue;
    weightedScore += clamp01(signal.score) * signal.weight;
    activeWeight += signal.weight;
  }

  if (activeWeight <= 0) return 0;
  return clamp01(weightedScore / activeWeight);
}

// Location helpers
function haversine(lat1, lon1, lat2, lon2) {
  const aLat = toFiniteNumber(lat1);
  const aLon = toFiniteNumber(lon1);
  const bLat = toFiniteNumber(lat2);
  const bLon = toFiniteNumber(lon2);
  if (aLat === null || aLon === null || bLat === null || bLon === null) return null;

  const earthRadiusKm = 6371;
  const toRad = (deg) => deg * Math.PI / 180;
  const dLat = toRad(bLat - aLat);
  const dLon = toRad(bLon - aLon);

  const h = Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(aLat)) * Math.cos(toRad(bLat)) * Math.sin(dLon / 2) ** 2;

  return earthRadiusKm * 2 * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
}

function normalizeCity(value) {
  return normalizeText(value)
    .replace(/\b(germany|deutschland|austria|osterreich|österreich|switzerland|schweiz)\b/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function citiesMatch(cityA, cityB) {
  if (!cityA || !cityB) return false;
  const a = normalizeCity(cityA);
  const b = normalizeCity(cityB);
  if (!a || !b) return false;
  return a === b || a.includes(b) || b.includes(a);
}

function canonicalizeApplyUrl(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';

  try {
    const parsed = new URL(raw);
    parsed.hash = '';

    // Tracking parameters do not make a new vacancy.
    const trackingKeys = [
      'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
      'ref', 'source', 'tracking', 'trk'
    ];
    for (const key of trackingKeys) parsed.searchParams.delete(key);

    const query = parsed.searchParams.toString();
    const pathname = parsed.pathname.replace(/\/+$/, '') || '/';

    return `${parsed.hostname.toLowerCase()}${pathname}${query ? `?${query}` : ''}`;
  } catch {
    return normalizeText(raw).replace(/\s+/g, '');
  }
}

function normalizeVacancyFamilyTitle(title) {
  let value = cleanJobTitle(title);

  value = value
    .replace(/\((?:m\/w\/d|w\/m\/d|d\/m\/w|m\/f\/d|all genders|gn)\)/gi, ' ')
    .replace(/\[(?:m\/w\/d|w\/m\/d|d\/m\/w|m\/f\/d|all genders|gn)\]/gi, ' ')
    .replace(/\b(?:m\/w\/d|w\/m\/d|d\/m\/w|m\/f\/d|all genders)\b/gi, ' ')
    .replace(/\s+für\s+alle\s+standorte\b.*$/i, ' ')
    .replace(/\s*[-–—]\s*regional\s*:\s*.+$/i, ' ')
    .replace(/\s*[-–—]\s*in\s+[A-ZÄÖÜ][^/|]{1,80}$/i, ' ')
    .replace(/\s+\bin\s+[A-ZÄÖÜ][A-Za-zÄÖÜäöüß().\-\s]{2,70}$/i, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  return normalizeText(value);
}

function normalizedCompanyIdentity(companyName) {
  return normalizedComparable(companyName || '')
    .replace(/\b(?:nicht shortlisten|do not shortlist|not shortlist)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function vacancyIdentityKeys(match) {
  const keys = [];
  const url = canonicalizeApplyUrl(match.apply_url);
  const company = normalizedCompanyIdentity(match.company_name);
  const title = normalizeText(cleanJobTitle(match.job_title));
  const familyTitle = normalizeVacancyFamilyTitle(match.job_title);
  const location = normalizeText(match.location || '');
  const remote = isRemoteJob(match.remote_type);

  if (url) keys.push(`url:${url}`);

  if (company && title) {
    keys.push(`exact:${company}|${title}|${location}`);
  }

  // A fully remote vacancy published by the same company under the same base
  // title for several cities is one opportunity for shortlist purposes.
  if (remote && company && familyTitle) {
    keys.push(`remote-family:${company}|${familyTitle}`);
  }

  return keys;
}

function countUniqueMatches(matches) {
  const seen = new Set();
  let count = 0;

  for (const match of matches) {
    const keys = vacancyIdentityKeys(match);
    const primary = keys[0] || `job:${match.job_id}`;

    if (keys.some((key) => seen.has(key))) continue;

    count += 1;
    seen.add(primary);
    for (const key of keys) seen.add(key);
  }

  return count;
}

function dedupeSortedMatches(matches) {
  const seen = new Set();
  const unique = [];
  let rejected = 0;

  for (const match of matches) {
    const keys = vacancyIdentityKeys(match);

    if (keys.some((key) => seen.has(key))) {
      rejected += 1;
      continue;
    }

    unique.push(match);

    if (keys.length === 0) {
      seen.add(`job:${match.job_id}`);
    } else {
      for (const key of keys) seen.add(key);
    }
  }

  return { unique, rejected };
}

function calculatePrimaryRoleCompatibility(division, candidateFamilies, jobFamilies) {
  if (!candidateFamilies.length || !jobFamilies.length) return null;

  const candidatePrimary = candidateFamilies[0]?.family;
  const jobPrimary = jobFamilies[0]?.family;
  if (!candidatePrimary || !jobPrimary) return null;

  const relationship = (candidateFamily, jobFamily) => {
    if (candidateFamily === jobFamily) return 1;
    return ROLE_RELATIONSHIPS[division]?.[candidateFamily]?.[jobFamily] || 0;
  };

  let best = relationship(candidatePrimary, jobPrimary);

  // Secondary job families are supporting evidence, not equal to the vacancy's
  // primary profession.
  for (let index = 1; index < jobFamilies.length; index += 1) {
    best = Math.max(
      best,
      relationship(candidatePrimary, jobFamilies[index].family) * 0.75
    );
  }

  return clamp01(best);
}

function isRemoteJob(remoteType) {
  const value = normalizeText(remoteType);
  if (!value) return false;

  return (
    value === 'remote' ||
    value === 'fully remote' ||
    value === 'full remote' ||
    value === '100%' ||
    value === '100 percent' ||
    value.includes('100 remote') ||
    value.includes('100% remote') ||
    value.includes('fully remote') ||
    value.includes('full remote') ||
    value.includes('remote only') ||
    value.includes('homeoffice 100') ||
    value.includes('home office 100')
  );
}

function candidateRemotePreferenceState(remotePreference) {
  const value = normalizeText(remotePreference);

  if (
    !value ||
    /^(not specified|not_specified|unknown|unbekannt|keine angabe|nicht angegeben|n a|na)$/.test(
      value
    )
  ) {
    return 'unspecified';
  }

  if (
    /\b(no remote|not remote|kein remote|keine remote|nicht remote|onsite|on site|on-site|vor ort|office only|nur buro|nur büro)\b/.test(
      value
    )
  ) {
    return 'blocked';
  }

  if (
    /\b(remote|fully remote|full remote|hybrid|homeoffice|home office|flexible|flexibel)\b/.test(
      value
    )
  ) {
    return 'allowed';
  }

  return 'unspecified';
}

function candidateAllowsRemoteWork(remotePreference) {
  if (!CANDIDATE_REMOTE_RADIUS_EXCEPTION) return false;
  return candidateRemotePreferenceState(remotePreference) === 'allowed';
}

function candidateAllowsRemoteTop30Fallback(remotePreference) {
  if (!CANDIDATE_REMOTE_RADIUS_EXCEPTION) return false;

  const state = candidateRemotePreferenceState(remotePreference);
  if (state === 'allowed') return true;
  if (state === 'blocked') return false;

  return REMOTE_UNSPECIFIED_TOP30_FALLBACK;
}

// Company extraction
function extractCompanyNameFromTitle(title) {
  if (!title) return null;

  const patterns = [
    /[—–\-]\s*(.+?)(?:\s*\(|$)/,
    /\|\s*(.+?)(?:\s*\(|$)/,
    /bei\s+(.+?)(?:\s*\(|$)/i,
    /(?:für|an|mit)\s+(.+?)(?:\s*\(|$)/i,
  ];

  for (const pattern of patterns) {
    const match = String(title).match(pattern);
    if (match && match[1]) {
      const name = match[1].trim();
      if (name.length > 2 && name.length < 100) return name;
    }
  }

  const companyWords = ['GmbH', 'AG', 'KG', 'SE', 'e.V.', 'UG', 'GbR', 'OHG'];
  const parts = String(title).split(/[—–\-|\/]/);

  for (const part of parts.reverse()) {
    const trimmed = part.trim();
    if (companyWords.some((word) => trimmed.includes(word))) return trimmed;
  }

  return null;
}


// Schema safe match insert
//
// Supabase PostgREST can reject a full insert when the application sends an
// optional column that does not exist in the current matches table schema.
// Instead of failing the whole candidate match, retry after removing only the
// missing optional column reported by PostgREST.
//
// This makes deployments tolerant of older matches table schemas while still
// preserving all columns that are actually available.
function getMissingColumnFromSchemaError(error, tableName = 'matches') {
  if (!error) return null;

  const message = [
    error.message,
    error.details,
    error.hint,
    error.code,
  ].filter(Boolean).join(' ');

  const escapedTable = String(tableName).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

  const patterns = [
    new RegExp(`Could not find the ['"]([^'"]+)['"] column of ['"]?${escapedTable}['"]? in the schema cache`, 'i'),
    new RegExp(`column ['"]?([^'"]+)['"]? of relation ['"]?${escapedTable}['"]? does not exist`, 'i'),
    new RegExp(`column ${escapedTable}\\.([^\\s]+) does not exist`, 'i'),
  ];

  for (const pattern of patterns) {
    const match = message.match(pattern);
    if (match && match[1]) {
      return match[1]
        .replace(/^["']|["']$/g, '')
        .replace(/\\_/g, '_')
        .trim();
    }
  }

  return null;
}

async function insertMatchesSchemaSafe(rows) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return { removedColumns: [] };
  }

  let pendingRows = rows.map(row => ({ ...row }));
  const removedColumns = [];
  const maxAttempts = 20;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const { error } = await runDbRawWithRetry(
      `matches insert schema attempt ${attempt}`,
      () => supabase
        .from('matches')
        .insert(pendingRows)
        .abortSignal(AbortSignal.timeout(DB_REQUEST_TIMEOUT_MS))
    );

    if (!error) {
      if (removedColumns.length > 0) {
        console.warn(
          `Match insert succeeded after removing unavailable matches columns: ${removedColumns.join(', ')}`
        );
      }
      return { removedColumns };
    }

    const missingColumn = getMissingColumnFromSchemaError(error, 'matches');

    if (
      !missingColumn ||
      !pendingRows.some(row => Object.prototype.hasOwnProperty.call(row, missingColumn))
    ) {
      throw new Error(`Supabase match insert error: ${error.message}`);
    }

    if (missingColumn === 'candidate_id' || missingColumn === 'job_id') {
      throw new Error(
        `Supabase matches table is missing required column '${missingColumn}'. ` +
        `This cannot be auto corrected because the relationship keys are required.`
      );
    }

    console.warn(
      `Supabase matches table has no '${missingColumn}' column. ` +
      `Retrying insert without that optional field.`
    );

    removedColumns.push(missingColumn);
    pendingRows = pendingRows.map(row => {
      const cleanRow = { ...row };
      delete cleanRow[missingColumn];
      return cleanRow;
    });
  }

  throw new Error(
    `Supabase match insert failed after ${maxAttempts} schema compatibility retries.`
  );
}

const candidateEmbeddingCache = new Map();

function hashCandidateProfile(profile) {
  const built = buildCandidateSemanticProfile(profile);
  return crypto
    .createHash('sha256')
    .update(built.totalText || '')
    .digest('hex');
}

async function getCandidateEmbeddingCached(salesforceContactId, profile) {
  const profileHash = hashCandidateProfile(profile);
  const now = Date.now();
  const cached = candidateEmbeddingCache.get(salesforceContactId);

  if (
    cached &&
    cached.profileHash === profileHash &&
    (now - cached.timestamp) < CANDIDATE_EMBED_CACHE_TTL_MS
  ) {
    return {
      ...cached.result,
      fromCache: true,
      profileHash,
    };
  }

  const result = await embedCandidateProfile(profile);
  candidateEmbeddingCache.set(salesforceContactId, {
    profileHash,
    timestamp: now,
    result,
  });

  if (candidateEmbeddingCache.size > 500) {
    const oldestKey = candidateEmbeddingCache.keys().next().value;
    candidateEmbeddingCache.delete(oldestKey);
  }

  return {
    ...result,
    fromCache: false,
    profileHash,
  };
}

// Main matching function
async function matchCandidate(incomingData, radius, topK = DEFAULT_TOP_K) {
  const matchFunctionStart = Date.now();
  const stageTiming = {};
  let stageStartedAt = Date.now();

  const salesforceContactId = incomingData.salesforce_contact_id;
  if (!salesforceContactId) throw new Error('Missing salesforce_contact_id');

  // Read the full candidate row. This is deliberate. If Finance, Legal, IT or
  // Construction specific fields already exist in Supabase, they become part
  // of the semantic profile automatically without another code change.
  const { data: existingCandidate } = await runDbOperation(
    `candidate fetch ${salesforceContactId}`,
    () => supabase
      .from('candidates')
      .select('*')
      .eq('salesforce_contact_id', salesforceContactId)
      .maybeSingle()
      .abortSignal(AbortSignal.timeout(DB_REQUEST_TIMEOUT_MS))
  );

  stageTiming.candidate_fetch_ms = Date.now() - stageStartedAt;

  const existing = existingCandidate || {};

  const incomingPosition = getFirstValue(incomingData, ['Position', 'position', 'current_position', 'current position', 'job_title', 'job title'], null);
  const incomingName = getFirstValue(incomingData, ['name', 'Name', 'full_name', 'full name', 'candidate_name', 'candidate name'], null);
  const incomingEmployer = getFirstValue(incomingData, ['current_employer', 'current employer', 'current_company', 'current company', 'employer'], null);
  const incomingSummary = getFirstValue(incomingData, ['summary', 'Summary', 'candidate_summary', 'candidate summary', 'profile_summary', 'profile summary', 'professional_summary', 'professional summary'], null);
  const incomingSalary = getFirstValue(incomingData, ['salary_expectation', 'salary expectation', 'expected_salary', 'expected salary'], null);
  const incomingLanguage = getFirstValue(incomingData, ['language', 'languages', 'Language', 'Languages'], null);
  const incomingSeniority = getFirstValue(incomingData, ['seniority_level', 'seniority level', 'seniority'], null);
  const incomingRemote = getFirstValue(incomingData, ['remote_preference', 'remote preference', 'remote'], null);
  const incomingLocation = getFirstValue(incomingData, ['location', 'Location', 'city', 'City'], null);
  const incomingLat = getFirstValue(incomingData, ['location_lat', 'latitude', 'lat'], null);
  const incomingLng = getFirstValue(incomingData, ['location_lng', 'longitude', 'lng', 'lon'], null);

  const resolvedPosition = String(preferIncoming(incomingPosition, existing.Position, '') || '').trim();
  const resolvedName = String(preferIncoming(incomingName, existing.name, '') || '').trim();
  const resolvedCurrentEmployer = preferIncoming(incomingEmployer, existing.current_employer, null);
  const resolvedSummary = preferIncoming(incomingSummary, existing.summary, null);
  const resolvedSalary = preferIncoming(incomingSalary, existing.salary_expectation, null);
  const resolvedLanguage = preferIncoming(incomingLanguage, existing.language, null);
  const explicitTitleSeniority = inferExplicitSeniorityFromTitle(resolvedPosition);
  const rawResolvedSeniority = preferIncoming(incomingSeniority, existing.seniority_level, null);

  // Candidate titles such as "Angehende", "Ausbildung", "Junior", "Senior",
  // "Lead" or "Head" are more trustworthy than noisy Salesforce seniority
  // classifications. This prevents an entry stage Systemintegration candidate
  // from being treated as "executive".
  const resolvedSeniority =
    explicitTitleSeniority !== null && explicitTitleSeniority !== undefined
      ? explicitTitleSeniority
      : rawResolvedSeniority;
  const resolvedRemotePreference = preferIncoming(incomingRemote, existing.remote_preference, null);
  const candidateRemotePreferenceStateValue =
    candidateRemotePreferenceState(resolvedRemotePreference);
  const candidateAllowsRemote = candidateAllowsRemoteWork(resolvedRemotePreference);
  const candidateAllowsRemoteFallback =
    candidateAllowsRemoteTop30Fallback(resolvedRemotePreference);
  const resolvedLocation = preferIncoming(incomingLocation, existing.location, null);
  const resolvedLocationLat = preferIncoming(incomingLat, existing.location_lat, null);
  const resolvedLocationLng = preferIncoming(incomingLng, existing.location_lng, null);

  const incomingSkills = parseSkillScores(incomingData.skill_scores);
  const existingSkills = parseSkillScores(existing.skill_scores);
  const resolvedSkillScores = Object.keys(incomingSkills).length > 0 ? incomingSkills : existingSkills;

  // Merge the whole incoming body with the full stored candidate row.
  // Unknown MCG and Frontsheet fields are intentionally preserved here.
  const mergedProfile = mergeProfileData(existing, incomingData);
  mergedProfile.Position = resolvedPosition;
  mergedProfile.name = resolvedName;
  mergedProfile.current_employer = resolvedCurrentEmployer;
  mergedProfile.summary = resolvedSummary;
  mergedProfile.salary_expectation = resolvedSalary;
  mergedProfile.language = resolvedLanguage;
  mergedProfile.seniority_level = resolvedSeniority;
  mergedProfile.remote_preference = resolvedRemotePreference;
  mergedProfile.location = resolvedLocation;
  mergedProfile.skill_scores = resolvedSkillScores;

  const divisionContext = [
    resolvedPosition,
    resolvedSummary,
    buildFullSourceText(mergedProfile),
  ].filter(Boolean).join('\n');

  const incomingDivision = getFirstValue(incomingData, ['Division', 'division', 'business_division', 'business division'], null);
  const resolvedDivision = normalizeDivision(
    preferIncoming(incomingDivision, preferIncoming(existing.Division, existing.division, ''), ''),
    divisionContext
  );

  mergedProfile.Division = resolvedDivision;

  console.log(`[${salesforceContactId}] Division: ${resolvedDivision}`);
  console.log(`[${salesforceContactId}] Skills: ${Object.keys(resolvedSkillScores).length}`);

  // Persist only columns known to exist in the current schema used by v5.3.
  // The full source payload is still used for matching even if the candidates
  // table does not have dedicated MCG or Frontsheet JSON columns.
  const upsertPayload = {
    skill_scores: resolvedSkillScores,
    seniority_level: resolvedSeniority,
    remote_preference: resolvedRemotePreference,
    location: resolvedLocation,
    location_lat: toFiniteNumber(resolvedLocationLat),
    location_lng: toFiniteNumber(resolvedLocationLng),
    Position: resolvedPosition || null,
    summary: resolvedSummary || null,
    salary_expectation: resolvedSalary,
    language: resolvedLanguage,
    updated_at: new Date().toISOString(),
  };

  if (resolvedCurrentEmployer !== undefined) upsertPayload.current_employer = resolvedCurrentEmployer;
  if (incomingData.is_active !== undefined) upsertPayload.is_active = incomingData.is_active;

  // Build the complete semantic profile before writing the candidate so the
  // candidate data and its embedding are persisted in a single database call.
  stageStartedAt = Date.now();
  const profileEmbeddingResult = await getCandidateEmbeddingCached(
    salesforceContactId,
    mergedProfile
  );
  const skillEmbedding = profileEmbeddingResult.embedding;
  stageTiming.candidate_embedding_ms = Date.now() - stageStartedAt;
  upsertPayload.skill_embedding = skillEmbedding;

  let candidateId;
  stageStartedAt = Date.now();

  if (existingCandidate) {
    candidateId = existingCandidate.id;
    await runDbOperation(
      `candidate update ${salesforceContactId}`,
      () => supabase
        .from('candidates')
        .update(upsertPayload)
        .eq('id', candidateId)
        .abortSignal(AbortSignal.timeout(DB_REQUEST_TIMEOUT_MS))
    );
  } else {
    const { data: newCandidate } = await runDbOperation(
      `candidate insert ${salesforceContactId}`,
      () => supabase
        .from('candidates')
        .insert({
          salesforce_contact_id: salesforceContactId,
          ...upsertPayload,
          created_at: new Date().toISOString(),
        })
        .select('id')
        .single()
        .abortSignal(AbortSignal.timeout(DB_REQUEST_TIMEOUT_MS))
    );

    candidateId = newCandidate.id;
  }

  stageTiming.candidate_write_ms = Date.now() - stageStartedAt;

  console.log(
    `[${salesforceContactId}] Semantic profile: ${profileEmbeddingResult.characters} chars, ` +
    `${profileEmbeddingResult.chunks} chunks, full source=${profileEmbeddingResult.hasFullSourceContent}, ` +
    `truncated=${profileEmbeddingResult.truncated}, embedding cache=${profileEmbeddingResult.fromCache ? 'hit' : 'miss'}`
  );

  stageStartedAt = Date.now();
  const jobs = await getCachedJobs();
  stageTiming.jobs_cache_ms = Date.now() - stageStartedAt;
  const config = DIVISION_CONFIG[resolvedDivision] || DIVISION_CONFIG.default;

  const startMatch = Date.now();
  const matches = [];

  const candidateLat = toFiniteNumber(resolvedLocationLat);
  const candidateLng = toFiniteNumber(resolvedLocationLng);
  const parsedRadius = Number(radius);
  const effectiveRadius = Number.isFinite(parsedRadius)
    ? Math.max(0, parsedRadius)
    : DEFAULT_RADIUS_KM;
  const safeTopK = Math.min(
    Math.max(MIN_RETURN_MATCHES, Number.parseInt(topK, 10) || DEFAULT_TOP_K),
    MAX_TOP_K
  );
  const candidateLocationKnown =
    (candidateLat !== null && candidateLng !== null) || Boolean(resolvedLocation);

  const candidateTitleProfile = buildCandidateTitleProfile(resolvedPosition);
  const candidateSkillIndex = buildCandidateSkillIndex(resolvedSkillScores);

  const titleRoleFamilies = inferRoleFamilies(
    resolvedDivision,
    resolvedPosition,
    '',
    []
  );

  const candidateRoleContext = [
    resolvedSummary,
    buildFullSourceText(mergedProfile),
  ].filter(Boolean).join(' ');

  const businessSummaryMode =
    resolvedDivision === 'Business' &&
    candidateSkillIndex.weights.size === 0 &&
    String(resolvedSummary || '').trim().length >= BUSINESS_NO_SKILLS_SUMMARY_MIN_CHARS;

  const businessFullSourceText = businessSummaryMode
    ? buildFullSourceText(mergedProfile)
    : '';

  const businessSummaryRoleFamilies = businessSummaryMode
    ? inferBusinessRoleFamiliesFromProfile(
        resolvedPosition,
        String(resolvedSummary || ''),
        businessFullSourceText
      )
    : [];

  const businessKeywordProfile = businessSummaryMode
    ? buildBusinessKeywordProfile(
        resolvedPosition,
        String(resolvedSummary || ''),
        businessFullSourceText
      )
    : null;

  const candidateRoleFamilies = titleRoleFamilies.length
    ? titleRoleFamilies
    : (
        businessSummaryMode && businessSummaryRoleFamilies.length
          ? businessSummaryRoleFamilies
          : inferRoleFamilies(
              resolvedDivision,
              resolvedPosition,
              candidateRoleContext,
              Object.keys(resolvedSkillScores)
            )
      );

  // If Business has a clear profession in the title, such as Payroll
  // Assistant, keep that profession as the hard anchor. Summary similarity is
  // then used to rank related Payroll/HR/Admin jobs, not unrelated Business jobs.
  const businessTitleAnchoredMode =
    businessSummaryMode && titleRoleFamilies.length > 0;

  // Business-only seniority fallback. It changes matching only for Business
  // assistant/trainee titles and leaves the stored seniority value untouched.
  const matchingSeniority =
    resolvedDivision === 'Business' &&
    /\b(payroll assistant|hr assistant|administrative assistant|trainee|intern)\b/i.test(
      normalizeText(resolvedPosition)
    )
      ? 1
      : resolvedSeniority;

  if (businessSummaryMode) {
    console.log(
      `[${salesforceContactId}] Business summary mode enabled. ` +
      `title anchor=${businessTitleAnchoredMode ? 'yes' : 'no'}, ` +
      `summary families=${businessSummaryRoleFamilies.length
        ? businessSummaryRoleFamilies.map((item) => item.family).join(', ')
        : 'none'}`
    );
  }

  const candidateScriptText = [resolvedPosition, resolvedSummary]
    .filter(Boolean)
    .join(' ');

  const candidateJobs = getIndexedCandidateJobPool(
    jobs,
    resolvedDivision,
    candidateRoleFamilies
  );

  const candidateEmbeddingVector = Float32Array.from(skillEmbedding);
  const candidateEmbeddingNorm = vectorNorm(candidateEmbeddingVector);

  console.log(
    `[${salesforceContactId}] Matching position="${resolvedPosition}", ` +
    `division=${resolvedDivision}, radius=${effectiveRadius}km, role families=` +
    `${candidateRoleFamilies.length ? candidateRoleFamilies.map((item) => item.family).join(', ') : 'unknown'}`
  );

  const shortlist = [];

  let skippedRuntimeGarbage = 0;
  let skippedForeignScript = 0;
  let skippedNoRoleSignal = 0;
  let skippedDivisionMismatch = 0;
  let skippedRoleMismatch = 0;
  let skippedSeniority = 0;
  let skippedWeakRelevance = 0;
  let skippedLocation = 0;
  let missingGeo = 0;
  let skippedLowSemantic = 0;
  let skippedLowFinal = 0;

  // Stage 1 uses only cheap metadata, role, title, skill and location checks.
  // The expensive 1024 dimensional cosine comparison happens only after this.
  for (const job of candidateJobs) {
    const jobTitle = job.title;

    const runtimeGarbageReason = strictVacancyGuard(
      job,
      resolvedDivision,
      candidateScriptText
    );
    if (runtimeGarbageReason) {
      skippedRuntimeGarbage += 1;
      continue;
    }

    if (
      resolvedDivision === 'Business' &&
      isBusinessCandidateNoiseTitle(jobTitle)
    ) {
      skippedRuntimeGarbage += 1;
      continue;
    }

    if (foreignScriptMismatch(candidateScriptText, jobTitle)) {
      skippedForeignScript += 1;
      continue;
    }

    const roleSignalPresent = getPreparedRoleSignal(job, resolvedDivision);
    if (STRICT_JOB_TITLE_VALIDATION && !roleSignalPresent) {
      skippedNoRoleSignal += 1;
      continue;
    }

    const jobDivision = getCandidateAwareJobDivision(job, resolvedDivision, businessSummaryMode);
    const divisionScore = divisionCompatibility(resolvedDivision, jobDivision);

    if (
      STRICT_DIVISION_MATCH &&
      resolvedDivision !== 'default' &&
      jobDivision !== 'default' &&
      divisionScore !== null &&
      divisionScore < 0.30
    ) {
      skippedDivisionMismatch += 1;
      continue;
    }

    const jobRoleFamilies = getPreparedRoleFamilies(job, resolvedDivision);
    const genericRoleFamilyScore = calculateRoleFamilyCompatibility(
      resolvedDivision,
      candidateRoleFamilies,
      jobRoleFamilies
    );

    const roleFamilyScore = businessSummaryMode
      ? calculateBusinessProfessionFit(candidateRoleFamilies, jobRoleFamilies)
      : genericRoleFamilyScore;

    const titleMatch = calculatePreparedTitleMatch(
      candidateTitleProfile,
      jobTitle
    );

    const skillMatch = calculatePreparedSkillOverlap(
      candidateSkillIndex,
      job._skillKeys,
      job.structured_skills
    );

    const strongTitle =
      titleMatch.available && titleMatch.score >= STRONG_TITLE_OVERRIDE;

    const strongSkill =
      skillMatch.available &&
      skillMatch.score >= STRONG_SKILL_OVERRIDE &&
      roleSignalPresent &&
      roleFamilyScore !== null &&
      roleFamilyScore >= MIN_ROLE_FAMILY_SCORE;

    const businessSummarySemantic =
      businessSummaryMode
        ? cosineSimilarityPrepared(
            candidateEmbeddingVector,
            candidateEmbeddingNorm,
            job._embedding,
            job._embeddingNorm
          )
        : null;

    const businessCalibratedSemantic = businessSummaryMode
      ? calibrateBusinessProfileSemantic(businessSummarySemantic)
      : null;

    const businessProfileEvidence = businessSummaryMode
      ? calculateBusinessProfileEvidence(
          businessKeywordProfile,
          jobTitle,
          job.structured_skills
        )
      : null;

    const businessSummaryOverride =
      businessSummaryMode &&
      !businessTitleAnchoredMode &&
      (
        (Number.isFinite(businessSummarySemantic) &&
          businessSummarySemantic >= BUSINESS_NO_SKILLS_SUMMARY_OVERRIDE) ||
        (Number.isFinite(businessProfileEvidence) &&
          businessProfileEvidence >= 0.35)
      );

    const businessAnchoredRoleCompatible =
      !businessTitleAnchoredMode ||
      (roleFamilyScore !== null &&
        roleFamilyScore >= BUSINESS_PRIMARY_ROLE_MIN) ||
      strongTitle;

    if (
      !businessAnchoredRoleCompatible ||
      (
        roleFamilyScore !== null &&
        roleFamilyScore < MIN_ROLE_FAMILY_SCORE &&
        !strongTitle &&
        !strongSkill &&
        !businessSummaryOverride
      )
    ) {
      skippedRoleMismatch += 1;
      continue;
    }

    const seniorityMatch = calculateSeniorityCompatibility(
      matchingSeniority,
      resolvedPosition,
      job.seniority_level,
      jobTitle
    );

    if (seniorityMatch.reject) {
      skippedSeniority += 1;
      continue;
    }

    const concreteSignals = [
      titleMatch.available && titleMatch.score >= MIN_TITLE_SIGNAL,
      skillMatch.available && skillMatch.score >= MIN_SKILL_SIGNAL,
      roleFamilyScore !== null && roleFamilyScore >= MIN_ROLE_FAMILY_SCORE,
    ].filter(Boolean).length;

    if (
      concreteSignals === 0 &&
      !businessSummaryOverride
    ) {
      skippedWeakRelevance += 1;
      continue;
    }

    // Strict radius filtering happens before semantic matching.
    let distance = null;
    let includeByLocation = false;

    if (
      candidateLat !== null &&
      candidateLng !== null &&
      job._jobLat !== null &&
      job._jobLng !== null
    ) {
      distance = haversine(
        candidateLat,
        candidateLng,
        job._jobLat,
        job._jobLng
      );
    } else {
      missingGeo += 1;
    }

    if (!candidateLocationKnown) {
      includeByLocation = true;
    } else if (distance !== null) {
      includeByLocation = STRICT_SELECTED_RADIUS
        ? (
            distance <= effectiveRadius ||
            (job._remote && candidateAllowsRemote)
          )
        : (
            distance <= effectiveRadius ||
            (job._remote && (candidateAllowsRemote || ALLOW_REMOTE_OUTSIDE_RADIUS))
          );
    } else if (
      resolvedLocation &&
      job.location &&
      citiesMatch(resolvedLocation, job.location)
    ) {
      includeByLocation = true;
    } else if (job._remote && candidateAllowsRemote) {
      includeByLocation = true;
    } else if (
      !STRICT_SELECTED_RADIUS &&
      job._remote &&
      ALLOW_REMOTE_OUTSIDE_RADIUS
    ) {
      includeByLocation = true;
    } else if (
      !STRICT_SELECTED_RADIUS &&
      job._remote &&
      ALLOW_REMOTE_UNKNOWN_DISTANCE
    ) {
      includeByLocation = true;
    }

    if (!includeByLocation) {
      skippedLocation += 1;
      continue;
    }

    // Cheap prescore ensures exact occupation titles are always included in
    // the semantic shortlist ahead of generic adjacent roles.
    const cheapSignals = businessSummaryMode
      ? [
          { score: titleMatch.available ? titleMatch.score : null, weight: 0.34 },
          { score: roleFamilyScore, weight: 0.30 },
          {
            score: Number.isFinite(businessCalibratedSemantic)
              ? businessCalibratedSemantic
              : null,
            weight: 0.22,
          },
          { score: businessProfileEvidence, weight: 0.08 },
          { score: seniorityMatch.available ? seniorityMatch.score : null, weight: 0.10 },
        ]
      : [
          { score: titleMatch.available ? titleMatch.score : null, weight: 0.42 },
          { score: roleFamilyScore, weight: 0.30 },
          { score: skillMatch.available ? skillMatch.score : null, weight: 0.18 },
          { score: seniorityMatch.available ? seniorityMatch.score : null, weight: 0.10 },
        ];

    let cheapScore = 0;
    let cheapWeight = 0;

    for (const signal of cheapSignals) {
      if (
        signal.score === null ||
        signal.score === undefined ||
        !Number.isFinite(signal.score)
      ) {
        continue;
      }

      cheapScore += signal.score * signal.weight;
      cheapWeight += signal.weight;
    }

    cheapScore = cheapWeight > 0 ? cheapScore / cheapWeight : 0;

    shortlist.push({
      job,
      jobTitle,
      jobDivision,
      divisionScore,
      jobRoleFamilies,
      roleFamilyScore,
      titleMatch,
      skillMatch,
      seniorityMatch,
      distance,
      cheapScore,
      businessProfileEvidence,
    });
  }

  shortlist.sort((a, b) => b.cheapScore - a.cheapScore);
  const semanticShortlist = shortlist.slice(0, SEMANTIC_SHORTLIST_SIZE);
  const semanticStart = Date.now();

  // Stage 2 computes semantic similarity only for plausible vacancies.
  for (const item of semanticShortlist) {
    const {
      job,
      jobTitle,
      jobDivision,
      divisionScore,
      jobRoleFamilies,
      roleFamilyScore,
      titleMatch,
      skillMatch,
      seniorityMatch,
      distance,
      businessProfileEvidence,
    } = item;

    const semanticSimilarity = cosineSimilarityPrepared(
      candidateEmbeddingVector,
      candidateEmbeddingNorm,
      job._embedding,
      job._embeddingNorm
    );

    if (semanticSimilarity < MIN_SEMANTIC_SIMILARITY) {
      skippedLowSemantic += 1;
      continue;
    }

    const calibratedSemantic = calibrateSemanticSimilarity(semanticSimilarity);
    const businessSemanticScore = businessSummaryMode
      ? calibrateBusinessProfileSemantic(semanticSimilarity)
      : null;
    const titleFit = businessSummaryMode
      ? effectiveBusinessTitleFit(titleMatch, roleFamilyScore)
      : effectiveTitleFit(titleMatch, roleFamilyScore);

    let finalScore = businessSummaryMode
      ? calculateBusinessSummaryFinalScore({
          semantic: businessSemanticScore,
          title: titleFit,
          division: divisionScore,
          role: roleFamilyScore,
          seniority: seniorityMatch.available ? seniorityMatch.score : null,
          profileEvidence: businessProfileEvidence,
        })
      : calculateDynamicFinalScore({
          semantic: calibratedSemantic,
          skill: skillMatch.available ? skillMatch.score : null,
          title: titleFit,
          division: divisionScore,
          role: roleFamilyScore,
          seniority: seniorityMatch.available ? seniorityMatch.score : null,
        });

    if (titleMatch.exactPhrase) {
      finalScore = clamp01(finalScore + TITLE_BONUS);
    }

    const candidateEntryStage =
      inferExplicitSeniorityFromTitle(resolvedPosition) === 1;
    const jobEntryStage =
      inferExplicitSeniorityFromTitle(jobTitle) === 1;

    if (candidateEntryStage && jobEntryStage) {
      finalScore = clamp01(finalScore + 0.05);
    }

    const candidateTitleNorm = normalizeText(resolvedPosition);
    const jobTitleNorm = normalizeText(jobTitle);
    if (
      candidateTitleNorm.includes('fachinformatiker') &&
      candidateTitleNorm.includes('systemintegration') &&
      jobTitleNorm.includes('fachinformatiker') &&
      jobTitleNorm.includes('systemintegration')
    ) {
      finalScore = clamp01(finalScore + 0.03);
    }

    if (distance !== null && distance <= Math.min(25, effectiveRadius)) {
      finalScore = clamp01(finalScore + 0.015);
    } else if (distance !== null && distance <= Math.min(50, effectiveRadius)) {
      finalScore = clamp01(finalScore + 0.005);
    }

    if (finalScore < MIN_FINAL_SCORE) {
      skippedLowFinal += 1;
      continue;
    }

    // Company name already exists on the job row in the supplied dataset.
    // Avoid loading and scanning the entire companies table on every request.
    const companyName = job.company_name || null;
    const careerPageUrl = null;
    const lastCrawledAt = null;
    const crawlStatus = null;

    matches.push({
      job_id: job.id,
      job_title: jobTitle,
      company_id: job.company_id,
      company_name: companyName,
      apply_url: job.apply_url || null,
      career_page_url: careerPageUrl,
      last_crawled_at: lastCrawledAt,
      crawl_status: crawlStatus,
      location: job.location,
      remote_type: job.remote_type,
      seniority_level: job.seniority_level,
      // Preserve the historical API contract used by the n8n Split Matches
      // node. top_skills is a comma separated string for the webhook response,
      // while top_skills_array is retained for PostgreSQL text[] storage.
      top_skills: job.structured_skills.slice(0, 5).join(', '),
      top_skills_array: job.structured_skills.slice(0, 5),
      matched_candidate_skills: skillMatch.matchedSkills.slice(0, 10),
      candidate_division: resolvedDivision,
      job_division: jobDivision,
      similarity_score: semanticSimilarity,
      semantic_score: semanticSimilarity,
      calibrated_semantic_score: calibratedSemantic,
      skill_overlap: skillMatch.available ? skillMatch.score : null,
      title_match: titleFit,
      division_match: divisionScore,
      role_family_match: roleFamilyScore,
      candidate_role_families: candidateRoleFamilies.map((entry) => entry.family),
      job_role_families: jobRoleFamilies.map((entry) => entry.family),
      seniority_match: seniorityMatch.available ? seniorityMatch.score : null,
      match_quality: matchQualityLabel(finalScore),
      final_score: Math.round(finalScore * 10000) / 100,
      location_distance_km:
        distance === null ? null : Math.round(distance * 10) / 10,
    });
  }


  // Adaptive clean backfill.
  // The strict pass above intentionally prioritizes precision. When Salesforce
  // asks for 30 rows and the strict pass returns fewer, run a second pass that
  // keeps all hard filters but relaxes only the soft role, title and skill
  // thresholds. This prevents garbage from returning while still filling the
  // requested result count whenever enough legitimate vacancies exist.
  const strictMatchesCount = matches.length;
  const desiredUniqueTarget = EXACT_TOP_30 ? Math.max(30, safeTopK) : safeTopK;
  const rawCollectionTarget = Math.min(
    MAX_TOP_K,
    Math.max(
      desiredUniqueTarget,
      desiredUniqueTarget * UNIQUE_TOP30_COLLECTION_MULTIPLIER
    )
  );

  let fallbackPoolSize = 0;
  let fallbackSemanticEvaluated = 0;
  let backfillAdded = 0;
  let broadBackfillPoolSize = 0;
  let broadBackfillSemanticEvaluated = 0;
  let broadBackfillAdded = 0;

  if (GUARANTEE_TOP_K && countUniqueMatches(matches) < desiredUniqueTarget) {
    const alreadyMatchedIds = new Set(matches.map((match) => String(match.job_id)));
    const fallbackPool = [];

    for (const job of candidateJobs) {
      if (alreadyMatchedIds.has(String(job.id))) continue;

      const jobTitle = job.title;

      const runtimeGarbageReason = strictVacancyGuard(
        job,
        resolvedDivision,
        candidateScriptText
      );
      if (runtimeGarbageReason) continue;

      if (
        resolvedDivision === 'Business' &&
        isBusinessCandidateNoiseTitle(jobTitle)
      ) continue;

      if (foreignScriptMismatch(candidateScriptText, jobTitle)) continue;

      const roleSignalPresent = getPreparedRoleSignal(job, resolvedDivision);
      if (STRICT_JOB_TITLE_VALIDATION && !roleSignalPresent) continue;

      const jobDivision = getCandidateAwareJobDivision(job, resolvedDivision, businessSummaryMode);
      const divisionScore = divisionCompatibility(resolvedDivision, jobDivision);

      if (
        STRICT_DIVISION_MATCH &&
        resolvedDivision !== 'default' &&
        jobDivision !== 'default' &&
        divisionScore !== null &&
        divisionScore < 0.30
      ) {
        continue;
      }

      const jobRoleFamilies = getPreparedRoleFamilies(job, resolvedDivision);
      const genericRoleFamilyScore = calculateRoleFamilyCompatibility(
        resolvedDivision,
        candidateRoleFamilies,
        jobRoleFamilies
      );

      const roleFamilyScore = businessSummaryMode
        ? calculateBusinessProfessionFit(candidateRoleFamilies, jobRoleFamilies)
        : genericRoleFamilyScore;

      const titleMatch = calculatePreparedTitleMatch(candidateTitleProfile, jobTitle);
      const skillMatch = calculatePreparedSkillOverlap(
        candidateSkillIndex,
        job._skillKeys,
        job.structured_skills
      );

      const seniorityMatch = calculateSeniorityCompatibility(
        matchingSeniority,
        resolvedPosition,
        job.seniority_level,
        jobTitle
      );
      if (seniorityMatch.reject) continue;

      let distance = null;
      let includeByLocation = false;

      if (
        candidateLat !== null &&
        candidateLng !== null &&
        job._jobLat !== null &&
        job._jobLng !== null
      ) {
        distance = haversine(candidateLat, candidateLng, job._jobLat, job._jobLng);
      }

      if (!candidateLocationKnown) {
        includeByLocation = true;
      } else if (distance !== null) {
        includeByLocation = STRICT_SELECTED_RADIUS
          ? (
              distance <= effectiveRadius ||
              (job._remote && candidateAllowsRemote)
            )
          : (
              distance <= effectiveRadius ||
              (job._remote && (candidateAllowsRemote || ALLOW_REMOTE_OUTSIDE_RADIUS))
            );
      } else if (
        resolvedLocation &&
        job.location &&
        citiesMatch(resolvedLocation, job.location)
      ) {
        includeByLocation = true;
      } else if (job._remote && candidateAllowsRemote) {
        includeByLocation = true;
      } else if (
        !STRICT_SELECTED_RADIUS &&
        job._remote &&
        ALLOW_REMOTE_OUTSIDE_RADIUS
      ) {
        includeByLocation = true;
      } else if (
        !STRICT_SELECTED_RADIUS &&
        job._remote &&
        ALLOW_REMOTE_UNKNOWN_DISTANCE
      ) {
        includeByLocation = true;
      }

      if (!includeByLocation) continue;

      const businessSummarySemantic =
        businessSummaryMode
          ? cosineSimilarityPrepared(
              candidateEmbeddingVector,
              candidateEmbeddingNorm,
              job._embedding,
              job._embeddingNorm
            )
          : null;

      const businessCalibratedSemantic = businessSummaryMode
        ? calibrateBusinessProfileSemantic(businessSummarySemantic)
        : null;

      const businessProfileEvidence = businessSummaryMode
        ? calculateBusinessProfileEvidence(
            businessKeywordProfile,
            jobTitle,
            job.structured_skills
          )
        : null;

      const businessSummaryRelevant =
        businessSummaryMode &&
        !businessTitleAnchoredMode &&
        (
          (Number.isFinite(businessSummarySemantic) &&
            businessSummarySemantic >= BUSINESS_NO_SKILLS_SUMMARY_OVERRIDE) ||
          (Number.isFinite(businessProfileEvidence) &&
            businessProfileEvidence >= 0.30)
        );

      const businessAnchoredRelevant =
        businessTitleAnchoredMode &&
        (
          (roleFamilyScore !== null &&
            roleFamilyScore >= BUSINESS_PRIMARY_ROLE_MIN) ||
          (titleMatch.available && titleMatch.score >= STRONG_TITLE_OVERRIDE)
        );

      const relaxedRelevant = businessTitleAnchoredMode
        ? businessAnchoredRelevant
        : (
            (titleMatch.available && titleMatch.score >= BACKFILL_MIN_TITLE_SIGNAL) ||
            (skillMatch.available && skillMatch.score >= BACKFILL_MIN_SKILL_SIGNAL) ||
            (roleFamilyScore !== null && roleFamilyScore >= BACKFILL_MIN_ROLE_FAMILY_SCORE) ||
            businessSummaryRelevant
          );

      if (!relaxedRelevant) continue;

      const cheapSignals = businessSummaryMode
        ? [
            { score: titleMatch.available ? titleMatch.score : null, weight: 0.34 },
            { score: roleFamilyScore, weight: 0.30 },
            {
              score: Number.isFinite(businessCalibratedSemantic)
                ? businessCalibratedSemantic
                : null,
              weight: 0.22,
            },
            { score: businessProfileEvidence, weight: 0.08 },
            { score: seniorityMatch.available ? seniorityMatch.score : null, weight: 0.10 },
          ]
        : [
            { score: titleMatch.available ? titleMatch.score : null, weight: 0.46 },
            { score: roleFamilyScore, weight: 0.26 },
            { score: skillMatch.available ? skillMatch.score : null, weight: 0.18 },
            { score: seniorityMatch.available ? seniorityMatch.score : null, weight: 0.10 },
          ];

      let cheapScore = 0;
      let cheapWeight = 0;
      for (const signal of cheapSignals) {
        if (
          signal.score === null ||
          signal.score === undefined ||
          !Number.isFinite(signal.score)
        ) {
          continue;
        }
        cheapScore += signal.score * signal.weight;
        cheapWeight += signal.weight;
      }
      cheapScore = cheapWeight > 0 ? cheapScore / cheapWeight : 0;

      fallbackPool.push({
        job,
        jobTitle,
        jobDivision,
        divisionScore,
        jobRoleFamilies,
        roleFamilyScore,
        titleMatch,
        skillMatch,
        seniorityMatch,
        distance,
        cheapScore,
      });
    }

    fallbackPool.sort((a, b) => b.cheapScore - a.cheapScore);
    fallbackPoolSize = fallbackPool.length;
    const fallbackSemanticPool = fallbackPool.slice(0, BACKFILL_SEMANTIC_LIMIT);

    for (const item of fallbackSemanticPool) {
      if (matches.length >= rawCollectionTarget) break;

      const {
        job,
        jobTitle,
        jobDivision,
        divisionScore,
        jobRoleFamilies,
        roleFamilyScore,
        titleMatch,
        skillMatch,
        seniorityMatch,
        distance,
      } = item;

      fallbackSemanticEvaluated += 1;

      const semanticSimilarity = cosineSimilarityPrepared(
        candidateEmbeddingVector,
        candidateEmbeddingNorm,
        job._embedding,
        job._embeddingNorm
      );

      if (semanticSimilarity < BACKFILL_MIN_SEMANTIC_SIMILARITY) continue;

      const calibratedSemantic = calibrateSemanticSimilarity(semanticSimilarity);
      const businessSemanticScore = businessSummaryMode
        ? calibrateBusinessProfileSemantic(semanticSimilarity)
        : null;
      const titleFit = businessSummaryMode
        ? effectiveBusinessTitleFit(titleMatch, roleFamilyScore)
        : effectiveTitleFit(titleMatch, roleFamilyScore);
      const businessProfileEvidenceForScore = businessSummaryMode
        ? calculateBusinessProfileEvidence(
            businessKeywordProfile,
            jobTitle,
            job.structured_skills
          )
        : null;

      let finalScore = businessSummaryMode
        ? calculateBusinessSummaryFinalScore({
            semantic: businessSemanticScore,
            title: titleFit,
            division: divisionScore,
            role: roleFamilyScore,
            seniority: seniorityMatch.available ? seniorityMatch.score : null,
            profileEvidence: businessProfileEvidenceForScore,
          })
        : calculateDynamicFinalScore({
            semantic: calibratedSemantic,
            skill: skillMatch.available ? skillMatch.score : null,
            title: titleFit,
            division: divisionScore,
            role: roleFamilyScore,
            seniority: seniorityMatch.available ? seniorityMatch.score : null,
          });

      if (titleMatch.exactPhrase) {
        finalScore = clamp01(finalScore + TITLE_BONUS);
      }

      const candidateEntryStage = inferExplicitSeniorityFromTitle(resolvedPosition) === 1;
      const jobEntryStage = inferExplicitSeniorityFromTitle(jobTitle) === 1;
      if (candidateEntryStage && jobEntryStage) {
        finalScore = clamp01(finalScore + 0.05);
      }

      const candidateTitleNorm = normalizeText(resolvedPosition);
      const jobTitleNorm = normalizeText(jobTitle);
      if (
        candidateTitleNorm.includes('fachinformatiker') &&
        candidateTitleNorm.includes('systemintegration') &&
        jobTitleNorm.includes('fachinformatiker') &&
        jobTitleNorm.includes('systemintegration')
      ) {
        finalScore = clamp01(finalScore + 0.03);
      }

      if (distance !== null && distance <= Math.min(25, effectiveRadius)) {
        finalScore = clamp01(finalScore + 0.015);
      } else if (distance !== null && distance <= Math.min(50, effectiveRadius)) {
        finalScore = clamp01(finalScore + 0.005);
      }

      if (finalScore < CLEAN_BACKFILL_MIN_FINAL_SCORE) {
        continue;
      }

      const companyName = job.company_name || null;
      const topSkillsArray = job.structured_skills.slice(0, 5);

      matches.push({
        job_id: job.id,
        job_title: jobTitle,
        company_id: job.company_id,
        company_name: companyName,
        apply_url: job.apply_url || null,
        career_page_url: null,
        last_crawled_at: null,
        crawl_status: null,
        location: job.location,
        remote_type: job.remote_type,
        seniority_level: job.seniority_level,
        top_skills: topSkillsArray.join(', '),
        top_skills_array: topSkillsArray,
        matched_candidate_skills: skillMatch.matchedSkills.slice(0, 10),
        candidate_division: resolvedDivision,
        job_division: jobDivision,
        similarity_score: semanticSimilarity,
        semantic_score: semanticSimilarity,
        calibrated_semantic_score: calibratedSemantic,
        skill_overlap: skillMatch.available ? skillMatch.score : null,
        title_match: titleFit,
        division_match: divisionScore,
        role_family_match: roleFamilyScore,
        candidate_role_families: candidateRoleFamilies.map((entry) => entry.family),
        job_role_families: jobRoleFamilies.map((entry) => entry.family),
        seniority_match: seniorityMatch.available ? seniorityMatch.score : null,
        match_quality: matchQualityLabel(finalScore),
        selection_tier: 'clean_backfill',
        final_score: Math.round(finalScore * 10000) / 100,
        location_distance_km:
          distance === null ? null : Math.round(distance * 10) / 10,
      });

      alreadyMatchedIds.add(String(job.id));
      backfillAdded += 1;
    }
  }


  // Final company safe broad backfill.
  // This pass exists specifically to satisfy the required 30 result target.
  // It never admits a vacancy without a company name and never bypasses the
  // garbage, division, seniority or selected radius gates. The only thing it
  // broadens is the profession pool inside the candidate's own division.
  if (GUARANTEE_TOP_K && countUniqueMatches(matches) < desiredUniqueTarget) {
    const alreadyMatchedIds = new Set(matches.map((match) => String(match.job_id)));
    const divisionJobs =
      cachedJobSearchIndex?.byDivision?.get(resolvedDivision) || jobs;
    const broadPool = [];

    for (const job of divisionJobs) {
      if (!job?.id || alreadyMatchedIds.has(String(job.id))) continue;
      if (REQUIRE_COMPANY_NAME && !hasValidCompanyName(job.company_name)) continue;

      const jobTitle = job.title;
      const runtimeGarbageReason = strictVacancyGuard(
        job,
        resolvedDivision,
        candidateScriptText
      );
      if (runtimeGarbageReason) continue;

      if (
        resolvedDivision === 'Business' &&
        isBusinessCandidateNoiseTitle(jobTitle)
      ) continue;

      if (foreignScriptMismatch(candidateScriptText, jobTitle)) continue;

      const roleSignalPresent = getPreparedRoleSignal(job, resolvedDivision);
      if (STRICT_JOB_TITLE_VALIDATION && !roleSignalPresent) continue;

      const jobDivision = getCandidateAwareJobDivision(job, resolvedDivision, businessSummaryMode);
      const divisionScore = divisionCompatibility(resolvedDivision, jobDivision);

      if (
        STRICT_DIVISION_MATCH &&
        resolvedDivision !== 'default' &&
        jobDivision !== 'default' &&
        divisionScore !== null &&
        divisionScore < 0.30
      ) {
        continue;
      }

      const jobRoleFamilies = getPreparedRoleFamilies(job, resolvedDivision);
      const genericRoleFamilyScore = calculateRoleFamilyCompatibility(
        resolvedDivision,
        candidateRoleFamilies,
        jobRoleFamilies
      );

      const roleFamilyScore = businessSummaryMode
        ? calculateBusinessProfessionFit(candidateRoleFamilies, jobRoleFamilies)
        : genericRoleFamilyScore;

      const titleMatch = calculatePreparedTitleMatch(candidateTitleProfile, jobTitle);
      const skillMatch = calculatePreparedSkillOverlap(
        candidateSkillIndex,
        job._skillKeys,
        job.structured_skills
      );

      const seniorityMatch = calculateSeniorityCompatibility(
        matchingSeniority,
        resolvedPosition,
        job.seniority_level,
        jobTitle
      );
      if (seniorityMatch.reject) continue;

      let distance = null;
      let includeByLocation = false;

      if (
        candidateLat !== null &&
        candidateLng !== null &&
        job._jobLat !== null &&
        job._jobLng !== null
      ) {
        distance = haversine(candidateLat, candidateLng, job._jobLat, job._jobLng);
      }

      if (!candidateLocationKnown) {
        includeByLocation = true;
      } else if (distance !== null) {
        includeByLocation = STRICT_SELECTED_RADIUS
          ? (
              distance <= effectiveRadius ||
              (job._remote && candidateAllowsRemote)
            )
          : (
              distance <= effectiveRadius ||
              (job._remote && (candidateAllowsRemote || ALLOW_REMOTE_OUTSIDE_RADIUS))
            );
      } else if (
        resolvedLocation &&
        job.location &&
        citiesMatch(resolvedLocation, job.location)
      ) {
        includeByLocation = true;
      } else if (job._remote && candidateAllowsRemote) {
        includeByLocation = true;
      } else if (
        !STRICT_SELECTED_RADIUS &&
        job._remote &&
        ALLOW_REMOTE_OUTSIDE_RADIUS
      ) {
        includeByLocation = true;
      } else if (
        !STRICT_SELECTED_RADIUS &&
        job._remote &&
        ALLOW_REMOTE_UNKNOWN_DISTANCE
      ) {
        includeByLocation = true;
      }

      if (!includeByLocation) continue;

      if (
        businessTitleAnchoredMode &&
        !(
          (roleFamilyScore !== null &&
            roleFamilyScore >= BUSINESS_PRIMARY_ROLE_MIN) ||
          (titleMatch.available && titleMatch.score >= STRONG_TITLE_OVERRIDE)
        )
      ) {
        continue;
      }

      const broadProfessionRelevant =
        (roleFamilyScore !== null &&
          roleFamilyScore >= BROAD_MIN_ROLE_SIGNAL) ||
        (titleMatch.available &&
          titleMatch.score >= BROAD_MIN_TITLE_SIGNAL) ||
        (skillMatch.available &&
          skillMatch.score >= BROAD_MIN_SKILL_SIGNAL);

      if (!broadProfessionRelevant) continue;

      const titleScore = titleMatch.available ? titleMatch.score : 0;
      const roleScore = Number.isFinite(roleFamilyScore) ? roleFamilyScore : 0;
      const skillScore = skillMatch.available ? skillMatch.score : 0;
      const seniorityScore = seniorityMatch.available ? seniorityMatch.score : 0.65;
      const divisionFit = Number.isFinite(divisionScore) ? divisionScore : 0.75;

      // This broad pass can consider weaker literal title overlap, but the job
      // must still be a recognizable vacancy in the correct division. Ranking
      // strongly favors profession, role family and skills before semantics.
      const cheapScore = clamp01(
        (titleScore * 0.38) +
        (roleScore * 0.27) +
        (skillScore * 0.15) +
        (seniorityScore * 0.10) +
        (divisionFit * 0.10)
      );

      broadPool.push({
        job,
        jobTitle,
        jobDivision,
        divisionScore,
        jobRoleFamilies,
        roleFamilyScore,
        titleMatch,
        skillMatch,
        seniorityMatch,
        distance,
        cheapScore,
      });
    }

    broadPool.sort((a, b) => b.cheapScore - a.cheapScore);
    broadBackfillPoolSize = broadPool.length;

    const semanticPool = broadPool.slice(0, BROAD_BACKFILL_SEMANTIC_LIMIT);

    for (const item of semanticPool) {
      if (matches.length >= rawCollectionTarget) break;

      const {
        job,
        jobTitle,
        jobDivision,
        divisionScore,
        jobRoleFamilies,
        roleFamilyScore,
        titleMatch,
        skillMatch,
        seniorityMatch,
        distance,
      } = item;

      broadBackfillSemanticEvaluated += 1;

      const semanticSimilarity = cosineSimilarityPrepared(
        candidateEmbeddingVector,
        candidateEmbeddingNorm,
        job._embedding,
        job._embeddingNorm
      );

      if (semanticSimilarity < BROAD_BACKFILL_MIN_SEMANTIC_SIMILARITY) continue;

      const calibratedSemantic = calibrateSemanticSimilarity(semanticSimilarity);
      const businessSemanticScore = businessSummaryMode
        ? calibrateBusinessProfileSemantic(semanticSimilarity)
        : null;
      const titleFit = businessSummaryMode
        ? effectiveBusinessTitleFit(titleMatch, roleFamilyScore)
        : effectiveTitleFit(titleMatch, roleFamilyScore);
      const businessProfileEvidenceForScore = businessSummaryMode
        ? calculateBusinessProfileEvidence(
            businessKeywordProfile,
            jobTitle,
            job.structured_skills
          )
        : null;

      let finalScore = businessSummaryMode
        ? calculateBusinessSummaryFinalScore({
            semantic: businessSemanticScore,
            title: titleFit,
            division: divisionScore,
            role: roleFamilyScore,
            seniority: seniorityMatch.available ? seniorityMatch.score : null,
            profileEvidence: businessProfileEvidenceForScore,
          })
        : calculateDynamicFinalScore({
            semantic: calibratedSemantic,
            skill: skillMatch.available ? skillMatch.score : null,
            title: titleFit,
            division: divisionScore,
            role: roleFamilyScore,
            seniority: seniorityMatch.available ? seniorityMatch.score : null,
          });

      if (titleMatch.exactPhrase) {
        finalScore = clamp01(finalScore + TITLE_BONUS);
      }

      const candidateEntryStage = inferExplicitSeniorityFromTitle(resolvedPosition) === 1;
      const jobEntryStage = inferExplicitSeniorityFromTitle(jobTitle) === 1;
      if (candidateEntryStage && jobEntryStage) {
        finalScore = clamp01(finalScore + 0.05);
      }

      const candidateTitleNorm = normalizeText(resolvedPosition);
      const jobTitleNorm = normalizeText(jobTitle);
      if (
        candidateTitleNorm.includes('fachinformatiker') &&
        candidateTitleNorm.includes('systemintegration') &&
        jobTitleNorm.includes('fachinformatiker') &&
        jobTitleNorm.includes('systemintegration')
      ) {
        finalScore = clamp01(finalScore + 0.03);
      }

      if (distance !== null && distance <= Math.min(25, effectiveRadius)) {
        finalScore = clamp01(finalScore + 0.015);
      } else if (distance !== null && distance <= Math.min(50, effectiveRadius)) {
        finalScore = clamp01(finalScore + 0.005);
      }

      if (finalScore < BROAD_BACKFILL_MIN_FINAL_SCORE) {
        continue;
      }

      const topSkillsArray = job.structured_skills.slice(0, 5);

      matches.push({
        job_id: job.id,
        job_title: jobTitle,
        company_id: job.company_id,
        company_name: String(job.company_name).trim(),
        apply_url: job.apply_url || null,
        career_page_url: null,
        last_crawled_at: null,
        crawl_status: null,
        location: job.location,
        remote_type: job.remote_type,
        seniority_level: job.seniority_level,
        top_skills: topSkillsArray.join(', '),
        top_skills_array: topSkillsArray,
        matched_candidate_skills: skillMatch.matchedSkills.slice(0, 10),
        candidate_division: resolvedDivision,
        job_division: jobDivision,
        similarity_score: semanticSimilarity,
        semantic_score: semanticSimilarity,
        calibrated_semantic_score: calibratedSemantic,
        skill_overlap: skillMatch.available ? skillMatch.score : null,
        title_match: titleFit,
        division_match: divisionScore,
        role_family_match: roleFamilyScore,
        candidate_role_families: candidateRoleFamilies.map((entry) => entry.family),
        job_role_families: jobRoleFamilies.map((entry) => entry.family),
        seniority_match: seniorityMatch.available ? seniorityMatch.score : null,
        match_quality: matchQualityLabel(finalScore),
        selection_tier: 'company_safe_broad_backfill',
        final_score: Math.round(finalScore * 10000) / 100,
        location_distance_km:
          distance === null ? null : Math.round(distance * 10) / 10,
      });

      alreadyMatchedIds.add(String(job.id));
      broadBackfillAdded += 1;
    }
  }


  // Exact top 30 guarantee pass.
  //
  // Client requirement: return 30 unique legitimate vacancies every time.
  // All quality gates remain hard except selected radius. If the in-radius
  // strict and clean backfill pools cannot reach 30, use the nearest clean,
  // company-identified, CV-relevant vacancies outside the radius.
  //
  // This pass NEVER allows:
  //   * missing company names
  //   * garbage/crawler headings
  //   * invalid/non-job URLs
  //   * duplicate jobs
  //   * foreign-script garbage
  //   * clearly incompatible seniority
  //   * clearly incompatible professional role families
  //
  // Outside-radius fallback rows are intentionally penalized so all valid
  // in-radius results rank above otherwise similar distant jobs.
  let outsideRadiusBackfillPool = 0;
  let outsideRadiusSemanticEvaluated = 0;
  let outsideRadiusBackfillAdded = 0;

  const exactTarget = desiredUniqueTarget;

  if (
    GUARANTEE_TOP_K &&
    (
      (!STRICT_SELECTED_RADIUS && TOP30_ALLOW_OUTSIDE_RADIUS_FALLBACK) ||
      (STRICT_SELECTED_RADIUS && candidateAllowsRemoteFallback)
    ) &&
    countUniqueMatches(matches) < exactTarget
  ) {
    const alreadyMatchedIds = new Set(matches.map((match) => String(match.job_id)));
    const outsidePool = [];

    // Prefer the candidate's division index, but include the complete prepared
    // cache as a safety net because some crawler rows have job_division=default
    // even when their title is a valid IT/Finance/Legal/Construction vacancy.
    const preferredDivisionJobs =
      cachedJobSearchIndex?.byDivision?.get(resolvedDivision) || [];
    const sourceJobs = preferredDivisionJobs.length
      ? [...preferredDivisionJobs, ...jobs]
      : jobs;

    const seenOutsideIds = new Set();

    for (const job of sourceJobs) {
      if (!job?.id) continue;

      const jobId = String(job.id);
      if (seenOutsideIds.has(jobId) || alreadyMatchedIds.has(jobId)) continue;
      seenOutsideIds.add(jobId);

      if (REQUIRE_COMPANY_NAME && !hasValidCompanyName(job.company_name)) continue;

      const jobTitle = job.title;
      const runtimeGarbageReason = strictVacancyGuard(
        job,
        resolvedDivision,
        candidateScriptText
      );
      if (runtimeGarbageReason) continue;

      if (
        resolvedDivision === 'Business' &&
        isBusinessCandidateNoiseTitle(jobTitle)
      ) continue;

      if (foreignScriptMismatch(candidateScriptText, jobTitle)) continue;

      const roleSignalPresent = getPreparedRoleSignal(job, resolvedDivision);
      if (STRICT_JOB_TITLE_VALIDATION && !roleSignalPresent) continue;

      const jobDivision = getCandidateAwareJobDivision(job, resolvedDivision, businessSummaryMode);
      const divisionScore = divisionCompatibility(resolvedDivision, jobDivision);

      // Keep obvious cross-division mismatches blocked. Allow "default" because
      // many valid crawler jobs are not explicitly classified.
      if (
        STRICT_DIVISION_MATCH &&
        resolvedDivision !== 'default' &&
        jobDivision !== 'default' &&
        divisionScore !== null &&
        divisionScore < 0.30
      ) {
        continue;
      }

      const jobRoleFamilies = getPreparedRoleFamilies(job, resolvedDivision);
      const genericRoleFamilyScore = calculateRoleFamilyCompatibility(
        resolvedDivision,
        candidateRoleFamilies,
        jobRoleFamilies
      );

      const roleFamilyScore = businessSummaryMode
        ? calculateBusinessProfessionFit(candidateRoleFamilies, jobRoleFamilies)
        : genericRoleFamilyScore;

      const titleMatch = calculatePreparedTitleMatch(
        candidateTitleProfile,
        jobTitle
      );

      const skillMatch = calculatePreparedSkillOverlap(
        candidateSkillIndex,
        job._skillKeys,
        job.structured_skills
      );

      const strongTitle =
        titleMatch.available && titleMatch.score >= STRONG_TITLE_OVERRIDE;

      const primaryRoleCompatibility = calculatePrimaryRoleCompatibility(
        resolvedDivision,
        candidateRoleFamilies,
        jobRoleFamilies
      );

      const remoteFallbackRelevant =
        (primaryRoleCompatibility !== null &&
          primaryRoleCompatibility >= REMOTE_TOP30_MIN_ROLE_SIGNAL) ||
        (titleMatch.available &&
          titleMatch.score >= REMOTE_TOP30_MIN_TITLE_SIGNAL) ||
        strongTitle;

      const roleCompatible = businessTitleAnchoredMode
        ? (
            (roleFamilyScore !== null &&
              roleFamilyScore >= BUSINESS_PRIMARY_ROLE_MIN) ||
            strongTitle
          )
        : remoteFallbackRelevant;

      if (!roleCompatible) continue;

      const seniorityMatch = calculateSeniorityCompatibility(
        matchingSeniority,
        resolvedPosition,
        job.seniority_level,
        jobTitle
      );
      if (seniorityMatch.reject) continue;

      let distance = null;

      if (
        candidateLat !== null &&
        candidateLng !== null &&
        job._jobLat !== null &&
        job._jobLng !== null
      ) {
        distance = haversine(
          candidateLat,
          candidateLng,
          job._jobLat,
          job._jobLng
        );
      }

      // Under strict radius, this fallback is REMOTE ONLY and only when the
      // candidate explicitly accepts remote work. Onsite/hybrid jobs outside
      // the radius can never be used to complete Top 30.
      if (STRICT_SELECTED_RADIUS) {
        if (!candidateAllowsRemoteFallback || !job._remote) continue;
      } else if (
        candidateLocationKnown &&
        distance !== null &&
        distance <= effectiveRadius
      ) {
        continue;
      }

      const titleScore = titleMatch.available ? titleMatch.score : 0;
      const roleScore = Number.isFinite(primaryRoleCompatibility)
        ? primaryRoleCompatibility
        : (Number.isFinite(roleFamilyScore) ? roleFamilyScore : 0);
      const skillScore = skillMatch.available ? skillMatch.score : 0;
      const seniorityScore = seniorityMatch.available ? seniorityMatch.score : 0.65;
      const divisionFit = Number.isFinite(divisionScore) ? divisionScore : 0.75;

      const distancePenalty =
        distance === null
          ? 0.08
          : Math.min(0.20, Math.max(0, (distance - effectiveRadius) / 2500));

      const cheapScore = clamp01(
        (titleScore * 0.40) +
        (roleScore * 0.27) +
        (skillScore * 0.15) +
        (seniorityScore * 0.10) +
        (divisionFit * 0.08) -
        distancePenalty
      );

      outsidePool.push({
        job,
        jobTitle,
        jobDivision,
        divisionScore,
        jobRoleFamilies,
        roleFamilyScore,
        titleMatch,
        skillMatch,
        seniorityMatch,
        distance,
        cheapScore,
      });
    }

    outsidePool.sort((a, b) => {
      if (b.cheapScore !== a.cheapScore) return b.cheapScore - a.cheapScore;
      return (a.distance ?? Infinity) - (b.distance ?? Infinity);
    });

    outsideRadiusBackfillPool = outsidePool.length;

    const semanticPool = outsidePool.slice(0, TOP30_OUTSIDE_RADIUS_LIMIT);

    for (const item of semanticPool) {
      if (matches.length >= rawCollectionTarget) break;

      const {
        job,
        jobTitle,
        jobDivision,
        divisionScore,
        jobRoleFamilies,
        roleFamilyScore,
        titleMatch,
        skillMatch,
        seniorityMatch,
        distance,
      } = item;

      outsideRadiusSemanticEvaluated += 1;

      const semanticSimilarity = cosineSimilarityPrepared(
        candidateEmbeddingVector,
        candidateEmbeddingNorm,
        job._embedding,
        job._embeddingNorm
      );

      if (semanticSimilarity < REMOTE_TOP30_MIN_SEMANTIC) continue;

      const calibratedSemantic = calibrateSemanticSimilarity(semanticSimilarity);
      const businessSemanticScore = businessSummaryMode
        ? calibrateBusinessProfileSemantic(semanticSimilarity)
        : null;
      const titleFit = businessSummaryMode
        ? effectiveBusinessTitleFit(titleMatch, roleFamilyScore)
        : effectiveTitleFit(titleMatch, roleFamilyScore);
      const businessProfileEvidenceForScore = businessSummaryMode
        ? calculateBusinessProfileEvidence(
            businessKeywordProfile,
            jobTitle,
            job.structured_skills
          )
        : null;

      let finalScore = businessSummaryMode
        ? calculateBusinessSummaryFinalScore({
            semantic: businessSemanticScore,
            title: titleFit,
            division: divisionScore,
            role: roleFamilyScore,
            seniority: seniorityMatch.available ? seniorityMatch.score : null,
            profileEvidence: businessProfileEvidenceForScore,
          })
        : calculateDynamicFinalScore({
            semantic: calibratedSemantic,
            skill: skillMatch.available ? skillMatch.score : null,
            title: titleFit,
            division: divisionScore,
            role: roleFamilyScore,
            seniority: seniorityMatch.available ? seniorityMatch.score : null,
          });

      if (titleMatch.exactPhrase) {
        finalScore = clamp01(finalScore + TITLE_BONUS);
      }

      const candidateEntryStage =
        inferExplicitSeniorityFromTitle(resolvedPosition) === 1;
      const jobEntryStage =
        inferExplicitSeniorityFromTitle(jobTitle) === 1;

      if (candidateEntryStage && jobEntryStage) {
        finalScore = clamp01(finalScore + 0.05);
      }

      const candidateTitleNorm = normalizeText(resolvedPosition);
      const jobTitleNorm = normalizeText(jobTitle);

      if (
        candidateTitleNorm.includes('fachinformatiker') &&
        candidateTitleNorm.includes('systemintegration') &&
        jobTitleNorm.includes('fachinformatiker') &&
        jobTitleNorm.includes('systemintegration')
      ) {
        finalScore = clamp01(finalScore + 0.03);
      }

      // Explicit penalty for breaking the selected-radius preference.
      // This keeps these fallback matches below comparable local matches.
      finalScore = clamp01(finalScore - 0.07);

      if (finalScore < REMOTE_TOP30_MIN_FINAL_SCORE) {
        continue;
      }

      const topSkillsArray = job.structured_skills.slice(0, 5);

      matches.push({
        job_id: job.id,
        job_title: jobTitle,
        company_id: job.company_id,
        company_name: String(job.company_name).trim(),
        apply_url: job.apply_url || null,
        career_page_url: null,
        last_crawled_at: null,
        crawl_status: null,
        location: job.location,
        remote_type: job.remote_type,
        seniority_level: job.seniority_level,
        top_skills: topSkillsArray.join(', '),
        top_skills_array: topSkillsArray,
        matched_candidate_skills: skillMatch.matchedSkills.slice(0, 10),
        candidate_division: resolvedDivision,
        job_division: jobDivision,
        similarity_score: semanticSimilarity,
        semantic_score: semanticSimilarity,
        calibrated_semantic_score: calibratedSemantic,
        skill_overlap: skillMatch.available ? skillMatch.score : null,
        title_match: titleFit,
        division_match: divisionScore,
        role_family_match: roleFamilyScore,
        candidate_role_families: candidateRoleFamilies.map(
          (entry) => entry.family
        ),
        job_role_families: jobRoleFamilies.map((entry) => entry.family),
        seniority_match: seniorityMatch.available ? seniorityMatch.score : null,
        match_quality: matchQualityLabel(finalScore),
        selection_tier: STRICT_SELECTED_RADIUS
          ? 'exact_top30_remote_fallback'
          : 'exact_top30_outside_radius_fallback',
        outside_selected_radius:
          distance !== null ? distance > effectiveRadius : null,
        radius_exception:
          STRICT_SELECTED_RADIUS && job._remote && candidateAllowsRemote
            ? 'candidate_remote_preference'
            : (
                STRICT_SELECTED_RADIUS &&
                job._remote &&
                candidateAllowsRemoteFallback
                  ? 'remote_preference_unspecified_top30_fallback'
                  : null
              ),
        final_score: Math.round(finalScore * 10000) / 100,
        location_distance_km:
          distance === null ? null : Math.round(distance * 10) / 10,
      });

      alreadyMatchedIds.add(String(job.id));
      outsideRadiusBackfillAdded += 1;
    }
  }

  matches.sort((a, b) => {
    if (b.final_score !== a.final_score) {
      return b.final_score - a.final_score;
    }

    if ((b.title_match || 0) !== (a.title_match || 0)) {
      return (b.title_match || 0) - (a.title_match || 0);
    }

    if (b.semantic_score !== a.semantic_score) {
      return b.semantic_score - a.semantic_score;
    }

    return (
      (a.location_distance_km ?? Infinity) -
      (b.location_distance_km ?? Infinity)
    );
  });

  // Final fail closed validation immediately before response and persistence.
  // This is intentionally redundant with cache and stage 1 filtering so a
  // stale cache or future crawler regression cannot leak page headings into
  // Salesforce.
  let finalGuardRejected = 0;
  const cleanFinalMatches = matches.filter((match) => {
    const hasKnownDistance =
      match.location_distance_km !== null &&
      match.location_distance_km !== undefined &&
      Number.isFinite(Number(match.location_distance_km));

    const isOutsideSelectedRadius =
      STRICT_SELECTED_RADIUS &&
      hasKnownDistance &&
      Number(match.location_distance_km) > effectiveRadius;

    // Hard radius rule:
    // Any onsite or hybrid vacancy outside the selected radius is rejected.
    // Only a fully remote vacancy can ever be considered outside the radius.
    if (
      isOutsideSelectedRadius &&
      !isRemoteJob(match.remote_type)
    ) {
      finalGuardRejected += 1;
      return false;
    }

    // Keep the existing candidate-aware remote rule unchanged.
    // A fully remote vacancy outside the radius is allowed only when the
    // candidate accepts remote work, or when the existing Top-30 remote
    // fallback is being used for an unspecified remote preference.
    if (
      isOutsideSelectedRadius &&
      isRemoteJob(match.remote_type) &&
      !(
        candidateAllowsRemote ||
        (
          candidateAllowsRemoteFallback &&
          match.selection_tier === 'exact_top30_remote_fallback'
        )
      )
    ) {
      finalGuardRejected += 1;
      return false;
    }

    if (REQUIRE_COMPANY_NAME && !hasValidCompanyName(match.company_name)) {
      finalGuardRejected += 1;
      return false;
    }

    const reason = strictVacancyGuard(
      {
        title: match.job_title,
        company_name: match.company_name,
        apply_url: match.apply_url,
        ats_source: 'prepared',
      },
      resolvedDivision,
      candidateScriptText
    );

    if (reason) {
      finalGuardRejected += 1;
      return false;
    }

    return true;
  });

  const finalTarget = EXACT_TOP_30 ? Math.max(30, safeTopK) : safeTopK;

  const {
    unique: uniqueFinalMatches,
    rejected: finalDuplicateRejected,
  } = dedupeSortedMatches(cleanFinalMatches);

  const topMatches = uniqueFinalMatches.slice(0, finalTarget);
  const semanticStageMs = Date.now() - semanticStart;
  const matchLoopMs = Date.now() - startMatch;

  console.log(
    `[${salesforceContactId}] Fast match: cache=${jobs.length} jobs, indexed pool=${candidateJobs.length}, ` +
    `cheap shortlist=${shortlist.length}, semantic shortlist=${semanticShortlist.length}, ` +
    `semantic stage=${semanticStageMs}ms, total loop=${matchLoopMs}ms, ` +
    `deduped=${finalDuplicateRejected}, returned=${topMatches.length}.`
  );

  console.log(
    `[${salesforceContactId}] Skipped: runtime garbage=${skippedRuntimeGarbage}, foreign=${skippedForeignScript}, ` +
    `no role=${skippedNoRoleSignal}, division=${skippedDivisionMismatch}, ` +
    `role=${skippedRoleMismatch}, seniority=${skippedSeniority}, ` +
    `weak=${skippedWeakRelevance}, location=${skippedLocation}, ` +
    `low semantic=${skippedLowSemantic}, low final=${skippedLowFinal}, ` +
    `final guard=${finalGuardRejected}, missing geo=${missingGeo}.`
  );

  stageStartedAt = Date.now();

  // Always delete old matches first. v5.3 left stale matches in the database
  // whenever a new run returned zero matches.
  await runDbOperation(
    `old matches delete ${salesforceContactId}`,
    () => supabase
      .from('matches')
      .delete()
      .eq('candidate_id', candidateId)
      .abortSignal(AbortSignal.timeout(DB_REQUEST_TIMEOUT_MS))
  );

  if (topMatches.length > 0) {
    const rows = topMatches.map((match) => ({
      candidate_id: candidateId,
      job_id: match.job_id,
      similarity_score: Math.round(match.similarity_score * 10000) / 100,
      location_distance_km:
        match.location_distance_km === null || match.location_distance_km === undefined
          ? null
          : String(match.location_distance_km),
      final_score: match.final_score,
      job_title: match.job_title,
      company_name: match.company_name,
      job_location: match.location,
      remote_type: match.remote_type,
      seniority_level: match.seniority_level,
      top_skills: Array.isArray(match.top_skills_array)
        ? match.top_skills_array.map((skill) => String(skill))
        : (Array.isArray(match.top_skills)
            ? match.top_skills.map((skill) => String(skill))
            : (typeof match.top_skills === 'string'
                ? match.top_skills.split(',').map((skill) => skill.trim()).filter(Boolean)
                : [])),
      "Apply URL": match.apply_url || null,
      created_at: new Date().toISOString(),
    }));

    await insertMatchesSchemaSafe(rows);
  }

  stageTiming.matches_persist_ms = Date.now() - stageStartedAt;
  stageTiming.total_match_function_ms = Date.now() - matchFunctionStart;

  return {
    engine_version: '10.2.0',
    build_id: BUILD_ID,
    candidate_id: candidateId,
    salesforce_contact_id: salesforceContactId,
    candidate_name: resolvedName,
    position_used: resolvedPosition || null,
    division_used: resolvedDivision || null,
    semantic_profile: {
      characters_used: profileEmbeddingResult.characters,
      chunks_embedded: profileEmbeddingResult.chunks,
      full_source_content_included: profileEmbeddingResult.hasFullSourceContent,
      truncated: profileEmbeddingResult.truncated,
    },
    scoring: {
      semantic_weight: SEMANTIC_WEIGHT,
      skill_weight: SKILL_WEIGHT,
      title_weight: TITLE_WEIGHT,
      division_weight: DIVISION_WEIGHT,
      role_weight: ROLE_WEIGHT,
      seniority_weight: SENIORITY_WEIGHT,
      dynamic_weight_redistribution: true,
    },
    performance: {
      ...stageTiming,
      cached_jobs: jobs.length,
      indexed_candidate_pool: candidateJobs.length,
      cheap_shortlist: shortlist.length,
      semantic_shortlist: semanticShortlist.length,
      final_guard_rejected: finalGuardRejected,
      duplicate_results_rejected: finalDuplicateRejected,
      unique_candidates_before_slice: uniqueFinalMatches.length,
      raw_collection_target: rawCollectionTarget,
      semantic_stage_ms: semanticStageMs,
      matching_loop_ms: matchLoopMs,
      candidate_embedding_cache_hit: Boolean(profileEmbeddingResult.fromCache),
      business_summary_mode: businessSummaryMode,
      business_title_anchored_mode: businessTitleAnchoredMode,
      business_summary_role_families: businessSummaryRoleFamilies.map((item) => item.family),
      business_profile_semantic_floor: BUSINESS_PROFILE_SEMANTIC_FLOOR,
      business_profile_semantic_ceiling: BUSINESS_PROFILE_SEMANTIC_CEILING,
      business_primary_role_min: BUSINESS_PRIMARY_ROLE_MIN,
      requested_top_k: safeTopK,
      strict_matches_before_backfill: strictMatchesCount,
      fallback_pool: fallbackPoolSize,
      fallback_semantic_evaluated: fallbackSemanticEvaluated,
      backfill_added: backfillAdded,
      broad_backfill_pool: broadBackfillPoolSize,
      broad_backfill_semantic_evaluated: broadBackfillSemanticEvaluated,
      broad_backfill_added: broadBackfillAdded,
      company_name_required: REQUIRE_COMPANY_NAME,
      top_k_target_reached: topMatches.length >= (EXACT_TOP_30 ? Math.max(30, safeTopK) : safeTopK),
      exact_top_30_required: EXACT_TOP_30,
      strict_selected_radius: STRICT_SELECTED_RADIUS,
      candidate_remote_preference: resolvedRemotePreference,
      candidate_remote_preference_state: candidateRemotePreferenceStateValue,
      candidate_allows_remote: candidateAllowsRemote,
      candidate_allows_remote_top30_fallback: candidateAllowsRemoteFallback,
      effective_radius_km: effectiveRadius,
      outside_radius_fallback_enabled:
        !STRICT_SELECTED_RADIUS && TOP30_ALLOW_OUTSIDE_RADIUS_FALLBACK,
      outside_radius_backfill_pool: outsideRadiusBackfillPool,
      outside_radius_semantic_evaluated: outsideRadiusSemanticEvaluated,
      outside_radius_backfill_added: outsideRadiusBackfillAdded,
      target_shortfall: Math.max(
        0,
        (EXACT_TOP_30 ? Math.max(30, safeTopK) : safeTopK) - topMatches.length
      ),
    },
    matches_count: topMatches.length,
    matches: topMatches,
  };
}

// Webhook handler
app.post('/webhook/match-candidate', authenticateMatchWebhook, async (req, res) => {
  const requestId = uuidv4();
  const startTime = Date.now();
  console.log(`[${requestId}] Match request received`);

  try {
    if (!req.body || typeof req.body !== 'object' || Array.isArray(req.body)) {
      throw new Error('Request body must be a JSON object');
    }

    const incoming = { ...req.body };
    const salesforceContactId = incoming.salesforce_contact_id;
    if (!salesforceContactId) throw new Error('Missing salesforce_contact_id');

    const parsedSkills = parseSkillScores(incoming.skill_scores);
    if (Object.keys(parsedSkills).length === 0) {
      console.warn(`[${requestId}] skill_scores is empty. Semantic full profile matching will still run.`);
    }

    const parsedRadius = Number(incoming.radius ?? DEFAULT_RADIUS_KM);
    const radius = Number.isFinite(parsedRadius) ? Math.max(0, parsedRadius) : DEFAULT_RADIUS_KM;
    const requestedTopK = Number.parseInt(incoming.top_k ?? DEFAULT_TOP_K, 10) || DEFAULT_TOP_K;
    const topK = Math.min(
      Math.max(MIN_RETURN_MATCHES, requestedTopK),
      MAX_TOP_K
    );

    // Keep the whole body for matching. Only radius and top_k are operational
    // controls and are ignored by the semantic flattener automatically.
    const result = await matchSemaphore.run(() => matchCandidate(incoming, radius, topK));

    const elapsed = Date.now() - startTime;
    console.log(`[${requestId}] Completed in ${elapsed}ms with ${result.matches_count} matches`);

    res.status(200).json({
      success: true,
      request_id: requestId,
      ...result,
      elapsed_ms: elapsed,
      message: `Successfully matched candidate with ${result.matches_count} job(s)`,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    const elapsed = Date.now() - startTime;
    console.error(`[${requestId}] Error after ${elapsed}ms: ${error.stack || error.message}`);

    res.status(500).json({
      success: false,
      request_id: requestId,
      error: error.message,
      elapsed_ms: elapsed,
      timestamp: new Date().toISOString(),
    });
  }
});

app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'ok',
    version: '10.2.0',
    build_id: BUILD_ID,
    cache_warm: Boolean(cachedJobs),
    jobs_cached: cachedJobs ? cachedJobs.length : 0,
    database_reliability: {
      max_retries: DB_MAX_RETRIES,
      request_timeout_ms: DB_REQUEST_TIMEOUT_MS,
      job_fetch_page_size: JOB_FETCH_PAGE_SIZE,
      company_fetch_page_size: COMPANY_FETCH_PAGE_SIZE,
      max_concurrent_matches: MAX_CONCURRENT_MATCHES,
    },
    matching_quality: {
      min_semantic_similarity: MIN_SEMANTIC_SIMILARITY,
      min_final_score: MIN_FINAL_SCORE,
      min_role_family_score: MIN_ROLE_FAMILY_SCORE,
      strict_division_match: STRICT_DIVISION_MATCH,
      allow_remote_outside_radius: ALLOW_REMOTE_OUTSIDE_RADIUS,
      allow_remote_unknown_distance: ALLOW_REMOTE_UNKNOWN_DISTANCE,
      semantic_shortlist_size: SEMANTIC_SHORTLIST_SIZE,
      semantic_calibration_floor: SEMANTIC_CALIBRATION_FLOOR,
      semantic_calibration_ceiling: SEMANTIC_CALIBRATION_CEILING,
      max_indexed_candidate_pool: MAX_INDEXED_CANDIDATE_POOL,
      final_garbage_guard: FINAL_GARBAGE_GUARD,
      strict_job_title_validation: STRICT_JOB_TITLE_VALIDATION,
      reject_non_job_urls: REJECT_NON_JOB_URLS,
      guarantee_top_k: GUARANTEE_TOP_K,
      minimum_return_matches: MIN_RETURN_MATCHES,
      exact_top_30: EXACT_TOP_30,
      strict_selected_radius: STRICT_SELECTED_RADIUS,
      candidate_remote_radius_exception: CANDIDATE_REMOTE_RADIUS_EXCEPTION,
      remote_unspecified_top30_fallback: REMOTE_UNSPECIFIED_TOP30_FALLBACK,
      remote_top30_min_role_signal: REMOTE_TOP30_MIN_ROLE_SIGNAL,
      remote_top30_min_final_score: REMOTE_TOP30_MIN_FINAL_SCORE,
      unique_top30_collection_multiplier: UNIQUE_TOP30_COLLECTION_MULTIPLIER,
      top30_allow_outside_radius_fallback:
        !STRICT_SELECTED_RADIUS && TOP30_ALLOW_OUTSIDE_RADIUS_FALLBACK,
      clean_backfill_min_final_score: CLEAN_BACKFILL_MIN_FINAL_SCORE,
      broad_backfill_min_final_score: BROAD_BACKFILL_MIN_FINAL_SCORE,
      broad_min_role_signal: BROAD_MIN_ROLE_SIGNAL,
      require_company_name: REQUIRE_COMPANY_NAME,
      backfill_min_role_family_score: BACKFILL_MIN_ROLE_FAMILY_SCORE,
      backfill_semantic_limit: BACKFILL_SEMANTIC_LIMIT,
      broad_backfill_semantic_limit: BROAD_BACKFILL_SEMANTIC_LIMIT,
      broad_backfill_min_semantic_similarity: BROAD_BACKFILL_MIN_SEMANTIC_SIMILARITY,
    },
    profile_embedding: {
      model: VOYAGE_MODEL,
      chunk_chars: EMBED_CHUNK_CHARS,
      batch_size: EMBED_BATCH_SIZE,
      max_profile_chars: MAX_PROFILE_CHARS === 0 ? 'unlimited' : MAX_PROFILE_CHARS,
    },
    divisions: ['IT Consulting', 'Construction', 'Business', 'Finance', 'Legal'],
    timestamp: new Date().toISOString(),
  });
});

// Start server
const SSL_KEY_PATH = process.env.SSL_KEY || '/etc/ssl/private/server.key';
const SSL_CERT_PATH = process.env.SSL_CERT || '/etc/ssl/certs/server.crt';

let useHttps = false;
try {
  useHttps = fs.existsSync(SSL_KEY_PATH) && fs.existsSync(SSL_CERT_PATH);
} catch {
  useHttps = false;
}

async function startServer() {
  runGarbageSelfTest();
  console.log('Garbage filter self test passed.');
  const warmed = await warmupCache();
  if (!warmed) {
    console.error('Cache warmup failed. Exiting so the process manager can restart the service.');
    process.exit(1);
  }

  const server = useHttps
    ? https.createServer(
      {
        key: fs.readFileSync(SSL_KEY_PATH),
        cert: fs.readFileSync(SSL_CERT_PATH),
      },
      app
    )
    : app;

  server.requestTimeout = TIMEOUT_MS;
  server.headersTimeout = Math.max(65000, Math.min(TIMEOUT_MS, 120000));

  server.listen(PORT, HOST, () => {
    console.log(`${useHttps ? 'HTTPS' : 'HTTP'} matching engine v10.2.0 running on ${HOST}:${PORT}`);
    console.log(`Build: ${BUILD_ID}`);
    console.log(`Company name required: ${REQUIRE_COMPANY_NAME ? 'yes' : 'no'}`);
    console.log(`Minimum returned matches target: ${MIN_RETURN_MATCHES}`);
    console.log(`Exact top 30 guarantee: ${EXACT_TOP_30 ? 'enabled' : 'disabled'}`);
    console.log(
      `Strict selected radius: ${STRICT_SELECTED_RADIUS ? 'enabled' : 'disabled'}`
    );
    console.log(
      `Candidate-aware remote radius exception: ${
        CANDIDATE_REMOTE_RADIUS_EXCEPTION ? 'enabled' : 'disabled'
      }`
    );
    console.log(
      `Top 30 outside-radius fallback: ${
        !STRICT_SELECTED_RADIUS && TOP30_ALLOW_OUTSIDE_RADIUS_FALLBACK
          ? 'enabled'
          : 'disabled'
      }`
    );
    console.log('POST /webhook/match-candidate');
    console.log('GET  /health');
    console.log(`Concurrency: ${MAX_CONCURRENT_MATCHES}`);
    console.log(`Cache TTL: ${JOB_CACHE_TTL_MS / 1000}s`);
    console.log(`Request timeout: ${TIMEOUT_MS / 1000}s`);
    console.log(
      `Weights: semantic ${SEMANTIC_WEIGHT}, skills ${SKILL_WEIGHT}, ` +
      `title ${TITLE_WEIGHT}, division ${DIVISION_WEIGHT}, ` +
      `role ${ROLE_WEIGHT}, seniority ${SENIORITY_WEIGHT}`
    );
    console.log(
      `Full profile embedding: chunk size ${EMBED_CHUNK_CHARS} chars, ` +
      `batch size ${EMBED_BATCH_SIZE}, max profile chars ${MAX_PROFILE_CHARS || 'unlimited'}`
    );
    console.log(
      `Quality gates: semantic >= ${MIN_SEMANTIC_SIMILARITY}, final >= ${MIN_FINAL_SCORE}, ` +
      `role family >= ${MIN_ROLE_FAMILY_SCORE}, seniority gap <= ${MAX_SENIORITY_GAP}`
    );
    console.log(`Remote jobs outside selected radius: ${ALLOW_REMOTE_OUTSIDE_RADIUS ? 'allowed' : 'blocked'}`);
    console.log(`Strict job title validation: ${STRICT_JOB_TITLE_VALIDATION ? 'enabled' : 'disabled'}`);
    console.log(`Non job URL rejection: ${REJECT_NON_JOB_URLS ? 'enabled' : 'disabled'}`);
    console.log(`Semantic shortlist size: ${SEMANTIC_SHORTLIST_SIZE}`);
    console.log(`Candidate embedding cache TTL: ${CANDIDATE_EMBED_CACHE_TTL_MS / 1000}s`);
  });
}

startServer().catch((error) => {
  console.error('Fatal startup error:', error);
  process.exit(1);
});
