require('dotenv').config();

const axios = require('axios');
const crypto = require('crypto');
const cheerio = require('cheerio');
const { CRAWLER_TIMEOUTS } = require('./crawler-timeouts');
const { classifyJobWithLLM } = require('../ai/job-classifier');

const geocodeCache = new Map();
const embeddingCache = new Map();
let lastGeoRequestAt = 0;

const LOCATION_UNKNOWN = 'Unknown';
const hqLocationCache = new Map();
const hqLocationInFlightCache = new Map();

function asTrimmedString(value) {
    if (value === null || value === undefined) return '';
    return String(value).trim();
}

function normalizeLocation(location) {
    const value = asTrimmedString(location);
    return value.length > 0 ? value : null;
}

function normalizeActualLocation(location) {
    const value = normalizeLocationCandidate(location);
    if (!value) return null;
    const normalized = value.toLowerCase().replace(/[.]/g, '').trim();
    if ([
        LOCATION_UNKNOWN.toLowerCase(), 'n/a', 'na', 'null', 'none',
        'remote', 'fully remote', 'hybrid', 'home office', 'homeoffice',
        'anywhere', 'various locations', 'multiple locations', 'flexible location'
    ].includes(normalized)) return null;
    if (/\b(?:remote|home[- ]?office|homeoffice|anywhere)\b/i.test(normalized)) return null;
    return value;
}

const LOCATION_FIELD_BOUNDARY_PATTERN = /(?:^|[;|•]|\s+\/\s+|\n)\s*[^\d,;|\n]{2,80}?\s*:\s*\S+/u;
const LOCATION_METADATA_URL_PATTERN = /(?:https?:\/\/|www\.)\S+/i;
const LOCATION_NON_VALUE_PATTERN = /^(?:remote|hybrid|homeoffice|home office|vollzeit|teilzeit|full[- ]?time|part[- ]?time|n\/a|none|unknown|unspecified)$/i;

function normalizeLocationCandidate(location) {
    const value = asTrimmedString(location)
        .replace(/\u00a0/g, ' ')
        .replace(/\s+/g, ' ')
        .replace(/^['"`]+|['"`]+$/g, '')
        .replace(/[\s,;|]+$/, '')
        .trim();
    if (!value || value.length > 220) return null;
    if (LOCATION_METADATA_URL_PATTERN.test(value) || /@/.test(value)) return null;
    if (LOCATION_FIELD_BOUNDARY_PATTERN.test(value)) return null;
    if (LOCATION_NON_VALUE_PATTERN.test(value)) return null;
    if (/^(?:telefon|tel\.?|phone|fax|email|e-mail|register|legal form|rechtsform|supervisory board|aufsichtsrat)\b/i.test(value)) return null;
    if (/^[+\d\s()./-]{6,}$/.test(value)) return null;
    return value.replace(/,\s*DE$/i, ', Germany');
}

function sanitizeLocationEvidence(location) {
    const rawValue = asTrimmedString(location).replace(/\u00a0/g, ' ');
    if (!rawValue) return null;
    const boundary = rawValue.search(/(?:\s*;\s*|\s*\|\s*|\s+\/\s+|\s*•\s*|\r?\n)\s*[^\d,;|\n]{2,80}?\s*:\s*\S+/u);
    const candidate = boundary > 1 ? rawValue.slice(0, boundary).trim() : rawValue;
    return normalizeLocationCandidate(candidate);
}

function selectAuthoritativeJobLocation({ deterministicEvidence, llmLocation } = {}) {
    const cleaned = normalizeLocationCandidate(llmLocation);
    if (cleaned) return { location: cleaned, source: 'llm' };
    const fallback = sanitizeLocationEvidence(deterministicEvidence);
    if (fallback) return { location: fallback, source: 'deterministic_fallback' };
    return { location: null, source: 'unavailable' };
}

function employmentText(job = {}) {
    return [
        job.title, job.raw_description, job.description, job.requirements,
        job.responsibilities, job.employment_type, job.employmentType,
        job.remote_evidence, job.remoteEvidence,
        job.remote_type === 'hybrid' ? 'hybrid' : ''
    ].filter(Boolean).map(asTrimmedString).join('\n');
}

function isTechnicalRemoteContext(text, matchIndex) {
    const context = text.slice(Math.max(0, matchIndex - 70), matchIndex + 100).toLowerCase();
    return /\b(?:server|access|support|monitor(?:ing)?|system|desktop|connection|administration|admin|maintenance|software|network|vpn|infrastructure)\b/.test(context);
}

function hasHybridEmploymentEvidence(text) {
    const value = asTrimmedString(text);
    if (!value) return false;
    const patterns = [
        /\bhybrid(?:arbeit| working| work| position| role)?\b/i,
        /\bhome[- ]?office\b/i,
        /\bwork(?:ing)?\s+from\s+home\b/i,
        /\bteilweise\s+(?:remote|im\s+homeoffice)\b/i,
        /\b(?:mobiles?|mobile)\s+arbeiten\b/i,
        /\bremote\s*(?:und|\/|\+|&)\s*(?:vor\s+ort|office|büro|onsite|on[- ]site)\b/i,
        /\b(?:vor\s+ort|office|büro|onsite|on[- ]site)\s*(?:und|\/|\+|&)\s*remote\b/i,
    ];
    return patterns.some(pattern => {
        const match = pattern.exec(value);
        return match && !isTechnicalRemoteContext(value, match.index);
    });
}

function hasExplicitFullRemoteEvidence(text) {
    const value = asTrimmedString(text);
    if (!value) return false;
    const patterns = [
        /\b100\s*%\s*(?:remote|remote[- ]?work|home[- ]?office|homeoffice)\b/i,
        /\b(?:fully|completely|entirely|fully)\s+remote\b/i,
        /\bremote\s+only\b/i,
        /\bwork(?:ing)?\s+fully\s+remotely\b/i,
        /\bvoll(?:ständig|kommen)\s+(?:remote|im\s+homeoffice)\b/i,
        /\bausschließlich\s+(?:remote|im\s+homeoffice)\b/i
    ];
    return patterns.some(pattern => {
        const match = pattern.exec(value);
        return match && !isTechnicalRemoteContext(value, match.index);
    });
}

function resolveAuthoritativeRemoteLocation({ jobLocation, companyHq, jobText = '' } = {}) {
    const actualJobLocation = normalizeActualLocation(jobLocation);
    const hq = normalizeActualLocation(companyHq);
    const hybrid = hasHybridEmploymentEvidence(jobText);

    if (actualJobLocation) {
        return {
            location: actualJobLocation,
            remote_type: hybrid ? 'hybrid' : 'onsite',
            source: 'job',
            evidence: hybrid ? 'job_location_plus_hybrid' : 'job_location_plus_no_hybrid'
        };
    }
    if (hq) {
        return {
            location: hq,
            remote_type: hybrid ? 'hybrid' : 'onsite',
            source: 'company_hq',
            evidence: hybrid ? 'company_hq_plus_hybrid' : 'company_hq_plus_no_hybrid'
        };
    }
    return {
        location: LOCATION_UNKNOWN,
        remote_type: hasExplicitFullRemoteEvidence(jobText) ? 'remote' : 'onsite',
        source: 'unavailable',
        evidence: hasExplicitFullRemoteEvidence(jobText) ? 'explicit_full_remote' : 'no_location_or_full_remote'
    };
}

function companyWebsiteFrom(job = {}) {
    return job.company_website || job.company_website_url || job.companyWebsiteUrl ||
        job.website || job.company?.Website || job.company_metadata?.website || null;
}

function candidateWebsiteUrls(website) {
    if (!website) return [];
    try {
        const base = new URL(/^https?:\/\//i.test(website) ? website : `https://${website}`);
        base.hash = '';
        base.search = '';
        base.pathname = base.pathname.replace(/\/+$/, '');
        const origin = base.origin;
        return [...new Set([
            base.toString().replace(/\/$/, ''),
            ...['impressum', 'imprint', 'legal-notice', 'mentions-legales', 'aviso-legal', 'colophon',
                'de/impressum', 'de/imprint', 'unternehmen/impressum',
                'unternehmen/imprint', 'legal/impressum', 'legal/imprint', 'kontakt/impressum',
                'company/impressum', 'about/impressum', 'datenschutz/impressum']
                .map(path => `${origin}/${path}`)
        ])];
    } catch {
        return [];
    }
}

function absoluteUrl(value, baseUrl) {
    try { return new URL(value, baseUrl).toString(); } catch { return null; }
}

function extractSitemapUrls(text, baseUrl) {
    return [...String(text || '').matchAll(/<loc[^>]*>\s*([^<]+)\s*<\/loc>/gi)]
        .map(match => absoluteUrl(match[1].trim(), baseUrl))
        .filter(Boolean);
}

function extractRobotsSitemaps(text, baseUrl) {
    return String(text || '').split(/\r?\n/)
        .map(line => line.match(/^\s*sitemap\s*:\s*(\S+)/i)?.[1])
        .map(value => absoluteUrl(value, baseUrl))
        .filter(Boolean);
}

async function discoverImpressumUrls(website, { signal } = {}) {
    if (!website) return [];
    let base;
    try { base = new URL(/^https?:\/\//i.test(website) ? website : `https://${website}`); } catch { return []; }
    base.hash = ''; base.search = '';
    const origin = base.origin;
    const urls = new Set(candidateWebsiteUrls(base.toString()));
    try {
        const homepage = await axios.get(base.toString(), {
            timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
            maxRedirects: 5,
            signal,
            headers: { 'User-Agent': process.env.CRAWLER_USER_AGENT || 'customer-matching-crawler/1.0' }
        });
        const $ = cheerio.load(String(homepage.data || ''));
        $('a[href]').each((_, element) => {
            const href = absoluteUrl($(element).attr('href'), base.toString());
            const text = `${$(element).text()} ${href || ''}`;
            if (href && /impressum|imprint|legal[- ]?notice|mentions[- ]?legales|aviso[- ]?legal|colophon|rechtlich|company information|corporate information/i.test(text)) {
                urls.add(href);
            }
        });
    } catch { /* homepage link discovery is optional */ }
    const sitemapQueue = [`${origin}/robots.txt`, `${origin}/sitemap.xml`, `${origin}/sitemap_index.xml`];
    const visited = new Set();
    let sitemapCount = 0;
    while (sitemapQueue.length > 0 && sitemapCount < 25) {
        const url = sitemapQueue.shift();
        if (!url || visited.has(url)) continue;
        visited.add(url); sitemapCount++;
        try {
            const response = await axios.get(url, {
                timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
                maxRedirects: 5,
                signal,
                headers: { 'User-Agent': process.env.CRAWLER_USER_AGENT || 'customer-matching-crawler/1.0' }
            });
            const body = String(response.data || '');
            const discoveredSitemaps = url.endsWith('/robots.txt')
                ? extractRobotsSitemaps(body, url)
                : extractSitemapUrls(body, url);
            for (const discovered of discoveredSitemaps) {
                if (/sitemap|\.xml(?:$|\?)/i.test(discovered) && !visited.has(discovered)) sitemapQueue.push(discovered);
                if (/impressum|imprint|legal|rechtlich|mentions-legales|legal-notice|aviso-legal|colophon/i.test(discovered)) urls.add(discovered);
            }
            if (!url.endsWith('/robots.txt')) {
                for (const discovered of extractSitemapUrls(body, url)) {
                    if (/impressum|imprint|legal|rechtlich|mentions-legales|legal-notice|aviso-legal|colophon/i.test(discovered)) urls.add(discovered);
                }
            }
            if (url.endsWith('/robots.txt')) {
                for (const link of body.match(/https?:\/\/[^\s]+/gi) || []) {
                    if (/impressum|imprint|legal-notice|mentions-legales|aviso-legal|colophon/i.test(link)) urls.add(link.replace(/[)>,.;]+$/, ''));
                }
            }
        } catch { /* optional discovery source */ }
    }
    return [...urls].filter(url => /impressum|imprint|legal|rechtlich|mentions-legales|legal-notice|aviso-legal|colophon/i.test(url));
}

const HQ_LABEL_PATTERN = /\b(?:sitz|geschäftssitz|unternehmenssitz|geschäftsanschrift|anschrift|adresse|registered office|headquarters?|company address)\b/i;
const HQ_NEGATIVE_PATTERN = /\b(?:supervisory board|aufsichtsrat|vorstand|legal form|rechtsform|register court|registergericht|registration number|handelsregister|tax(?:\s+id| number)|ust\.?|steuer|telefon|phone|fax|email|e-mail)\b/i;

function cleanAddressText(value) {
    return asTrimmedString(value)
        .replace(/\s+/g, ' ')
        .replace(/[|]+/g, ' ')
        .replace(/\s+(?:telefon|tel\.?|phone|fax|email|e-mail|www\.?)\b.*$/i, '')
        .replace(/[;,]+$/, '')
        .trim();
}

function addressHasPostalCode(value) {
    return /\b\d{4,6}\s+[A-ZÄÖÜÀ-ÖØ-Þ][A-Za-zÄÖÜÀ-öø-ÿ' .-]{2,80}\b/.test(value);
}

function addressHasStreetAndNumber(value) {
    return /\b[A-ZÄÖÜÀ-ÖØ-Þ][A-Za-zÄÖÜÀ-öø-ÿ' .-]{2,70}\s+\d+[A-Za-z]?\b/.test(value) ||
        /\b\d+[A-Za-z]?\s+[A-ZÄÖÜÀ-ÖØ-Þ][A-Za-zÄÖÜÀ-öø-ÿ' .-]{2,70}\b/.test(value);
}

function validatePostalAddress(value, { structured = false, structuredLocality = false } = {}) {
    const candidate = cleanAddressText(value);
    if (!candidate || candidate.length < 5 || candidate.length > 400) return null;
    if (HQ_NEGATIVE_PATTERN.test(candidate)) return null;
    const postal = addressHasPostalCode(candidate);
    const street = addressHasStreetAndNumber(candidate);
    const locality = structuredLocality || /\b(?:city|locality|ort|stadt|town)\s*[:\-]/i.test(candidate);
    if (!postal && !(structured && street && locality)) return null;
    if (!street && !postal) return null;
    if (!postal && structured && !locality) return null;
    return candidate;
}

function addressObjectToCandidate(address) {
    if (!address || typeof address !== 'object' || Array.isArray(address)) return null;
    const street = address.streetAddress || address.street || '';
    const postal = address.postalCode || address.zip || '';
    const locality = address.addressLocality || address.city || '';
    const region = address.addressRegion || address.region || '';
    const country = typeof address.addressCountry === 'object'
        ? (address.addressCountry.name || address.addressCountry.value || '')
        : (address.addressCountry || address.country || '');
    const parts = [street, [postal, locality].filter(Boolean).join(' '), region, country]
        .map(cleanAddressText)
        .filter(Boolean);
    return parts.length > 0
        ? validatePostalAddress(parts.join(', '), { structured: true, structuredLocality: Boolean(locality) })
        : null;
}

function collectJsonLdAddresses(value, output = []) {
    if (!value || typeof value !== 'object') return output;
    if (Array.isArray(value)) {
        value.forEach(item => collectJsonLdAddresses(item, output));
        return output;
    }
    if (value.address && typeof value.address === 'object') {
        const candidate = addressObjectToCandidate(value.address);
        if (candidate) output.push({ value: candidate, source: 'json_ld', score: 100 });
    }
    if (String(value['@type'] || '').toLowerCase().includes('postaladdress')) {
        const candidate = addressObjectToCandidate(value);
        if (candidate) output.push({ value: candidate, source: 'json_ld', score: 105 });
    }
    Object.values(value).forEach(child => collectJsonLdAddresses(child, output));
    return output;
}

function extractJsonLdAddressCandidates($) {
    const candidates = [];
    $('script[type="application/ld+json"]').each((_, element) => {
        try {
            const parsed = JSON.parse($(element).contents().text().trim());
            collectJsonLdAddresses(parsed, candidates);
        } catch { /* malformed JSON-LD is ignored */ }
    });
    return candidates;
}

function extractMicrodataAddressCandidates($) {
    const candidates = [];
    $('[itemprop="address"]').each((_, element) => {
        const root = $(element);
        const fields = {};
        root.find('[itemprop]').addBack('[itemprop]').each((__, field) => {
            const key = $(field).attr('itemprop');
            if (key && !fields[key]) fields[key] = $(field).attr('content') || $(field).text();
        });
        const candidate = addressObjectToCandidate(fields) || validatePostalAddress(root.text());
        if (candidate) candidates.push({ value: candidate, source: 'microdata', score: 90 });
    });
    const rdfa = $('[property="streetAddress"], [property="schema:streetAddress"]');
    if (rdfa.length > 0) {
        const fields = {};
        rdfa.add('[property="postalCode"], [property="schema:postalCode"], [property="addressLocality"], [property="schema:addressLocality"], [property="addressCountry"], [property="schema:addressCountry"]').each((_, field) => {
            const property = ($(field).attr('property') || '').split(':').pop();
            fields[property] = $(field).attr('content') || $(field).text();
        });
        const candidate = addressObjectToCandidate(fields);
        if (candidate) candidates.push({ value: candidate, source: 'rdfa', score: 88 });
    }
    return candidates;
}

function extractAddressElementCandidates($) {
    const candidates = [];
    $('address').each((_, element) => {
        const root = $(element);
        const clone = root.clone();
        clone.find('br').replaceWith(' ');
        const text = cleanAddressText(clone.text());
        const contextRoot = root.closest('section, article, main, div').first();
        const contextText = cleanAddressText(String(contextRoot.html() || '').replace(/<[^>]+>/g, ' '));
        const context = `${contextText} ${root.parent().text()} ${root.attr('class') || ''} ${root.attr('id') || ''}`;
        const candidate = validatePostalAddress(text);
        if (candidate) candidates.push({
            value: candidate,
            source: 'address_element',
            score: 70 + (HQ_LABEL_PATTERN.test(context) ? 25 : 0) - (HQ_NEGATIVE_PATTERN.test(context) ? 50 : 0)
        });
    });
    return candidates;
}

function extractLabelledAddressCandidates($) {
    const candidates = [];
    $('main, section, article, div, p, li, footer').each((_, element) => {
        const root = $(element);
        const text = cleanAddressText(root.text());
        if (!HQ_LABEL_PATTERN.test(text) || text.length > 800) return;
        const labelled = text.match(/(?:sitz|geschäftssitz|unternehmenssitz|geschäftsanschrift|anschrift|adresse|registered office|headquarters?|company address)\s*[:\-]?\s*(.{5,300}?)(?=\b(?:supervisory board|aufsichtsrat|vorstand|legal form|rechtsform|register court|registergericht|registration number|handelsregister|tax(?:\s+id| number)|ust\.?|steuer|telefon|phone|fax|email|e-mail)\b|$)/i);
        const labelledText = labelled ? labelled[1] : text;
        const candidate = validatePostalAddress(labelledText);
        if (candidate) candidates.push({
            value: candidate,
            source: 'labelled_block',
            score: 60 + (/\b(?:headquarters?|registered office|company address|unternehmenssitz|geschäftsanschrift|geschäftssitz)\b/i.test(text) ? 25 : 0)
        });
    });
    return candidates;
}

function extractLegalCompanyAddressEvidence(html) {
    if (!html) return null;
    const $ = cheerio.load(html);
    const candidates = [
        ...extractJsonLdAddressCandidates($),
        ...extractMicrodataAddressCandidates($),
        ...extractAddressElementCandidates($),
        ...extractLabelledAddressCandidates($)
    ];
    const grouped = new Map();
    for (const candidate of candidates) {
        const key = candidate.value.toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
        const previous = grouped.get(key);
        if (!previous || candidate.score > previous.score) grouped.set(key, candidate);
    }
    const ranked = [...grouped.values()].sort((a, b) => b.score - a.score);
    if (ranked.length === 0) return null;
    if (ranked.length > 1 && ranked[0].score === ranked[1].score && ranked[0].value.toLowerCase() !== ranked[1].value.toLowerCase()) return null;
    return ranked[0];
}

function extractLegalCompanyAddress(html) {
    return extractLegalCompanyAddressEvidence(html)?.value || null;
}

async function findCompanyHqLocation(website, { signal } = {}) {
    const urls = await discoverImpressumUrls(website, { signal });
    if (urls.length === 0) return null;
    const cacheKey = String(website).toLowerCase().replace(/\/$/, '');
    if (hqLocationCache.has(cacheKey)) return hqLocationCache.get(cacheKey);
    if (hqLocationInFlightCache.has(cacheKey)) return hqLocationInFlightCache.get(cacheKey);

    const lookupPromise = (async () => {
        for (const url of urls) {
            try {
                const response = await axios.get(url, {
                    timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
                    maxRedirects: 5,
                    signal,
                    headers: { 'User-Agent': process.env.CRAWLER_USER_AGENT || 'customer-matching-crawler/1.0' }
                });
                const location = extractLegalCompanyAddress(response.data);
                if (location) {
                    hqLocationCache.set(cacheKey, location);
                    return location;
                }
            } catch {
                // An unavailable optional company page must not reject the job.
            }
        }
        hqLocationCache.set(cacheKey, null);
        return null;
    })();
    hqLocationInFlightCache.set(cacheKey, lookupPromise);
    try {
        return await lookupPromise;
    } finally {
        hqLocationInFlightCache.delete(cacheKey);
    }
}

async function resolveJobLocation(job = {}, { signal, companyHq } = {}) {
    const hq = companyHq === undefined
        ? await findCompanyHqLocation(companyWebsiteFrom(job), { signal })
        : companyHq;
    const authority = resolveAuthoritativeRemoteLocation({
        jobLocation: job.location,
        companyHq: hq,
        jobText: employmentText(job)
    });
    const location = authority.location;
    const source = authority.source;
    const shouldGeocode = location !== LOCATION_UNKNOWN &&
        (job.location_lat === null || job.location_lat === undefined || job.location_lng === null || job.location_lng === undefined);
    const geo = shouldGeocode ? await geocodeCity(location, { signal }) : {
        lat: job.location_lat ?? null,
        lng: job.location_lng ?? null
    };
    return { ...authority, location_lat: geo.lat, location_lng: geo.lng };
}

function buildEmbeddingText(job = {}) {
    const parts = [];

    const title = normalizeLocation(job.title);
    const companyName = normalizeLocation(job.company_name);
    const location = normalizeLocation(job.location);
    const employmentType = normalizeLocation(job.employment_type);
    const remoteType = normalizeLocation(job.remote_type);
    const rawDescription = asTrimmedString(job.raw_description || job.description);

    if (title) parts.push(`Title: ${title}`);
    if (companyName) parts.push(`Company: ${companyName}`);
    if (location) parts.push(`Location: ${location}`);
    if (employmentType) parts.push(`Employment: ${employmentType}`);
    if (remoteType) parts.push(`Remote: ${remoteType}`);

    if (Array.isArray(job.structured_skills) && job.structured_skills.length > 0) {
        parts.push(`Skills: ${job.structured_skills.map(asTrimmedString).filter(Boolean).join(', ')}`);
    }

    if (rawDescription) {
        parts.push(`Description: ${rawDescription}`);
    }

    return parts.join('\n').trim();
}

async function geocodeCity(location, { signal } = {}) {
    const normalized = normalizeLocation(location);
    if (!normalized) return { lat: null, lng: null };

    const cacheKey = normalized.toLowerCase();
    if (geocodeCache.has(cacheKey)) return geocodeCache.get(cacheKey);

    const elapsed = Date.now() - lastGeoRequestAt;
    if (elapsed < 1100) {
        await new Promise(resolve => setTimeout(resolve, 1100 - elapsed));
    }
    lastGeoRequestAt = Date.now();

    try {
        const response = await axios.get('https://nominatim.openstreetmap.org/search', {
            params: {
                q: `${normalized}, Germany`,
                format: 'jsonv2',
                limit: 1
            },
            headers: {
                'User-Agent': process.env.NOMINATIM_USER_AGENT || 'customer-matching-crawler/1.0'
            },
            timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS,
            signal
        });

        const hit = Array.isArray(response.data) ? response.data[0] : null;
        const result = hit
            ? { lat: Number.parseFloat(hit.lat), lng: Number.parseFloat(hit.lon) }
            : { lat: null, lng: null };

        geocodeCache.set(cacheKey, result);
        return result;
    } catch (error) {
        const result = { lat: null, lng: null };
        geocodeCache.set(cacheKey, result);
        return result;
    }
}

async function embedWithVoyage(text) {
    const normalized = asTrimmedString(text);
    if (normalized.length < 10) return null;

    const cacheKey = crypto.createHash('sha256').update(normalized).digest('hex');
    if (embeddingCache.has(cacheKey)) return embeddingCache.get(cacheKey);

    const apiKey = process.env.VOYAGE_API_KEY;
    if (!apiKey || apiKey.trim() === '') return null;

    try {
        const response = await axios.post(
            'https://api.voyageai.com/v1/embeddings',
            {
                input: [normalized.slice(0, 16000)],
                model: process.env.VOYAGE_MODEL || 'voyage-3-large',
                input_type: 'document'
            },
            {
                headers: {
                    Authorization: `Bearer ${apiKey}`,
                    'Content-Type': 'application/json'
                },
                timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS
            }
        );

        const embedding = response.data?.data?.[0]?.embedding || null;
        embeddingCache.set(cacheKey, embedding);
        return embedding;
    } catch (error) {
        embeddingCache.set(cacheKey, null);
        return null;
    }
}

async function enrichJobForStorage(job = {}) {
    const row = {
        ...job
    };

    row.raw_description = asTrimmedString(row.raw_description || row.description) || null;
    const companyHq = await findCompanyHqLocation(companyWebsiteFrom(row));
    const classification = await classifyJobWithLLM({
        ...row,
        classification_description: row.classification_description || row.raw_description || row.description || null,
        source_url: row.source_url || row.apply_url || row.url || null,
        crawler_location: row.location || null,
        structured_location: row.structured_location || null,
        company_hq: companyHq
    });

    const cleanedJobLocation = classification.ok
        ? normalizeLocationCandidate(classification.data.job_location)
        : null;

    if (cleanedJobLocation) {
        row.remote_type = classification.data.remote_type;
        row.location = cleanedJobLocation;
        row.location_city = classification.data.location_city;
        row.location_country = classification.data.location_country;
        row._classification_source = 'llm';
    } else {
        // Never use a keyword or heuristic classifier after an LLM failure or
        // contaminated LLM location. Preserve only a safely sanitized source
        // location before using company HQ.
        row.remote_type = 'onsite';
        row._classification_source = 'failed';
        row.location = sanitizeLocationEvidence(row.location);
    }

    const resolvedLocation = await resolveJobLocation(row, { companyHq });
    row.location = resolvedLocation.location;
    row.remote_type = resolvedLocation.remote_type;
    row.location_lat = resolvedLocation.location_lat;
    row.location_lng = resolvedLocation.location_lng;
    row._location_source = resolvedLocation.source;
    row._classification_source = 'authoritative';

    if (!classification.ok || !cleanedJobLocation) {
        console.log(
            '[LOCATION FALLBACK] company=' + (row.company_name || 'unknown') +
            ' external_job_id=' + (row.external_job_id || 'unknown') +
            ' reason=no_job_location' +
            ' fallback=' + (resolvedLocation.source === 'company_hq' ? 'company_hq' : 'unavailable') +
            ' location=' + (row.location || LOCATION_UNKNOWN)
        );
    }

    const needsEmbedding = !row.skill_embedding;

    const embedding = needsEmbedding ? await embedWithVoyage(buildEmbeddingText(row)) : null;

    if (embedding) {
        row.skill_embedding = embedding;
    }

    return row;
}

async function preserveAuthoritativeFieldsForUpsert(supabase, row = {}) {
    const cleanRow = { ...row };
    const classificationSource = cleanRow._classification_source;
    const locationSource = cleanRow._location_source;
    delete cleanRow._classification_source;
    delete cleanRow._location_source;
    delete cleanRow.classification_description;
    delete cleanRow.location_city;
    delete cleanRow.location_country;

    const { data: existing, error } = await supabase
        .from('jobs')
        .select('remote_type, location, location_lat, location_lng')
        .eq('company_id', cleanRow.company_id)
        .eq('external_job_id', cleanRow.external_job_id)
        .maybeSingle();

    if (error) {
        throw new Error('Existing job authority lookup failed: ' + error.message);
    }

    const existingRemote = existing?.remote_type;
    const existingLocation = normalizeLocation(existing?.location);
    const existingRemoteIsValid = ['remote', 'hybrid', 'onsite', 'unknown'].includes(
        String(existingRemote || '').trim().toLowerCase()
    );
    const existingLocationIsAuthoritative = existingLocation &&
        existingLocation.toLowerCase() !== LOCATION_UNKNOWN.toLowerCase();

    if (!['llm', 'authoritative'].includes(classificationSource) && existingRemoteIsValid) {
        cleanRow.remote_type = existingRemote;
    }

    if (!['llm', 'authoritative'].includes(locationSource) && existingLocationIsAuthoritative) {
        cleanRow.location = existing.location;
        cleanRow.location_lat = existing.location_lat ?? null;
        cleanRow.location_lng = existing.location_lng ?? null;
    }

    return cleanRow;
}

async function enrichJobRows(rows, options = {}) {
    if (!Array.isArray(rows) || rows.length === 0) return [];

    const enriched = [];
    for (const job of rows) {
        enriched.push(await enrichJobForStorage({
            ...job,
            company_id: job.company_id ?? options.companyId ?? null,
            company_name: job.company_name ?? options.companyName ?? null,
            company_website: job.company_website ?? options.companyWebsite ?? null,
            ats_source: job.ats_source ?? options.atsSource ?? null
        }));
    }
    return enriched;
}

module.exports = {
    buildEmbeddingText,
    embedWithVoyage,
    enrichJobForStorage,
    enrichJobRows,
    geocodeCity,
    normalizeLocationCandidate,
    sanitizeLocationEvidence,
    selectAuthoritativeJobLocation,
    preserveAuthoritativeFieldsForUpsert,
    resolveJobLocation,
    findCompanyHqLocation,
    resolveAuthoritativeRemoteLocation,
    hasHybridEmploymentEvidence,
    hasExplicitFullRemoteEvidence,
    discoverImpressumUrls,
    extractLegalCompanyAddressEvidence,
    extractLegalCompanyAddress,
    LOCATION_UNKNOWN
};
