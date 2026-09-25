require('dotenv').config();

const axios = require('axios');
const crypto = require('crypto');
const cheerio = require('cheerio');
const { CRAWLER_TIMEOUTS } = require('./crawler-timeouts');

const geocodeCache = new Map();
const embeddingCache = new Map();
let lastGeoRequestAt = 0;

const LOCATION_NOT_AVAILABLE = 'Location Not Available';
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

function resolveRemoteType(description) {
    const text = asTrimmedString(description).toLowerCase();
    if (!text) return 'onsite';

    const negativeRemote = /\b(?:not remote|no remote|remote(?: work)? not available|no possibility to work remotely|office only)\b/i;
    if (negativeRemote.test(text)) return 'onsite';

    // Remove technical uses of "remote" before classifying employment mode.
    const employmentText = text.replace(/\bremote (?:monitoring|access|support|diagnostics|system|administration)\b/gi, ' ');
    const fullyRemote = /\b(?:fully remote|full remote|100\s*%?\s*remote|remote position|work from (?:anywhere|home)|work anywhere)\b/i;
    const hybrid = /\b(?:hybrid(?: working| work| role)?|flexible remote|partly remote|partially remote|teilweise remote|mobile work)\b/i;
    const remote = /\bremote\b|\bhome[ -]?office\b/i;

    if (fullyRemote.test(employmentText)) return 'remote';
    if (hybrid.test(employmentText)) return 'hybrid';
    if (remote.test(employmentText)) return 'remote';
    return 'onsite';
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
            ...['about', 'about-us', 'company', 'contact', 'contact-us', 'impressum', 'imprint']
                .map(path => `${origin}/${path}`)
        ])];
    } catch {
        return [];
    }
}

function extractCompanyLocationFromHtml(html) {
    if (!html) return null;
    const $ = cheerio.load(html);
    const fragments = [];
    $('address, [itemprop="address"], footer, .address, [class*="address"], [class*="impressum"], [class*="contact"]').each((_, el) => {
        const text = $(el).text().replace(/\s+/g, ' ').trim();
        if (text) fragments.push(text);
    });
    fragments.push($('body').text().replace(/\s+/g, ' ').trim());

    for (const fragment of fragments) {
        const postal = fragment.match(/\b\d{4,5}\s+[A-ZÄÖÜ][A-Za-zÄÖÜäöüß' -]{2,60}/);
        if (postal) return postal[0].trim().replace(/[.,;]+$/, '');

        const labelled = fragment.match(/(?:address|adresse|standort|location|sitz|headquarters?)\s*[:\-]\s*([^|]{2,100})/i);
        if (labelled) return labelled[1].trim().replace(/[.,;]+$/, '');
    }
    return null;
}

async function findCompanyHqLocation(website, { signal } = {}) {
    const urls = candidateWebsiteUrls(website);
    if (urls.length === 0) return null;
    const cacheKey = urls[0];
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
                const location = extractCompanyLocationFromHtml(response.data);
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

async function resolveJobLocation(job = {}, { signal } = {}) {
    const normalizedExisting = normalizeLocation(job.location);
    const existing = normalizedExisting && normalizedExisting.toLowerCase() !== LOCATION_NOT_AVAILABLE.toLowerCase()
        ? normalizedExisting
        : null;
    const location = existing || await findCompanyHqLocation(companyWebsiteFrom(job), { signal }) || LOCATION_NOT_AVAILABLE;
    const shouldGeocode = location !== LOCATION_NOT_AVAILABLE &&
        (job.location_lat === null || job.location_lat === undefined || job.location_lng === null || job.location_lng === undefined);
    const geo = shouldGeocode ? await geocodeCity(location, { signal }) : {
        lat: job.location_lat ?? null,
        lng: job.location_lng ?? null
    };
    return { location, location_lat: geo.lat, location_lng: geo.lng };
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
        parts.push(`Description: ${rawDescription.slice(0, 12000)}`);
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
    const resolvedLocation = await resolveJobLocation(row);
    row.location = resolvedLocation.location;
    row.location_lat = resolvedLocation.location_lat;
    row.location_lng = resolvedLocation.location_lng;
    row.remote_type = resolveRemoteType(row.raw_description);
    const needsEmbedding = !row.skill_embedding;

    const embedding = needsEmbedding ? await embedWithVoyage(buildEmbeddingText(row)) : null;

    if (embedding) {
        row.skill_embedding = embedding;
    }

    return row;
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
    resolveRemoteType,
    resolveJobLocation,
    findCompanyHqLocation,
    LOCATION_NOT_AVAILABLE
};
