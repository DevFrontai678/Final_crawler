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

async function resolveJobLocation(job = {}, { signal, companyHq } = {}) {
    const normalizedExisting = normalizeLocation(job.location);
    const existing = normalizedExisting && normalizedExisting.toLowerCase() !== LOCATION_UNKNOWN.toLowerCase()
        ? normalizedExisting
        : null;
    const hq = companyHq === undefined
        ? await findCompanyHqLocation(companyWebsiteFrom(job), { signal })
        : companyHq;
    const location = existing || hq || LOCATION_UNKNOWN;
    const source = existing ? 'job' : (hq ? 'company_hq' : 'unavailable');
    const shouldGeocode = location !== LOCATION_UNKNOWN &&
        (job.location_lat === null || job.location_lat === undefined || job.location_lng === null || job.location_lng === undefined);
    const geo = shouldGeocode ? await geocodeCity(location, { signal }) : {
        lat: job.location_lat ?? null,
        lng: job.location_lng ?? null
    };
    return { location, location_lat: geo.lat, location_lng: geo.lng, source };
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

    if (classification.ok) {
        row.remote_type = classification.data.remote_type;
        row.location = classification.data.job_location;
        row.location_city = classification.data.location_city;
        row.location_country = classification.data.location_country;
        row._classification_source = 'llm';
    } else {
        // Never use a keyword or heuristic classifier after an LLM failure.
        // Preserve a valid crawler location for fallback before using company HQ.
        row.remote_type = 'unknown';
        row._classification_source = 'failed';
        row.location = normalizeLocation(row.location);
    }

    const resolvedLocation = await resolveJobLocation(row, { companyHq });
    row.location = resolvedLocation.location;
    row.location_lat = resolvedLocation.location_lat;
    row.location_lng = resolvedLocation.location_lng;
    row._location_source = classification.ok && classification.data.job_location
        ? 'llm'
        : resolvedLocation.source;

    if (!classification.ok || !classification.data.job_location) {
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

    if (classificationSource !== 'llm' && existingRemoteIsValid) {
        cleanRow.remote_type = existingRemote;
    }

    if (locationSource !== 'llm' && existingLocationIsAuthoritative) {
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
    preserveAuthoritativeFieldsForUpsert,
    resolveJobLocation,
    findCompanyHqLocation,
    LOCATION_UNKNOWN
};
