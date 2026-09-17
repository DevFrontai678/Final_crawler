require('dotenv').config();

const axios = require('axios');
const crypto = require('crypto');
const { CRAWLER_TIMEOUTS } = require('./crawler-timeouts');

const geocodeCache = new Map();
const embeddingCache = new Map();
let lastGeoRequestAt = 0;

function asTrimmedString(value) {
    if (value === null || value === undefined) return '';
    return String(value).trim();
}

function normalizeLocation(location) {
    const value = asTrimmedString(location);
    return value.length > 0 ? value : null;
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

async function geocodeCity(location) {
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
            timeout: CRAWLER_TIMEOUTS.HTTP_TIMEOUT_MS
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

    row.location = normalizeLocation(row.location);
    row.raw_description = asTrimmedString(row.raw_description || row.description) || null;

    const needsGeo = row.location && (row.location_lat === null || row.location_lat === undefined || row.location_lng === null || row.location_lng === undefined);
    const needsEmbedding = !row.skill_embedding;

    const [geo, embedding] = await Promise.all([
        needsGeo ? geocodeCity(row.location) : Promise.resolve(null),
        needsEmbedding ? embedWithVoyage(buildEmbeddingText(row)) : Promise.resolve(null)
    ]);

    if (geo) {
        row.location_lat = geo.lat;
        row.location_lng = geo.lng;
    }

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
    geocodeCity
};
