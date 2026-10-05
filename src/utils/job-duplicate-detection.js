'use strict';

const {
    classifyJobDuplicateWithLLM
} = require('../ai/job-classifier');

const MAX_CANDIDATES = 200;
const MAX_LLM_CANDIDATES = 5;
const DUPLICATE_CONFIDENCE_THRESHOLD = 0.9;

function normalizeText(value) {
    return String(value || '')
        .normalize('NFKC')
        .toLowerCase()
        .replace(/[^\p{L}\p{N}]+/gu, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function tokens(value) {
    return new Set(normalizeText(value).split(' ').filter(token => token.length > 2));
}

function tokenSimilarity(first, second) {
    const a = tokens(first);
    const b = tokens(second);
    if (!a.size || !b.size) return 0;
    let overlap = 0;
    for (const token of a) if (b.has(token)) overlap++;
    return overlap / Math.max(a.size, b.size);
}

function normalizeUrl(value) {
    if (!value) return null;
    try {
        const url = new URL(String(value));
        url.hash = '';
        for (const key of [...url.searchParams.keys()]) {
            if (/^(?:utm_|fbclid$|gclid$|msclkid$|mc_|yclid$|language$|lang$)/i.test(key)) {
                url.searchParams.delete(key);
            }
        }
        url.pathname = url.pathname.replace(/\/{2,}/g, '/');
        if (url.pathname.length > 1) url.pathname = url.pathname.replace(/\/+$/, '');
        return url.href;
    } catch {
        return null;
    }
}

function urlList(row = {}) {
    const metadata = row._dedupe_metadata || {};
    return [
        row.apply_url,
        row.source_url,
        metadata.canonical_url,
        ...(metadata.alternate_urls || [])
    ].map(normalizeUrl).filter(Boolean);
}

function languagePathKey(value) {
    const normalized = normalizeUrl(value);
    if (!normalized) return null;
    try {
        const url = new URL(normalized);
        const segments = url.pathname.split('/').filter(Boolean);
        const withoutLanguage = segments.filter(segment =>
            !/^[a-z]{2,3}(?:[-_][a-z]{2})?$/i.test(segment)
        );
        // The terminal vacancy slug is the useful cross-language signal. A
        // preceding segment is often translated (career/karriere), so it is
        // intentionally excluded from this blocking key.
        return [url.hostname.toLowerCase(), withoutLanguage.at(-1)].join('/');
    } catch {
        return null;
    }
}

function sameCompany(first, second) {
    return Boolean(first?.company_id && second?.company_id &&
        String(first.company_id) === String(second.company_id));
}

function locationsCompatible(first, second) {
    const a = normalizeText(first?.location);
    const b = normalizeText(second?.location);
    return !a || !b || a === b;
}

function relatedUrls(first, second) {
    const firstUrls = urlList(first);
    const secondUrls = urlList(second);
    if (!firstUrls.length || !secondUrls.length) return false;
    if (firstUrls.some(url => secondUrls.includes(url))) return true;
    const secondSet = new Set(secondUrls);
    const firstKeys = firstUrls.map(languagePathKey).filter(Boolean);
    return firstKeys.some(key => secondUrls.some(url => languagePathKey(url) === key) &&
        firstUrls.some(url => !secondSet.has(url)));
}

function deterministicDuplicate(first, second) {
    if (!sameCompany(first, second) || !locationsCompatible(first, second)) return false;
    const urlRelation = relatedUrls(first, second);
    if (!urlRelation) return false;

    // A URL relationship is never used alone. A compatible location is
    // required; an explicit alternate/canonical relationship is also allowed
    // to handle translated titles and descriptions.
    const firstMetadata = first._dedupe_metadata || {};
    const secondMetadata = second._dedupe_metadata || {};
    const explicitAlternate = (firstMetadata.alternate_urls || [])
        .map(normalizeUrl)
        .some(url => urlList(second).includes(url)) ||
        (secondMetadata.alternate_urls || [])
            .map(normalizeUrl)
            .some(url => urlList(first).includes(url));
    const titleMatch = tokenSimilarity(first.title, second.title) >= 0.5 ||
        normalizeText(first.title) === normalizeText(second.title);
    const descriptionMatch = tokenSimilarity(first.raw_description, second.raw_description) >= 0.35;
    return explicitAlternate || titleMatch || descriptionMatch;
}

function plausibleCandidate(first, second) {
    if (!sameCompany(first, second) || !locationsCompatible(first, second)) return false;
    if (relatedUrls(first, second)) return true;
    return tokenSimilarity(first.title, second.title) >= 0.35 ||
        tokenSimilarity(first.raw_description, second.raw_description) >= 0.3;
}

function duplicatePrompt(first, second) {
    return [
        'Determine whether these two job records represent the same underlying vacancy published in different languages or through different URLs.',
        'Use the complete records. Same title alone is insufficient. Different locations or different requisition/source IDs mean they are different jobs.',
        'Return ONLY valid JSON in exactly this shape:',
        '{"is_same_job":true,"confidence":0.0,"reason":"short explanation"}',
        '',
        'JOB A:', JSON.stringify(serializableJob(first)),
        '',
        'JOB B:', JSON.stringify(serializableJob(second))
    ].join('\n');
}

function serializableJob(row = {}) {
    const metadata = row._dedupe_metadata || {};
    return {
        title: row.title || null,
        description: row.raw_description || null,
        location: row.location || null,
        employment_type: row.employment_type || null,
        apply_url: row.apply_url || null,
        source_url: row.source_url || metadata.source_url || null,
        canonical_url: metadata.canonical_url || null,
        alternate_urls: metadata.alternate_urls || [],
        relevant_metadata: {
            ats_source: row.ats_source || null,
            department: row.department || null,
            requisition_id: metadata.requisition_id || null,
            source_job_id: metadata.source_job_id || null
        }
    };
}

async function findDuplicateJob(supabase, row, options = {}) {
    if (!row?.company_id || row._dedupe_metadata?.stable_provider_id) return null;

    let query = supabase
        .from('jobs')
        .select('id, company_id, external_job_id, external_hash, title, raw_description, location, employment_type, apply_url, ats_source, last_seen_at')
        .eq('company_id', row.company_id)
        .order('last_seen_at', { ascending: false })
        .limit(options.maxCandidates || MAX_CANDIDATES);
    const { data, error } = await query;
    if (error) throw new Error('Duplicate candidate lookup failed: ' + error.message);

    const candidates = (data || []).filter(candidate => plausibleCandidate(row, candidate));
    for (const candidate of candidates) {
        if (deterministicDuplicate(row, candidate)) return candidate;
    }

    for (const candidate of candidates.slice(0, options.maxLLMCandidates || MAX_LLM_CANDIDATES)) {
        const classifier = options.classifier || classifyJobDuplicateWithLLM;
        const result = await classifier(row, candidate, options);
        if (result?.ok && result.data.is_same_job === true &&
            result.data.confidence >= DUPLICATE_CONFIDENCE_THRESHOLD) {
            return candidate;
        }
    }
    return null;
}

async function resolveDuplicateIdentity(supabase, row, options = {}) {
    const duplicate = await findDuplicateJob(supabase, row, options);
    if (!duplicate) return row;
    return {
        ...row,
        external_job_id: duplicate.external_job_id,
        external_hash: row.external_hash ?? duplicate.external_hash,
        _duplicate_of: duplicate.id || duplicate.external_job_id
    };
}

module.exports = {
    MAX_CANDIDATES,
    MAX_LLM_CANDIDATES,
    DUPLICATE_CONFIDENCE_THRESHOLD,
    normalizeText,
    tokenSimilarity,
    languagePathKey,
    relatedUrls,
    deterministicDuplicate,
    plausibleCandidate,
    duplicatePrompt,
    serializableJob,
    findDuplicateJob,
    resolveDuplicateIdentity
};
