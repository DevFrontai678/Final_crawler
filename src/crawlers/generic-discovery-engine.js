'use strict';

const crypto = require('crypto');

const TRACKING_PARAMS = new Set([
    'fbclid', 'gclid', 'dclid', 'msclkid', 'mc_cid', 'mc_eid',
    'utm_campaign', 'utm_content', 'utm_medium', 'utm_source', 'utm_term'
]);

function normalizeUrl(rawUrl, baseUrl) {
    if (!rawUrl) return null;
    try {
        const url = new URL(String(rawUrl), baseUrl || undefined);
        if (!/^https?:$/.test(url.protocol)) return null;
        [...url.searchParams.keys()].forEach(key => {
            if (TRACKING_PARAMS.has(key.toLowerCase())) url.searchParams.delete(key);
        });
        url.hash = '';
        return url.href;
    } catch {
        return null;
    }
}

function fingerprint(value) {
    return crypto.createHash('sha1').update(String(value || '')).digest('hex');
}

function text(value, max = 2000) {
    return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

function numericOrBoolean(value) {
    return value !== undefined && value !== null && value !== '' && value !== false;
}

function jobEvidence(candidate = {}) {
    const title = text(candidate.title || candidate.jobTitle || candidate.positionTitle, 240);
    const description = text(candidate.description || candidate.jobDescription || candidate.rawDescription, 12000);
    const location = text(candidate.location || candidate.jobLocation || candidate.locations, 500);
    const identifier = candidate.externalJobId || candidate.external_job_id || candidate.externalId ||
        candidate.jobId || candidate.job_id || candidate.requisitionId || candidate.requisition_id ||
        candidate.requisitionNumber || candidate.referenceId || candidate.reference_id || candidate.reference ||
        candidate.jobNumber || candidate.stableApiId || candidate.apiJobId || candidate.api_id;
    const detailUrl = candidate.detailUrl || candidate.jobUrl || candidate.url || candidate.link;
    const applicationUrl = candidate.applyUrl || candidate.applicationUrl || candidate.application_url;
    const apiSource = candidate.sourceType === 'api' || candidate.apiRecord === true;
    const metadata = Boolean(candidate.metadata || candidate.department || candidate.category || candidate.responsibilities ||
        candidate.requirements || candidate.salary || candidate.salaryRange || candidate.applicationDeadline ||
        candidate.deadline || candidate.jobType || candidate.job_type);
    const evidence = {
        title: Boolean(title),
        description: description.length >= 100,
        location: Boolean(location),
        identifier: numericOrBoolean(identifier),
        detailUrl: Boolean(detailUrl),
        applicationUrl: Boolean(applicationUrl),
        employment: numericOrBoolean(candidate.employmentType || candidate.employment_type),
        metadata,
        structuredData: Boolean(candidate.jsonLd || candidate.isJobPosting || candidate.schemaType === 'JobPosting')
    };
    // Description is necessary but not sufficient: marketing pages frequently have
    // long copy, a location, and a generic Apply link.  Require three independent
    // employment signals, including one signal that identifies the vacancy itself
    // or describes its employment terms.
    const strongSignals = [evidence.description, evidence.identifier, evidence.applicationUrl,
        evidence.employment, evidence.metadata, evidence.structuredData].filter(Boolean).length;
    const supportingSignals = [evidence.location, evidence.detailUrl, apiSource && evidence.identifier].filter(Boolean).length;
    const score = (evidence.title ? 3 : 0) + (evidence.description ? 4 : 0) +
        (evidence.identifier ? 3 : 0) + (evidence.applicationUrl ? 3 : 0) +
        (evidence.location ? 2 : 0) + (evidence.employment ? 2 : 0) +
        (evidence.metadata ? 2 : 0) + (evidence.structuredData ? 4 : 0);

    return {
        ...evidence,
        apiSource,
        score,
        strongSignals,
        supportingSignals,
        valid: evidence.title && evidence.description && strongSignals >= 2 &&
            (evidence.identifier || evidence.structuredData || evidence.employment ||
                (strongSignals >= 3 && evidence.metadata)) && supportingSignals >= 1
    };
}

function validateJobCandidate(candidate = {}) {
    const evidence = jobEvidence(candidate);
    const reasons = [];
    if (!evidence.title) reasons.push('missing_title');
    if (!evidence.description) reasons.push('insufficient_description');
    if (evidence.strongSignals < 2 ||
        (!evidence.identifier && !evidence.structuredData && !evidence.employment &&
            !(evidence.strongSignals >= 3 && evidence.metadata))) {
        reasons.push('insufficient_independent_employment_signals');
    }
    if (evidence.supportingSignals < 1) reasons.push('missing_detail_or_location_evidence');
    return { valid: reasons.length === 0, score: evidence.score, evidence, reasons };
}

function createDiscoveryState(rootUrl) {
    const state = {
        rootUrl: normalizeUrl(rootUrl) || rootUrl || null,
        visitedUrls: new Set(),
        queuedUrls: new Set(),
        visitedStates: new Set(),
        visitedApiRequests: new Set(),
        jobIdentities: new Set(),
        candidateIdentities: new Set(),
        interactionStates: new Set(),
        resultHashes: new Set(),
        pendingTasks: new Map(),
        metrics: {
            careerPagesDiscovered: 0,
            listingPagesDiscovered: 0,
            interactiveStatesExplored: 0,
            apiEndpointsDiscovered: 0,
            apiJobRecordsDiscovered: 0,
            iframeSourcesDiscovered: 0,
            paginationStatesExplored: 0,
            jobCandidatesDiscovered: 0,
            jobCandidatesAccepted: 0,
            jobCandidatesRejected: 0,
            duplicateCandidates: 0,
            jobDetailPagesFetched: 0,
            jobDetailFailures: 0,
            tasksRemaining: 0,
            discoveryExhausted: false,
            safetyLimitsReached: [],
            quietCycles: 0
        },
        normalize: (url, base) => normalizeUrl(url, base || state.rootUrl),
        pageState(url, domFingerprint, routeState = '') {
            const normalized = normalizeUrl(url, state.rootUrl) || url;
            const key = `${normalized}|${fingerprint(domFingerprint)}|${text(routeState, 500)}`;
            const fresh = !state.visitedStates.has(key);
            state.visitedStates.add(key);
            return { key, fresh };
        },
        apiState(method, url, body = '') {
            const normalized = normalizeUrl(url, state.rootUrl) || url;
            const key = `${String(method || 'GET').toUpperCase()} ${normalized} ${fingerprint(body)}`;
            const fresh = !state.visitedApiRequests.has(key);
            state.visitedApiRequests.add(key);
            if (fresh) state.metrics.apiEndpointsDiscovered++;
            return { key, fresh };
        },
        interactionState(pageUrl, elementFingerprint, routeState = '') {
            const key = `${normalizeUrl(pageUrl, state.rootUrl) || pageUrl}|${elementFingerprint}|${routeState}`;
            const fresh = !state.interactionStates.has(key);
            state.interactionStates.add(key);
            if (fresh) state.metrics.interactiveStatesExplored++;
            return { key, fresh };
        },
        recordResultSet(records) {
            const key = fingerprint((records || []).map(record => record.identity || record.url || record.title || record).sort().join('|'));
            const fresh = !state.resultHashes.has(key);
            state.resultHashes.add(key);
            if (fresh) state.metrics.paginationStatesExplored++;
            return { key, fresh };
        },
        recordCandidate(candidate, accepted = null) {
            const identity = candidate?.identity || candidate?.jobId || candidate?.requisitionId ||
                candidate?.detailUrl || candidate?.jobUrl || candidate?.url ||
                `${text(candidate?.title, 200)}|${text(candidate?.location, 200)}`;
            const key = fingerprint(String(identity).toLowerCase());
            state.metrics.jobCandidatesDiscovered++;
            if (state.candidateIdentities.has(key)) {
                state.metrics.duplicateCandidates++;
                return { key, fresh: false };
            }
            state.candidateIdentities.add(key);
            if (accepted === true) {
                state.jobIdentities.add(key);
                state.metrics.jobCandidatesAccepted++;
            } else if (accepted === false) {
                state.metrics.jobCandidatesRejected++;
            }
            return { key, fresh: true };
        },
        recordValidation(candidate, accepted) {
            const identity = candidate?.identity || candidate?.jobId || candidate?.requisitionId ||
                candidate?.detailUrl || candidate?.jobUrl || candidate?.url ||
                `${text(candidate?.title, 200)}|${text(candidate?.location, 200)}`;
            const key = fingerprint(String(identity).toLowerCase());
            if (accepted) {
                state.jobIdentities.add(key);
                state.metrics.jobCandidatesAccepted++;
            } else {
                state.metrics.jobCandidatesRejected++;
            }
            return { key };
        },
        updateRemaining() {
            state.metrics.tasksRemaining = state.pendingTasks.size;
            return state.metrics.tasksRemaining;
        },
        recordLimit(name, detail = null) {
            const value = detail ? `${name}:${detail}` : name;
            if (!state.metrics.safetyLimitsReached.includes(value)) state.metrics.safetyLimitsReached.push(value);
            state.metrics.discoveryExhausted = false;
            return value;
        },
        recordDiscoveryCycle({ meaningful = false } = {}) {
            state.metrics.quietCycles = meaningful ? 0 : state.metrics.quietCycles + 1;
            return state.metrics.quietCycles;
        },
        finish({ finalCycleComplete = false } = {}) {
            state.updateRemaining();
            state.metrics.discoveryExhausted = Boolean(
                finalCycleComplete &&
                state.pendingTasks.size === 0 &&
                state.metrics.quietCycles >= 1 &&
                state.metrics.safetyLimitsReached.length === 0
            );
            return state.metrics.discoveryExhausted;
        }
    };
    return state;
}

function semanticInteractionScore({ role = '', tagName = '', textContent = '', ariaLabel = '',
    title = '', href = '', nearbyRecordCount = 0, nearbyLinkCount = 0,
    hasFormControl = false, hasTarget = false } = {}) {
    const structural = [role, tagName, ariaLabel, title, href].join(' ').toLowerCase();
    let score = 0;
    if (['button', 'tab', 'menuitem', 'link'].some(value => structural.includes(value))) score += 1;
    if (hasFormControl) score += 2;
    if (nearbyRecordCount > 0) score += Math.min(3, nearbyRecordCount);
    if (nearbyLinkCount > 0) score += 1;
    if (hasTarget) score += 2;
    if (text(textContent, 300).length > 0) score += 1;
    if (href) score += 1;
    return score;
}

function extractContinuationUrls(payload, responseUrl) {
    const urls = new Set();
    const seen = new Set();
    const visit = (value, depth = 0) => {
        if (depth > 8 || value == null) return;
        if (typeof value === 'string') {
            const normalized = normalizeUrl(value, responseUrl);
            if (normalized) urls.add(normalized);
            return;
        }
        if (typeof value !== 'object' || seen.has(value)) return;
        seen.add(value);
        if (Array.isArray(value)) {
            value.forEach(item => visit(item, depth + 1));
            return;
        }
        for (const [key, child] of Object.entries(value)) {
            const normalizedKey = key.toLowerCase().replace(/[^a-z]/g, '');
            if (/(next|continuation|nextpage|nexturl|moreurl|paginationurl)/.test(normalizedKey)) {
                visit(child, depth + 1);
            } else if (child && typeof child === 'object') {
                visit(child, depth + 1);
            }
        }
    };
    visit(payload);
    return [...urls];
}

function extractContinuationRequests(payload, responseUrl, request = {}) {
    const requests = [];
    const requestKeys = new Set();
    const add = (url, method = request.method || 'GET', body = request.body || null) => {
        const normalized = normalizeUrl(url, responseUrl);
        const normalizedMethod = String(method).toUpperCase();
        const key = `${normalizedMethod} ${normalized || ''} ${body || ''}`;
        if (normalized && !requestKeys.has(key)) {
            requestKeys.add(key);
            requests.push({ url: normalized, method: normalizedMethod, body });
        }
    };
    const addCursorRequest = cursor => {
        if (!cursor || !request.url) return;
        const method = String(request.method || 'GET').toUpperCase();
        if (method === 'GET') {
            try {
                const url = new URL(request.url, responseUrl);
                const key = ['after', 'cursor', 'pageToken', 'continuationToken', 'offset', 'page']
                    .find(name => url.searchParams.has(name)) || 'cursor';
                url.searchParams.set(key, String(cursor));
                add(url.href, method, null);
            } catch {}
            return;
        }
        let body = request.body;
        try {
            const parsed = body ? JSON.parse(body) : {};
            const variables = parsed.variables && typeof parsed.variables === 'object' ? parsed.variables : parsed;
            const existingField = ['after', 'cursor', 'nextCursor', 'continuationToken', 'pageToken', 'offset', 'page']
                .find(field => field in variables);
            const graphqlField = parsed.query && /\$cursor\b/i.test(parsed.query) ? 'cursor' : 'after';
            variables[existingField || graphqlField] = cursor;
            if (!parsed.variables || typeof parsed.variables !== 'object') parsed.variables = variables;
            body = JSON.stringify(parsed);
        } catch { return; }
        add(request.url, method, body);
    };
    const walk = (value, depth = 0) => {
        if (depth > 8 || value == null || typeof value !== 'object') return;
        if (Array.isArray(value)) return value.forEach(item => walk(item, depth + 1));
        for (const [key, child] of Object.entries(value)) {
            const normalizedKey = key.toLowerCase().replace(/[^a-z]/g, '');
            if (typeof child === 'string' && /^(next|nexturl|continuationurl|moreurl|nextpageurl)$/.test(normalizedKey)) {
                add(child);
            } else if (typeof child === 'string' && /^(nextcursor|endcursor|cursor|nexttoken|continuationtoken|pagetoken)$/.test(normalizedKey)) {
                const hasNext = value.hasNextPage ?? value.hasMore ?? value.has_next ?? true;
                if (hasNext !== false) addCursorRequest(child);
            } else if (child && typeof child === 'object' && /^(next|pageinfo|continuation|pagination)$/.test(normalizedKey)) {
                const nextUrl = child.url || child.href || child.nextUrl;
                if (nextUrl) add(nextUrl);
                const cursor = child.endCursor || child.nextCursor || child.cursor || child.nextToken || child.continuationToken;
                const hasNext = child.hasNextPage ?? child.hasMore ?? child.has_next ?? true;
                if (cursor && hasNext !== false) addCursorRequest(cursor);
                walk(child, depth + 1);
            } else {
                walk(child, depth + 1);
            }
        }
    };
    walk(payload);
    return requests;
}

function discoveryCompleteness(state) {
    if (!state) return { exhausted: false, reason: 'missing_state' };
    const remaining = state.pendingTasks?.size || 0;
    if (remaining > 0) return { exhausted: false, reason: 'tasks_remaining', remaining };
    if (state.metrics?.safetyLimitsReached?.length) {
        return { exhausted: false, reason: 'safety_limit_reached', limits: state.metrics.safetyLimitsReached, remaining: 0 };
    }
    return { exhausted: Boolean(state.metrics?.discoveryExhausted), reason: state.metrics?.discoveryExhausted ? 'exhausted' : 'not_finalized', remaining: 0 };
}

module.exports = {
    createDiscoveryState,
    fingerprint,
    jobEvidence,
    validateJobCandidate,
    semanticInteractionScore,
    discoveryCompleteness,
    extractContinuationUrls,
    extractContinuationRequests,
    normalizeDiscoveryUrl: normalizeUrl
};
