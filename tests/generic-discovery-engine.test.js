const test = require('node:test');
const assert = require('node:assert/strict');

const engine = require('../src/crawlers/generic-discovery-engine');
const crawler = (() => {
    process.env.CRAWLER_TEST_MODE = '1';
    process.env.CRAWLER_NO_RUNTIME = '1';
    return require('../src/crawlers/custom-crawler-queue');
})();

test('generic fixture exposes a career route, real detail candidates, and pagination without domain rules', () => {
    const fs = require('fs');
    const html = fs.readFileSync(require.resolve('./fixtures/generic-career-portal.html'), 'utf8');
    const result = crawler.extractLinksFromHtml(
        html.replace(' hidden', ''),
        'https://example.test/careers',
        'https://example.test/careers'
    );

    assert.equal(result.jobs.has('https://example.test/opportunities/role-alpha'), true);
    assert.equal(result.jobs.has('https://example.test/opportunities/role-beta'), true);
    assert.equal(result.listings.has('https://example.test/opportunities?cursor=next-page'), true);
    assert.equal(result.jobs.has('https://example.test/opportunities'), false);
});

test('generic evidence accepts a detail record and rejects a navigation record', () => {
    const accepted = engine.validateJobCandidate({
        title: 'Role Alpha',
        rawDescription: 'A meaningful employment description '.repeat(8),
        location: 'Berlin',
        detailUrl: 'https://example.test/opportunities/role-alpha',
        applicationUrl: 'https://example.test/apply/role-alpha',
        employmentType: 'full-time',
        requisitionId: 'REQ-1'
    });
    const rejected = engine.validateJobCandidate({
        title: 'Explore opportunities',
        detailUrl: 'https://example.test/opportunities'
    });

    assert.equal(accepted.valid, true);
    assert.equal(rejected.valid, false);
    assert.ok(rejected.reasons.includes('insufficient_description'));
});

test('discovery state makes pagination and API states idempotent', () => {
    const state = engine.createDiscoveryState('https://example.test/careers');
    state.pendingTasks.set('https://example.test/opportunities?cursor=one', { type: 'page' });
    assert.equal(state.pageState('https://example.test/opportunities?cursor=one', 'same-dom').fresh, true);
    assert.equal(state.pageState('https://example.test/opportunities?cursor=one', 'same-dom').fresh, false);
    assert.equal(state.apiState('GET', 'https://example.test/api/opportunities', 'a=1').fresh, true);
    assert.equal(state.apiState('GET', 'https://example.test/api/opportunities', 'a=1').fresh, false);
    state.pendingTasks.clear();
    state.recordDiscoveryCycle({ meaningful: false });
    state.finish({ finalCycleComplete: true });
    assert.equal(engine.discoveryCompleteness(state).exhausted, true);
});

test('job identity deduplication does not merge distinct jobs with the same title', () => {
    const state = engine.createDiscoveryState('https://example.test/careers');
    state.recordCandidate({ identity: 'job-1', title: 'Engineer', location: 'Berlin' }, true);
    state.recordCandidate({ identity: 'job-2', title: 'Engineer', location: 'Berlin' }, true);
    state.recordCandidate({ identity: 'job-1', title: 'Engineer', location: 'Berlin' }, true);
    assert.equal(state.metrics.jobCandidatesAccepted, 2);
    assert.equal(state.metrics.duplicateCandidates, 1);
});

test('generic validation rejects long marketing copy with only a generic Apply link', () => {
    const result = engine.validateJobCandidate({
        title: 'Build your future with us',
        rawDescription: 'Join our team and learn more about our culture and opportunities '.repeat(20),
        location: 'Berlin',
        detailUrl: 'https://example.test/careers',
        applicationUrl: 'https://example.test/apply'
    });
    assert.equal(result.valid, false);
    assert.ok(result.reasons.includes('insufficient_independent_employment_signals'));
});

test('API continuation extraction supports cursor POST and GraphQL pageInfo state', () => {
    const requests = engine.extractContinuationRequests({
        data: { pageInfo: { hasNextPage: true, endCursor: 'cursor-2' } }
    }, 'https://example.test/graphql', {
        url: 'https://example.test/graphql',
        method: 'POST',
        body: JSON.stringify({ query: 'jobs', variables: { after: 'cursor-1' } })
    });
    assert.equal(requests.length, 1);
    assert.equal(requests[0].method, 'POST');
    assert.match(requests[0].body, /cursor-2/);
});

test('GraphQL continuation adds a missing pagination variable without changing the query', () => {
    const requests = engine.extractContinuationRequests({
        data: { pageInfo: { hasNextPage: true, endCursor: 'ABC' } }
    }, 'https://example.test/graphql', {
        url: 'https://example.test/graphql',
        method: 'POST',
        body: JSON.stringify({ query: 'query Jobs { jobs { pageInfo { endCursor } } }', variables: {} })
    });
    assert.equal(requests.length, 1);
    const body = JSON.parse(requests[0].body);
    assert.equal(body.variables.after, 'ABC');
    assert.match(body.query, /query Jobs/);
});

test('API identifiers are sufficient employment evidence without an HTML detail URL', () => {
    const job = engine.validateJobCandidate({
        jobId: 'API-42',
        title: 'Software Engineer',
        description: 'Design, implement, test, and maintain production software systems. '.repeat(8),
        location: 'Berlin'
    });
    assert.equal(job.valid, true);
    const requisitionJob = engine.validateJobCandidate({
        sourceType: 'api',
        requisitionId: 'REQ-9',
        title: 'Data Analyst',
        description: 'Analyze business data, build reports, and collaborate with stakeholders. '.repeat(8)
    });
    assert.equal(requisitionJob.valid, true);
});

test('interaction scoring rejects generic marketing buttons and favors listing controls', () => {
    assert.ok(engine.semanticInteractionScore({ tagName: 'BUTTON', textContent: 'Learn more' }) < 4);
    assert.ok(engine.semanticInteractionScore({
        tagName: 'BUTTON', textContent: 'Load more jobs', nearbyRecordCount: 1,
        nearbyLinkCount: 1, hasTarget: true
    }) >= 4);
});

test('safety limits prevent a false exhausted result', () => {
    const state = engine.createDiscoveryState('https://example.test/careers');
    state.recordLimit('scroll_budget', 100);
    state.finish({ finalCycleComplete: true });
    const completeness = engine.discoveryCompleteness(state);
    assert.equal(completeness.exhausted, false);
    assert.equal(completeness.reason, 'safety_limit_reached');
});

test('measured verification remains incomplete when it discovers new employment state', () => {
    const state = engine.createDiscoveryState('https://example.test/careers');
    state.recordDiscoveryCycle({ meaningful: true });
    state.finish({ finalCycleComplete: false });
    assert.equal(engine.discoveryCompleteness(state).exhausted, false);
});

test('measured quiet verification is required before exhaustion', () => {
    const state = engine.createDiscoveryState('https://example.test/careers');
    state.recordDiscoveryCycle({ meaningful: false });
    state.finish({ finalCycleComplete: true });
    assert.equal(engine.discoveryCompleteness(state).exhausted, true);
});
