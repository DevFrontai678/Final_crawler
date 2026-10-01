const test = require('node:test');
const assert = require('node:assert/strict');

process.env.CRAWLER_TEST_MODE = '1';
process.env.CRAWLER_NO_RUNTIME = '1';

const crawler = require('../src/crawlers/custom-crawler-queue');

const responseUrl = 'https://company.example/careers';

test('company timeout primitive rejects with a timeout error for partial handling', async () => {
    await assert.rejects(
        crawler.withTimeout(signal => new Promise((resolve, reject) => {
            signal.addEventListener('abort', () => reject(signal.reason));
        }), 5, 'test company timeout'),
        error => error.name === 'TimeoutError'
    );
});

test('API fixture 1: jobs array produces one normalized candidate', () => {
    const candidates = crawler.extractJobCandidatesFromApiPayload({
        jobs: [{ title: 'Software Engineer', id: '123', url: '/jobs/123' }]
    }, 'https://company.example/api/jobs', responseUrl);

    assert.equal(candidates.length, 1);
    assert.equal(candidates[0].title, 'Software Engineer');
    assert.equal(candidates[0].detailUrl, 'https://company.example/jobs/123');
});

test('API fixture 2: deeply nested positions are detected', () => {
    const candidates = crawler.extractJobCandidatesFromApiPayload({
        data: { results: { positions: [{ jobTitle: 'IT Administrator', jobId: '42', detailUrl: '/jobs/42' }] } }
    }, 'https://company.example/api/search', responseUrl);

    assert.equal(candidates.length, 1);
    assert.equal(candidates[0].jobId, '42');
});

test('API fixture 3: title plus location, description, and requisition is accepted', () => {
    const candidates = crawler.extractJobCandidatesFromApiPayload({
        title: 'Project Engineer',
        location: 'Berlin',
        description: 'Design and deliver construction projects.',
        requisitionId: 'REQ-7'
    }, 'https://company.example/api/jobs', responseUrl);

    assert.equal(candidates.length, 1);
    assert.equal(candidates[0].requisitionId, 'REQ-7');
});

test('API fixture 4: analytics JSON is ignored', () => {
    const candidates = crawler.extractJobCandidatesFromApiPayload({
        events: [{ event: 'page_view', name: 'career_page', id: '123' }]
    }, 'https://company.example/analytics/events', responseUrl);

    assert.equal(candidates.length, 0);
});

test('API fixture 5: CMS navigation JSON is ignored', () => {
    const candidates = crawler.extractJobCandidatesFromApiPayload({
        navigation: [{ name: 'Careers', url: '/careers' }],
        pages: [{ title: 'About us', slug: 'about-us' }]
    }, 'https://company.example/api/navigation', responseUrl);

    assert.equal(candidates.length, 0);
});

test('API fixture 6: name alone is insufficient job evidence', () => {
    const candidates = crawler.extractJobCandidatesFromApiPayload({
        name: 'Software Engineer'
    }, 'https://company.example/api/content', responseUrl);

    assert.equal(candidates.length, 0);
});

test('API fixture 7: duplicate records are emitted once', () => {
    const candidates = crawler.extractJobCandidatesFromApiPayload({
        jobs: [
            { title: 'Software Engineer', jobId: '123', url: '/jobs/123' },
            { title: 'Software Engineer', jobId: '123', url: '/jobs/123' }
        ]
    }, 'https://company.example/api/jobs', responseUrl);

    assert.equal(candidates.length, 1);
});

test('API fixture 8: relative URLs are normalized against the response URL', () => {
    const candidates = crawler.extractJobCandidatesFromApiPayload({
        results: [{ title: 'Developer', jobId: '9', detailUrl: '../jobs/9' }]
    }, 'https://company.example/api/v2/jobs', responseUrl);

    assert.equal(candidates[0].detailUrl, 'https://company.example/api/jobs/9');
});

test('API fixture 9: oversized response is ignored safely', () => {
    const body = Buffer.from(JSON.stringify({ jobs: [{ title: 'Developer', jobId: '1' }] }));
    assert.equal(crawler.parseJobApiResponseBody(body, body.length - 1), null);
});

test('API fixture 10: invalid JSON is ignored safely', () => {
    assert.equal(crawler.parseJobApiResponseBody('{not-json}'), null);
});

test('API fixture 11: valid API candidates are not truncated by job-count ceilings', () => {
    const jobs = Array.from({ length: 500 }, (_, index) => ({
        title: `Software Engineer ${index}`,
        jobId: String(index),
        url: `/jobs/${index}`
    }));
    const candidates = crawler.extractJobCandidatesFromApiPayload(
        { jobs },
        'https://company.example/api/jobs',
        responseUrl,
        { maxCandidates: 500 }
    );

    assert.equal(candidates.length, 500);
    assert.equal(crawler.extractJobCandidatesFromApiPayload({ jobs }, 'https://company.example/api/jobs', responseUrl).length, 500);
    const state = { jobsDetected: 0, identities: new Set() };
    const accepted = crawler.acceptApiCandidatesForCompany(candidates, state);
    assert.equal(accepted.length, 500);
});

test('API fixture 11d: wrapped response still obeys the response byte safety limit', () => {
    const oversized = `<jobs>${JSON.stringify({ job: { title: 'Developer', slug: 'developer', entry: 'Entry', activity: 'Activity' } })}</jobs>`;
    assert.equal(crawler.parseJobApiResponseBody(oversized, Buffer.byteLength(oversized) - 1), null);
});

test('API fixture 12: unrelated external API response remains metadata-eligible only when job-shaped', () => {
    assert.equal(crawler.isJobApiResponseMetadata({
        url: 'https://unrelated.example/api/jobs',
        status: 200,
        contentType: 'application/json',
        resourceType: 'fetch'
    }), true);

    const candidates = crawler.extractJobCandidatesFromApiPayload({
        jobs: [{ title: 'Developer', jobId: '1', url: 'https://unrelated.example/contact' }]
    }, 'https://unrelated.example/api/jobs', responseUrl);
    assert.equal(candidates.length, 1);
    assert.equal(crawler.classifyLink(candidates[0].detailUrl, candidates[0].title, 'job listing', responseUrl), 'ignore');
});

test('API fixture 13: trusted external career URL can be classified as a candidate', () => {
    const candidates = crawler.extractJobCandidatesFromApiPayload({
        jobs: [{ title: 'Developer', jobId: '1', detailUrl: '/company/jobs/1' }]
    }, 'https://careers.provider.example/api/jobs', responseUrl);

    assert.equal(candidates[0].detailUrl, 'https://careers.provider.example/company/jobs/1');
    assert.equal(
        crawler.classifyLink(
            candidates[0].detailUrl,
            candidates[0].title,
            'career job listing',
            responseUrl,
            { allowUnknownExternal: true }
        ),
        'job'
    );
});

test('response filter accepts JSON XHR/fetch and rejects binary or failed responses', () => {
    assert.equal(crawler.isJobApiResponseMetadata({
        url: 'https://company.example/api/jobs', status: 200,
        contentType: 'application/json', resourceType: 'xhr'
    }), true);
    assert.equal(crawler.isJobApiResponseMetadata({
        url: 'https://company.example/assets/app.js', status: 200,
        contentType: 'application/javascript', resourceType: 'script'
    }), false);
    assert.equal(crawler.isJobApiResponseMetadata({
        url: 'https://company.example/api/jobs', status: 500,
        contentType: 'application/json', resourceType: 'fetch'
    }), false);
});

test('response listener prefilter rejects irrelevant responses before task creation', () => {
    const response = (url, status, contentType, resourceType) => ({
        url: () => url,
        status: () => status,
        headers: () => ({ 'content-type': contentType }),
        request: () => ({ resourceType: () => resourceType })
    });

    assert.equal(crawler.isPotentialJobApiResponse(
        response('https://company.example/assets/logo.png', 200, 'image/png', 'image')
    ), false);
    assert.equal(crawler.isPotentialJobApiResponse(
        response('https://company.example/api/jobs', 200, 'application/json', 'fetch')
    ), true);
    assert.equal(crawler.isPotentialJobApiResponse(
        response('https://company.example/api/jobs', 500, 'application/json', 'xhr')
    ), false);
});
