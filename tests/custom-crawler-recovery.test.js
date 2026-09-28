const test = require('node:test');
const assert = require('node:assert/strict');

process.env.CRAWLER_TEST_MODE = '1';
process.env.CRAWLER_NO_RUNTIME = '1';

const crawler = require('../src/crawlers/custom-crawler-queue');

test('case 1: career_page_url is the fallback operational URL', () => {
    assert.equal(
        crawler.selectOperationalCareerUrl({ career_page_url: 'https://example.com/careers' }),
        'https://example.com/careers'
    );
});

test('case 2: detected_career_url remains preferred', () => {
    assert.equal(
        crawler.selectOperationalCareerUrl({
            career_page_url: 'https://example.com/careers',
            detected_career_url: 'https://example.com/jobs'
        }),
        'https://example.com/jobs'
    );
});

test('case 2b: neither career URL produces no operational URL', () => {
    assert.equal(crawler.selectOperationalCareerUrl({}), null);
});

test('case 3: a verified external career URL is trusted by provenance', () => {
    const company = {
        career_page_url: 'https://jobs.provider.example/company',
        detected_career_url: 'https://jobs.provider.example/company',
        career_page_status: 'ok'
    };
    assert.equal(crawler.hasVerifiedCareerDetection(company), true);
    assert.equal(crawler.isTrustedCareerEntryUrl(company.detected_career_url, company), true);
    assert.equal(
        crawler.isTrustedCareerRedirect(
            'https://jobs.provider.example/company/openings',
            company.detected_career_url
        ),
        true
    );
});

test('case 4: random unrelated external URLs remain blocked', () => {
    assert.equal(
        crawler.classifyLink(
            'https://third-party.example/contact',
            'Read more',
            'General information',
            'https://company.example/careers',
            { allowUnknownExternal: true }
        ),
        'ignore'
    );
});

test('case 5: unknown external careers URL is a bounded candidate', () => {
    const result = crawler.extractLinksFromHtml(
        '<a href="https://careers.provider.example/company/jobs">View careers</a>',
        'https://company.example/careers',
        'https://company.example/careers',
        { allowUnknownExternal: true }
    );
    assert.equal(result.listings.has('https://careers.provider.example/company/jobs'), true);
});

test('case 6: unknown external generic content is blocked', () => {
    const result = crawler.extractLinksFromHtml(
        '<a href="https://provider.example/contact">Contact provider</a>',
        'https://company.example/careers',
        'https://company.example/careers',
        { allowUnknownExternal: true }
    );
    assert.equal(result.pages.size, 0);
    assert.equal(result.listings.size, 0);
    assert.equal(result.jobs.size, 0);
});

test('case 6b: trusted external roots still reject generic same-host navigation', () => {
    const result = crawler.extractLinksFromHtml(
        '<a href="https://jobs.provider.example/company/contact">Contact</a>',
        'https://jobs.provider.example/company',
        'https://company.example/careers',
        {
            allowUnknownExternal: false,
            restrictedExternalHost: 'jobs.provider.example'
        }
    );
    assert.equal(result.pages.size, 0);
    assert.equal(result.listings.size, 0);
    assert.equal(result.jobs.size, 0);
});

test('case 7: career slug with strong URL evidence is a job', () => {
    assert.equal(
        crawler.isJobDetailUrl('https://example.com/karriere/it-systemadministrator-m-w-d'),
        true
    );
});

test('case 8: generic anchor text does not defeat strong job URL evidence', () => {
    assert.equal(
        crawler.classifyLink(
            'https://example.com/karriere/it-systemadministrator-m-w-d',
            'Mehr erfahren',
            'IT Systemadministrator (m/w/d) Vollzeit',
            'https://example.com/karriere'
        ),
        'job'
    );
});

test('case 9: a successfully parsed page with no job evidence yields no job links', () => {
    const result = crawler.extractLinksFromHtml(
        '<h1>Karriere</h1><p>Join our team.</p>',
        'https://example.com/karriere',
        'https://example.com/karriere'
    );
    assert.equal(result.jobs.size, 0);
});

test('case 10: technical discovery input is not converted into a job link', () => {
    const result = crawler.extractLinksFromHtml(
        '',
        'https://example.com/karriere',
        'https://example.com/karriere'
    );
    assert.equal(result.jobs.size, 0);
    assert.equal(result.listings.size, 0);
});

test('case 11: onclick navigation yields a bounded job URL', () => {
    const result = crawler.extractLinksFromHtml(
        `<div onclick="window.location='/karriere/software-engineer'">Software Engineer</div>`,
        'https://example.com/karriere',
        'https://example.com/karriere'
    );
    assert.equal(result.jobs.has('https://example.com/karriere/software-engineer'), true);
});

test('case 11b: window.open and relative onclick navigation are supported', () => {
    const result = crawler.extractLinksFromHtml(
        `<button onclick="window.open('/jobs/12345')">Open position</button>`,
        'https://example.com/careers',
        'https://example.com/careers'
    );
    assert.equal(result.jobs.has('https://example.com/jobs/12345'), true);
});

test('case 12: embedded nested JSON yields a job candidate', () => {
    const result = crawler.extractLinksFromHtml(
        `<script id="__NEXT_DATA__" type="application/json">${JSON.stringify({
            props: { pageProps: { data: { positions: [{ title: 'Software Engineer', id: '42', url: '/jobs/42' }] } } }
        })}</script>`,
        'https://example.com/careers',
        'https://example.com/careers'
    );
    assert.equal(result.jobs.has('https://example.com/jobs/42'), true);
    assert.equal(result.embeddedApiCandidates.length, 1);
});

test('case 13: analytics and generic embedded JSON do not become jobs', () => {
    const result = crawler.extractLinksFromHtml(
        '<script type="application/json">{"name":"Software Engineer","analytics":{"event":"view"}}</script>',
        'https://example.com/careers',
        'https://example.com/careers'
    );
    assert.equal(result.jobs.size, 0);
    assert.equal(result.embeddedApiCandidates.length, 0);
});

test('case 14: job card data URL is accepted while generic cards are ignored', () => {
    const result = crawler.extractLinksFromHtml(
        `<div class="job-card" data-url="/karriere/it-systemadministrator-m-w-d">
            <h3>IT Systemadministrator</h3><button>Mehr erfahren</button>
        </div>
        <div class="department-card" data-url="/services/it"><h3>Services</h3></div>`,
        'https://example.com/karriere',
        'https://example.com/karriere'
    );
    assert.equal(result.jobs.has('https://example.com/karriere/it-systemadministrator-m-w-d'), true);
    assert.equal(result.jobs.has('https://example.com/services/it'), false);
});

test('case 15: pagination candidates are bounded and listing-shaped', () => {
    const result = crawler.extractLinksFromHtml(
        '<a rel="next" href="/karriere?page=2">Next page</a><a href="/karriere/job-1">Job</a>',
        'https://example.com/karriere',
        'https://example.com/karriere'
    );
    assert.equal(result.listings.has('https://example.com/karriere?page=2'), true);
    assert.equal(result.jobs.has('https://example.com/karriere/job-1'), true);
});

test('case 16: malformed embedded JSON is ignored safely', () => {
    const result = crawler.extractLinksFromHtml(
        '<script id="__DATA__" type="application/json">{"jobs":[</script>',
        'https://example.com/careers',
        'https://example.com/careers'
    );
    assert.equal(result.jobs.size, 0);
    assert.equal(result.embeddedApiCandidates.length, 0);
});

test('case 17: oversized embedded JSON is ignored safely', () => {
    const result = crawler.extractLinksFromHtml(
        `<script id="__DATA__" type="application/json">${'x'.repeat(1024 * 1024 + 1)}</script>`,
        'https://example.com/careers',
        'https://example.com/careers'
    );
    assert.equal(result.jobs.size, 0);
    assert.equal(result.embeddedApiCandidates.length, 0);
});

test('case 18: trusted and ATS external discovery pages are queue-allowed, random domains are blocked', () => {
    const trustedUrl = 'https://careers.provider.example/company/jobs';
    const trustedHost = new Set(['careers.provider.example']);
    const trustedQueued = new Set([trustedUrl]);
    assert.equal(
        crawler.isAllowedQueuedDiscoveryUrl(trustedUrl, 'https://company.example/careers', trustedQueued, trustedHost),
        true
    );

    const atsUrl = 'https://jobs.lever.co/company/jobs';
    assert.equal(
        crawler.isAllowedQueuedDiscoveryUrl(atsUrl, 'https://company.example/careers', new Set([atsUrl]), new Set()),
        true
    );

    assert.equal(
        crawler.isAllowedQueuedDiscoveryUrl(
            'https://random.example/news',
            'https://company.example/careers',
            new Set(),
            new Set()
        ),
        false
    );
});

test('case 19: numeric pagination requires pagination context', () => {
    assert.equal(crawler.isPaginationControlEvidence({ text: '2', pageNumber: '2' }), true);
    assert.equal(crawler.isPaginationControlEvidence({ text: '2' }), false);
    assert.equal(crawler.isPaginationControlEvidence({ aria: 'Next page' }), true);
});

test('case 20: zero-link and technical job failures have distinct statuses', () => {
    assert.equal(crawler.getZeroLinkStatus({ stats: { failedListingPages: 0 } }), 'not_found');
    assert.equal(crawler.getZeroLinkStatus({ stats: { failedListingPages: 1 } }), 'failed');
    assert.equal(crawler.hasTechnicalJobFailure({ failedPages: 1 }, new Map()), true);
    assert.equal(crawler.hasTechnicalJobFailure({ failedPages: 0 }, new Map([['http_404', 1]])), true);
    assert.equal(crawler.hasTechnicalJobFailure({ failedPages: 0 }, new Map([['not_a_job', 1]])), false);
});
