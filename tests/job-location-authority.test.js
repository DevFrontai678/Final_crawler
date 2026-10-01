const assert = require('assert');
const axios = require('axios');
const {
    resolveAuthoritativeRemoteLocation,
    hasHybridEmploymentEvidence,
    hasExplicitFullRemoteEvidence,
    discoverImpressumUrls,
    extractLegalCompanyAddress
} = require('../src/utils/job-enrichment');

function expect(result, location, remoteType) {
    assert.strictEqual(result.location, location);
    assert.strictEqual(result.remote_type, remoteType);
}

async function run() {
    expect(resolveAuthoritativeRemoteLocation({ jobLocation: 'Berlin', jobText: 'Hybrid working available' }), 'Berlin', 'hybrid');
    expect(resolveAuthoritativeRemoteLocation({ jobLocation: 'Berlin', jobText: 'Remote possible' }), 'Berlin', 'onsite');
    expect(resolveAuthoritativeRemoteLocation({ jobLocation: 'Berlin', jobText: '100% remote' }), 'Berlin', 'onsite');
    expect(resolveAuthoritativeRemoteLocation({ jobLocation: 'Berlin', jobText: 'Remote / Hybrid' }), 'Berlin', 'hybrid');
    expect(resolveAuthoritativeRemoteLocation({ companyHq: 'Munich', jobText: 'Hybrid position' }), 'Munich', 'hybrid');
    expect(resolveAuthoritativeRemoteLocation({ companyHq: 'Munich', jobText: 'On-site role' }), 'Munich', 'onsite');
    expect(resolveAuthoritativeRemoteLocation({ jobText: '100% remote' }), 'Unknown', 'remote');
    expect(resolveAuthoritativeRemoteLocation({ jobText: 'Remote support systems' }), 'Unknown', 'onsite');
    expect(resolveAuthoritativeRemoteLocation({ jobText: 'No location information' }), 'Unknown', 'onsite');

    assert.strictEqual(hasHybridEmploymentEvidence('teilweise im Homeoffice'), true);
    assert.strictEqual(hasHybridEmploymentEvidence('remote server administration'), false);
    assert.strictEqual(hasExplicitFullRemoteEvidence('100% remote'), true);
    assert.strictEqual(hasExplicitFullRemoteEvidence('remote access and support'), false);

    const html = `
        <section class="impressum">
          <h1>Impressum</h1>
          <p>Unternehmenssitz: 80331 München</p>
          <p>Telefon: +49 89 123456</p>
        </section>`;
    assert.strictEqual(extractLegalCompanyAddress(html), '80331 München');

    const originalGet = axios.get;
    const requested = [];
    axios.get = async url => {
        requested.push(url);
        if (url.endsWith('/robots.txt')) return { data: 'Sitemap: https://example.test/sitemap-index.xml' };
        if (url.endsWith('/sitemap-index.xml')) return { data: '<sitemapindex><sitemap><loc>https://example.test/jobs.xml</loc></sitemap></sitemapindex>' };
        if (url.endsWith('/jobs.xml')) return { data: '<urlset><url><loc>https://example.test/de/impressum</loc></url></urlset>' };
        return { data: html };
    };
    try {
        const urls = await discoverImpressumUrls('https://example.test');
        assert.ok(urls.includes('https://example.test/de/impressum'));
        assert.ok(requested.some(url => url.endsWith('/robots.txt')));
        assert.ok(requested.some(url => url.endsWith('/sitemap-index.xml')));
    } finally {
        axios.get = originalGet;
    }

    console.log('job-location-authority tests passed');
}

run().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
