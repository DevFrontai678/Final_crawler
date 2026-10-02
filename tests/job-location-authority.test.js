const assert = require('assert');
const axios = require('axios');
const {
    resolveAuthoritativeRemoteLocation,
    hasHybridEmploymentEvidence,
    hasExplicitFullRemoteEvidence,
    discoverImpressumUrls,
    extractLegalCompanyAddress,
    normalizeLocationCandidate,
    sanitizeLocationEvidence,
    selectAuthoritativeJobLocation
} = require('../src/utils/job-enrichment');

function expect(result, location, remoteType) {
    assert.strictEqual(result.location, location);
    assert.strictEqual(result.remote_type, remoteType);
}

async function run() {
    const meschedeEvidence = 'Im Schlahbruch 31, 59872 Meschede, DE; Meschede / Wochenarbeitszeit: 40 Stunden';
    assert.strictEqual(normalizeLocationCandidate(meschedeEvidence), null);
    assert.strictEqual(sanitizeLocationEvidence(meschedeEvidence), 'Im Schlahbruch 31, 59872 Meschede, Germany');
    for (const suffix of [
        'Arbeitszeit: 40 Stunden',
        'Benefits: Jobticket',
        'Contact: jobs@example.test',
        'Zusatzinformationen: Bitte bewerben',
        'Navigation: Home | Careers'
    ]) {
        assert.strictEqual(sanitizeLocationEvidence(`40210 Düsseldorf; ${suffix}`).includes(suffix), false, suffix);
    }
    assert.strictEqual(sanitizeLocationEvidence('40210 Düsseldorf\nContact: jobs@example.test'), '40210 Düsseldorf');
    assert.strictEqual(normalizeLocationCandidate('Düsseldorf'), 'Düsseldorf');
    assert.strictEqual(normalizeLocationCandidate('Niedersachsen'), 'Niedersachsen');
    assert.strictEqual(normalizeLocationCandidate('10115 Berlin'), '10115 Berlin');
    assert.strictEqual(normalizeLocationCandidate('Düsseldorf; Dortmund'), 'Düsseldorf; Dortmund');
    assert.deepStrictEqual(
        selectAuthoritativeJobLocation({ deterministicEvidence: meschedeEvidence, llmLocation: 'Meschede, Germany' }),
        { location: 'Meschede, Germany', source: 'llm' }
    );
    assert.deepStrictEqual(
        selectAuthoritativeJobLocation({ deterministicEvidence: meschedeEvidence, llmLocation: meschedeEvidence }),
        { location: 'Im Schlahbruch 31, 59872 Meschede, Germany', source: 'deterministic_fallback' }
    );
    assert.deepStrictEqual(
        selectAuthoritativeJobLocation({ deterministicEvidence: null, llmLocation: null }),
        { location: null, source: 'unavailable' }
    );

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
