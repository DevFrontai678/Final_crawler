const assert = require('assert');
const axios = require('axios');
const {
    extractLegalCompanyAddress,
    extractLegalCompanyAddressEvidence,
    discoverImpressumUrls,
    findCompanyHqLocation,
    resolveJobLocation
} = require('../src/utils/job-enrichment');

function includesAddress(html, expected) {
    const value = extractLegalCompanyAddress(html);
    assert.ok(value, `expected an address, got ${value}`);
    assert.ok(value.includes(expected), `${value} does not include ${expected}`);
    return value;
}

async function run() {
    includesAddress(`<script type="application/ld+json">${JSON.stringify({
        '@context': 'https://schema.org',
        '@type': 'Organization',
        address: { '@type': 'PostalAddress', streetAddress: 'Musterstraße 12', postalCode: '40210', addressLocality: 'Düsseldorf', addressCountry: 'DE' }
    })}</script>`, '40210 Düsseldorf');

    includesAddress(`<script type="application/ld+json">${JSON.stringify({
        '@graph': [{ '@type': 'Organization', address: { streetAddress: 'Hauptstraße 5', postalCode: '40213', addressLocality: 'Düsseldorf' } }]
    })}</script>`, '40213 Düsseldorf');

    includesAddress(`<div itemscope itemtype="https://schema.org/Organization"><div itemprop="address" itemscope itemtype="https://schema.org/PostalAddress"><span itemprop="streetAddress">Musterstraße 12</span><span itemprop="postalCode">40210</span><span itemprop="addressLocality">Düsseldorf</span></div></div>`, '40210 Düsseldorf');
    includesAddress('<address class="company-address">Musterstraße 12<br>40210 Düsseldorf</address>', '40210 Düsseldorf');
    includesAddress('<section class="impressum"><h1>Impressum</h1><p>Sitz: Musterstraße 12, 40210 Düsseldorf</p></section>', '40210 Düsseldorf');
    includesAddress('<section>Registered office: Main Street 4, 10115 Berlin, Germany</section>', '10115 Berlin');

    const invalid = [
        'Sitz: Supervisory Board Member: Peter Rommerskirchen Legal Form: Registered Cooperative',
        'Address: you are also welcome to register for our newsletter',
        'Register Court: Düsseldorf',
        'Legal Form: GmbH',
        'Supervisory Board: Peter Rommerskirchen',
        'Call us at +49 211 123456',
        'Email: office@example.test',
        'Home | Products | Careers | Contact'
    ];
    for (const text of invalid) assert.strictEqual(extractLegalCompanyAddress(`<div class="impressum">${text}</div>`), null, text);

    const clearHeadquarters = extractLegalCompanyAddress(`
        <section><h2>Registered office</h2><address>Musterstraße 12<br>40210 Düsseldorf</address></section>
        <section><h2>Branch office</h2><address>Branchweg 8<br>50667 Köln</address></section>`);
    assert.ok(clearHeadquarters.includes('40210 Düsseldorf'));

    const ambiguous = extractLegalCompanyAddress(`
        <address>Hauptstraße 1<br>10115 Berlin</address>
        <address>Parkweg 2<br>20095 Hamburg</address>`);
    assert.strictEqual(ambiguous, null);

    const evidence = extractLegalCompanyAddressEvidence('<address>Musterstraße 12<br>40210 Düsseldorf</address>');
    assert.strictEqual(evidence.source, 'address_element');

    const originalGet = axios.get;
    const requested = [];
    axios.get = async url => {
        requested.push(url);
        if (url === 'https://hq-links.example.test/') {
            return { data: '<a href="/de/impressum">Impressum</a>' };
        }
        if (url.endsWith('/robots.txt') || url.endsWith('/sitemap.xml') || url.endsWith('/sitemap_index.xml')) {
            return { data: '' };
        }
        if (url === 'https://hq-links.example.test/de/impressum') {
            return { data: '<address>Musterstraße 12<br>40210 Düsseldorf</address>' };
        }
        return { data: '<div class="impressum">Legal Form: GmbH</div>' };
    };
    try {
        const discovered = await discoverImpressumUrls('https://hq-links.example.test/');
        assert.ok(discovered.includes('https://hq-links.example.test/de/impressum'));
        assert.ok(requested.includes('https://hq-links.example.test/'));
        assert.ok((await findCompanyHqLocation('https://hq-links.example.test/')).includes('40210 Düsseldorf'));
    } finally {
        axios.get = originalGet;
    }

    const noHq = await resolveJobLocation({ location: null, location_lat: null, location_lng: null }, { companyHq: null });
    assert.strictEqual(noHq.location, 'Unknown');
    const validHq = await resolveJobLocation({ location: null, location_lat: 0, location_lng: 0 }, { companyHq: '40210 Düsseldorf' });
    assert.strictEqual(validHq.location, '40210 Düsseldorf');
    assert.strictEqual(validHq.source, 'company_hq');
    const jobLocation = await resolveJobLocation({ location: '10115 Berlin', raw_description: '100% remote', location_lat: 0, location_lng: 0 }, { companyHq: '40210 Düsseldorf' });
    assert.strictEqual(jobLocation.location, '10115 Berlin');
    assert.strictEqual(jobLocation.remote_type, 'onsite');

    console.log('company-hq-extraction tests passed');
}

run().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
