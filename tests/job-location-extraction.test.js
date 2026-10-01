const assert = require('assert');
const crawler = require('../src/crawlers/custom-crawler-queue');
const { resolveAuthoritativeRemoteLocation } = require('../src/utils/job-enrichment');

function extract(html) {
    return crawler.extractRawJobFromHtml(`<main><h1>Example role</h1>${html}</main>`, 'https://example.test/job/example', 'Example');
}

function location(html, expected) {
    const result = extract(html);
    assert.strictEqual(result.location, expected);
    assert.ok(result.locationEvidence.values.length > 0);
    return result;
}

function run() {
    location('<div><span>Location</span><span>Berlin</span></div>', 'Berlin');
    location('<dl><dt>Arbeitsort</dt><dd>Hamburg</dd></dl>', 'Hamburg');
    location('<div><span>Standort</span><span>Niedersachsen</span></div>', 'Niedersachsen');
    location('<p>Einsatzort: Düsseldorf</p>', 'Düsseldorf');
    location('<dl><dt>Einsatzorte</dt><dd>Düsseldorf</dd><dd>Dortmund</dd></dl>', 'Düsseldorf; Dortmund');
    location('<table><tr><th>Dienstort</th><td>München</td></tr></table>', 'München');
    location('<div><span>Arbeitsplatz</span><span>Berlin</span></div>', 'Berlin');
    location('<div><span>Locations</span><span>Berlin</span><span>Hamburg</span></div>', 'Berlin; Hamburg');
    location('<div><span>Place of work</span><span>London</span></div>', 'London');
    location('<p>Arbeitsplatzort: Berlin</p>', 'Berlin');
    location('<p>Work Location: London</p>', 'London');
    location('<p>Die Stelle ist in Hamburg angesiedelt.</p>', 'Hamburg');
    location('<p>Position based in London.</p>', 'London');
    location('<address>Hauptstraße 5, 40213 Düsseldorf</address>', 'Hauptstraße 5, 40213 Düsseldorf');

    const jsonSingle = extract(`<script type="application/ld+json">${JSON.stringify({
        '@type': 'JobPosting',
        jobLocation: { '@type': 'Place', address: { '@type': 'PostalAddress', addressLocality: 'Berlin', postalCode: '10115' } }
    })}</script>`);
    assert.strictEqual(jsonSingle.location, '10115 Berlin');

    const jsonMultiple = extract(`<script type="application/ld+json">${JSON.stringify({
        '@graph': [{ '@type': 'JobPosting', jobLocation: [
            { address: { addressLocality: 'Düsseldorf' } },
            { address: { addressLocality: 'Dortmund' } }
        ] }]
    })}</script>`);
    assert.strictEqual(jsonMultiple.location, 'Düsseldorf; Dortmund');

    location(`<div itemprop="jobLocation" itemscope itemtype="https://schema.org/Place"><div itemprop="address" itemscope itemtype="https://schema.org/PostalAddress"><span itemprop="addressLocality">Berlin</span></div></div>`, 'Berlin');
    location('<div property="jobLocation">London</div>', 'London');
    location('<dl><dt>Location</dt><dd>Berlin</dd><dd>Hamburg</dd></dl>', 'Berlin; Hamburg');
    location('<table><tr><th>Location</th><td>Berlin</td></tr></table>', 'Berlin');
    location('<div class="job-meta"><span>Location</span><span>London</span></div>', 'London');

    const footerOnly = crawler.extractRawJobFromHtml('<main><h1>Role</h1><p>Meaningful job description '.repeat(30) + '</p><footer><address>Company Street 1, 24534 Neumünster</address></footer></main>', 'https://example.test/job/footer', 'Example');
    assert.strictEqual(footerOnly.location, null);

    assert.strictEqual(resolveAuthoritativeRemoteLocation({ jobLocation: 'Niedersachsen', jobText: 'Homeoffice' }).location, 'Niedersachsen');
    assert.strictEqual(resolveAuthoritativeRemoteLocation({ jobLocation: 'Niedersachsen', jobText: 'Homeoffice' }).remote_type, 'hybrid');
    assert.strictEqual(resolveAuthoritativeRemoteLocation({ jobLocation: 'Berlin', jobText: '100% remote' }).remote_type, 'onsite');
    assert.strictEqual(resolveAuthoritativeRemoteLocation({ jobLocation: 'Berlin', jobText: 'remote possible' }).remote_type, 'onsite');
    assert.strictEqual(resolveAuthoritativeRemoteLocation({ jobLocation: 'Berlin; Hamburg', jobText: 'remote / hybrid' }).remote_type, 'hybrid');
    assert.strictEqual(resolveAuthoritativeRemoteLocation({ companyHq: 'Munich', jobText: 'Hybrid role' }).location, 'Munich');
    assert.strictEqual(resolveAuthoritativeRemoteLocation({ companyHq: 'Munich', jobText: 'Hybrid role' }).remote_type, 'hybrid');
    assert.strictEqual(resolveAuthoritativeRemoteLocation({ companyHq: 'Munich', jobText: 'Office role' }).remote_type, 'onsite');
    assert.strictEqual(resolveAuthoritativeRemoteLocation({ jobText: '100% remote' }).remote_type, 'remote');
    assert.strictEqual(resolveAuthoritativeRemoteLocation({ jobText: 'No location information' }).remote_type, 'onsite');

    console.log('job-location-extraction tests passed');
}

try {
    run();
} catch (error) {
    console.error(error);
    process.exitCode = 1;
}
