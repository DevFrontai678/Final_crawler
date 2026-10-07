'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

process.env.SCRAPERAPI_API_KEY = 'test-key';
process.env.SCRAPERAPI_TIMEOUT_MS = '30000';
delete process.env.SCRAPERAPI_COUNTRY_CODE;

const {
    SCRAPERAPI_CONFIG,
    buildRequestUrl
} = require('../src/utils/scraperapi-config');

test('ScraperAPI omits country_code when country targeting is not configured', () => {
    assert.equal(SCRAPERAPI_CONFIG.countryCode, null);

    const requestUrl = new URL(buildRequestUrl('https://example.test/careers', {
        renderJs: true
    }));

    assert.equal(requestUrl.searchParams.has('country_code'), false);
    assert.equal(requestUrl.searchParams.get('render'), 'true');
});

test('ScraperAPI preserves an explicitly configured country code', () => {
    const modulePath = require.resolve('../src/utils/scraperapi-config');
    const previous = process.env.SCRAPERAPI_COUNTRY_CODE;
    process.env.SCRAPERAPI_COUNTRY_CODE = 'fr';
    delete require.cache[modulePath];
    const configured = require('../src/utils/scraperapi-config');
    const requestUrl = new URL(configured.buildRequestUrl('https://example.test/careers', { renderJs: true }));

    assert.equal(requestUrl.searchParams.get('country_code'), 'fr');

    if (previous === undefined) delete process.env.SCRAPERAPI_COUNTRY_CODE;
    else process.env.SCRAPERAPI_COUNTRY_CODE = previous;
    delete require.cache[modulePath];
});
