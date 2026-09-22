/**
 * ============================================================================
 * ScraperAPI Configuration Module
 * ============================================================================
 *
 * Single source of truth for everything related to ScraperAPI:
 *   - Environment variable validation
 *   - Centralized configuration (timeouts, retries, backoff, geo-targeting)
 *   - The low-level HTTP client used to fetch rendered HTML through ScraperAPI
 *
 * Every other module in the project should import `fetchWithScraperAPI` from
 * here instead of building its own request. This keeps the ScraperAPI
 * integration in exactly one place, so changing providers or tuning retry
 * behavior never requires touching crawler logic.
 *
 * Required environment variable (.env):
 *   SCRAPERAPI_API_KEY=your_api_key_here
 *
 * Optional environment variables (.env):
 *   SCRAPERAPI_COUNTRY_CODE=de        // geo-target requests (default: de)
 *   SCRAPERAPI_PREMIUM=false          // use premium residential proxies
 *   SCRAPERAPI_TIMEOUT_MS=30000       // shared per-request timeout (required)
 *   SCRAPERAPI_INITIAL_BACKOFF_MS=2000
 * ============================================================================
 */

require('dotenv').config();
const { proxyFetch } = require('./proxy');

// ---------------------------------------------------------------------------
// Environment validation — fail fast and loudly if misconfigured
// ---------------------------------------------------------------------------
const SCRAPERAPI_API_KEY = process.env.SCRAPERAPI_API_KEY;
const SCRAPERAPI_TIMEOUT_MS = process.env.SCRAPERAPI_TIMEOUT_MS;

if (!SCRAPERAPI_API_KEY || SCRAPERAPI_API_KEY.trim() === '') {
    throw new Error(
        'Missing required environment variable: SCRAPERAPI_API_KEY. ' +
        'ScraperAPI cannot be used without an API key. ' +
        'Add SCRAPERAPI_API_KEY=your_api_key_here to your .env file before starting the crawler.'
    );
}

if (!SCRAPERAPI_TIMEOUT_MS || String(SCRAPERAPI_TIMEOUT_MS).trim() === '') {
    throw new Error(
        'Missing required environment variable: SCRAPERAPI_TIMEOUT_MS. ' +
        'Set SCRAPERAPI_TIMEOUT_MS in your .env file so all ScraperAPI consumers share the same timeout.'
    );
}

// ---------------------------------------------------------------------------
// Centralized configuration object
// ---------------------------------------------------------------------------
const SCRAPERAPI_CONFIG = Object.freeze({
    apiKey: SCRAPERAPI_API_KEY,
    baseUrl: 'https://api.scraperapi.com',
    renderJs: true,
    countryCode: process.env.SCRAPERAPI_COUNTRY_CODE || 'de',
    premium: process.env.SCRAPERAPI_PREMIUM === 'true',
    requestTimeoutMs: parseInt(SCRAPERAPI_TIMEOUT_MS, 10),
    maxRetries: 1,
    initialBackoffMs: 0,
    backoffMultiplier: 2
});

/**
 * Sleep helper used between retry attempts (exponential backoff).
 * @param {number} ms
 */
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Build the full ScraperAPI request URL for a given target URL.
 * @param {string} targetUrl
 * @param {object} options
 * @returns {string}
 */
function buildRequestUrl(targetUrl, options) {
    const params = new URLSearchParams({
        api_key: SCRAPERAPI_CONFIG.apiKey,
        url: targetUrl,
        render: options.renderJs ? 'true' : 'false'
    });

    if (SCRAPERAPI_CONFIG.countryCode) {
        params.set('country_code', SCRAPERAPI_CONFIG.countryCode);
    }
    if (SCRAPERAPI_CONFIG.premium) {
        params.set('premium', 'true');
    }
    if (options.waitForSelector) {
        params.set('wait_for_selector', options.waitForSelector);
    }

    return `${SCRAPERAPI_CONFIG.baseUrl}/?${params.toString()}`;
}

/**
 * Fetch a URL through ScraperAPI with JS rendering, request timeout handling,
 * and exponential backoff retries.
 *
 * @param {string} targetUrl - The URL to fetch.
 * @param {object} [options]
 * @param {boolean} [options.renderJs] - Whether to render JavaScript (default: true).
 * @param {string} [options.waitForSelector] - CSS selector to wait for before returning HTML.
 * @returns {Promise<string>} The HTML content.
 * @throws {Error} If every retry attempt fails.
 */
async function fetchWithScraperAPI(targetUrl, options = {}) {
    if (options.signal?.aborted) {
        throw options.signal.reason || new Error('ScraperAPI request aborted');
    }
    const renderJs = options.renderJs !== undefined ? options.renderJs : SCRAPERAPI_CONFIG.renderJs;
    const requestUrl = buildRequestUrl(targetUrl, { ...options, renderJs });

    let lastError = null;
    let backoff = SCRAPERAPI_CONFIG.initialBackoffMs;

    for (let attempt = 1; attempt <= SCRAPERAPI_CONFIG.maxRetries; attempt++) {
        const startedAt = Date.now();
        const controller = new AbortController();
        const abortFromParent = () => controller.abort(options.signal?.reason);
        if (options.signal) {
            if (options.signal.aborted) abortFromParent();
            else options.signal.addEventListener('abort', abortFromParent, { once: true });
        }
        const timeoutId = setTimeout(() => controller.abort(), SCRAPERAPI_CONFIG.requestTimeoutMs);

        try {
            console.log(`   [ScraperAPI] Request ${attempt}/${SCRAPERAPI_CONFIG.maxRetries} started for ${targetUrl}`);
            const response = await proxyFetch(requestUrl, {
                timeout: SCRAPERAPI_CONFIG.requestTimeoutMs,
                signal: controller.signal
            });

            if (!response.ok) {
                throw new Error(`ScraperAPI responded with HTTP ${response.status} for ${targetUrl}`);
            }

            const html = await response.text();
            console.log(`   [ScraperAPI] Request ${attempt}/${SCRAPERAPI_CONFIG.maxRetries} succeeded for ${targetUrl} in ${Date.now() - startedAt}ms`);
            return html;
        } catch (error) {
            if (options.signal?.aborted) {
                throw options.signal.reason || error;
            }
            lastError = error.name === 'AbortError'
                ? new Error(`ScraperAPI request timed out after ${SCRAPERAPI_CONFIG.requestTimeoutMs}ms`)
                : error;

            const isLastAttempt = attempt === SCRAPERAPI_CONFIG.maxRetries;
            console.log(`   [ScraperAPI] Request ${attempt}/${SCRAPERAPI_CONFIG.maxRetries} failed for ${targetUrl} after ${Date.now() - startedAt}ms: ${lastError.message}`);

            if (!isLastAttempt) {
                await sleep(backoff);
                backoff *= SCRAPERAPI_CONFIG.backoffMultiplier;
            }
        } finally {
            clearTimeout(timeoutId);
            if (options.signal) options.signal.removeEventListener('abort', abortFromParent);
        }
    }

    throw new Error(`ScraperAPI failed after ${SCRAPERAPI_CONFIG.maxRetries} attempts for ${targetUrl}: ${lastError ? lastError.message : 'unknown error'}`);
}

module.exports = {
    SCRAPERAPI_CONFIG,
    fetchWithScraperAPI
};
