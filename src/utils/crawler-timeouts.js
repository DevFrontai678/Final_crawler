require('dotenv').config();

function parseTimeout(name, fallback) {
    const raw = process.env[name];
    const value = raw === undefined ? fallback : Number.parseInt(raw, 10);
    return Number.isFinite(value) && value > 0 ? value : fallback;
}

const CRAWLER_TIMEOUTS = Object.freeze({
    HTTP_TIMEOUT_MS: parseTimeout('CRAWLER_HTTP_TIMEOUT_MS', 15000),
    NAVIGATION_TIMEOUT_MS: parseTimeout('CRAWLER_NAVIGATION_TIMEOUT_MS', 60000),
    SELECTOR_TIMEOUT_MS: parseTimeout('CRAWLER_SELECTOR_TIMEOUT_MS', 10000),
    LOAD_STATE_TIMEOUT_MS: parseTimeout('CRAWLER_LOAD_STATE_TIMEOUT_MS', 12000),
    WAIT_TIMEOUT_MS: parseTimeout('CRAWLER_WAIT_TIMEOUT_MS', 2000),
    CLICK_TIMEOUT_MS: parseTimeout('CRAWLER_CLICK_TIMEOUT_MS', 2500),
    VISIBILITY_TIMEOUT_MS: parseTimeout('CRAWLER_VISIBILITY_TIMEOUT_MS', 1500),
    POPUP_TIMEOUT_MS: parseTimeout('CRAWLER_POPUP_TIMEOUT_MS', 1500),
    ATS_DETECTION_COMPANY_TIMEOUT_MS: parseTimeout('ATS_DETECTION_COMPANY_TIMEOUT_MS', 300000),
    CUSTOM_CRAWLER_COMPANY_TIMEOUT_MS: parseTimeout('CUSTOM_CRAWLER_COMPANY_TIMEOUT_MS', 900000),
    JOB_TIMEOUT_MS: parseTimeout('CRAWLER_JOB_TIMEOUT_MS', 120000),
    PAGE_CONTENT_TIMEOUT_MS: parseTimeout('CRAWLER_PAGE_CONTENT_TIMEOUT_MS', 60000),
});

module.exports = { CRAWLER_TIMEOUTS };
