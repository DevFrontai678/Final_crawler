/**
 * HTTP Fetcher — Production‑grade with HTTP/HTTPS fallback & metadata capture
 * 
 * Features:
 *   - Try HTTPS first, fallback to HTTP (configurable)
 *   - Captures status, redirects, timing, content type, etc.
 *   - Handles redirects with configurable depth
 *   - Timeout protection
 *   - User‑agent rotation (optional)
 */

const axios = require('axios');
const { URL } = require('url');

/**
 * Normalize URL: add https:// if missing, try both protocols if needed
 */
function normalizeUrl(url, preferHttps = true) {
  if (!url) return null;
  let u = url.trim();
  if (!u.startsWith('http://') && !u.startsWith('https://')) {
    u = (preferHttps ? 'https://' : 'http://') + u;
  }
  return u;
}

/**
 * Fetch a URL with full metadata, trying HTTPS first then HTTP fallback
 */
async function fetchWithMetadata(url, options = {}) {
  const startTime = Date.now();
  const result = {
    url,
    html: null,
    status: null,
    statusText: null,
    redirects: [],
    finalUrl: url,
    responseTimeMs: 0,
    ttfbMs: null,
    contentType: null,
    contentLength: null,
    headers: {},
    error: null,
    protocolUsed: null,
  };

  const timeout = options.timeout || 30000;
  const maxRedirects = options.maxRedirects || 5;
  const userAgent = options.userAgent || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

  // List of protocols to try: HTTPS first, then HTTP
  const protocols = options.protocols || ['https', 'http'];

  for (const protocol of protocols) {
    let targetUrl = normalizeUrl(url, protocol === 'https');
    if (!targetUrl) continue;

    // Only change protocol if URL doesn't already have one
    const urlObj = new URL(targetUrl);
    if (urlObj.protocol !== `${protocol}:`) {
      // Replace protocol
      targetUrl = targetUrl.replace(/^https?:\/\//, `${protocol}://`);
    }

    try {
      const response = await axios.get(targetUrl, {
        timeout,
        maxRedirects,
        headers: {
          'User-Agent': userAgent,
          'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        },
        validateStatus: (status) => status < 500,
        beforeRedirect: (redirectOptions, responseDetails) => {
          result.redirects.push({
            from: redirectOptions.currentUrl,
            to: redirectOptions.newUrl,
            status: responseDetails.statusCode,
          });
        },
      });

      const elapsed = Date.now() - startTime;

      result.protocolUsed = protocol;
      result.status = response.status;
      result.statusText = response.statusText;
      result.finalUrl = response.config.url || response.request?.res?.responseUrl || targetUrl;
      result.responseTimeMs = elapsed;
      result.ttfbMs = response.headers['x-response-time'] ? parseFloat(response.headers['x-response-time']) : null;
      result.contentType = response.headers['content-type'] || null;
      result.contentLength = parseInt(response.headers['content-length'], 10) || null;
      result.headers = response.headers;
      result.html = typeof response.data === 'string' ? response.data : String(response.data);

      return result;

    } catch (error) {
      // If this was the last protocol, store the error
      if (protocol === protocols[protocols.length - 1]) {
        const elapsed = Date.now() - startTime;
        result.responseTimeMs = elapsed;
        result.error = error;
        result.protocolUsed = protocol;

        if (error.response) {
          result.status = error.response.status;
          result.statusText = error.response.statusText;
          result.html = error.response.data || null;
        } else if (error.request) {
          result.status = 0;
          result.statusText = 'No response (network error)';
        } else {
          result.status = 0;
          result.statusText = error.message || 'Request setup error';
        }
        return result;
      }
      // Otherwise, try the next protocol
      continue;
    }
  }

  return result;
}

/**
 * Build a readable redirect chain string
 */
function redirectChainToString(redirects) {
  if (!redirects || redirects.length === 0) return '';
  return redirects.map(r => `${r.from} → ${r.to} (${r.status})`).join(' → ');
}

module.exports = { fetchWithMetadata, redirectChainToString, normalizeUrl };
