'use strict';

const axios = require('axios');
const { SocksProxyAgent } = require('socks-proxy-agent');

const DEFAULT_PROXY_URL = null;

let cachedProxyUrl = null;
let cachedAgent = null;

function getProxyUrl() {
  const raw = process.env.SOCKS_PROXY_URL || process.env.SOCKS5_PROXY_URL || DEFAULT_PROXY_URL;
  const proxyUrl = String(raw || '').trim();
  return proxyUrl ? proxyUrl : null;
}

function isProxyEnabled() {
  return Boolean(getProxyUrl());
}

function createProxyAgent(proxyUrl) {
  try {
    return new SocksProxyAgent(proxyUrl, { keepAlive: true });
  } catch (err) {
    console.error(`[PROXY] Failed to initialize SOCKS proxy "${proxyUrl}": ${err.message}`);
    throw err;
  }
}

function getProxyAgent() {
  const proxyUrl = getProxyUrl();
  if (!proxyUrl) return null;

  if (cachedAgent && cachedProxyUrl === proxyUrl) {
    return cachedAgent;
  }

  cachedProxyUrl = proxyUrl;
  cachedAgent = createProxyAgent(proxyUrl);
  return cachedAgent;
}

function applyProxyToAxiosConfig(config = {}) {
  const proxyAgent = getProxyAgent();
  if (!proxyAgent) return config;

  return {
    ...config,
    proxy: false,
    httpAgent: proxyAgent,
    httpsAgent: proxyAgent,
  };
}

function normalizeHeaders(headers = {}) {
  if (typeof Headers !== 'undefined' && headers instanceof Headers) {
    const out = {};
    headers.forEach((value, key) => {
      out[key.toLowerCase()] = String(value);
    });
    return out;
  }

  if (Array.isArray(headers)) {
    const out = {};
    for (const [key, value] of headers) {
      out[String(key).toLowerCase()] = String(value);
    }
    return out;
  }

  const out = {};
  for (const [key, value] of Object.entries(headers || {})) {
    out[String(key).toLowerCase()] = String(value);
  }
  return out;
}

function buildHeadersFacade(headers = {}) {
  const normalized = normalizeHeaders(headers);

  return {
    get(name) {
      return normalized[String(name).toLowerCase()] || null;
    },
    has(name) {
      return Object.prototype.hasOwnProperty.call(normalized, String(name).toLowerCase());
    },
    entries() {
      return Object.entries(normalized)[Symbol.iterator]();
    },
    forEach(callback) {
      for (const [key, value] of Object.entries(normalized)) {
        callback(value, key, this);
      }
    },
  };
}

function isProxyError(err) {
  const code = err?.code || '';
  const message = String(err?.message || '').toLowerCase();

  return (
    ['ECONNREFUSED', 'ETIMEDOUT', 'EHOSTUNREACH', 'ENETUNREACH', 'ECONNRESET', 'EAI_AGAIN'].includes(code) ||
    message.includes('socks') ||
    message.includes('proxy')
  );
}

function logProxyFailure(context, target, err) {
  if (!isProxyError(err)) return;

  console.error(
    `[PROXY] ${context} failed for ${target || 'unknown target'}: ` +
    `${err?.code || err?.name || 'Error'}: ${err?.message || String(err)}`
  );
}

async function proxyFetch(input, init = {}) {
  const proxyUrl = getProxyUrl();
  const targetUrl =
    typeof input === 'string'
      ? input
      : input?.url || String(input || '');

  const method = String(init.method || input?.method || 'GET').toUpperCase();
  const headers = normalizeHeaders(init.headers || input?.headers || {});
  const body = init.body !== undefined ? init.body : input?.body;
  const timeout = init.timeout || 30000;

  const requestConfig = applyProxyToAxiosConfig({
    url: targetUrl,
    method,
    headers,
    data: body,
    timeout,
    maxRedirects: init.redirect === 'manual' ? 0 : (init.maxRedirects || 8),
    validateStatus: () => true,
    responseType: 'arraybuffer',
    decompress: true,
  });

  try {
    const response = await axios.request(requestConfig);
    const responseBody = Buffer.isBuffer(response.data)
      ? response.data
      : Buffer.from(response.data || []);

    let cachedText = null;
    const readText = async () => {
      if (cachedText === null) {
        cachedText = responseBody.toString('utf8');
      }
      return cachedText;
    };

    return {
      ok: response.status >= 200 && response.status < 300,
      status: response.status,
      statusText: response.statusText || '',
      url: response.request?.res?.responseUrl || response.config?.url || targetUrl,
      headers: buildHeadersFacade(response.headers || {}),
      text: readText,
      json: async () => JSON.parse(await readText()),
      arrayBuffer: async () =>
        responseBody.buffer.slice(
          responseBody.byteOffset,
          responseBody.byteOffset + responseBody.byteLength
        ),
      clone: () => ({
        ok: response.status >= 200 && response.status < 300,
        status: response.status,
        statusText: response.statusText || '',
        url: response.request?.res?.responseUrl || response.config?.url || targetUrl,
        headers: buildHeadersFacade(response.headers || {}),
        text: readText,
        json: async () => JSON.parse(await readText()),
        arrayBuffer: async () =>
          responseBody.buffer.slice(
            responseBody.byteOffset,
            responseBody.byteOffset + responseBody.byteLength
          ),
      }),
    };
  } catch (err) {
    if (proxyUrl) {
      logProxyFailure('HTTP request', targetUrl, err);
    }
    throw err;
  }
}

module.exports = {
  DEFAULT_PROXY_URL,
  getProxyUrl,
  isProxyEnabled,
  getProxyAgent,
  applyProxyToAxiosConfig,
  proxyFetch,
  isProxyError,
  logProxyFailure,
};
