'use strict';

// Compatibility entry point for the historical structuring worker. Keep all
// authoritative job classification in the shared LLM classifier so callers do
// not receive a deterministic seniority or employment fallback.
const { classifyJobWithLLM } = require('./job-classifier');

async function structureJob(job = {}, options = {}) {
    const result = await classifyJobWithLLM(job, options);
    return result.ok ? result.data : null;
}

module.exports = { structureJob };
