'use strict';

const OpenAI = require('openai');

const MODEL = process.env.OPENAI_MODEL || 'gpt-4o-mini';
const VALID_REMOTE_TYPES = new Set(['remote', 'hybrid', 'onsite', 'unknown']);
const VALID_SENIORITY_LEVELS = new Set([
    'Entry Level', 'Junior', 'Mid Level', 'Senior', 'Lead', 'Manager', 'Director', 'Executive'
]);
const VALID_SUPPORT_LEVELS = new Set([
    '1st Level', '2nd Level', '3rd Level', 'Multi Level', 'Not Applicable'
]);
const VALID_EMPLOYMENT_TYPES = new Set([
    'Full Time', 'Part Time', 'Internship', 'Apprenticeship', 'Working Student',
    'Contract', 'Temporary', 'Freelance', 'Other', 'Unknown'
]);
const RETRY_DELAYS_MS = [0, 2000, 5000];

let client;

function getClient() {
    if (!client) {
        if (!process.env.OPENAI_API_KEY) {
            throw new Error('OPENAI_API_KEY is not configured');
        }
        client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
    }
    return client;
}

function nullableString(value) {
    if (value === null || value === undefined) return null;
    const normalized = String(value).trim();
    return normalized.length > 0 ? normalized : null;
}

function contextValue(value) {
    if (value === null || value === undefined || value === '') return 'none';
    if (typeof value === 'string') return value;
    try {
        return JSON.stringify(value);
    } catch {
        return String(value);
    }
}

function buildClassificationPrompt(job = {}) {
    const description = job.classification_description || job.raw_description || job.description || '';
    const evidence = job.location_evidence || job.crawler_location || null;
    return [
        'You are the authoritative classifier for a production job-matching system.',
        '',
        'Classify seniority, support level, employment type, employment arrangement, and the actual location where the employee is expected to work.',
        'The seniority, support level, and employment type decisions are authoritative. Analyze the complete job context, not isolated keywords.',
        '',
        'Company: ' + (job.company_name || job.company || 'unknown'),
        'Job title: ' + (job.title || 'unknown'),
        'Source URL: ' + (job.source_url || job.apply_url || job.url || 'unknown'),
        'Employment type evidence: ' + contextValue(job.employment_type || job.employmentType),
        'Work arrangement evidence: ' + (job.work_arrangement || job.workplace || 'none'),
        'Department and structured job fields: ' + contextValue(job.department || job.structured_fields),
        'Responsibilities: ' + contextValue(job.responsibilities),
        'Requirements: ' + contextValue(job.requirements),
        'Qualifications: ' + contextValue(job.qualifications || job.qualification),
        'Skills: ' + contextValue(job.skills),
        'Experience requirements: ' + contextValue(job.experience_requirements || job.experienceRequirements),
        'Employment metadata: ' + contextValue(job.employment_metadata),
        'Other job metadata: ' + contextValue(job.metadata || job.json_ld || job.jsonLd),
        'Crawler-supplied location evidence: ' + (evidence || 'none'),
        'Structured location evidence: ' + (job.structured_location || 'none'),
        'Company HQ evidence: ' + (job.company_hq || 'none'),
        'Job description:',
        description,
        '',
        'Rules:',
        '- remote_type MUST be exactly one of: remote, hybrid, onsite, unknown.',
        '- Use unknown when the employment arrangement cannot be determined confidently.',
        '- Never default to onsite, remote, or hybrid when the evidence is insufficient.',
        '- Fully remote, 100% remote, work from anywhere, and remote within Germany are remote.',
        '- Hybrid, 2 days remote, occasional remote work with office presence, and office plus remote are hybrid.',
        '- Remote technical terms such as remote monitoring, remote access, and remote customer support tools do not make employment remote.',
        '- Remote after probation must be classified from the complete employment arrangement.',
        '- If the employment arrangement is ambiguous or unsupported, return remote_type unknown.',
        '- seniority_level MUST be exactly one of: Entry Level, Junior, Mid Level, Senior, Lead, Manager, Director, Executive.',
        '- Determine seniority from the complete title, description, responsibilities, requirements, qualifications, skills, experience, scope, complexity, ownership, and leadership expectations.',
        '- Use an explicit level when present. Otherwise infer the strongest reasonable supported level; do not return a weaker level merely because the exact label is absent.',
        '- Do not invent seniority beyond what the complete posting supports.',
        '- support_level MUST be exactly one of: 1st Level, 2nd Level, 3rd Level, Multi Level, Not Applicable.',
        '- Infer support level from actual support responsibilities and technical scope. Use Multi Level only when the role explicitly or substantively covers multiple support levels. Use Not Applicable for a non-support role.',
        '- employment_type MUST be exactly one of: Full Time, Part Time, Internship, Apprenticeship, Working Student, Contract, Temporary, Freelance, Other, Unknown.',
        '- Determine employment_type from the complete posting. Use Unknown only when the available job content does not support a more specific value.',
        '- Determine job_location only when the employee actual work location is supported by the posting.',
        '- Prefer explicit employment-location statements such as Location, Based in, Office located in, or office days in a city.',
        '- For job_location, clean and normalize the supplied location evidence, then return ONLY the actual employee work location.',
        '- Remove unrelated metadata and any following field labels or values, including working hours, weekly hours, benefits, contact information, legal information, navigation, salary information, application instructions, and other metadata.',
        '- Preserve legitimate street addresses, cities, regions, and all legitimate multiple job locations; do not reduce multiple valid locations to one.',
        '- Example: "Im Schlahbruch 31, 59872 Meschede, DE; Meschede / Wochenarbeitszeit: 40 Stunden" becomes "Im Schlahbruch 31, 59872 Meschede, Germany".',
        '- If the actual location cannot be isolated confidently from the evidence and description, return job_location null.',
        '- Do not use company headquarters, customer cities, project destinations, travel destinations, or meeting locations as job_location unless the posting says the employee works there.',
        '- Remote position with occasional meetings in Munich has job_location null.',
        '- Based in Berlin, serving customers throughout Germany has job_location Berlin.',
        '- If the actual employment location cannot be established, job_location, location_city, and location_country must be null.',
        '- Never invent a location.',
        '',
        'Return ONLY valid JSON with this shape:',
        '{',
        '  "is_job": true,',
        '  "is_relevant": true,',
        '  "reason": null,',
        '  "relevance_reason": null,',
        '  "division": null,',
        '  "cleaned_title": null,',
        '  "skills": [],',
        '  "seniority_level": "Entry Level|Junior|Mid Level|Senior|Lead|Manager|Director|Executive",',
        '  "support_level": "1st Level|2nd Level|3rd Level|Multi Level|Not Applicable",',
        '  "employment_type": "Full Time|Part Time|Internship|Apprenticeship|Working Student|Contract|Temporary|Freelance|Other|Unknown",',
        '  "remote_type": "remote|hybrid|onsite|unknown",',
        '  "job_location": null,',
        '  "location_city": null,',
        '  "location_country": null',
        '}'
    ].join('\n');
}

function validateClassification(value) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
        throw new Error('LLM classification must be a JSON object');
    }
    if (!VALID_REMOTE_TYPES.has(value.remote_type)) {
        throw new Error('LLM returned an invalid remote_type');
    }
    if (!VALID_SENIORITY_LEVELS.has(value.seniority_level)) {
        throw new Error('LLM returned an invalid seniority_level');
    }
    if (!VALID_SUPPORT_LEVELS.has(value.support_level)) {
        throw new Error('LLM returned an invalid support_level');
    }
    if (!VALID_EMPLOYMENT_TYPES.has(value.employment_type)) {
        throw new Error('LLM returned an invalid employment_type');
    }
    const skills = Array.isArray(value.skills)
        ? value.skills.map(nullableString).filter(Boolean).slice(0, 20)
        : [];
    return {
        is_job: value.is_job !== false,
        is_relevant: value.is_relevant !== false,
        reason: nullableString(value.reason),
        relevance_reason: nullableString(value.relevance_reason),
        division: nullableString(value.division),
        cleaned_title: nullableString(value.cleaned_title),
        skills,
        seniority_level: value.seniority_level,
        support_level: value.support_level,
        employment_type: value.employment_type,
        remote_type: value.remote_type,
        job_location: nullableString(value.job_location),
        location_city: nullableString(value.location_city),
        location_country: nullableString(value.location_country)
    };
}

function sleep(ms) {
    return ms > 0 ? new Promise(resolve => setTimeout(resolve, ms)) : Promise.resolve();
}

async function classifyJobWithLLM(job = {}, { signal, clientOverride } = {}) {
    let lastError;
    for (let index = 0; index < RETRY_DELAYS_MS.length; index++) {
        const attempt = index + 1;
        try {
            const requestClient = clientOverride || getClient();
            await sleep(RETRY_DELAYS_MS[index]);
            const response = await requestClient.chat.completions.create({
                model: job.model || MODEL,
                messages: [
                    { role: 'system', content: 'Return only valid JSON. Never add explanatory text.' },
                    { role: 'user', content: buildClassificationPrompt(job) }
                ],
                temperature: 0,
                max_tokens: 900,
                response_format: { type: 'json_object' }
            }, signal ? { signal } : undefined);
            const content = response.choices?.[0]?.message?.content?.trim() || '';
            const result = validateClassification(JSON.parse(content));
            console.log('[LLM CLASSIFY] company=' + (job.company_name || job.company || '-') +
                ' external_job_id=' + (job.external_job_id || '-') +
                ' remote_type=' + result.remote_type +
                ' job_location=' + (result.job_location || 'null') +
                ' location_city=' + (result.location_city || 'null') +
                ' location_country=' + (result.location_country || 'null'));
            return { ok: true, data: result, attempts: attempt };
        } catch (error) {
            lastError = error;
            if (signal?.aborted) throw error;
        }
    }
    console.error('[LLM CLASSIFY FAILED] company=' + (job.company_name || job.company || '-') +
        ' external_job_id=' + (job.external_job_id || '-') +
        ' source_url=' + (job.source_url || job.apply_url || job.url || '-') +
        ' attempts=' + RETRY_DELAYS_MS.length +
        ' reason=' + (lastError?.message || 'unknown error'));
    return { ok: false, data: null, attempts: RETRY_DELAYS_MS.length, error: lastError };
}

module.exports = {
    MODEL,
    VALID_REMOTE_TYPES,
    VALID_SENIORITY_LEVELS,
    VALID_SUPPORT_LEVELS,
    VALID_EMPLOYMENT_TYPES,
    buildClassificationPrompt,
    classifyJobWithLLM,
    validateClassification
};
