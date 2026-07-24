/**
 * src/ai/job-structurer.js
 *
 * Hybrid rule‑based structurer:
 * - Uses a curated list (skills-curated.json) + ESCO skills (skills-esco.json)
 * - Falls back to noun‑phrase extraction with skill indicators
 * - Caches results by description hash
 * - No API calls – 100% free
 */

'use strict';

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

// ─── LOAD DICTIONARIES ──────────────────────────────────────────────────
const SKILL_DICT = new Map();

function loadDictionary(fileName) {
    try {
        const filePath = path.join(__dirname, fileName);
        if (fs.existsSync(filePath)) {
            const raw = fs.readFileSync(filePath, 'utf8');
            const list = JSON.parse(raw);
            list.forEach(s => SKILL_DICT.set(s.toLowerCase().trim(), s));
            console.log(`✅ Loaded ${list.length} skills from ${fileName}`);
        }
    } catch (e) {
        console.warn(`⚠️ Could not load ${fileName}:`, e.message);
    }
}

// Load both curated and ESCO dictionaries
loadDictionary('skills-curated.json');
loadDictionary('skills-esco.json');

console.log(`📚 Total skills loaded: ${SKILL_DICT.size}`);

// ─── STOPWORDS ──────────────────────────────────────────────────────────
const STOPWORDS = new Set([
    'a', 'an', 'the', 'of', 'for', 'on', 'with', 'at', 'by', 'in', 'to',
    'from', 'into', 'through', 'during', 'including', 'without', 'per',
    'und', 'der', 'die', 'das', 'den', 'dem', 'des', 'ein', 'eine', 'einen',
    'einer', 'eines', 'für', 'mit', 'auf', 'bei', 'zur', 'zum', 'durch',
    'und', 'oder', 'von', 'mit', 'als', 'wie', 'ist', 'sind', 'werden',
    'wurde', 'wird', 'haben', 'hat', 'hatte', 'sein', 'war', 'waren',
    'wir', 'sie', 'ich', 'du', 'er', 'es', 'nicht', 'kein', 'keine',
    'you', 'our', 'your', 'we', 'us', 'their', 'them', 'its', 'his', 'her',
    'sich', 'einem', 'einen', 'einer', 'eines', 'der', 'die', 'das',
    'karriere', 'jobs', 'career', 'careers', 'join', 'team', 'company',
    'employees', 'work', 'working', 'job', 'position', 'stellen', 'stelle',
    'bewerben', 'bewerbung', 'bewirb', 'apply', 'application', 'cv', 'resume',
    'linkedin', 'xing', 'recruiting', 'hire', 'hiring', 'recruitment',
]);

// ─── SKILL INDICATORS (for fallback extraction) ────────────────────────
const SKILL_INDICATORS = [
    'management', 'engineer', 'developer', 'analyst', 'consultant', 'specialist',
    'expert', 'coordinator', 'supervisor', 'director', 'manager', 'leader',
    'technician', 'operator', 'driver', 'care', 'nurse', 'doctor', 'teacher',
    'instructor', 'trainer', 'sales', 'marketing', 'finance', 'accounting',
    'logistics', 'warehouse', 'production', 'quality', 'maintenance',
    'repair', 'installation', 'programming', 'design', 'testing',
    'planning', 'administration', 'supervision', 'coaching', 'mentoring',
    'research', 'communication', 'negotiation', 'leadership', 'problem solving',
];

// ─── PATTERNS ──────────────────────────────────────────────────────────
const PATTERNS = {
    seniority: [
        { level: 'junior', keywords: ['junior', 'entry', 'einstieg', 'trainee', 'praktikant', 'werkstudent', 'berufsanfänger'] },
        { level: 'mid', keywords: ['mid', 'professional', 'regular', 'erfahren', 'fachkraft'] },
        { level: 'senior', keywords: ['senior', 'sr.', 'experienced', 'lead', 'expert', 'spezialist'] },
        { level: 'lead', keywords: ['lead', 'team lead', 'principal', 'head of', 'bereichsleiter'] },
        { level: 'executive', keywords: ['director', 'vp', 'c-level', 'geschäftsführer', 'vorstand'] },
    ],
    remote: [
        { type: 'remote', keywords: ['remote', 'homeoffice', 'von zuhause', '100% remote', 'full remote'] },
        { type: 'hybrid', keywords: ['hybrid', 'teilweise remote', 'mobile work', 'flexible'] },
        { type: 'onsite', keywords: ['onsite', 'vor ort', 'präsenz'] },
    ],
    employment: [
        { type: 'fulltime', keywords: ['full-time', 'full time', 'vollzeit', 'unbefristet'] },
        { type: 'parttime', keywords: ['part-time', 'part time', 'teilzeit', 'minijob'] },
        { type: 'contract', keywords: ['contract', 'befristet', 'freelance', 'freiberuflich', 'projekt'] },
        { type: 'internship', keywords: ['internship', 'praktikum', 'werkstudent', 'ausbildung', 'duales studium'] },
    ],
    city: /(?:in|Standort:|Ort:|Location:)\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)/i,
};

// ─── CACHE ──────────────────────────────────────────────────────────────
const descriptionCache = new Map();
const CACHE_SIZE = 10000;

function getCachedResult(descHash) {
    return descriptionCache.get(descHash) || null;
}

function setCachedResult(descHash, result) {
    if (descriptionCache.size > CACHE_SIZE) {
        const firstKey = descriptionCache.keys().next().value;
        descriptionCache.delete(firstKey);
    }
    descriptionCache.set(descHash, result);
}

// ─── EXTRACT SKILLS ────────────────────────────────────────────────────
function extractSkills(text) {
    const lower = text.toLowerCase();
    const found = new Set();

    // Tokenize
    const tokens = lower.split(/[\s,.;!?()"']+/).filter(t => t.length > 1 && !STOPWORDS.has(t));

    // 1. Dictionary match (single, bigram, trigram)
    for (const t of tokens) {
        if (SKILL_DICT.has(t)) found.add(t);
    }
    for (let i = 0; i < tokens.length - 1; i++) {
        const b = tokens[i] + ' ' + tokens[i+1];
        if (SKILL_DICT.has(b)) found.add(b);
        if (i < tokens.length - 2) {
            const t = tokens[i] + ' ' + tokens[i+1] + ' ' + tokens[i+2];
            if (SKILL_DICT.has(t)) found.add(t);
        }
    }

    // 2. Fallback: extract phrases with skill indicators
    if (found.size === 0) {
        for (let i = 0; i < tokens.length - 1; i++) {
            const w1 = tokens[i], w2 = tokens[i+1];
            if (!STOPWORDS.has(w1) && !STOPWORDS.has(w2) && w1.length > 2 && w2.length > 2 && !/\d/.test(w1+w2)) {
                const bigram = w1 + ' ' + w2;
                const hasIndicator = SKILL_INDICATORS.some(ind => w1.includes(ind) || w2.includes(ind) || bigram.includes(ind));
                if (hasIndicator) {
                    found.add(bigram);
                }
            }
            if (i < tokens.length - 2) {
                const w3 = tokens[i+2];
                if (!STOPWORDS.has(w3) && w3.length > 2) {
                    const trigram = w1 + ' ' + w2 + ' ' + w3;
                    const hasIndicator = SKILL_INDICATORS.some(ind => trigram.includes(ind));
                    if (hasIndicator && !/\d/.test(trigram)) {
                        found.add(trigram);
                    }
                }
            }
        }
    }

    // 3. Last resort: any 2‑word phrase that is not all digits/stopwords
    if (found.size === 0) {
        for (let i = 0; i < tokens.length - 1; i++) {
            const w1 = tokens[i], w2 = tokens[i+1];
            if (!STOPWORDS.has(w1) && !STOPWORDS.has(w2) && w1.length > 2 && w2.length > 2 && !/\d/.test(w1+w2)) {
                found.add(w1 + ' ' + w2);
            }
        }
    }

    return [...found].slice(0, 10);
}

// ─── HELPERS FOR OTHER FIELDS ────────────────────────────────────────
function matchPattern(text, patterns) {
    const lower = text.toLowerCase();
    for (const p of patterns) {
        if (p.keywords.some(kw => lower.includes(kw))) {
            return p.type || p.level;
        }
    }
    return null;
}

function extractCity(text) {
    const match = text.match(PATTERNS.city);
    if (match) {
        const city = match[1];
        if (city && city.length > 1 && !['der', 'die', 'das', 'den', 'dem', 'einer', 'eines', 'einen'].includes(city.toLowerCase())) {
            return city;
        }
    }
    return null;
}

// ─── MAIN STRUCTURE FUNCTION ──────────────────────────────────────────
async function structureJob(job) {
    const title = job.title || '';
    const description = job.raw_description || '';
    const fullText = `${title} ${description}`;
    const descHash = crypto.createHash('md5').update(description || '').digest('hex');

    const cached = getCachedResult(descHash);
    if (cached) return cached;

    const skills = extractSkills(fullText);
    const seniority = matchPattern(fullText, PATTERNS.seniority) || 'mid';
    const remote = matchPattern(fullText, PATTERNS.remote) || 'onsite';
    const employment = matchPattern(fullText, PATTERNS.employment) || 'fulltime';
    const city = extractCity(fullText) || null;

    const result = {
        skills,
        seniority_level: seniority,
        remote_type: remote,
        employment_type: employment,
        location_city: city,
        location_country: null,
        job_category: null,
    };

    if (description && description.length > 50) {
        setCachedResult(descHash, result);
    }

    return result;
}

module.exports = { structureJob };
