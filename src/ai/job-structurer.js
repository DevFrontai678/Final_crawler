/**
 * src/ai/job-structurer.js
 * 
 * Production-grade rule-based job structurer.
 * Extracts skills from job descriptions using a large, domain‑agnostic dictionary.
 * 
 * Performance: ~0.5ms per job.
 */

'use strict';

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

// ─── SKILL DICTIONARY ──────────────────────────────────────────────────────
const SKILL_DICT = new Map();

// 1. Base tech skills (fallback)
const BASE_SKILLS = [
    // Programming Languages
    'python', 'javascript', 'java', 'c++', 'c#', 'ruby', 'php', 'go', 'rust',
    'typescript', 'kotlin', 'swift', 'scala', 'perl', 'lua', 'r', 'matlab',
    'sql', 'nosql', 'graphql', 'rest api', 'soap', 'json', 'xml',
    // Frameworks & Libraries
    'react', 'angular', 'vue', 'svelte', 'next.js', 'nuxt', 'gatsby',
    'django', 'flask', 'spring', 'spring boot', 'hibernate', 'laravel',
    'express', 'node.js', 'asp.net', '.net core', 'rails', 'phoenix',
    // Cloud & DevOps
    'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'terraform', 'ansible',
    'puppet', 'chef', 'jenkins', 'gitlab ci', 'github actions', 'circleci',
    'linux', 'windows server', 'unix', 'bash', 'powershell', 'shell scripting',
    // Databases
    'mysql', 'postgresql', 'mongodb', 'redis', 'elasticsearch', 'cassandra',
    'oracle', 'sql server', 'firebase', 'dynamodb', 'cosmos db',
    // Data Science & AI
    'machine learning', 'deep learning', 'nlp', 'computer vision', 'llm',
    'tensorflow', 'pytorch', 'scikit-learn', 'pandas', 'numpy', 'spark',
    'hadoop', 'kafka', 'airflow', 'mlflow', 'kubeflow',
    // Security
    'cybersecurity', 'network security', 'application security', 'penetration testing',
    'siem', 'firewalls', 'vpn', 'zero trust', 'iam', 'pki',
    // Project Management & Methodologies
    'agile', 'scrum', 'kanban', 'waterfall', 'jira', 'confluence',
    'project management', 'program management', 'portfolio management',
    'risk management', 'change management', 'stakeholder management',
    // Business & Soft Skills
    'sales', 'marketing', 'business development', 'negotiation', 'communication',
    'leadership', 'team building', 'coaching', 'mentoring', 'decision making',
    // German-specific
    'projektmanagement', 'vertrieb', 'marketing', 'buchhaltung', 'controlling',
    'personalmanagement', 'einkauf', 'logistik', 'qualitätsmanagement',
    // Healthcare, Education, Engineering
    'healthcare', 'patient care', 'emr', 'education', 'teaching', 'engineering',
    'mechanical engineering', 'electrical engineering', 'civil engineering',
    // Add more as needed
];

// 2. Load custom dictionary from the generated JSON file
let CUSTOM_SKILLS = [];
try {
    const dictPath = path.join(__dirname, 'skills-dictionary.json');
    if (fs.existsSync(dictPath)) {
        const raw = fs.readFileSync(dictPath, 'utf8');
        const parsed = JSON.parse(raw);
        CUSTOM_SKILLS = Object.keys(parsed);
        console.log(`✅ Loaded ${CUSTOM_SKILLS.length} custom skills from dictionary.`);
    } else {
        console.warn('⚠️ skills-dictionary.json not found – using base skills only.');
    }
} catch (e) {
    console.warn('⚠️ Could not load custom skills-dictionary.json:', e.message);
}

// 3. Merge and populate SKILL_DICT (deduplicate)
const ALL_SKILLS = [...BASE_SKILLS, ...CUSTOM_SKILLS];
ALL_SKILLS.forEach(skill => {
    const key = skill.toLowerCase().trim();
    SKILL_DICT.set(key, key);
});

console.log(`📚 Total skills in dictionary: ${SKILL_DICT.size}`);

// ─── SYNONYMS ──────────────────────────────────────────────────────────────
const SYNONYMS = {
    'kubernetes': 'k8s',
    'javascript': 'js',
    'typescript': 'ts',
    'machine learning': 'ml',
    'deep learning': 'dl',
    'natural language processing': 'nlp',
    'cyber security': 'cybersecurity',
    'project management': 'project manager',
    'product management': 'product manager',
};

// ─── PATTERNS ──────────────────────────────────────────────────────────────
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

// ─── CACHE ──────────────────────────────────────────────────────────────────
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

// ─── EXTRACT SKILLS ────────────────────────────────────────────────────────
function extractSkills(text) {
    const lower = text.toLowerCase();
    const found = new Set();

    const tokens = lower.split(/[\s,.;!?()"']+/).filter(t => t.length > 1);
    
    for (const token of tokens) {
        if (SKILL_DICT.has(token)) {
            found.add(token);
        }
    }
    
    for (let i = 0; i < tokens.length - 1; i++) {
        const bigram = tokens[i] + ' ' + tokens[i+1];
        if (SKILL_DICT.has(bigram)) {
            found.add(bigram);
        }
        if (i < tokens.length - 2) {
            const trigram = tokens[i] + ' ' + tokens[i+1] + ' ' + tokens[i+2];
            if (SKILL_DICT.has(trigram)) {
                found.add(trigram);
            }
        }
    }

    const result = [];
    for (const skill of found) {
        result.push(SYNONYMS[skill] || skill);
    }
    return result;
}

// ─── MATCH PATTERNS ──────────────────────────────────────────────────────
function matchPattern(text, patterns) {
    const lower = text.toLowerCase();
    for (const p of patterns) {
        if (p.keywords.some(kw => lower.includes(kw))) {
            return p.type || p.level;
        }
    }
    return null;
}

// ─── EXTRACT CITY ────────────────────────────────────────────────────────
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

// ─── MAIN STRUCTURE FUNCTION ──────────────────────────────────────────────
async function structureJob(job) {
    const title = job.title || '';
    const description = job.raw_description || '';
    const fullText = `${title} ${description}`;
    
    const descHash = crypto.createHash('md5').update(description).digest('hex');
    
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

    if (description.length > 50) {
        setCachedResult(descHash, result);
    }

    return result;
}

module.exports = { structureJob };
