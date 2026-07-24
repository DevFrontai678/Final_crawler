/**
 * src/ai/gpt-structurer.js
 *
 * Strict GPT-4.1 Mini structurer.
 * Skips only obvious non‑jobs. Always infers skills from title.
 */

const OpenAI = require('openai');
const crypto = require('crypto');

// ─── CONFIG ──────────────────────────────────────────────────────────────
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_MODEL = 'gpt-4.1-mini';

if (!OPENAI_API_KEY) {
    console.error('❌ OPENAI_API_KEY is not set in .env');
    process.exit(1);
}

const client = new OpenAI({ apiKey: OPENAI_API_KEY });
console.log(`✅ OpenAI client initialized (model: ${OPENAI_MODEL}).`);

// ─── CACHE ──────────────────────────────────────────────────────────────
const descriptionCache = new Map();
const CACHE_SIZE = 5000;

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

// ─── 🔥 LESS AGGRESSIVE PRE‑FILTER ──────────────────────────────────────
function isObviousNonJob(title) {
    const lower = title.toLowerCase();
    const obvious = [
        '404', 'fehler', 'error', 'page not found',
        'empfohlen', 'recommended', 'produkte', 'products',
        'mögliche aufgaben', 'possible tasks',
        'datenschutz', 'impressum', 'cookie',
        'untitled', 'workday'
    ];
    for (const pattern of obvious) {
        if (lower.includes(pattern)) return true;
    }
    // Only skip if title is extremely short and generic
    if (title.length < 3) return true;
    return false;
}

// ─── PROMPT ──────────────────────────────────────────────────────────────
function buildPrompt(title, description) {
    return `
You are an expert HR data analyst. Extract structured information from this job posting.

**Job Title:** ${title || 'Not provided'}
**Job Description:**
${description || 'No description provided.'}

Return ONLY valid JSON:
{
  "cleaned_title": "standardised job title (e.g., Senior Backend Engineer)",
  "skills": ["skill1", "skill2", ...],
  "seniority_level": "junior|mid|senior|lead|executive",
  "employment_type": "fulltime|parttime|contract|internship",
  "remote_type": "remote|hybrid|onsite",
  "location_city": "city or null"
}

Rules:
- cleaned_title: remove location, company name, "m/w/d", fluff – just the role.
- skills: Extract real skills. If description is short, infer from title. ALWAYS include at least 3 skills.
- If seniority unclear → "mid". If remote unclear → "onsite". If employment unclear → "fulltime".
- Return ONLY JSON. No extra text.
`;
}

// ─── EXTRACT WITH GPT ──────────────────────────────────────────────────
async function extractWithGPT(title, description) {
    const prompt = buildPrompt(title, description);
    try {
        const response = await client.chat.completions.create({
            model: OPENAI_MODEL,
            messages: [
                { role: 'system', content: 'You are a precise job data extractor. Return only valid JSON.' },
                { role: 'user', content: prompt }
            ],
            temperature: 0,
            max_tokens: 500,
            response_format: { type: 'json_object' }
        });

        const content = response.choices[0]?.message?.content?.trim() || '';
        const parsed = JSON.parse(content);

        let skills = Array.isArray(parsed.skills) ? parsed.skills.slice(0, 15) : [];
        // Filter blacklist
        const blacklist = ['professional experience', 'general professional skills', 'team player', 'communication', 'problem solving', 'teamwork', 'collaboration', 'leadership', 'time management', 'flexibility', 'adaptability'];
        skills = skills.filter(s => {
            const lower = s.toLowerCase().trim();
            return lower.length > 1 && !blacklist.includes(lower);
        });

        // 🔥 Ensure at least 3 skills – infer from title if needed
        if (skills.length < 3) {
            const inferred = inferSkillsFromTitle(title);
            for (const skill of inferred) {
                if (!skills.includes(skill)) skills.push(skill);
                if (skills.length >= 5) break;
            }
        }

        return {
            skills,
            seniority_level: parsed.seniority_level || 'mid',
            remote_type: parsed.remote_type || 'onsite',
            employment_type: parsed.employment_type || 'fulltime',
            location_city: parsed.location_city || null,
            cleaned_title: parsed.cleaned_title || title || 'Untitled',
        };
    } catch (err) {
        console.warn(`⚠️ GPT extraction failed: ${err.message}`);
        return null;
    }
}

// ─── TITLE‑BASED SKILL INFERENCE ──────────────────────────────────────
function inferSkillsFromTitle(title) {
    const lower = title.toLowerCase();
    const map = {
        'entwickler|developer|programmierer|coder|engineer': ['Programming', 'Software Development', 'Git', 'Agile', 'Testing'],
        'backend': ['Python', 'Java', 'SQL', 'API Design', 'Microservices'],
        'frontend': ['JavaScript', 'React', 'CSS', 'HTML', 'UI/UX'],
        'devops|cloud|infrastructure': ['Cloud Computing', 'CI/CD', 'Docker', 'Kubernetes', 'Automation'],
        'data scientist|machine learning|ai': ['Python', 'Machine Learning', 'Statistics', 'Data Analysis', 'TensorFlow'],
        'data analyst|business intelligence': ['SQL', 'Excel', 'Data Visualization', 'Statistical Analysis', 'Tableau'],
        'project manager|product owner|scrum master': ['Project Management', 'Agile', 'Stakeholder Management', 'Risk Management', 'JIRA'],
        'sales|vertrieb|verkäufer': ['Sales', 'Negotiation', 'CRM', 'Business Development', 'Lead Generation'],
        'marketing': ['Digital Marketing', 'SEO/SEM', 'Content Strategy', 'Google Analytics', 'Social Media'],
        'accountant|buchhalter|controller': ['Accounting', 'Financial Reporting', 'Tax Compliance', 'Auditing', 'Excel'],
        'nurse|pfleger|kranken|gesundheit': ['Patient Care', 'Medical Documentation', 'Vital Signs', 'Medication Administration', 'Empathy'],
        'teacher|lehrer|dozent|professor': ['Teaching', 'Curriculum Development', 'Lesson Planning', 'Student Assessment', 'Communication'],
        'truck driver|fahrer|logistik': ['Driving', 'Logistics', 'Route Planning', 'Safety Compliance', 'Vehicle Maintenance'],
        'technician|techniker|mechaniker|elektriker': ['Technical Skills', 'Troubleshooting', 'Repair', 'Safety Protocols', 'Installation'],
        'management|manager|leitung|director': ['Leadership', 'Strategic Planning', 'Team Building', 'Decision Making', 'Communication'],
        'kaufmann|kauffrau|handel|commerce': ['Business Administration', 'Commercial Management', 'Logistics', 'Customer Service', 'Accounting'],
        'handwerker|craftsman|montage|installation': ['Technical Skills', 'Installation', 'Repair', 'Safety Protocols', 'Tool Usage'],
        'laboratory|labor|microbiologie': ['Laboratory Techniques', 'Microbiology', 'Sample Analysis', 'Quality Control', 'Documentation'],
        'ausbildung|apprenticeship|praktikum': ['Vocational Training', 'Learning', 'Technical Skills', 'Communication', 'Teamwork'],
        'it support|helpdesk|systemadmin': ['IT Support', 'System Administration', 'Networking', 'Troubleshooting', 'Windows/Linux'],
        'consultant|berater': ['Consulting', 'Analytical Thinking', 'Communication', 'Project Management', 'Industry Knowledge'],
        'projekt|project|agile|scrum': ['Project Management', 'Agile', 'Scrum', 'Stakeholder Management', 'JIRA'],
        'software': ['Programming', 'Software Development', 'Git', 'Agile', 'Testing'],
        'linux|unix': ['Linux', 'Shell Scripting', 'System Administration', 'Networking', 'Security'],
        'testing|qa|quality assurance': ['Testing', 'Quality Assurance', 'Automation', 'CI/CD', 'Bug Tracking'],
        'security|cyber': ['Network Security', 'Vulnerability Assessment', 'Firewalls', 'SIEM', 'Incident Response'],
    };
    for (const [pattern, skills] of Object.entries(map)) {
        if (new RegExp(pattern, 'i').test(lower)) {
            return skills.slice();
        }
    }
    return ['Professional Skills', 'Teamwork', 'Communication', 'Problem Solving', 'Time Management'];
}

// ─── MAIN STRUCTURE FUNCTION ──────────────────────────────────────────
async function structureJob(job) {
    const title = job.title || '';
    const description = job.raw_description || '';
    const descHash = crypto.createHash('md5').update(description || '').digest('hex');

    const cached = getCachedResult(descHash);
    if (cached) return cached;

    // Only skip obvious garbage
    if (isObviousNonJob(title)) {
        console.log(`⏭️ Skipping non‑job: "${title}"`);
        return {
            skills: [],
            seniority_level: 'mid',
            remote_type: 'onsite',
            employment_type: 'fulltime',
            location_city: null,
            cleaned_title: title,
        };
    }

    let result = await extractWithGPT(title, description);

    if (!result) {
        const skills = inferSkillsFromTitle(title);
        result = {
            skills,
            seniority_level: 'mid',
            remote_type: 'onsite',
            employment_type: 'fulltime',
            location_city: null,
            cleaned_title: title || 'Untitled',
        };
    }

    if (description && description.length > 50) {
        setCachedResult(descHash, result);
    }

    return result;
}

module.exports = { structureJob };
