const Anthropic = require('@anthropic-ai/sdk');
require('dotenv').config();

const client = new Anthropic();

// ─── Retry helper ──────────────────────────────────────────────────────────
async function withRetry(fn, retries = 3, delayMs = 2000) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      const isLast = attempt === retries;
      const isRateLimit = err.status === 429 || err.message?.includes('rate');

      if (isLast) throw err;

      const wait = isRateLimit ? delayMs * 4 : delayMs * attempt;
      console.warn(`    ⚠️  Attempt ${attempt} failed: ${err.message} — retrying in ${wait}ms`);
      await new Promise(r => setTimeout(r, wait));
    }
  }
}

// ─── JSON extractor — handles markdown fences, extra text, etc ─────────────
function extractJSON(text) {
  // Try direct parse first
  try { return JSON.parse(text.trim()); } catch {}

  // Strip markdown fences
  const fenced = text.match(/```(?:json)?\s*([\s\S]*?)```/);
  if (fenced) {
    try { return JSON.parse(fenced[1].trim()); } catch {}
  }

  // Find first { ... } block
  const start = text.indexOf('{');
  const end   = text.lastIndexOf('}');
  if (start !== -1 && end !== -1) {
    try { return JSON.parse(text.slice(start, end + 1)); } catch {}
  }

  throw new Error('No valid JSON found in response');
}

// ─── Normalizer — bad values ko clean karo ────────────────────────────────
function normalize(data) {
  const VALID_SENIORITY  = ['junior', 'mid', 'senior', 'lead', 'executive'];
  const VALID_REMOTE     = ['onsite', 'hybrid', 'remote'];
  const VALID_EMPLOYMENT = ['fulltime', 'parttime', 'contract', 'internship'];

  return {
    skills: Array.isArray(data.skills)
      ? data.skills.filter(s => typeof s === 'string' && s.length > 0).slice(0, 15)
      : [],

    seniority_level: VALID_SENIORITY.includes(data.seniority_level)
      ? data.seniority_level
      : null,

    remote_type: VALID_REMOTE.includes(data.remote_type)
      ? data.remote_type
      : null,

    employment_type: VALID_EMPLOYMENT.includes(data.employment_type)
      ? data.employment_type
      : null,

    // City: null/empty/"null"/"unknown"/"various" sab null kar do
    location_city: (
      data.location_city &&
      typeof data.location_city === 'string' &&
      !['null', 'unknown', 'various', 'n/a', 'remote', ''].includes(data.location_city.toLowerCase())
    ) ? data.location_city.trim() : null,

    location_country: data.location_country || null,
    job_category:     data.job_category     || null,
  };
}

// ─── Main function ─────────────────────────────────────────────────────────
async function structureJob(job) {
  try {
    const textToAnalyze = job.raw_description || job.title || '';

    if (!textToAnalyze || textToAnalyze.length < 5) {
      console.log(`    ⚠️  No content for: "${job.title}"`);
      return null;
    }

    // 5000 chars tak lo — zyada info = better results
    const description = textToAnalyze.substring(0, 5000);

    // Job mein already location hai? Claude ko bata do
    const existingLocation = job.location
      ? `Known location (already in DB): ${job.location}`
      : 'Location: Not provided in DB — extract from description if mentioned';

    const structured = await withRetry(async () => {
      const response = await client.messages.create({
        model:      'claude-sonnet-4-6',
        max_tokens: 1000,
        messages: [{
          role:    'user',
          content: `You are an expert HR data analyst. Analyze ANY job posting (tech, medical, education, trade, etc.) and extract structured data.

Job Title: ${job.title}
${existingLocation}

Job Description:
${description}

Return ONLY a valid JSON object — no explanation, no markdown:
{
  "skills": ["skill1", "skill2"],
  "seniority_level": "junior|mid|senior|lead|executive|null",
  "remote_type": "onsite|hybrid|remote",
  "employment_type": "fulltime|parttime|contract|internship",
  "location_city": "city name or null",
  "location_country": "2-letter country code or null",
  "job_category": "software|infrastructure|data|security|management|healthcare|education|engineering|finance|other"
}

STRICT RULES — follow exactly:
1. skills: Up to 15 key skills. Use English names. Include both hard skills (tools, languages, certifications) and domain skills.
2. seniority_level:
   - "junior"    → 0-2 years, Berufseinsteiger, Trainee, Werkstudent, Praktikant
   - "mid"       → 2-5 years, no specific level mentioned → DEFAULT to "mid"
   - "senior"    → 5+ years, "Senior", "Sr.", "erfahren", "Führungserfahrung"
   - "lead"      → Team Lead, Teamleiter, Head of, Principal
   - "executive" → Director, VP, C-level, Geschäftsführer
   - NEVER return null for seniority — always pick the closest match
3. remote_type:
   - "remote"  → fully remote, "100% remote", "von zuhause"
   - "hybrid"  → "hybrid", "teilweise remote", "flexibel", "Home-Office möglich"
   - "onsite"  → no remote mentioned, "vor Ort", "Präsenz" → DEFAULT to "onsite"
   - NEVER return null for remote_type
4. employment_type:
   - "fulltime"    → Vollzeit, full-time, unbefristet → DEFAULT if unclear
   - "parttime"    → Teilzeit, part-time, Minijob
   - "contract"    → Freelance, befristet, contract, projektbasiert
   - "internship"  → Praktikum, Werkstudent, Ausbildung, Ausbildungsvertrag
5. location_city: Extract city from description text if not already provided. Look for "in [City]", "Standort: [City]", "Arbeitsort", PLZ codes (German zip = city). If truly not found, return null.
6. NEVER return null for skills — if no technical skills, return domain/soft skills relevant to the role.`
        }]
      });

      return extractJSON(response.content[0].text);
    });

    return normalize(structured);

  } catch (err) {
    console.error(`    ❌  structureJob error for "${job.title}": ${err.message}`);
    return null;
  }
}

module.exports = { structureJob };
