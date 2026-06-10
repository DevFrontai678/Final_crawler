const Anthropic = require('@anthropic-ai/sdk');
require('dotenv').config();

const client = new Anthropic();

async function structureJob(job) {
  try {
    // Raw description nahi hai toh sirf title use karo
    const textToAnalyze = job.raw_description || job.title || '';
    
    if (!textToAnalyze || textToAnalyze.length < 10) {
      console.log(`    ⚠️ No description for: ${job.title}`);
      return null;
    }

    const response = await client.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 1000,
      messages: [{
        role: 'user',
        content: `You are an expert at analyzing job postings for IT/tech roles.

Analyze this job posting and extract structured information.

Job Title: ${job.title}
Location: ${job.location || 'Unknown'}
Description: ${textToAnalyze.substring(0, 3000)}

Return ONLY a valid JSON object with these exact fields:
{
  "skills": ["skill1", "skill2"],
  "seniority_level": "junior|mid|senior|lead|executive",
  "remote_type": "onsite|hybrid|remote",
  "employment_type": "fulltime|parttime|contract|internship",
  "location_city": "city name or null",
  "location_country": "DE|AT|CH or null",
  "job_category": "software|infrastructure|data|security|management|other"
}

Rules:
- skills: max 15 most important technical skills
- Use English skill names (e.g. "Docker" not "Docker-Container")
- seniority: guess from years of experience or job title
- Return ONLY JSON, no explanation`
      }]
    });

    const text = response.content[0].text.trim();
    
    // JSON parse karo
    const clean = text.replace(/```json|```/g, '').trim();
    const structured = JSON.parse(clean);
    
    return structured;

  } catch (err) {
    console.log(`    Claude error: ${err.message}`);
    return null;
  }
}

module.exports = { structureJob };