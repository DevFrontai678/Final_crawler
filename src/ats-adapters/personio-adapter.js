const axios = require('axios');
const cheerio = require('cheerio');
require('dotenv').config();

// Personio career page se company slug/ID nikalo
async function extractPersonioSlug(careerPageUrl) {
  try {
    const response = await axios.get(careerPageUrl, {
      timeout: 15000,
      headers: { 'User-Agent': 'Mozilla/5.0' }
    });

    const html = response.data;
    const $ = cheerio.load(html);

    // Method 1: Direct personio subdomain
    // Pattern: companyname.jobs.personio.de
    if (careerPageUrl.includes('personio')) {
      const match = careerPageUrl.match(/https?:\/\/([^.]+)\.jobs\.personio/);
      if (match) {
        console.log(`    Slug from URL: ${match[1]}`);
        return { type: 'subdomain', slug: match[1] };
      }
    }

    // Method 2: Personio links dhundو page pe
    let personioSlug = null;
    $('a, iframe').each((_, el) => {
      const href = $(el).attr('href') || $(el).attr('src') || '';
      
      // Pattern: companyname.jobs.personio.de
      const subdomainMatch = href.match(/([^/]+)\.jobs\.personio\.de/);
      if (subdomainMatch) {
        personioSlug = subdomainMatch[1];
        console.log(`    Slug from link: ${personioSlug}`);
      }

      // Pattern: personio.de/job/COMPANY_ID
      const idMatch = href.match(/personio\.de\/job\/(\d+)/);
      if (idMatch) {
        console.log(`    Company ID from link: ${idMatch[1]}`);
      }
    });

    if (personioSlug) return { type: 'subdomain', slug: personioSlug };

    // Method 3: Script tags mein personio config
    let scriptSlug = null;
    $('script').each((_, el) => {
      const src = $(el).attr('src') || '';
      const content = $(el).html() || '';

      // personio widget script
      const srcMatch = src.match(/([^/]+)\.jobs\.personio/);
      if (srcMatch) scriptSlug = srcMatch[1];

      // content mein company slug
      const contentMatch = content.match(/personio['":\s]+['"]([a-z0-9-]+)['"]/i);
      if (contentMatch) scriptSlug = contentMatch[1];
    });

    if (scriptSlug) {
      console.log(`    Slug from script: ${scriptSlug}`);
      return { type: 'subdomain', slug: scriptSlug };
    }

    // Method 4: Network se widget URL check karo
    // HTML mein data attributes
    const dataSlug = $('[data-personio-url]').attr('data-personio-url') ||
                     $('[data-company]').attr('data-company');
    if (dataSlug) {
      console.log(`    Slug from data attr: ${dataSlug}`);
      return { type: 'subdomain', slug: dataSlug };
    }

    return null;

  } catch (err) {
    console.log(`    Error: ${err.message}`);
    return null;
  }
}

// Personio API se jobs fetch karo
async function fetchPersonioJobs(slug) {
  const results = [];

  // Method 1: Public JSON API
  try {
    const apiUrl = `https://${slug}.jobs.personio.de/xml`;
    console.log(`    Trying XML API: ${apiUrl}`);
    
    const response = await axios.get(apiUrl, {
      timeout: 15000,
      headers: { 'User-Agent': 'Mozilla/5.0' }
    });

    // XML parse karo
    const xml = response.data;
    const jobMatches = xml.matchAll(/<position[^>]*>([\s\S]*?)<\/position>/gi);
    
    for (const match of jobMatches) {
      const jobXml = match[1];
      const getId = (tag) => {
        const m = jobXml.match(new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, 'i'));
        return m ? m[1].replace(/<!\[CDATA\[|\]\]>/g, '').trim() : null;
      };

      results.push({
        external_job_id: getId('id') || String(Math.random()),
        title: getId('name') || getId('title') || 'Unknown',
        location: getId('office') || getId('location') || null,
        employment_type: getId('schedule') || null,
        raw_description: getId('jobDescriptions') || getId('description') || null,
        department: getId('department') || null,
        apply_url: `https://${slug}.jobs.personio.de/job/${getId('id')}`,
        ats_source: 'personio'
      });
    }

    if (results.length > 0) {
      console.log(`    ✅ ${results.length} jobs from XML API`);
      return results;
    }
  } catch (err) {
    console.log(`    XML API failed: ${err.message}`);
  }

  // Method 2: JSON API
  try {
    const jsonUrl = `https://${slug}.jobs.personio.de/api/v1/positions`;
    console.log(`    Trying JSON API: ${jsonUrl}`);

    const response = await axios.get(jsonUrl, {
      timeout: 15000,
      headers: {
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0'
      }
    });

    const jobs = response.data?.data || response.data || [];
    console.log(`    ✅ ${jobs.length} jobs from JSON API`);

    return jobs.map(job => ({
      external_job_id: String(job.id),
      title: job.attributes?.name || job.name || 'Unknown',
      location: job.attributes?.office?.attributes?.name || null,
      employment_type: job.attributes?.schedule || null,
      raw_description: job.attributes?.jobDescriptions?.[0]?.value || null,
      department: job.attributes?.department?.attributes?.name || null,
      apply_url: `https://${slug}.jobs.personio.de/job/${job.id}`,
      ats_source: 'personio'
    }));

  } catch (err) {
    console.log(`    JSON API failed: ${err.message}`);
  }

  // Method 3: HTML scraping fallback
  try {
    const pageUrl = `https://${slug}.jobs.personio.de`;
    console.log(`    Trying HTML scrape: ${pageUrl}`);

    const response = await axios.get(pageUrl, {
      timeout: 15000,
      headers: { 'User-Agent': 'Mozilla/5.0' }
    });

    const $ = cheerio.load(response.data);
    const jobs = [];

    // Job listings parse karo
    $('[data-position-id], .job-position, .position-item').each((_, el) => {
      const id = $(el).attr('data-position-id') || 
                 $(el).attr('data-id') || 
                 String(Math.random());
      const title = $(el).find('h2, h3, .title, [class*="title"]').first().text().trim();
      const location = $(el).find('[class*="location"], [class*="office"]').first().text().trim();

      if (title) {
        jobs.push({
          external_job_id: id,
          title,
          location: location || null,
          raw_description: null,
          apply_url: `https://${slug}.jobs.personio.de/job/${id}`,
          ats_source: 'personio'
        });
      }
    });

    console.log(`    ✅ ${jobs.length} jobs from HTML`);
    return jobs;

  } catch (err) {
    console.log(`    HTML scrape failed: ${err.message}`);
    return [];
  }
}

// Main function
async function processPersonioCompany(company) {
  console.log(`\n🔍 Processing: ${company.Name}`);
  console.log(`   Career URL: ${company.detected_career_url}`);

  const slugData = await extractPersonioSlug(company.detected_career_url);

  if (!slugData) {
    console.log(`   ❌ Could not find Personio slug`);
    return { company, jobs: [], error: 'No slug found' };
  }

  console.log(`   ✅ Slug: ${slugData.slug}`);
  const jobs = await fetchPersonioJobs(slugData.slug);
  console.log(`   📋 Total jobs: ${jobs.length}`);

  return { company, slug: slugData.slug, jobs, error: null };
}

module.exports = { processPersonioCompany, fetchPersonioJobs, extractPersonioSlug };