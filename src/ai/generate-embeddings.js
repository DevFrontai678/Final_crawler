// src/ai/generate-embeddings.js
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
require('dotenv').config();

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

const VOYAGE_API_KEY = process.env.VOYAGE_API_KEY;
const VOYAGE_MODEL = 'voyage-3';

async function getEmbedding(text, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const response = await axios.post(
        'https://api.voyageai.com/v1/embeddings',
        { model: VOYAGE_MODEL, input: [text] },
        {
          headers: {
            Authorization: `Bearer ${VOYAGE_API_KEY}`,
            'Content-Type': 'application/json',
          },
        }
      );
      return response.data.data[0].embedding;
    } catch (err) {
      if (err.response?.status === 429 && attempt < retries) {
        const wait = attempt * 5000;
        console.log(`  Rate limited. Waiting ${wait/1000}s before retry ${attempt}/${retries}...`);
        await new Promise(r => setTimeout(r, wait));
      } else {
        throw err;
      }
    }
  }
}

function jobToEmbeddingText(job) {
  const skills = Array.isArray(job.structured_skills)
    ? job.structured_skills.join(', ')
    : (job.structured_skills || '');
  const level = job.seniority_level || '';
  const remote = job.remote_type || '';
  const location = job.location || '';

  return `${job.title}. Skills: ${skills}. Level: ${level}. Remote: ${remote}. Location: ${location}`;
}

async function generateJobEmbeddings() {
  console.log('Fetching jobs without embeddings...');

  const { data: jobs, error } = await supabase
    .from('jobs')
    .select('id, title, structured_skills, seniority_level, remote_type, location')
    .is('skill_embedding', null);

  if (error) throw error;

  console.log(`${jobs.length} jobs need embeddings\n`);

  let success = 0;
  let failed = 0;

  for (const job of jobs) {
    try {
      const text = jobToEmbeddingText(job);
      console.log(`Processing: ${job.title}`);
      console.log(`  Text: ${text.substring(0, 100)}...`);

      const embedding = await getEmbedding(text);

      const { error: updateError } = await supabase
        .from('jobs')
        .update({ skill_embedding: embedding })
        .eq('id', job.id);

      if (updateError) throw updateError;

      console.log(`  Done (${embedding.length} dims)\n`);
      success++;

      await new Promise(r => setTimeout(r, 3000));

    } catch (err) {
      console.error(`  Failed: ${err.message}\n`);
      failed++;
    }
  }

  console.log('---');
  console.log(`Success : ${success}`);
  console.log(`Failed  : ${failed}`);
  console.log(`Total   : ${jobs.length}`);
}

generateJobEmbeddings().catch(console.error);