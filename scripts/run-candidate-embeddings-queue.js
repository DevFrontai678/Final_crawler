const { Queue, Worker } = require('bullmq');
const Redis = require('ioredis');
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');
const axios = require('axios');
require('dotenv').config();

// ─── CONFIG ──────────────────────────────────────────────────────────────────
const BATCH_SIZE = 10;
const CONCURRENCY = 3;
const MAX_RETRIES = 3;
const PAGE_SIZE = 1000;
const MAX_CANDIDATES = parseInt(process.argv.find(a => a.startsWith('--limit='))?.split('=')[1] || 0);
const RESUME_FROM = parseInt(process.argv.find(a => a.startsWith('--resume-from='))?.split('=')[1] || 0);
const DRY_RUN = process.argv.includes('--dry-run');

// ─── SUPABASE ──────────────────────────────────────────────────────────────
const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY,
    { realtime: { transport: ws } }
);

// ─── REDIS ──────────────────────────────────────────────────────────────────
const redisConnection = new Redis({
    host: 'localhost',
    port: 6379,
    maxRetriesPerRequest: null
});

const QUEUE_NAME = 'candidate-embedding-queue';
const candidateQueue = new Queue(QUEUE_NAME, { connection: redisConnection });

// ─── VOYAGE AI ──────────────────────────────────────────────────────────────
const VOYAGE_API_KEY = process.env.VOYAGE_API_KEY;
const VOYAGE_URL = 'https://api.voyageai.com/v1/embeddings';
const VOYAGE_MODEL = 'voyage-3-large';

// ─── CANDIDATE TO TEXT ──────────────────────────────────────────────────────
function candidateToText(candidate) {
    let skillsText = '';
    if (candidate.skill_scores && typeof candidate.skill_scores === 'object') {
        const entries = Object.entries(candidate.skill_scores);
        if (entries.length > 0) {
            skillsText = entries
                .map(([skill, score]) => `${skill} (${score}/5)`)
                .join(', ');
        }
    }

    const parts = [];
    if (candidate.name) parts.push(`Role: ${candidate.name}`);
    if (skillsText) parts.push(`Skills: ${skillsText}`);
    if (candidate.seniority_level) parts.push(`Seniority: ${candidate.seniority_level}`);
    if (candidate.remote_preference) parts.push(`Remote: ${candidate.remote_preference}`);
    if (candidate.location) parts.push(`Location: ${candidate.location}`);

    return parts.join('. ') || 'No data available';
}

// ─── FETCH CANDIDATES ──────────────────────────────────────────────────────
async function fetchCandidates() {
    let allCandidates = [];
    let page = RESUME_FROM;
    let hasMore = true;

    console.log(`📋 Fetching candidates without embeddings (page size: ${PAGE_SIZE})...`);
    if (RESUME_FROM > 0) console.log(`   Resuming from page ${RESUME_FROM}`);

    while (hasMore) {
        const offset = page * PAGE_SIZE;
        console.log(`   Fetching page ${page + 1} (offset: ${offset})...`);

        let query = supabase
            .from('candidates')
            .select('id, salesforce_contact_id, name, skill_scores, seniority_level, remote_preference, location')
            .is('skill_embedding', null)
            .order('created_at', { ascending: true })
            .range(offset, offset + PAGE_SIZE - 1);

        if (MAX_CANDIDATES > 0 && allCandidates.length >= MAX_CANDIDATES) {
            break;
        }

        const { data, error } = await query;
        if (error) throw new Error(`Supabase fetch error: ${error.message}`);

        if (!data || data.length === 0) {
            console.log(`   ✅ No more candidates.`);
            hasMore = false;
        } else {
            const valid = data.filter(c => 
                c.skill_scores && 
                typeof c.skill_scores === 'object' && 
                Object.keys(c.skill_scores).length > 0
            );
            allCandidates = allCandidates.concat(valid);
            console.log(`   ✅ Page ${page + 1}: ${valid.length} valid candidates (total: ${allCandidates.length})`);

            if (data.length < PAGE_SIZE) {
                hasMore = false;
            }
            page++;

            if (MAX_CANDIDATES > 0 && allCandidates.length >= MAX_CANDIDATES) {
                allCandidates = allCandidates.slice(0, MAX_CANDIDATES);
                console.log(`   ⏹️ Reached limit of ${MAX_CANDIDATES} candidates`);
                break;
            }
        }
    }

    console.log(`\n✅ Total candidates fetched: ${allCandidates.length}`);
    return allCandidates;
}

// ─── ADD CANDIDATES TO QUEUE ──────────────────────────────────────────────
async function addCandidatesToQueue() {
    const candidates = await fetchCandidates();
    if (candidates.length === 0) {
        console.log('✅ No candidates need embeddings.');
        return;
    }

    console.log(`📋 Adding ${candidates.length} candidates to queue...`);

    let added = 0;
    for (const candidate of candidates) {
        await candidateQueue.add('embed-candidate', {
            candidateId: candidate.id,
            candidateData: candidate
        }, {
            attempts: MAX_RETRIES,
            backoff: { type: 'exponential', delay: 5000 }
        });
        added++;
        if (added % 100 === 0) {
            console.log(`   ✅ Added ${added}/${candidates.length}`);
        }
    }

    console.log(`✅ Added ${added} candidates.`);
}

// ─── EMBED BATCH ────────────────────────────────────────────────────────────
async function embedBatch(texts) {
    if (!texts || texts.length === 0) return [];
    
    try {
        const response = await axios.post(VOYAGE_URL, {
            model: VOYAGE_MODEL,
            input: texts
        }, {
            headers: {
                'Authorization': `Bearer ${VOYAGE_API_KEY}`,
                'Content-Type': 'application/json'
            },
            timeout: 60000
        });
        return response.data.data.map(item => item.embedding);
    } catch (err) {
        if (err.response?.status === 429) {
            const retryAfter = parseInt(err.response?.headers?.['retry-after']) || 30;
            return { rateLimit: true, retryAfter };
        }
        throw err;
    }
}

// ─── WORKER ──────────────────────────────────────────────────────────────────
const worker = new Worker(QUEUE_NAME, async job => {
    const { candidateId, candidateData } = job.data;

    if (DRY_RUN) {
        console.log(`🔍 [DRY] Would embed candidate ${candidateData.salesforce_contact_id || candidateId}`);
        return;
    }

    const text = candidateToText(candidateData);
    if (!text || text.length < 5) {
        console.log(`⚠️ Skipping ${candidateData.salesforce_contact_id || candidateId} — no data`);
        return;
    }

    console.log(`🔄 Embedding: ${candidateData.salesforce_contact_id || candidateData.name || candidateId}`);
    console.log(`   Text: ${text.substring(0, 100)}...`);

    const result = await embedBatch([text]);

    if (result.rateLimit) {
        console.log(`⏳ Rate limit, waiting ${result.retryAfter}s...`);
        await new Promise(r => setTimeout(r, result.retryAfter * 1000));
        throw new Error('Rate limit retry');
    }

    const embedding = result[0];
    if (!embedding || embedding.length === 0) {
        throw new Error('Empty embedding returned');
    }

    const { error: updateError } = await supabase
        .from('candidates')
        .update({ 
            skill_embedding: embedding,
            updated_at: new Date().toISOString()
        })
        .eq('id', candidateId);

    if (updateError) {
        console.error(`❌ Update error: ${updateError.message}`);
        throw updateError;
    }

    console.log(`✅ ${candidateData.salesforce_contact_id || candidateId} — embedded (${embedding.length} dims)`);
}, {
    connection: redisConnection,
    concurrency: CONCURRENCY
});

// ─── EVENTS ──────────────────────────────────────────────────────────────────
worker.on('completed', job => console.log(`✅ Job ${job.id} completed`));
worker.on('failed', (job, err) => console.error(`❌ Job ${job?.id} failed:`, err.message));

// ─── GRACEFUL SHUTDOWN ──────────────────────────────────────────────────────
process.on('SIGINT', async () => {
    console.log('\n⏹️ Shutting down gracefully...');
    await worker.close();
    await candidateQueue.close();
    await redisConnection.quit();
    process.exit(0);
});

// ─── START ──────────────────────────────────────────────────────────────────
(async () => {
    console.log('\n🧠 Candidate Embedding Queue (Production Scale)');
    console.log(`   Page Size   : ${PAGE_SIZE}`);
    console.log(`   Batch Size  : ${BATCH_SIZE}`);
    console.log(`   Concurrency : ${CONCURRENCY}`);
    console.log(`   Retries     : ${MAX_RETRIES}`);
    console.log(`   Max Candidates : ${MAX_CANDIDATES || 'Unlimited'}`);
    console.log(`   Resume From : ${RESUME_FROM > 0 ? `page ${RESUME_FROM}` : 'Start'}\n`);

    await addCandidatesToQueue();
    const count = await candidateQueue.count();
    console.log(`\n🚀 Queue ready with ${count} candidates. Workers running...\n`);
})();
