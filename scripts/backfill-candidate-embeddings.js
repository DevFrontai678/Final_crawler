// scripts/backfill-candidate-embeddings.js

const { createClient } = require('@supabase/supabase-js');
const dotenv = require('dotenv');
const path = require('path');

// Load environment
const envFile = process.env.NODE_ENV === 'production' ? '.env.production' : '.env.development';
dotenv.config({ path: path.resolve(process.cwd(), envFile) });

console.log(`🔧 Backfilling embeddings in ${process.env.NODE_ENV || 'development'} mode`);

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_ANON_KEY
);

// Import the matching engine
const {
    buildCandidateEmbeddingText,
    generateCandidateEmbedding,
    updateCandidateEmbedding
} = require('../src/matching/match-engine');

/**
 * Backfill all candidates with embeddings
 */
async function backfillAllCandidates(batchSize = 10) {
    console.log('Starting backfill...');
    
    let processed = 0;
    let errors = 0;
    let lastId = null;
    
    while (true) {
        // Get candidates without embeddings or with old embedding version
        let query = supabase
            .from('candidates')
            .select('id, summary, experience, education, skills, certifications, languages, projects, frontsheet_text, cv_text, raw_text, description, embedding_version')
            .order('id')
            .limit(batchSize);
        
        if (lastId) {
            query = query.gt('id', lastId);
        }
        
        const { data: candidates, error } = await query;
        
        if (error) {
            console.error('Error fetching candidates:', error);
            break;
        }
        
        if (!candidates || candidates.length === 0) break;
        
        console.log(`Processing batch of ${candidates.length} candidates...`);
        
        for (const candidate of candidates) {
            try {
                // Build the full text
                const fullText = buildCandidateEmbeddingText(candidate);
                
                // Generate embedding
                const { embedding, textLength, wordCount } = await generateCandidateEmbedding(candidate);
                
                // Update candidate
                const { error: updateError } = await supabase
                    .from('candidates')
                    .update({
                        full_text_for_embedding: fullText,
                        embedding: embedding,
                        embedding_version: (candidate.embedding_version || 0) + 1,
                        updated_at: new Date().toISOString()
                    })
                    .eq('id', candidate.id);
                
                if (updateError) throw updateError;
                
                processed++;
                console.log(`✅ Candidate ${candidate.id}: ${textLength} chars, ${wordCount} words`);
                
            } catch (error) {
                console.error(`Error processing candidate ${candidate.id}:`, error);
                errors++;
            }
        }
        
        lastId = candidates[candidates.length - 1].id;
        console.log(`Progress: ${processed} candidates processed, ${errors} errors`);
        
        // Add a small delay to avoid rate limiting
        await new Promise(resolve => setTimeout(resolve, 500));
    }
    
    console.log(`✅ Backfill complete: ${processed} processed, ${errors} errors`);
    return { processed, errors };
}

// Run the backfill
backfillAllCandidates(10)
    .then(result => {
        console.log('Backfill result:', result);
        process.exit(0);
    })
    .catch(error => {
        console.error('Backfill failed:', error);
        process.exit(1);
    });
