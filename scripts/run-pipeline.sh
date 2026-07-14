#!/bin/bash
# ============================================================================
# run-pipeline.sh — Complete Pipeline Runner (ATS → Embeddings)
# ============================================================================
# Correct Order:
#   1. ATS Detection (--concurrency 5, not 10)
#   2. All Adapters + Crawlers (Personio, Softgarden, Custom)
#   3. Google Jobs Search
#   4. Backfill Locations
#   5. Geocode Jobs
#   6. Job Structuring
#   7. Job Embeddings
#   8. Candidate Embeddings (LAST)
# ============================================================================

set -e

# ─── CONFIG ──────────────────────────────────────────────────────────────────

PROJECT_DIR="/home/customer-matching-crawler"
LOG_DIR="$PROJECT_DIR/logs"
LOCK_FILE="$PROJECT_DIR/.pipeline.lock"
CHECKPOINT_FILE="$PROJECT_DIR/.pipeline-checkpoint"
ENV_FILE="$PROJECT_DIR/.env"

# ─── CHECKPOINT FUNCTIONS ──────────────────────────────────────────────────

get_checkpoint() {
    if [ -f "$CHECKPOINT_FILE" ]; then
        cat "$CHECKPOINT_FILE"
    else
        echo "start"
    fi
}

set_checkpoint() {
    echo "$1" > "$CHECKPOINT_FILE"
}

clear_checkpoint() {
    rm -f "$CHECKPOINT_FILE"
}

# ─── FUNCTIONS ──────────────────────────────────────────────────────────────

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_DIR/pipeline.log"
}

run_script() {
    local cmd="$1"
    local checkpoint_name="${2:-$1}"
    
    local script_path=$(echo "$cmd" | awk '{print $1}')
    local args="${cmd#* }"
    if [ "$args" == "$script_path" ]; then
        args=""
    fi
    
    local script_name=$(basename "$script_path")
    
    local current_checkpoint=$(get_checkpoint)
    if [ "$current_checkpoint" == "done_$checkpoint_name" ]; then
        log "⏩ Skipping: $script_name (already completed)"
        return 0
    fi
    
    if [ ! -f "$script_path" ]; then
        log "⚠️  Skipping: $script_name (file not found)"
        set_checkpoint "done_$checkpoint_name"
        return 0
    fi
    
    log "▶️  Starting: $script_name $args"
    if node "$script_path" $args >> "$LOG_DIR/${script_name%.js}.log" 2>&1; then
        log "✅ Completed: $script_name"
        set_checkpoint "done_$checkpoint_name"
        return 0
    else
        log "❌ Failed: $script_name (exit code: $?)"
        return 1
    fi
}

# ─── ADAPTER + CRAWLER SCRIPTS ────────────────────────────────────────────

run_all_adapter_scripts() {
    log "📋 Running all adapter scripts (Personio, Softgarden, Custom, etc.)..."
    
    # Adapter scripts (excluding embeddings, structuring, etc.)
    local adapter_scripts=$(find "$PROJECT_DIR/scripts" -maxdepth 1 -name "run-*.js" \
        ! -name "run-ats-detection.js" \
        ! -name "run-pipeline.js" \
        ! -name "run-embeddings-queue.js" \
        ! -name "run-embeddings.js" \
        ! -name "run-job-structuring-queue.js" \
        ! -name "run-job-structuring-worker.js" \
        ! -name "run-google-jobs.js" \
        ! -name "run-google-jobs-candidate.js" \
        ! -name "backfill-locations-google.js" \
        ! -name "geocode-jobs.js" \
        ! -name "geocode-candidates.js" \
        ! -name "run-candidate-embeddings-queue.js" \
        ! -name "run-matching.js" \
        ! -name "run-match-single.js" \
        ! -name "run-api-cost-report.js" \
        ! -name "run-candidate-embeddings.js" | sort)
    
    if [ -z "$adapter_scripts" ]; then
        log "⚠️  No adapter scripts found."
        return 0
    fi
    
    for script in $adapter_scripts; do
        run_script "$script" || {
            log "⚠️  Adapter script failed: $(basename "$script") — continuing..."
        }
    done
}

# ─── TRUNCATE TABLES ──────────────────────────────────────────────────────

truncate_jobs() {
    log "🗑️  Truncating jobs table (fresh start) — with proper deletion order..."

    if [ ! -f "$ENV_FILE" ]; then
        log "⚠️  .env file not found — skipping truncate"
        return 0
    fi
    
    while IFS='=' read -r key value; do
        [[ -z "$key" || "$key" =~ ^# ]] && continue
        key=$(echo "$key" | xargs)
        value=$(echo "$value" | xargs)
        if [[ -n "$key" ]]; then
            export "$key=$value"
        fi
    done < "$ENV_FILE"
    
    node -e "
        const { createClient } = require('@supabase/supabase-js');
        const ws = require('ws');
        const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY, { realtime: { transport: ws } });
        
        async function deleteAll() {
            console.log('🗑️  Deleting all matches...');
            const { error: e1 } = await supabase.from('matches').delete().not('id', 'is', null);
            if (e1) { console.error('❌ Matches delete error:', e1.message); process.exit(1); }
            console.log('✅ Matches deleted.');
            
            console.log('🗑️  Deleting all candidates...');
            const { error: e2 } = await supabase.from('candidates').delete().not('id', 'is', null);
            if (e2) { console.error('❌ Candidates delete error:', e2.message); process.exit(1); }
            console.log('✅ Candidates deleted.');
            
            console.log('🗑️  Deleting all jobs...');
            const { error: e3 } = await supabase.from('jobs').delete().not('id', 'is', null);
            if (e3) { console.error('❌ Jobs delete error:', e3.message); process.exit(1); }
            console.log('✅ Jobs deleted.');
            
            console.log('✅ All tables truncated successfully.');
        }
        
        deleteAll().catch(err => {
            console.error('❌ Fatal error during truncate:', err.message);
            process.exit(1);
        });
    " >> "$LOG_DIR/truncate.log" 2>&1
    
    log "✅ All tables truncated (matches → candidates → jobs)"
}

# ─── LOAD .env ──────────────────────────────────────────────────────────────

load_env() {
    if [ -f "$ENV_FILE" ]; then
        log "📄 Loading environment variables from $ENV_FILE"
        while IFS='=' read -r key value; do
            [[ -z "$key" || "$key" =~ ^# ]] && continue
            key=$(echo "$key" | xargs)
            value=$(echo "$value" | xargs)
            if [[ -n "$key" ]]; then
                export "$key=$value"
            fi
        done < "$ENV_FILE"
        log "✅ Environment variables loaded successfully"
    else
        log "⚠️  .env file not found at $ENV_FILE"
    fi
}

# ─── MAIN ──────────────────────────────────────────────────────────────────

mkdir -p "$LOG_DIR"
load_env

if [ -f "$LOCK_FILE" ]; then
    log "⚠️  Another pipeline run is already in progress (lock file exists). Exiting."
    exit 0
fi

trap 'rm -f "$LOCK_FILE"; clear_checkpoint; log "🔒 Pipeline interrupted — lock removed"' EXIT
touch "$LOCK_FILE"

log "═══════════════════════════════════════════"
log "🚀 PIPELINE START — $(date)"
log "═══════════════════════════════════════════"

cd "$PROJECT_DIR" || { log "❌ Failed to cd to $PROJECT_DIR"; exit 1; }

# ─── STEP 1: TRUNCATE TABLES ──────────────────────────────────────────────
truncate_jobs
set_checkpoint "done_truncate"

# ─── STEP 2: ATS Detection (REDUCED CONCURRENCY) ──────────────────────────
run_script "scripts/run-ats-detection.js --all --concurrency 5" "ats_detection" || {
    log "❌ ATS Detection failed — continuing with pipeline..."
}

# ─── STEP 3: ALL ADAPTERS + CRAWLERS ──────────────────────────────────────
run_all_adapter_scripts

# ─── STEP 4: CRITICAL CRAWLERS (if not already run by adapters) ──────────
run_script "src/crawlers/softgarden-crawler-queue.js" "softgarden" || {
    log "⚠️  Softgarden crawler failed — continuing..."
}
run_script "src/crawlers/custom-crawler-queue.js" "custom" || {
    log "⚠️  Custom crawler failed — continuing..."
}

# ─── STEP 5: GOOGLE JOBS SEARCH ──────────────────────────────────────────
run_script "scripts/run-google-jobs.js --concurrency 3 --resume" "google_jobs" || {
    log "⚠️  Company Google Jobs failed — continuing..."
}
run_script "scripts/run-google-jobs-candidate.js --concurrency 3 --resume" "google_jobs_candidate" || {
    log "⚠️  Candidate Google Jobs failed — continuing..."
}

# ─── STEP 6: BACKFILL LOCATIONS ──────────────────────────────────────────
run_script "scripts/backfill-locations-google.js" "backfill_locations_google" || {
    log "⚠️  Backfill locations failed — continuing..."
}

# ─── STEP 7: GEOCODE JOBS ──────────────────────────────────────────────────
run_script "scripts/geocode-jobs.js" "geocode_jobs" || {
    log "⚠️  Geocode jobs failed — continuing..."
}

# ─── STEP 8: JOB STRUCTURING ──────────────────────────────────────────────
run_script "scripts/run-job-structuring-queue.js --resume" "structuring" || {
    log "❌ Job Structuring failed — continuing to embeddings..."
}

# ─── STEP 9: JOB EMBEDDINGS ──────────────────────────────────────────────
run_script "scripts/run-embeddings-queue.js --resume" "embeddings" || {
    log "❌ Job Embeddings failed"
}

# ─── STEP 10: CANDIDATE EMBEDDINGS (LAST) ────────────────────────────────
run_script "scripts/run-candidate-embeddings-queue.js --resume" "candidate_embeddings" || {
    log "❌ Candidate Embeddings failed"
}

log "═══════════════════════════════════════════"
log "✅ PIPELINE COMPLETE — $(date)"
log "═══════════════════════════════════════════"

rm -f "$LOCK_FILE"
clear_checkpoint
trap - EXIT

exit 0
