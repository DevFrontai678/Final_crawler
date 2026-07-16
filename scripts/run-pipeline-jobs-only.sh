#!/bin/bash
# ============================================================================
# run-pipeline-jobs-only.sh — Job Extraction Only (No ATS)
# Runs every 48 hours via cron
# SEQUENTIAL — each step waits for the previous to complete
# ✅ INCLUDES: Reset ALL companies to 'pending' at the start
# ============================================================================

PROJECT_DIR="/home/customer-matching-crawler"
LOG_DIR="$PROJECT_DIR/logs"
LOCK_FILE="$PROJECT_DIR/.pipeline.lock"
CHECKPOINT_FILE="$PROJECT_DIR/.pipeline-checkpoint"
ENV_FILE="$PROJECT_DIR/.env"
CYCLE_START_FILE="$PROJECT_DIR/.cycle-start-time"

# ─── LOAD ENV ──────────────────────────────────────────────────────────────

load_env() {
    if [ -f "$ENV_FILE" ]; then
        while IFS='=' read -r key value; do
            [[ -z "$key" || "$key" =~ ^# ]] && continue
            key=$(echo "$key" | xargs)
            value=$(echo "$value" | xargs)
            if [[ -n "$key" ]]; then
                export "$key=$value"
            fi
        done < "$ENV_FILE"
    fi
}

# ─── FUNCTIONS ──────────────────────────────────────────────────────────────

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_DIR/pipeline.log"
}

run_script() {
    local cmd="$1"
    local script_path=$(echo "$cmd" | awk '{print $1}')
    local args="${cmd#* }"
    if [ "$args" == "$script_path" ]; then args=""; fi
    local script_name=$(basename "$script_path")

    if [ ! -f "$script_path" ]; then
        log "⚠️  Skipping: $script_name (file not found)"
        return 0
    fi

    log "▶️  Starting: $script_name $args"
    if node "$script_path" $args >> "$LOG_DIR/${script_name%.js}.log" 2>&1; then
        log "✅ Completed: $script_name"
        return 0
    else
        log "❌ Failed: $script_name (exit code: $?)"
        return 1
    fi
}

# ─── ADAPTER SCRIPTS (Run Sequentially) ──────────────────────────────────

run_all_adapter_scripts() {
    log "📋 Running all adapter scripts (sequentially)..."

    local adapter_scripts=(
        "scripts/run-concludis.js"
        "scripts/run-join.js"
        "scripts/run-onapply.js"
        "scripts/run-personio.js"
        "scripts/run-recruitee.js"
        "scripts/run-rexx.js"
        "scripts/run-smartrecruiters.js"
        "scripts/run-successfactors.js"
        "scripts/run-teamtailor.js"
        "scripts/run-umantis.js"
        "scripts/run-workday.js"
        "scripts/run-workwise.js"
    )

    for script in "${adapter_scripts[@]}"; do
        run_script "$script" || {
            log "⚠️  Adapter script failed: $(basename "$script") — continuing..."
        }
    done
}

# ─── RESET ALL COMPANIES TO PENDING ──────────────────────────────────────

reset_companies_to_pending() {
    log "🔄 Resetting ALL companies to 'pending' (forced fresh crawl)..."

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
        (async () => {
            const { error, count } = await supabase
                .from('companies')
                .update({ crawl_status: 'pending' })
                .not('Id', 'is', null)
                .select('count');
            if (error) { console.error('❌ Reset error:', error.message); process.exit(1); }
            console.log('✅ All companies reset to pending');
        })();
    " >> "$LOG_DIR/reset.log" 2>&1

    log "✅ All companies reset to 'pending'"
}

# ─── CYCLE START ──────────────────────────────────────────────────────────

start_new_cycle() {
    date -u +"%Y-%m-%dT%H:%M:%S" > "$CYCLE_START_FILE"
    log "🕐 Cycle start time recorded: $(cat $CYCLE_START_FILE)"

    log "🗑️  Clearing matches table only (jobs/candidates preserved)..."
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
        (async () => {
            const { error } = await supabase.from('matches').delete().not('id', 'is', null);
            if (error) { console.error('❌ Matches delete error:', error.message); process.exit(1); }
            console.log('✅ Matches cleared.');
        })();
    " >> "$LOG_DIR/truncate.log" 2>&1
    log "✅ Matches cleared (jobs & candidates untouched — zero downtime)"
}

# ─── STALE CLEANUP ──────────────────────────────────────────────────────────

cleanup_stale_records() {
    local cycle_start=$(cat "$CYCLE_START_FILE" 2>/dev/null)
    if [ -z "$cycle_start" ]; then
        log "⚠️  Cycle start time not found — SKIPPING stale cleanup"
        return 0
    fi

    log "🧹 Cleaning up stale jobs (last_seen_at < $cycle_start)..."
    node -e "
        const { createClient } = require('@supabase/supabase-js');
        const ws = require('ws');
        const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY, { realtime: { transport: ws } });
        (async () => {
            const { error, count } = await supabase
                .from('jobs')
                .delete({ count: 'exact' })
                .lt('last_seen_at', '$cycle_start');
            if (error) { console.error('❌ Stale jobs cleanup error:', error.message); process.exit(1); }
            console.log('✅ Stale jobs removed:', count);
        })();
    " >> "$LOG_DIR/cleanup.log" 2>&1

    log "🧹 Cleaning up stale candidates (last_synced_at < $cycle_start)..."
    node -e "
        const { createClient } = require('@supabase/supabase-js');
        const ws = require('ws');
        const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY, { realtime: { transport: ws } });
        (async () => {
            const { error, count } = await supabase
                .from('candidates')
                .delete({ count: 'exact' })
                .lt('last_synced_at', '$cycle_start');
            if (error) { console.error('❌ Stale candidates cleanup error:', error.message); process.exit(1); }
            console.log('✅ Stale candidates removed:', count);
        })();
    " >> "$LOG_DIR/cleanup.log" 2>&1

    log "✅ Stale cleanup done"
}

# ─── MAIN ──────────────────────────────────────────────────────────────────

mkdir -p "$LOG_DIR"
load_env

if [ -f "$LOCK_FILE" ]; then
    log "⚠️  Another pipeline run is already in progress (lock file exists). Exiting."
    exit 0
fi

trap 'rm -f "$LOCK_FILE"; log "🔒 Pipeline interrupted — lock removed"' EXIT
touch "$LOCK_FILE"

log "═══════════════════════════════════════════"
log "🚀 JOB-ONLY PIPELINE (No ATS) START — $(date)"
log "═══════════════════════════════════════════"

cd "$PROJECT_DIR" || { log "❌ Failed to cd to $PROJECT_DIR"; exit 1; }

# ─── STEP 1: Start cycle ───────────────────────────────────────────────────
start_new_cycle

# ─── STEP 2: RESET ALL COMPANIES TO PENDING ──────────────────────────────
reset_companies_to_pending

# ─── STEP 3: SKIP ATS Detection ──────────────────────────────────────────
log "⏩ Skipping ATS Detection (runs every 2 weeks only)"

# ─── STEP 4: Adapters (SEQUENTIAL) ────────────────────────────────────────
run_all_adapter_scripts

# ─── STEP 5: Softgarden Crawler ──────────────────────────────────────────
run_script "src/crawlers/softgarden-crawler-queue.js"

# ─── STEP 6: Custom Crawler ──────────────────────────────────────────────
run_script "src/crawlers/custom-crawler-queue.js"

# ─── STEP 7: Google Jobs ──────────────────────────────────────────────────
run_script "scripts/run-google-jobs.js --concurrency 3 --resume"
run_script "scripts/run-google-jobs-candidate.js --concurrency 3 --resume"

# ─── STEP 8: Backfill + Geocode ───────────────────────────────────────────
run_script "scripts/backfill-locations-google.js"
run_script "scripts/geocode-jobs.js"

# ─── STEP 9: Job Structuring ──────────────────────────────────────────────
run_script "scripts/run-job-structuring-worker.js"

# ─── STEP 10: Job Embeddings ──────────────────────────────────────────────
run_script "scripts/run-embeddings-queue.js --resume"

# ─── STEP 11: Candidate Embeddings ────────────────────────────────────────
run_script "scripts/run-candidate-embeddings-queue.js --resume"

# ─── STEP 12: Stale Cleanup ──────────────────────────────────────────────
cleanup_stale_records

log "═══════════════════════════════════════════"
log "✅ JOB-ONLY PIPELINE COMPLETE — $(date)"
log "═══════════════════════════════════════════"

rm -f "$LOCK_FILE"
trap - EXIT

exit 0
