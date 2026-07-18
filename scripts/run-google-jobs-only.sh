#!/bin/bash
# ============================================================================
# run-google-jobs-only.sh — Google Jobs (Company‑Based) Only
# Runs every 4 days via cron
# ============================================================================

set -e

PROJECT_DIR="/home/customer-matching-crawler"
LOG_DIR="$PROJECT_DIR/logs"
LOCK_FILE="$PROJECT_DIR/.google-jobs.lock"
ENV_FILE="$PROJECT_DIR/.env"

# ─── LOAD .env ──────────────────────────────────────────────────────────────

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
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_DIR/google-jobs.log"
}

# ─── MAIN ──────────────────────────────────────────────────────────────────

mkdir -p "$LOG_DIR"
load_env

if [ -f "$LOCK_FILE" ]; then
    log "⚠️  Another Google Jobs run is already in progress (lock file exists). Exiting."
    exit 0
fi

trap 'rm -f "$LOCK_FILE"; log "🔒 Google Jobs interrupted — lock removed"' EXIT
touch "$LOCK_FILE"

log "═══════════════════════════════════════════"
log "🚀 GOOGLE JOBS (Company‑Based) START — $(date)"
log "═══════════════════════════════════════════"

cd "$PROJECT_DIR" || { log "❌ Failed to cd to $PROJECT_DIR"; exit 1; }

# ─── Run Google Jobs ──────────────────────────────────────────────────────
log "▶️  Starting: run-google-jobs.js --concurrency 3 --resume"
if node scripts/run-google-jobs.js --concurrency 3 --resume >> "$LOG_DIR/run-google-jobs.log" 2>&1; then
    log "✅ Completed: run-google-jobs.js"
else
    log "❌ Failed: run-google-jobs.js (exit code: $?)"
fi

log "═══════════════════════════════════════════"
log "✅ GOOGLE JOBS COMPLETE — $(date)"
log "═══════════════════════════════════════════"

rm -f "$LOCK_FILE"
trap - EXIT

exit 0
