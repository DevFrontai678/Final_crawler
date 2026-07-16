#!/bin/bash
# ============================================================================
# run-ats-only.sh — Run ATS Detection Only
# Runs on 1st & 15th of every month (every 2 weeks)
# ============================================================================

PROJECT_DIR="/home/customer-matching-crawler"
LOG_DIR="$PROJECT_DIR/logs"
LOCK_FILE="$PROJECT_DIR/.pipeline.lock"
ENV_FILE="$PROJECT_DIR/.env"

# ─── LOAD ENV ──────────────────────────────────────────────────────────────

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

# ─── FUNCTIONS ──────────────────────────────────────────────────────────────

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_DIR/ats-only.log"
}

# ─── MAIN ──────────────────────────────────────────────────────────────────

mkdir -p "$LOG_DIR"

if [ -f "$LOCK_FILE" ]; then
    log "⚠️  Another pipeline is running (lock file exists). Exiting."
    exit 0
fi

trap 'rm -f "$LOCK_FILE"; log "🔒 ATS-only interrupted — lock removed"' EXIT
touch "$LOCK_FILE"

log "═══════════════════════════════════════════"
log "🚀 ATS DETECTION ONLY START — $(date)"
log "═══════════════════════════════════════════"

cd "$PROJECT_DIR" || { log "❌ Failed to cd to $PROJECT_DIR"; exit 1; }

log "▶️  Running ATS Detection..."
node scripts/run-ats-detection.js --all --concurrency 5 >> "$LOG_DIR/run-ats-detection.log" 2>&1

if [ $? -eq 0 ]; then
    log "✅ ATS Detection completed successfully"
else
    log "❌ ATS Detection failed (exit code: $?)"
fi

log "═══════════════════════════════════════════"
log "✅ ATS DETECTION COMPLETE — $(date)"
log "═══════════════════════════════════════════"

rm -f "$LOCK_FILE"
trap - EXIT

exit 0
