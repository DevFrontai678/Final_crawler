#!/usr/bin/env node

'use strict';

require('dotenv').config();

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { spawn, execFileSync } = require('child_process');
const { createClient } = require('@supabase/supabase-js');

const ROOT = path.resolve(__dirname, '..');
const PIPELINE_SCRIPT = path.join(__dirname, 'run-production-pipeline.js');
const STATE_FILE = process.env.PRODUCTION_SCHEDULER_STATE_FILE || path.join(ROOT, '.production-scheduler-state.json');
const LOCK_FILE = process.env.PRODUCTION_SCHEDULER_LOCK_FILE || `${STATE_FILE}.lock`;
const CHECKPOINT_FILE = process.env.PRODUCTION_RUN_CHECKPOINT_FILE || `${STATE_FILE}.checkpoint.json`;
const CHECK_INTERVAL_MS = 30 * 60 * 1000;
const PIPELINE_INTERVAL_MS = 72 * 60 * 60 * 1000;
const STARTING_STALE_MS = 2 * CHECK_INTERVAL_MS;

function iso(value) {
    return new Date(value).toISOString();
}

function parseState(raw) {
    if (!raw || typeof raw !== 'object') return {};
    return {
        ...raw,
        recoverySkips: raw.recoverySkips && typeof raw.recoverySkips === 'object'
            ? raw.recoverySkips
            : {}
    };
}

function loadState(file = STATE_FILE) {
    try {
        return parseState(JSON.parse(fs.readFileSync(file, 'utf8')));
    } catch (error) {
        if (error.code !== 'ENOENT') console.warn(`[SCHEDULER] State read failed: ${error.message}`);
        return parseState({});
    }
}

function saveState(state, file = STATE_FILE) {
    const directory = path.dirname(file);
    fs.mkdirSync(directory, { recursive: true });
    const temp = `${file}.${process.pid}.tmp`;
    fs.writeFileSync(temp, JSON.stringify(state, null, 2));
    fs.renameSync(temp, file);
}

function saveCheckpoint(checkpoint, file = CHECKPOINT_FILE) {
    const directory = path.dirname(file);
    fs.mkdirSync(directory, { recursive: true });
    const temp = `${file}.${process.pid}.tmp`;
    fs.writeFileSync(temp, JSON.stringify(checkpoint, null, 2));
    fs.renameSync(temp, file);
}

function loadCheckpoint(file = CHECKPOINT_FILE) {
    try {
        return JSON.parse(fs.readFileSync(file, 'utf8'));
    } catch (error) {
        if (error.code !== 'ENOENT') console.warn(`[SCHEDULER] Checkpoint read failed: ${error.message}`);
        return null;
    }
}

function createRunCheckpoint(companyIds, nowMs, recovery = false) {
    return {
        runId: crypto.randomUUID(),
        status: recovery ? 'recovery_starting' : 'starting',
        startedAt: iso(nowMs),
        companyIds: companyIds.map(String),
        completedCompanyIds: [],
        stuckCompanyIds: [],
        recoveryEligibleCompanyIds: [],
        recovery
    };
}

function recoveryEligibleIds(checkpoint, stuckIds) {
    if (!checkpoint?.companyIds) return [];
    const completed = new Set((checkpoint.completedCompanyIds || []).map(String));
    const stuck = new Set(stuckIds.map(String));
    return checkpoint.companyIds
        .map(String)
        .filter(companyId => !completed.has(companyId) && !stuck.has(companyId));
}

function acquireLock(file = LOCK_FILE) {
    try {
        const descriptor = fs.openSync(file, 'wx');
        fs.writeFileSync(descriptor, `${process.pid}\n`);
        return () => {
            try { fs.closeSync(descriptor); } catch (_) {}
            try { fs.unlinkSync(file); } catch (_) {}
        };
    } catch (error) {
        if (error.code === 'EEXIST') {
            try {
                const ownerPid = Number.parseInt(fs.readFileSync(file, 'utf8').trim(), 10);
                if (!isProcessAlive(ownerPid)) {
                    fs.unlinkSync(file);
                    return acquireLock(file);
                }
            } catch (_) {
                try { fs.unlinkSync(file); } catch (__) { return null; }
                return acquireLock(file);
            }
            return null;
        }
        throw error;
    }
}

function isProcessAlive(pid) {
    if (!Number.isInteger(pid) || pid <= 0) return false;
    try {
        process.kill(pid, 0);
        return true;
    } catch (_) {
        return false;
    }
}

function findRunningPipelinePid() {
    if (process.platform === 'win32') return null;
    try {
        const output = execFileSync('pgrep', ['-f', 'run-production-pipeline\\.js'], {
            encoding: 'utf8',
            stdio: ['ignore', 'pipe', 'ignore']
        });
        const pid = output.split(/\s+/)
            .map(value => Number.parseInt(value, 10))
            .find(value => Number.isInteger(value) && value > 0 && value !== process.pid);
        return pid || null;
    } catch (_) {
        return null;
    }
}

function activeSkipIds(state, nowMs) {
    return Object.entries(state.recoverySkips || {})
        .filter(([, record]) => Number(record.retryAfterMs || 0) > nowMs)
        .map(([companyId]) => companyId);
}

function pruneRecoverySkips(state, nowMs) {
    for (const [companyId, record] of Object.entries(state.recoverySkips || {})) {
        if (Number(record.retryAfterMs || 0) <= nowMs) delete state.recoverySkips[companyId];
    }
}

function scheduleNextPipeline(state, nowMs) {
    state.lastPipelineCompletedAt = iso(nowMs);
    state.nextPipelineRunAt = iso(nowMs + PIPELINE_INTERVAL_MS);
    state.lastDecision = 'pipeline_complete';
}

async function queryRecentActivity(supabase, nowMs) {
    const start = iso(nowMs - CHECK_INTERVAL_MS);
    const end = iso(nowMs);
    const { count, error } = await supabase
        .from('jobs')
        .select('id', { count: 'exact', head: true })
        .gte('updated_at', start)
        .lt('updated_at', end);
    if (error) throw new Error(`Recent jobs query failed: ${error.message}`);
    return { count: count || 0, start, end };
}

async function queryActiveCompanies(supabase) {
    const { data, error } = await supabase
        .from('companies')
        .select('Id, crawl_status, ats_type, updated_at, last_crawled_at')
        .in('crawl_status', ['ats_detected', 'in_progress']);
    if (error) throw new Error(`Active companies query failed: ${error.message}`);
    return Array.isArray(data) ? data : [];
}

async function queryAllCompanyIds(supabase, pageSize = 1000) {
    const companyIds = [];
    for (let page = 0; ; page++) {
        const { data, error } = await supabase
            .from('companies')
            .select('Id')
            .order('Id', { ascending: true })
            .range(page * pageSize, (page + 1) * pageSize - 1);
        if (error) throw new Error(`Company checkpoint query failed: ${error.message}`);
        if (!Array.isArray(data) || data.length === 0) break;
        companyIds.push(...data.map(company => String(company.Id)));
        if (data.length < pageSize) break;
    }
    return companyIds;
}

function spawnPipeline({
    skipIds = [],
    eligibleIds = [],
    runId,
    checkpointFile = CHECKPOINT_FILE,
    recovery = false,
    spawnImpl = spawn,
    env = process.env
} = {}) {
    const child = spawnImpl(process.execPath, [PIPELINE_SCRIPT], {
        cwd: ROOT,
        detached: true,
        stdio: 'ignore',
        env: {
            ...env,
            PRODUCTION_RUN_ID: runId || '',
            PRODUCTION_RUN_CHECKPOINT_FILE: checkpointFile,
            CRAWLER_RECOVERY_MODE: recovery ? '1' : '0',
            CRAWLER_RECOVERY_ELIGIBLE_COMPANY_IDS: '',
            CRAWLER_SKIP_COMPANY_IDS: recovery ? skipIds.join(',') : ''
        }
    });
    child.unref();
    return child.pid;
}

async function runCycle({
    supabase,
    now = Date.now(),
    state = {},
    spawnImpl = spawn,
    stateFile = STATE_FILE,
    checkpointFile = CHECKPOINT_FILE,
    isAlive = isProcessAlive,
    findPipelinePid = findRunningPipelinePid,
    env = process.env
} = {}) {
    console.log('[SCHEDULER] Started');
    const normalizedState = parseState(state);
    const dueRetryIds = new Set(Object.entries(normalizedState.recoverySkips)
        .filter(([, record]) => Number(record.retryAfterMs || 0) <= now)
        .map(([companyId]) => companyId));
    pruneRecoverySkips(normalizedState, now);

    let interruptedRun = false;
    const persistedRun = normalizedState.pipelineRun;
    const persistedCheckpoint = persistedRun?.checkpointFile
        ? loadCheckpoint(persistedRun.checkpointFile)
        : null;
    const persistedPipelinePid = persistedRun?.pid || persistedCheckpoint?.pipelinePid;
    if (persistedRun && ['starting', 'running', 'recovery_starting', 'recovery_running'].includes(persistedRun.status)) {
        if (isAlive(persistedPipelinePid)) {
            normalizedState.lastDecision = 'pipeline_running';
            saveState(normalizedState, stateFile);
            console.log('[SCHEDULER] Pipeline already running. No action.');
            return { action: 'pipeline_running', state: normalizedState };
        }

        const discoveredPid = findPipelinePid();
        if (isAlive(discoveredPid)) {
            normalizedState.pipelineRun.pid = discoveredPid;
            normalizedState.pipelineRun.status = persistedRun.recovery ? 'recovery_running' : 'running';
            normalizedState.pipelinePid = discoveredPid;
            normalizedState.lastDecision = 'pipeline_running';
            saveState(normalizedState, stateFile);
            console.log('[SCHEDULER] Pipeline already running. No action.');
            return { action: 'pipeline_running', pid: discoveredPid, state: normalizedState };
        }

        const startedAt = Date.parse(persistedRun.startedAt || '');
        const startIsStale = !persistedPipelinePid && Number.isFinite(startedAt) && now - startedAt >= STARTING_STALE_MS;
        if (!persistedPipelinePid && !startIsStale) {
            normalizedState.lastDecision = 'pipeline_starting';
            saveState(normalizedState, stateFile);
            console.log('[SCHEDULER] Pipeline launch is still being established. No action.');
            return { action: 'pipeline_starting', state: normalizedState };
        }

        normalizedState.pipelineRun.status = 'interrupted';
        normalizedState.pipelineRun.pid = null;
        normalizedState.pipelinePid = null;
        interruptedRun = true;
        saveState(normalizedState, stateFile);
    }

    if (isAlive(normalizedState.pipelinePid)) {
        normalizedState.lastDecision = 'pipeline_running';
        saveState(normalizedState, stateFile);
        console.log('[SCHEDULER] action: DO NOTHING');
        console.log('[SCHEDULER] Pipeline launch skipped because another pipeline is already running');
        console.log('[SCHEDULER] Production pipeline already running. No action.');
        return { action: 'pipeline_running', state: normalizedState };
    }

    const existingPipelinePid = findPipelinePid();
    if (isAlive(existingPipelinePid)) {
        normalizedState.pipelinePid = existingPipelinePid;
        normalizedState.lastDecision = 'pipeline_running';
        saveState(normalizedState, stateFile);
        console.log('[SCHEDULER] action: DO NOTHING');
        console.log('[SCHEDULER] Pipeline launch skipped because another pipeline is already running');
        console.log('[SCHEDULER] Production pipeline already running. No action.');
        return { action: 'pipeline_running', pid: existingPipelinePid, state: normalizedState };
    }

    const firstRun = !normalizedState.initialRunStartedAt;
    const normalRunDue = normalizedState.nextPipelineRunAt && now >= Date.parse(normalizedState.nextPipelineRunAt);
    if (firstRun || normalRunDue) {
        const companyIds = await queryAllCompanyIds(supabase);
        const checkpoint = createRunCheckpoint(companyIds, now);
        saveCheckpoint(checkpoint, checkpointFile);
        normalizedState.pipelineRun = {
            runId: checkpoint.runId,
            checkpointFile,
            status: 'starting',
            startedAt: checkpoint.startedAt,
            recovery: false,
            pid: null
        };
        normalizedState.initialRunStartedAt = normalizedState.initialRunStartedAt || iso(now);
        normalizedState.cycleStartedAt = iso(now);
        normalizedState.nextPipelineRunAt = null;
        normalizedState.lastDecision = 'initial_pipeline_started';
        saveState(normalizedState, stateFile);
        const pid = spawnPipeline({
            runId: checkpoint.runId,
            checkpointFile,
            spawnImpl,
            env
        });
        checkpoint.status = 'running';
        saveCheckpoint(checkpoint);
        normalizedState.pipelineRun.status = 'running';
        normalizedState.pipelineRun.pid = pid;
        normalizedState.pipelinePid = pid;
        saveState(normalizedState, stateFile);
        console.log('[SCHEDULER] action: PIPELINE STARTED');
        console.log(firstRun
            ? '[SCHEDULER] First run: starting production pipeline'
            : '[SCHEDULER] 72-hour cycle due: starting production pipeline');
        return { action: 'pipeline_started', pid, state: normalizedState };
    }

    if (normalizedState.nextPipelineRunAt && now < Date.parse(normalizedState.nextPipelineRunAt)) {
        normalizedState.lastDecision = 'waiting_for_72_hour_cycle';
        saveState(normalizedState, stateFile);
        console.log('[SCHEDULER] action: DO NOTHING');
        console.log(`[SCHEDULER] next pipeline allowed at: ${normalizedState.nextPipelineRunAt}`);
        return { action: 'waiting_for_72_hour_cycle', state: normalizedState };
    }

    const [activity, activeCompanies] = await Promise.all([
        queryRecentActivity(supabase, now),
        queryActiveCompanies(supabase)
    ]);

    const atsDetectedCompanies = activeCompanies.filter(company => company.crawl_status === 'ats_detected');
    const atsDetectedCount = atsDetectedCompanies.length;
    console.log(`[SCHEDULER] Jobs updated in last 30m: ${activity.count}`);
    console.log(`[SCHEDULER] ATS detected companies: ${atsDetectedCount}`);

    if (atsDetectedCompanies.length > 0 && !(interruptedRun && activity.count === 0)) {
        normalizedState.lastDecision = 'pipeline_still_progressing';
        saveState(normalizedState, stateFile);
        console.log('[SCHEDULER] action: DO NOTHING');
        console.log('[SCHEDULER] Pipeline still in progress. No action.');
        return { action: 'no_action_progressing', activity, activeCompanies, state: normalizedState };
    }

    if (activity.count > 0) {
        scheduleNextPipeline(normalizedState, now);
        const checkpoint = loadCheckpoint(normalizedState.pipelineRun?.checkpointFile || checkpointFile);
        if (checkpoint) {
            checkpoint.status = 'completed';
            saveCheckpoint(checkpoint, normalizedState.pipelineRun?.checkpointFile || checkpointFile);
        }
        saveState(normalizedState, stateFile);
        console.log('[SCHEDULER] action: PIPELINE COMPLETED');
        console.log(`[SCHEDULER] next pipeline allowed at: ${normalizedState.nextPipelineRunAt}`);
        console.log('[SCHEDULER] Pipeline completed. Next run scheduled in 72h.');
        return { action: 'scheduled_72_hours', activity, activeCompanies, state: normalizedState };
    }

    const skipIds = activeSkipIds(normalizedState, now);
    let recoverySkipIds = activeSkipIds(normalizedState, now);
    // `in_progress` is the only explicit active company state currently
    // written by the crawler. `ats_detected` also means eligible work, but
    // does not prove that a specific company was already running.
    const newlyStuck = activeCompanies
        .filter(company => company.crawl_status === 'in_progress')
        .map(company => String(company.Id))
        .filter(companyId => !dueRetryIds.has(companyId))
        .filter(companyId => !skipIds.includes(companyId));

    for (const companyId of newlyStuck) {
        normalizedState.recoverySkips[companyId] = {
            attempts: Number(normalizedState.recoverySkips[companyId]?.attempts || 0) + 1,
            firstObservedAt: normalizedState.recoverySkips[companyId]?.firstObservedAt || iso(now),
            retryAfterMs: now + PIPELINE_INTERVAL_MS
        };
    }

    recoverySkipIds = activeSkipIds(normalizedState, now);
    const recoveryRetryDue = [...dueRetryIds].some(companyId =>
        activeCompanies.some(company => String(company.Id) === companyId)
    );
    const checkpoint = loadCheckpoint(normalizedState.pipelineRun?.checkpointFile || checkpointFile);
    if (!checkpoint) {
        normalizedState.lastDecision = 'recovery_waiting_for_retry';
        normalizedState.recoveryRetryAt = null;
        saveState(normalizedState, stateFile);
        console.log('[SCHEDULER] action: DO NOTHING');
        console.log('[SCHEDULER] No stuck companies identified. Recovery not started.');
        return {
            action: 'recovery_waiting_for_retry',
            activity,
            activeCompanies,
            skippedCompanyIds: recoverySkipIds,
            state: normalizedState
        };
    }

    const currentStuckIds = activeCompanies
        .filter(company => company.crawl_status === 'in_progress')
        .map(company => String(company.Id));
    const excludedIds = [...new Set([...recoverySkipIds, ...currentStuckIds.filter(id => !dueRetryIds.has(id))])];
    const eligibleIds = recoveryEligibleIds(checkpoint, excludedIds);
    const canRecoverInterruptedRun =
        (interruptedRun && !persistedRun?.recovery) ||
        checkpoint.status === 'starting' ||
        checkpoint.status === 'running';
    if (newlyStuck.length === 0 && !recoveryRetryDue && !canRecoverInterruptedRun) {
        normalizedState.lastDecision = 'recovery_waiting_for_retry';
        normalizedState.recoveryRetryAt = null;
        saveState(normalizedState, stateFile);
        console.log('[SCHEDULER] action: DO NOTHING');
        console.log('[SCHEDULER] No new recovery work is due.');
        return {
            action: 'recovery_waiting_for_retry',
            activity,
            activeCompanies,
            skippedCompanyIds: recoverySkipIds,
            state: normalizedState
        };
    }

    if (eligibleIds.length === 0) {
        normalizedState.lastDecision = 'recovery_no_eligible_companies';
        saveState(normalizedState, stateFile);
        console.log('[SCHEDULER] No unprocessed companies remain in the interrupted run.');
        return {
            action: 'recovery_no_eligible_companies',
            activity,
            activeCompanies,
            skippedCompanyIds: recoverySkipIds,
            state: normalizedState
        };
    }

    checkpoint.status = 'recovery_starting';
    checkpoint.stuckCompanyIds = excludedIds;
    checkpoint.recoveryEligibleCompanyIds = eligibleIds;
    saveCheckpoint(checkpoint, normalizedState.pipelineRun?.checkpointFile || checkpointFile);

    normalizedState.lastRecoveryAt = iso(now);
    normalizedState.lastDecision = 'recovery_pipeline_started';
    normalizedState.pipelineRun = {
        runId: checkpoint.runId,
        checkpointFile: normalizedState.pipelineRun?.checkpointFile || checkpointFile,
        status: 'recovery_starting',
        startedAt: iso(now),
        recovery: true,
        pid: null
    };
    saveState(normalizedState, stateFile);
    const pid = spawnPipeline({
        skipIds: excludedIds,
        eligibleIds,
        runId: checkpoint.runId,
        checkpointFile: normalizedState.pipelineRun.checkpointFile,
        recovery: true,
        spawnImpl,
        env
    });
    checkpoint.status = 'recovery_running';
    saveCheckpoint(checkpoint, normalizedState.pipelineRun.checkpointFile);
    normalizedState.pipelineRun.status = 'recovery_running';
    normalizedState.pipelineRun.pid = pid;
    normalizedState.pipelinePid = pid;
    normalizedState.cycleStartedAt = iso(now);
    normalizedState.nextPipelineRunAt = null;
    normalizedState.recoveryRetryAt = null;
    saveState(normalizedState, stateFile);
    console.log('[SCHEDULER] action: RESUME PIPELINE');
    console.log('[SCHEDULER] No recent job activity detected');
    console.log(`[SCHEDULER] Stalled companies detected: ${recoverySkipIds.length}`);
    console.log('[SCHEDULER] Recovery required');
    console.log(`[SCHEDULER] Skipping stuck companies: ${recoverySkipIds.join(', ') || 'none'}`);
    console.log(`[SCHEDULER] Resuming pipeline and skipping stuck companies: ${recoverySkipIds.join(', ') || 'none'}`);
    console.log('[SCHEDULER] Starting recovery pipeline.');

    return {
        action: 'recovery_pipeline_started',
        pid,
        activity,
        activeCompanies,
        skippedCompanyIds: recoverySkipIds,
        state: normalizedState
    };
}

async function main() {
    const release = acquireLock();
    if (!release) {
        console.log('[SCHEDULER] Another scheduler execution is already active. Exiting.');
        return;
    }

    try {
        const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
        const result = await runCycle({ supabase, state: loadState() });
        console.log(`[SCHEDULER] ${result.action}`);
    } finally {
        release();
    }
}

if (require.main === module) {
    main().catch(error => {
        console.error(`[SCHEDULER] Failed: ${error.message}`);
        process.exitCode = 1;
    });
}

module.exports = {
    CHECK_INTERVAL_MS,
    PIPELINE_INTERVAL_MS,
    STARTING_STALE_MS,
    activeSkipIds,
    createRunCheckpoint,
    findRunningPipelinePid,
    isProcessAlive,
    loadState,
    loadCheckpoint,
    parseState,
    queryAllCompanyIds,
    queryActiveCompanies,
    queryRecentActivity,
    recoveryEligibleIds,
    runCycle,
    saveCheckpoint,
    saveState,
    scheduleNextPipeline
};
