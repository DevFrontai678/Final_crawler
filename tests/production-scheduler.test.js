'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');
const { Worker } = require('node:worker_threads');

const scheduler = require('../scripts/run-production-scheduler');
const {
    filterRecoveryCompanies,
    recordProductionRunCompanyCompleted,
    runCompaniesInBatches,
    updateCompanyCrawlStatusOrThrow
} = require('../src/utils/company-batch-runner');

function tempState() {
    return path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'production-scheduler-')), 'state.json');
}

function tempCheckpoint() {
    return path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'production-checkpoint-')), 'checkpoint.json');
}

function writeCheckpoint(file, { companyIds, completedCompanyIds = [], status = 'running' }) {
    scheduler.saveCheckpoint({
        runId: 'test-run',
        status,
        startedAt: '2026-01-01T00:00:00.000Z',
        companyIds: companyIds.map(String),
        completedCompanyIds,
        stuckCompanyIds: [],
        recovery: false
    }, file);
}

function writeCheckpointWithPid(file, pid) {
    writeCheckpoint(file, { companyIds: ['one'], status: 'running' });
    const checkpoint = JSON.parse(fs.readFileSync(file, 'utf8'));
    checkpoint.pipelinePid = pid;
    fs.writeFileSync(file, JSON.stringify(checkpoint, null, 2));
}

function fakeSupabase({ recentCount = 0, activeCompanies = [], allCompanyIds = ['one'], onJobsWindow } = {}) {
    return {
        from(table) {
            if (table === 'jobs') {
                return {
                    select() {
                        return {
                            gte(column, value) {
                                onJobsWindow?.gte?.(column, value);
                                return {
                                    lt: async (endColumn, endValue) => {
                                        onJobsWindow?.lt?.(endColumn, endValue);
                                        return { count: recentCount, error: null };
                                    }
                                };
                            }
                        };
                    }
                };
            }
            return {
                select() {
                    return {
                        in: async () => ({ data: activeCompanies, error: null }),
                        order() {
                            return {
                                range: async (start, end) => ({
                                    data: allCompanyIds.slice(start, end + 1).map(Id => ({ Id })),
                                    error: null
                                })
                            };
                        }
                    };
                }
            };
        }
    };
}

function fakeSpawn(pids = [1001]) {
    const calls = [];
    const spawnImpl = (command, args, options) => {
        calls.push({ command, args, options });
        return { pid: pids[calls.length - 1] || 1000, unref() {} };
    };
    return { calls, spawnImpl };
}

function dead() {
    return false;
}

test('first scheduler execution immediately starts the production pipeline', async () => {
    const stateFile = tempState();
    const checkpointFile = tempCheckpoint();
    const spawned = fakeSpawn();
    const result = await scheduler.runCycle({
        state: {},
        stateFile,
        checkpointFile,
        supabase: fakeSupabase(),
        now: Date.parse('2026-01-01T00:00:00.000Z'),
        isAlive: dead,
        spawnImpl: spawned.spawnImpl
    });

    assert.equal(result.action, 'pipeline_started');
    assert.equal(spawned.calls.length, 1);
    assert.equal(spawned.calls[0].options.detached, true);
    assert.equal(spawned.calls[0].options.env.CRAWLER_RECOVERY_MODE, '0');
    assert.equal(spawned.calls[0].options.env.CRAWLER_RECOVERY_ELIGIBLE_COMPANY_IDS, '');
    assert.deepEqual(JSON.parse(fs.readFileSync(checkpointFile, 'utf8')).companyIds, ['one']);
});

test('recent jobs plus active ATS companies ends the scheduler cycle with no action', async () => {
    const stateFile = tempState();
    const result = await scheduler.runCycle({
        state: { initialRunStartedAt: '2026-01-01T00:00:00.000Z' },
        stateFile,
        now: Date.parse('2026-01-01T01:00:00.000Z'),
        isAlive: dead,
        supabase: fakeSupabase({ recentCount: 2, activeCompanies: [{ Id: 'a', crawl_status: 'ats_detected' }] }),
        spawnImpl: fakeSpawn().spawnImpl
    });

    assert.equal(result.action, 'no_action_progressing');
});

test('a due 72-hour cycle starts a new normal run with no recovery eligibility', async () => {
    const stateFile = tempState();
    const checkpointFile = tempCheckpoint();
    const spawned = fakeSpawn();
    const result = await scheduler.runCycle({
        state: {
            initialRunStartedAt: '2026-01-01T00:00:00.000Z',
            nextPipelineRunAt: '2026-01-04T00:00:00.000Z'
        },
        stateFile,
        checkpointFile,
        now: Date.parse('2026-01-04T01:00:00.000Z'),
        isAlive: dead,
        supabase: fakeSupabase({ allCompanyIds: ['one', 'two'] }),
        spawnImpl: spawned.spawnImpl
    });
    assert.equal(result.action, 'pipeline_started');
    assert.equal(spawned.calls[0].options.env.CRAWLER_RECOVERY_MODE, '0');
    assert.equal(spawned.calls[0].options.env.CRAWLER_RECOVERY_ELIGIBLE_COMPANY_IDS, '');
    assert.deepEqual(JSON.parse(fs.readFileSync(checkpointFile, 'utf8')).companyIds, ['one', 'two']);
});

test('scheduler activity window is exactly the previous 30 minutes', async () => {
    const window = {};
    const now = Date.parse('2026-01-01T01:00:00.000Z');
    const result = await scheduler.queryRecentActivity(
        fakeSupabase({ recentCount: 1, onJobsWindow: {
            gte: (column, value) => { window.gte = { column, value }; },
            lt: (column, value) => { window.lt = { column, value }; }
        }}),
        now
    );

    assert.equal(result.count, 1);
    assert.deepEqual(window, {
        gte: { column: 'updated_at', value: '2026-01-01T00:30:00.000Z' },
        lt: { column: 'updated_at', value: '2026-01-01T01:00:00.000Z' }
    });
});

for (const [name, recentCount] of [
    ['recent jobs plus no active ATS companies', 2]
]) {
    test(`${name} schedules the next pipeline 72 hours later`, async () => {
        const stateFile = tempState();
        const now = Date.parse('2026-01-01T01:00:00.000Z');
        const result = await scheduler.runCycle({
            state: { initialRunStartedAt: '2026-01-01T00:00:00.000Z' },
            stateFile,
            now,
            isAlive: dead,
            supabase: fakeSupabase({ recentCount, activeCompanies: [] }),
            spawnImpl: fakeSpawn().spawnImpl
        });

        assert.equal(result.action, 'scheduled_72_hours');
        assert.equal(result.state.lastPipelineCompletedAt, '2026-01-01T01:00:00.000Z');
        assert.equal(Date.parse(result.state.nextPipelineRunAt), now + scheduler.PIPELINE_INTERVAL_MS);
    });
}

test('no recent jobs plus no ATS detected companies enters recovery and skips explicit in-progress companies', async () => {
    const stateFile = tempState();
    const checkpointFile = tempCheckpoint();
    writeCheckpoint(checkpointFile, { companyIds: ['stuck', 'remaining'] });
    const spawned = fakeSpawn();
    const result = await scheduler.runCycle({
        state: { initialRunStartedAt: '2026-01-01T00:00:00.000Z' },
        stateFile,
        checkpointFile,
        now: Date.parse('2026-01-01T01:00:00.000Z'),
        isAlive: dead,
        supabase: fakeSupabase({
            recentCount: 0,
            activeCompanies: [
                { Id: 'stuck', crawl_status: 'in_progress' }
            ]
        }),
        spawnImpl: spawned.spawnImpl
    });

    assert.equal(result.action, 'recovery_pipeline_started');
    assert.deepEqual(result.skippedCompanyIds, ['stuck']);
    assert.equal(spawned.calls[0].options.env.CRAWLER_SKIP_COMPANY_IDS, 'stuck');
    assert.equal(spawned.calls[0].options.env.CRAWLER_RECOVERY_ELIGIBLE_COMPANY_IDS, '');
    assert.deepEqual(JSON.parse(fs.readFileSync(checkpointFile, 'utf8')).recoveryEligibleCompanyIds, ['remaining']);
    assert.equal(result.state.recoverySkips.stuck.attempts, 1);
});

test('recovery passes custom-crawler stuck IDs to the pipeline skip mechanism', async () => {
    const stateFile = tempState();
    const checkpointFile = tempCheckpoint();
    writeCheckpoint(checkpointFile, { companyIds: ['custom-stuck', 'remaining'] });
    const spawned = fakeSpawn();
    const result = await scheduler.runCycle({
        state: { initialRunStartedAt: '2026-01-01T00:00:00.000Z' },
        stateFile,
        checkpointFile,
        now: Date.parse('2026-01-01T01:00:00.000Z'),
        isAlive: dead,
        supabase: fakeSupabase({
            recentCount: 0,
            activeCompanies: [
                { Id: 'custom-stuck', ats_type: 'custom', crawl_status: 'in_progress' }
            ]
        }),
        spawnImpl: spawned.spawnImpl
    });

    assert.equal(result.action, 'recovery_pipeline_started');
    assert.deepEqual(result.skippedCompanyIds, ['custom-stuck']);
    assert.equal(spawned.calls[0].options.env.CRAWLER_SKIP_COMPANY_IDS, 'custom-stuck');
    assert.equal(spawned.calls[0].options.env.CRAWLER_RECOVERY_ELIGIBLE_COMPANY_IDS, '');
    assert.deepEqual(JSON.parse(fs.readFileSync(checkpointFile, 'utf8')).recoveryEligibleCompanyIds, ['remaining']);
});

test('recovery does not repeatedly restart the same skipped companies before retry time', async () => {
    const stateFile = tempState();
    const checkpointFile = tempCheckpoint();
    writeCheckpoint(checkpointFile, { companyIds: ['stuck', 'remaining'], status: 'recovery_running' });
    const now = Date.parse('2026-01-01T01:00:00.000Z');
    const state = {
        initialRunStartedAt: '2026-01-01T00:00:00.000Z',
        recoverySkips: {
            stuck: { attempts: 1, retryAfterMs: now + scheduler.PIPELINE_INTERVAL_MS }
        }
    };
    const spawned = fakeSpawn();
    const result = await scheduler.runCycle({
        state,
        stateFile,
        checkpointFile,
        now,
        isAlive: dead,
        supabase: fakeSupabase({
            recentCount: 0,
            activeCompanies: [
                { Id: 'stuck', crawl_status: 'in_progress' }
            ]
        }),
        spawnImpl: spawned.spawnImpl
    });

    assert.equal(result.action, 'recovery_waiting_for_retry');
    assert.equal(result.state.recoverySkips.stuck.attempts, 1);
    assert.equal(spawned.calls.length, 0);
});

test('recovery eventually retries a previously skipped company after its retry time', async () => {
    const stateFile = tempState();
    const checkpointFile = tempCheckpoint();
    writeCheckpoint(checkpointFile, {
        companyIds: ['stuck', 'remaining'],
        completedCompanyIds: ['remaining'],
        status: 'recovery_running'
    });
    const now = Date.parse('2026-01-04T01:00:00.000Z');
    const spawned = fakeSpawn();
    const result = await scheduler.runCycle({
        state: {
            initialRunStartedAt: '2026-01-01T00:00:00.000Z',
            recoverySkips: {
                stuck: { attempts: 1, retryAfterMs: now - 1 }
            }
        },
        stateFile,
        checkpointFile,
        now,
        isAlive: dead,
        supabase: fakeSupabase({
            recentCount: 0,
            activeCompanies: [
                { Id: 'stuck', crawl_status: 'in_progress' }
            ]
        }),
        spawnImpl: spawned.spawnImpl
    });

    assert.equal(result.action, 'recovery_pipeline_started');
    assert.equal(spawned.calls[0].options.env.CRAWLER_SKIP_COMPANY_IDS, '');
    assert.equal(spawned.calls[0].options.env.CRAWLER_RECOVERY_ELIGIBLE_COMPANY_IDS, '');
});

test('shared ATS batch runner skips recovery IDs and still processes eligible companies', async () => {
    const previous = process.env.CRAWLER_SKIP_COMPANY_IDS;
    const previousMode = process.env.CRAWLER_RECOVERY_MODE;
    const previousEligible = process.env.CRAWLER_RECOVERY_ELIGIBLE_COMPANY_IDS;
    process.env.CRAWLER_SKIP_COMPANY_IDS = 'stuck';
    process.env.CRAWLER_RECOVERY_MODE = '1';
    process.env.CRAWLER_RECOVERY_ELIGIBLE_COMPANY_IDS = 'stuck,eligible';
    const processed = [];
    try {
        const result = await runCompaniesInBatches([
            { Id: 'stuck', Name: 'Stuck company' },
            { Id: 'eligible', Name: 'Eligible company' }
        ], {
            batchSize: 10,
            label: 'TEST',
            handler: async company => {
                processed.push(company.Id);
                return { jobs: [] };
            }
        });
        assert.deepEqual(processed, ['eligible']);
        assert.equal(result.skipped, 1);
    } finally {
        if (previous === undefined) delete process.env.CRAWLER_SKIP_COMPANY_IDS;
        else process.env.CRAWLER_SKIP_COMPANY_IDS = previous;
        if (previousMode === undefined) delete process.env.CRAWLER_RECOVERY_MODE;
        else process.env.CRAWLER_RECOVERY_MODE = previousMode;
        if (previousEligible === undefined) delete process.env.CRAWLER_RECOVERY_ELIGIBLE_COMPANY_IDS;
        else process.env.CRAWLER_RECOVERY_ELIGIBLE_COMPANY_IDS = previousEligible;
    }
});

test('shared ATS batch runner does not complete a company when its handler throws', async () => {
    const checkpointFile = tempCheckpoint();
    writeCheckpoint(checkpointFile, { companyIds: ['throws'] });
    const previousFile = process.env.PRODUCTION_RUN_CHECKPOINT_FILE;
    const previousRun = process.env.PRODUCTION_RUN_ID;
    process.env.PRODUCTION_RUN_CHECKPOINT_FILE = checkpointFile;
    process.env.PRODUCTION_RUN_ID = 'test-run';
    try {
        await runCompaniesInBatches([{ Id: 'throws' }], {
            handler: async () => { throw new Error('interrupted'); }
        });
        assert.deepEqual(JSON.parse(fs.readFileSync(checkpointFile, 'utf8')).completedCompanyIds, []);
    } finally {
        if (previousFile === undefined) delete process.env.PRODUCTION_RUN_CHECKPOINT_FILE;
        else process.env.PRODUCTION_RUN_CHECKPOINT_FILE = previousFile;
        if (previousRun === undefined) delete process.env.PRODUCTION_RUN_ID;
        else process.env.PRODUCTION_RUN_ID = previousRun;
    }
});

test('terminal status helper preserves successful handler completion', async () => {
    const checkpointFile = tempCheckpoint();
    writeCheckpoint(checkpointFile, { companyIds: ['completed'] });
    const previousFile = process.env.PRODUCTION_RUN_CHECKPOINT_FILE;
    const previousRun = process.env.PRODUCTION_RUN_ID;
    process.env.PRODUCTION_RUN_CHECKPOINT_FILE = checkpointFile;
    process.env.PRODUCTION_RUN_ID = 'test-run';
    const updates = [];
    const supabase = {
        from: () => ({
            update: payload => ({
                eq: async (column, value) => {
                    updates.push({ payload, column, value });
                    return { error: null };
                }
            })
        })
    };
    try {
        const result = await runCompaniesInBatches([{ Id: 'completed' }], {
            handler: async company => {
                await updateCompanyCrawlStatusOrThrow(supabase, company.Id, 'completed');
                return { status: 'completed', jobs: [] };
            }
        });
        assert.equal(result.processed, 1);
        assert.deepEqual(updates, [{ payload: { crawl_status: 'completed' }, column: 'Id', value: 'completed' }]);
        assert.deepEqual(JSON.parse(fs.readFileSync(checkpointFile, 'utf8')).completedCompanyIds, ['completed']);
    } finally {
        if (previousFile === undefined) delete process.env.PRODUCTION_RUN_CHECKPOINT_FILE;
        else process.env.PRODUCTION_RUN_CHECKPOINT_FILE = previousFile;
        if (previousRun === undefined) delete process.env.PRODUCTION_RUN_ID;
        else process.env.PRODUCTION_RUN_ID = previousRun;
    }
});

test('terminal failed status update succeeds without changing failure semantics', async () => {
    let payload;
    const supabase = {
        from: () => ({
            update: value => ({
                eq: async () => {
                    payload = value;
                    return { error: null };
                }
            })
        })
    };
    await updateCompanyCrawlStatusOrThrow(supabase, 'failed', 'failed');
    assert.deepEqual(payload, { crawl_status: 'failed' });
});

test('terminal status update errors propagate and do not record checkpoint completion', async () => {
    const checkpointFile = tempCheckpoint();
    writeCheckpoint(checkpointFile, { companyIds: ['failed'] });
    const previousFile = process.env.PRODUCTION_RUN_CHECKPOINT_FILE;
    const previousRun = process.env.PRODUCTION_RUN_ID;
    process.env.PRODUCTION_RUN_CHECKPOINT_FILE = checkpointFile;
    process.env.PRODUCTION_RUN_ID = 'test-run';
    const statusError = new Error('status update failed');
    const supabase = {
        from: () => ({
            update: () => ({ eq: async () => ({ error: statusError }) })
        })
    };
    try {
        await runCompaniesInBatches([{ Id: 'failed' }], {
            handler: async company => {
                await updateCompanyCrawlStatusOrThrow(supabase, company.Id, 'failed');
                return { status: 'failed', jobs: [] };
            }
        });
        assert.deepEqual(JSON.parse(fs.readFileSync(checkpointFile, 'utf8')).completedCompanyIds, []);
    } finally {
        if (previousFile === undefined) delete process.env.PRODUCTION_RUN_CHECKPOINT_FILE;
        else process.env.PRODUCTION_RUN_CHECKPOINT_FILE = previousFile;
        if (previousRun === undefined) delete process.env.PRODUCTION_RUN_ID;
        else process.env.PRODUCTION_RUN_ID = previousRun;
    }
});

test('all standard ATS handlers route terminal crawl status writes through the hardening helper', () => {
    for (const name of [
        'personio', 'softgarden', 'workday', 'workwise', 'successfactors',
        'teamtailor', 'onapply', 'concludis', 'recruitee', 'rexx',
        'smartrecruiters', 'umantis'
    ]) {
        const source = fs.readFileSync(path.join(__dirname, '..', `scripts/run-${name}.js`), 'utf8');
        assert.match(source, /updateCompanyCrawlStatusOrThrow/);
        assert.doesNotMatch(source, /\.update\(\{\s*crawl_status:/, name);
    }
});

test('checkpoint completion writes preserve IDs from concurrent workers', async () => {
    const checkpointFile = tempCheckpoint();
    writeCheckpoint(checkpointFile, { companyIds: ['3', '4'] });
    const childCode = `
        const { workerData } = require('node:worker_threads');
        process.env.PRODUCTION_RUN_ID = workerData.runId;
        process.env.PRODUCTION_RUN_CHECKPOINT_FILE = workerData.checkpointFile;
        const { recordProductionRunCompanyCompleted } = require('./src/utils/company-batch-runner');
        recordProductionRunCompanyCompleted(workerData.companyId);
    `;
    const runWorker = companyId => new Promise((resolve, reject) => {
        const worker = new Worker(childCode, {
            eval: true,
            workerData: {
                companyId,
                checkpointFile,
                runId: 'test-run'
            }
        });
        worker.on('error', reject);
        worker.on('exit', code => code === 0 ? resolve() : reject(new Error(`worker exited ${code}`)));
    });
    await Promise.all(Array.from({ length: 8 }, (_, index) => runWorker(String(index + 1))));
    const checkpoint = JSON.parse(fs.readFileSync(checkpointFile, 'utf8'));
    assert.deepEqual(new Set(checkpoint.completedCompanyIds), new Set(['1', '2', '3', '4', '5', '6', '7', '8']));
});

test('stale run IDs cannot write into a newer checkpoint', () => {
    const checkpointFile = tempCheckpoint();
    writeCheckpoint(checkpointFile, { companyIds: ['new'] });
    const previousFile = process.env.PRODUCTION_RUN_CHECKPOINT_FILE;
    const previousRun = process.env.PRODUCTION_RUN_ID;
    process.env.PRODUCTION_RUN_CHECKPOINT_FILE = checkpointFile;
    process.env.PRODUCTION_RUN_ID = 'old-run';
    try {
        recordProductionRunCompanyCompleted('old-company');
        assert.deepEqual(JSON.parse(fs.readFileSync(checkpointFile, 'utf8')).completedCompanyIds, []);
    } finally {
        if (previousFile === undefined) delete process.env.PRODUCTION_RUN_CHECKPOINT_FILE;
        else process.env.PRODUCTION_RUN_CHECKPOINT_FILE = previousFile;
        if (previousRun === undefined) delete process.env.PRODUCTION_RUN_ID;
        else process.env.PRODUCTION_RUN_ID = previousRun;
    }
});

test('malformed checkpoint is treated as unavailable rather than crashing recovery', () => {
    const checkpointFile = tempCheckpoint();
    fs.writeFileSync(checkpointFile, '{ malformed');
    assert.equal(scheduler.loadCheckpoint(checkpointFile), null);
});

test('Join recovery filtering uses the same skip mechanism as shared ATS runners', () => {
    const source = fs.readFileSync(path.join(__dirname, '..', 'scripts/run-join.js'), 'utf8');
    assert.match(source, /filterRecoveryCompanies/);

    const previous = process.env.CRAWLER_SKIP_COMPANY_IDS;
    const previousMode = process.env.CRAWLER_RECOVERY_MODE;
    const previousEligible = process.env.CRAWLER_RECOVERY_ELIGIBLE_COMPANY_IDS;
    process.env.CRAWLER_SKIP_COMPANY_IDS = 'stuck';
    process.env.CRAWLER_RECOVERY_MODE = '1';
    process.env.CRAWLER_RECOVERY_ELIGIBLE_COMPANY_IDS = 'stuck,eligible';
    try {
        const result = filterRecoveryCompanies([
            { Id: 'stuck' },
            { Id: 'eligible' }
        ]);
        assert.deepEqual(result.eligibleCompanies.map(company => company.Id), ['eligible']);
        assert.equal(result.skipped, 1);
    } finally {
        if (previous === undefined) delete process.env.CRAWLER_SKIP_COMPANY_IDS;
        else process.env.CRAWLER_SKIP_COMPANY_IDS = previous;
        if (previousMode === undefined) delete process.env.CRAWLER_RECOVERY_MODE;
        else process.env.CRAWLER_RECOVERY_MODE = previousMode;
        if (previousEligible === undefined) delete process.env.CRAWLER_RECOVERY_ELIGIBLE_COMPANY_IDS;
        else process.env.CRAWLER_RECOVERY_ELIGIBLE_COMPANY_IDS = previousEligible;
    }
});

test('custom crawler consumes recovery IDs without changing its crawl implementation', () => {
    const source = fs.readFileSync(path.join(__dirname, '..', 'src/crawlers/custom-crawler-queue.js'), 'utf8');
    assert.match(source, /filterRecoveryCompanies/);
    assert.match(source, /Recovery skips applied/);
    assert.match(source, /if \(!error && \['completed', 'no_jobs', 'not_found', 'failed', 'partial'\]/);
    assert.doesNotMatch(source, /markCompanyStatus\([^)]*'in_progress'[^)]*\)[\s\S]{0,200}recordProductionRunCompanyCompleted/);
});

test('custom crawler completion is terminal-status-only and recovery filtering precedes queueing', () => {
    const source = fs.readFileSync(path.join(__dirname, '..', 'src/crawlers/custom-crawler-queue.js'), 'utf8');
    assert.match(source, /const \{ eligibleCompanies, skipped \} = filterRecoveryCompanies\(data\)/);
    assert.match(source, /for \(const c of eligibleCompanies\)/);
    assert.match(source, /if \(!error && \['completed', 'no_jobs', 'not_found', 'failed', 'partial'\]/);
});

test('Join records completion only after a successful terminal status update', () => {
    const source = fs.readFileSync(path.join(__dirname, '..', 'scripts/run-join.js'), 'utf8');
    assert.match(source, /const \{ error: statusError \} = await supabase[\s\S]*if \(!statusError\) recordProductionRunCompanyCompleted/);
});

test('recovery eligibility excludes current-run completed and stuck companies', () => {
    const allIds = Array.from({ length: 100 }, (_, index) => String(index + 1));
    const completed = allIds.slice(0, 70);
    const stuck = allIds.slice(70);
    const eligible = scheduler.recoveryEligibleIds({ companyIds: allIds, completedCompanyIds: completed }, stuck);
    assert.deepEqual(eligible, []);
});

test('recovery eligibility processes exactly the unprocessed companies', () => {
    const allIds = Array.from({ length: 100 }, (_, index) => String(index + 1));
    const completed = allIds.slice(0, 50);
    const stuck = allIds.slice(50, 70);
    const eligible = scheduler.recoveryEligibleIds({ companyIds: allIds, completedCompanyIds: completed }, stuck);
    assert.deepEqual(eligible, allIds.slice(70));
});

test('normal runs ignore recovery eligibility state', () => {
    const previousMode = process.env.CRAWLER_RECOVERY_MODE;
    const previousEligible = process.env.CRAWLER_RECOVERY_ELIGIBLE_COMPANY_IDS;
    process.env.CRAWLER_RECOVERY_MODE = '0';
    process.env.CRAWLER_RECOVERY_ELIGIBLE_COMPANY_IDS = 'only-one';
    try {
        const result = filterRecoveryCompanies([{ Id: 'one' }, { Id: 'two' }]);
        assert.deepEqual(result.eligibleCompanies.map(company => company.Id), ['one', 'two']);
    } finally {
        if (previousMode === undefined) delete process.env.CRAWLER_RECOVERY_MODE;
        else process.env.CRAWLER_RECOVERY_MODE = previousMode;
        if (previousEligible === undefined) delete process.env.CRAWLER_RECOVERY_ELIGIBLE_COMPANY_IDS;
        else process.env.CRAWLER_RECOVERY_ELIGIBLE_COMPANY_IDS = previousEligible;
    }
});

test('starting state blocks a second launch before a child PID is persisted', async () => {
    const stateFile = tempState();
    const checkpointFile = tempCheckpoint();
    const spawned = fakeSpawn();
    const result = await scheduler.runCycle({
        state: {
            initialRunStartedAt: '2026-01-01T00:00:00.000Z',
            pipelineRun: {
                runId: 'run',
                status: 'starting',
                startedAt: '2026-01-01T01:00:00.000Z',
                checkpointFile,
                pid: null
            }
        },
        stateFile,
        checkpointFile,
        now: Date.parse('2026-01-01T01:15:00.000Z'),
        isAlive: dead,
        findPipelinePid: () => null,
        spawnImpl: spawned.spawnImpl
    });
    assert.equal(result.action, 'pipeline_starting');
    assert.equal(spawned.calls.length, 0);
});

test('checkpoint PID blocks a duplicate when scheduler PID persistence was missed', async () => {
    const stateFile = tempState();
    const checkpointFile = tempCheckpoint();
    writeCheckpointWithPid(checkpointFile, 456);
    const spawned = fakeSpawn();
    const result = await scheduler.runCycle({
        state: {
            initialRunStartedAt: '2026-01-01T00:00:00.000Z',
            pipelineRun: {
                runId: 'test-run',
                status: 'starting',
                startedAt: '2026-01-01T01:00:00.000Z',
                checkpointFile,
                pid: null
            }
        },
        stateFile,
        checkpointFile,
        isAlive: pid => pid === 456,
        findPipelinePid: () => null,
        spawnImpl: spawned.spawnImpl
    });
    assert.equal(result.action, 'pipeline_running');
    assert.equal(spawned.calls.length, 0);
});

test('an exited pipeline recovers from an early crash using its scoped checkpoint', async () => {
    const stateFile = tempState();
    const checkpointFile = tempCheckpoint();
    const allIds = Array.from({ length: 100 }, (_, index) => String(index + 1));
    writeCheckpoint(checkpointFile, { companyIds: allIds, completedCompanyIds: allIds.slice(0, 10) });
    const spawned = fakeSpawn();
    const result = await scheduler.runCycle({
        state: {
            initialRunStartedAt: '2026-01-01T00:00:00.000Z',
            pipelineRun: {
                runId: 'test-run',
                status: 'running',
                startedAt: '2026-01-01T00:00:00.000Z',
                checkpointFile,
                pid: 123
            }
        },
        stateFile,
        checkpointFile,
        now: Date.parse('2026-01-01T01:00:00.000Z'),
        isAlive: dead,
        findPipelinePid: () => null,
        supabase: fakeSupabase({ recentCount: 0, activeCompanies: [{ Id: '11', crawl_status: 'ats_detected' }] }),
        spawnImpl: spawned.spawnImpl
    });
    assert.equal(result.action, 'recovery_pipeline_started');
    assert.equal(spawned.calls[0].options.env.CRAWLER_RECOVERY_ELIGIBLE_COMPANY_IDS, '');
    assert.deepEqual(JSON.parse(fs.readFileSync(checkpointFile, 'utf8')).recoveryEligibleCompanyIds, allIds.slice(10));
});

test('a running pipeline prevents a second pipeline launch', async () => {
    const spawned = fakeSpawn();
    const result = await scheduler.runCycle({
        state: { pipelinePid: 123, initialRunStartedAt: '2026-01-01T00:00:00.000Z' },
        stateFile: tempState(),
        isAlive: pid => pid === 123,
        spawnImpl: spawned.spawnImpl
    });

    assert.equal(result.action, 'pipeline_running');
    assert.equal(spawned.calls.length, 0);
});

test('scheduler restart detects an already-running pipeline even without prior local state', async () => {
    const spawned = fakeSpawn();
    const result = await scheduler.runCycle({
        state: {},
        stateFile: tempState(),
        isAlive: pid => pid === 777,
        findPipelinePid: () => 777,
        spawnImpl: spawned.spawnImpl
    });

    assert.equal(result.action, 'pipeline_running');
    assert.equal(result.pid, 777);
    assert.equal(spawned.calls.length, 0);
});

test('scheduler source contains no explicit sleep or interval wait', () => {
    const source = fs.readFileSync(path.join(__dirname, '..', 'scripts/run-production-scheduler.js'), 'utf8');
    assert.doesNotMatch(source, /setTimeout|setInterval|await\s+sleep/);
});

test('scheduler main reloads persistent state for each execution', () => {
    const source = fs.readFileSync(path.join(__dirname, '..', 'scripts/run-production-scheduler.js'), 'utf8');
    assert.match(source, /runCycle\(\{\s*supabase,\s*state:\s*loadState\(\)\s*\}\)/);
});

test('shared persistence stamps updated_at for new and duplicate job upserts', () => {
    const source = fs.readFileSync(path.join(__dirname, '..', 'src/utils/job-enrichment.js'), 'utf8');
    assert.match(source, /row\.updated_at\s*=\s*new Date\(\)\.toISOString\(\)/);
    assert.match(source, /cleanRow\.updated_at\s*=\s*new Date\(\)\.toISOString\(\)/);
    assert.match(source, /preserveAuthoritativeFieldsForUpsert/);
});

test('production post-processing job updates also refresh updated_at', () => {
    for (const file of [
        'scripts/run-job-structuring.js',
        'scripts/run-job-structuring-worker.js',
        'scripts/run-embeddings-queue.js',
        'scripts/geocode-jobs.js',
        'src/ai/generate-embeddings.js'
    ]) {
        const source = fs.readFileSync(path.join(__dirname, '..', file), 'utf8');
        assert.match(source, /updated_at:\s*new Date\(\)\.toISOString\(\)/, file);
    }
});

console.log('production scheduler tests passed');
