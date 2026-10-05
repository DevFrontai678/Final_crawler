const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('path');
const { EventEmitter } = require('events');

const {
    runIsolatedCompany,
    forceTerminateAllCompanyRunners
} = require('../src/crawlers/company-runner-supervisor');

const fixture = path.join(__dirname, 'fixtures', 'isolated-company-runner-fixture.js');
const jobData = { companyId: 'company-test', companyName: 'Test Company' };
let lastChild = null;
const activeTestChildren = [];

class MockChild extends EventEmitter {
    constructor(mode) {
        super();
        this.mode = mode;
        this.pid = 12345;
        this.exitCode = null;
        this.signalCode = null;
        this.killCalls = [];
    }

    send(message, callback) {
        if (this.mode === 'normal' || this.mode === 'delayed-normal') {
            setImmediate(() => {
                this.emit('message', {
                    type: 'completed',
                    result: { status: 'completed', companyId: message.jobData.companyId, jobsSaved: 2 }
                });
                const exitDelay = this.mode === 'delayed-normal' ? 20 : 0;
                setTimeout(() => {
                    this.exitCode = 0;
                    this.emit('exit', 0, null);
                });
            });
        }

        if (this.mode === 'error') {
            setImmediate(() => this.emit('error', new Error('IPC channel failed')));
        }
        callback?.();
    }

    kill(signal) {
        this.killCalls.push(signal);
        this.signalCode = signal;
        this.exitCode = null;
        setImmediate(() => this.emit('exit', null, signal));
        return true;
    }
}

function runFixture(mode, timeoutMs = 80, terminationGraceMs = 80) {
    return runIsolatedCompany({
        jobData,
        runnerPath: fixture,
        runnerArgs: [],
        timeoutMs,
        terminationGraceMs,
        env: { ...process.env, COMPANY_RUNNER_FIXTURE_MODE: mode },
        forkImpl: () => {
            lastChild = new MockChild(mode);
            activeTestChildren.push(lastChild);
            return lastChild;
        }
    });
}

test('isolated company runner starts and normal completion returns its result', async () => {
    const result = await runFixture('normal');
    assert.equal(result.status, 'completed');
    assert.equal(result.jobsSaved, 2);
});

test('parent returns after a deliberately stuck child is forcibly terminated', async () => {
    const started = Date.now();
    const result = await runFixture('hang', 50, 50);
    assert.equal(result.status, 'partial');
    assert.equal(result.metrics.timed_out, true);
    assert.ok(Date.now() - started < 1000, 'parent must not wait for the stuck child indefinitely');
});

test('already saved jobs are represented by the child result and are not rolled back', async () => {
    const result = await runFixture('normal');
    assert.equal(result.jobsSaved, 2);
});

test('normal completion cancels the deadline before a delayed child exit', async () => {
    const result = await runFixture('delayed-normal', 10, 80);
    assert.equal(result.status, 'completed');
    assert.deepEqual(lastChild.killCalls, []);
});

test('child IPC error terminates a still-alive child before resolving', async () => {
    const result = await runFixture('error', 1000, 30);
    assert.equal(result.status, 'failed');
    assert.deepEqual(lastChild.killCalls, ['SIGTERM']);
});

test('repeated timeouts do not accumulate company runner processes', async () => {
    for (let index = 0; index < 3; index++) {
        const result = await runFixture('hang', 30, 30);
        assert.equal(result.metrics.timed_out, true);
    }
});

test('a hanging timeout callback cannot block parent completion', async () => {
    const result = await runIsolatedCompany({
        jobData,
        runnerPath: fixture,
        runnerArgs: [],
        timeoutMs: 30,
        terminationGraceMs: 30,
        forkImpl: () => new MockChild('hang'),
        onTimeout: () => new Promise(() => {})
    });
    assert.equal(result.metrics.timed_out, true);
});

test('shutdown cleanup force-terminates every active company runner', async () => {
    const before = activeTestChildren.length;
    const first = runFixture('hang', 1000, 30);
    const second = runFixture('hang', 1000, 30);
    forceTerminateAllCompanyRunners();
    const results = await Promise.all([first, second]);
    assert.equal(results.length, 2);
    assert.ok(activeTestChildren.slice(before).every(child => child.killCalls.includes('SIGKILL')));
});

test('Linux process-group termination is used for descendant cleanup', { skip: process.platform !== 'linux' }, async () => {
    const result = await runFixture('descendant', 40, 40);
    assert.equal(result.metrics.timed_out, true);
});
