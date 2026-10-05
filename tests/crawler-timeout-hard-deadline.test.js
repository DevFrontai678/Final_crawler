const test = require('node:test');
const assert = require('node:assert/strict');

process.env.CRAWLER_TEST_MODE = '1';
process.env.CRAWLER_NO_RUNTIME = '1';
process.env.CRAWLER_JOB_TIMEOUT_MS = '50';
process.env.CRAWLER_CLEANUP_TIMEOUT_MS = '20';

const crawler = require('../src/crawlers/custom-crawler-queue');

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

test('withTimeout allows a normal company operation to finish before its deadline', async () => {
    const result = await crawler.withTimeout(async signal => {
        await delay(5);
        assert.equal(signal.aborted, false);
        return 'completed';
    }, 100, 'normal company');

    assert.equal(result, 'completed');
});

test('withTimeout rejects at the deadline even when the task never settles', async () => {
    const started = Date.now();
    await assert.rejects(
        crawler.withTimeout(() => new Promise(() => {}), 10, 'stuck company'),
        error => error.name === 'TimeoutError'
    );
    assert.ok(Date.now() - started < 150, 'timeout path must not await the stuck task');
});

test('timeout aborts a stuck active task and drain returns immediately', async () => {
    const companyAbort = new AbortController();
    let started = false;
    let aborted = false;
    const processor = crawler.createStreamingJobProcessor({
        companyId: 'company-1',
        companyName: 'Test Company',
        companyWebsiteUrl: 'https://company.example',
        signal: companyAbort.signal,
        metrics: { failedPages: 0, jobsSaved: 0, jobPagesFetched: 0, jobsStructured: 0, invalidRejected: 0, duplicateSkipped: 0 },
        seen: new Set(),
        rejectedSamples: [],
        failedSamples: [],
        rejectionReasons: new Map(),
        processLink: async (_link, _id, _name, signal) => {
            started = true;
            signal.addEventListener('abort', () => { aborted = true; }, { once: true });
            await new Promise(() => {});
        }
    });

    processor.enqueue('https://company.example/jobs/1');
    while (!started) await delay(1);

    companyAbort.abort(new Error('company deadline'));
    const startedDrain = Date.now();
    await processor.drain({ cancelPending: true });

    assert.ok(Date.now() - startedDrain < 100, 'cancelled drain must not await activeTasks');
    assert.equal(aborted, true);
});

test('browser cleanup has a bounded deadline', async () => {
    const run = { pages: new Set([{ close: () => new Promise(() => {}) }]) };
    const started = Date.now();
    await crawler.closeCompanyPages(run);
    assert.ok(Date.now() - started < 150, 'page cleanup must be bounded');
    assert.equal(run.pages.size, 0);
});

test('timeout cleanup preserves the timeout result while late task rejection is contained', async () => {
    let abortObserved = false;
    const result = crawler.withTimeout(signal => new Promise((resolve, reject) => {
        signal.addEventListener('abort', () => {
            abortObserved = true;
            setTimeout(() => reject(new Error('late browser rejection')), 50);
        }, { once: true });
    }), 10, 'late browser operation').catch(error => error);

    const error = await result;
    assert.equal(error.name, 'TimeoutError');
    assert.equal(abortObserved, true);
});

test('a timed out company does not prevent the next company from starting', async () => {
    await assert.rejects(
        crawler.withTimeout(() => new Promise(() => {}), 10, 'company one'),
        error => error.name === 'TimeoutError'
    );

    const next = await crawler.withTimeout(async () => 'company two completed', 100, 'company two');
    assert.equal(next, 'company two completed');
});
