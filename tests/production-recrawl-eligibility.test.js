const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const atsDetection = require('../scripts/run-ats-detection');

const root = path.resolve(__dirname, '..');
const read = relative => fs.readFileSync(path.join(root, relative), 'utf8');

const standardRunners = [
  ['Personio', 'scripts/run-personio.js', 'personio'],
  ['Softgarden', 'scripts/run-softgarden.js', 'softgarden'],
  ['Workday', 'scripts/run-workday.js', 'workday'],
  ['Workwise', 'scripts/run-workwise.js', 'workwise'],
  ['SuccessFactors', 'scripts/run-successfactors.js', 'successfactors'],
  ['TeamTailor', 'scripts/run-teamtailor.js', 'teamtailor'],
  ['OnApply', 'scripts/run-onapply.js', 'onapply'],
  ['Concludis', 'scripts/run-concludis.js', 'concludis'],
  ['Recruitee', 'scripts/run-recruitee.js', 'recruitee'],
  ['Rexx', 'scripts/run-rexx.js', 'rexx'],
  ['SmartRecruiters', 'scripts/run-smartrecruiters.js', 'smartrecruiters'],
  ['Umantis', 'scripts/run-umantis.js', 'umantis'],
  ['Join', 'scripts/run-join.js', 'join'],
];

test('standard ATS runners do not use previous crawl status as an eligibility gate', () => {
  for (const [label, file, ats] of standardRunners) {
    const source = read(file);
    assert.match(source, new RegExp(`\\.eq\\('ats_type', '${ats}'\\)`), label);
    assert.doesNotMatch(source, /\.in\('crawl_status'/, label);
    assert.doesNotMatch(source, /\.eq\('crawl_status'/, label);
    assert.doesNotMatch(source, /last_crawled_at/, label);
  }
});

test('Custom crawler does not gate scheduled recrawls by status or timestamp', () => {
  const source = read('src/crawlers/custom-crawler-queue.js');
  assert.match(source, /\.eq\('ats_type', 'custom'\)/);
  assert.doesNotMatch(source, /last_crawled_at\.lt|crawl_status\.eq\.pending/);
  assert.doesNotMatch(source, /RECRAWL_INTERVAL_HOURS/);
});

test('same ATS and completed status still route to the current ATS crawler', () => {
  for (const [, file, ats] of standardRunners) {
    const source = read(file);
    assert.match(source, new RegExp(`\\.eq\\('ats_type', '${ats}'\\)`));
  }

  const custom = read('src/crawlers/custom-crawler-queue.js');
  assert.match(custom, /\.eq\('ats_type', 'custom'\)/);
});

test('transient ATS failure preserves a previously valid supported detection', () => {
  const fallback = atsDetection.buildStoredAtsFallback({
    ats_type: 'personio',
    ats_confidence: 0.99,
    career_page_url: 'https://example.com/careers',
    detected_career_url: 'https://example.com/careers',
  }, new Error('temporary timeout'));

  assert.equal(atsDetection.isOperationalFailure({ status: 503 }), true);
  assert.equal(fallback.ats_type, 'personio');
  assert.equal(fallback.crawl_status, 'ats_detected');
  assert.equal(fallback.career_page_url, 'https://example.com/careers');
  assert.equal(atsDetection.isOperationalFailure({
    operationalFailure: { status: 503 },
  }), true);
});

test('transient ATS failure preserves custom detection, but confirmed HTTP failure does not qualify', () => {
  const fallback = atsDetection.buildStoredAtsFallback({
    ats_type: 'custom',
    career_page_url: 'https://example.com/jobs',
  }, new Error('temporary network failure'));

  assert.equal(fallback.ats_type, 'custom');
  assert.equal(fallback.crawl_status, 'custom_detected');
  assert.equal(atsDetection.isOperationalFailure({ status: 404 }), false);
});

test('ATS production invocation reruns all companies without limit or resume', () => {
  const pipeline = read('scripts/run-production-pipeline.js');
  assert.match(pipeline, /run-ats-detection\.js', '--all'/);
  assert.doesNotMatch(pipeline, /--limit|--resume/);
});

test('job upsert paths remain independent of existing jobs', () => {
  const files = [
    'src/crawlers/custom-crawler-queue.js',
    ...standardRunners.map(([, file]) => file),
  ];

  for (const file of files) {
    const source = read(file);
    assert.match(source, /\.upsert\(/);
    assert.doesNotMatch(source, /from\(['"]jobs['"]\)[\s\S]{0,120}\.delete\(/);
  }
});
