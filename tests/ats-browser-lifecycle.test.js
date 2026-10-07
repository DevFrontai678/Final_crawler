'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const read = relative => fs.readFileSync(path.join(root, relative), 'utf8');

const retainedBrowserScripts = [
    ['scripts/run-workday.js', 'Workday'],
    ['scripts/run-workwise.js', 'Workwise'],
    ['scripts/run-successfactors.js', 'SuccessFactors'],
    ['scripts/run-teamtailor.js', 'TeamTailor'],
    ['scripts/run-onapply.js', 'OnApply'],
    ['scripts/run-concludis.js', 'Concludis']
];

for (const [file, label] of retainedBrowserScripts) {
    const source = read(file);
    assert.match(source, /async function closeBrowserInstance\(\)/, `${label} has an idempotent browser cleanup function`);
    assert.match(source, /browserInstance = null/, `${label} clears the retained browser reference`);
    assert.match(source, /\.finally\(\(\) => closeBrowserInstance\(\)\)/, `${label} cleans up on success and failure`);
    assert.doesNotMatch(source, /run\(\)\.catch\(console\.error\)/, `${label} does not hide crawler failures`);
}

const personio = read('scripts/run-personio.js');
assert.match(personio, /\.finally\(\(\) => closePersonioBrowser\(\)\)/);
assert.doesNotMatch(personio, /run\(\)\.catch\(console\.error\)/);

const softgarden = read('scripts/run-softgarden.js');
assert.match(softgarden, /async function closeSoftgardenBrowser\(\)/);
assert.match(softgarden, /\.finally\(\(\) => closeSoftgardenBrowser\(\)\)/);

const softgardenQueue = read('src/crawlers/softgarden-crawler-queue.js');
assert.match(softgardenQueue, /async function shutdown\(code = 0\)/);
assert.match(softgardenQueue, /await closeSoftgardenBrowser\(\)/);
assert.match(softgardenQueue, /async function maybeShutdownWhenDrained\(\)/);
assert.match(softgardenQueue, /getJobCounts\('waiting', 'active', 'delayed', 'prioritized'\)/);
assert.match(softgardenQueue, /if \(remaining === 0\) await shutdown\(0\)/);

for (const file of ['scripts/run-umantis.js', 'scripts/run-smartrecruiters.js', 'scripts/run-recruitee.js', 'scripts/run-rexx.js', 'scripts/run-join.js']) {
    const source = read(file);
    assert.doesNotMatch(source, /run\(\)\.catch\(console\.error\)/, `${file} reports failures with a nonzero exit code`);
}

console.log('ATS browser lifecycle tests passed');
