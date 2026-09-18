#!/usr/bin/env node

require('dotenv').config();

const { spawn } = require('child_process');
const path = require('path');

const ROOT = path.resolve(__dirname, '..');
const COMPANY_BATCH_SIZE = process.env.CRAWLER_COMPANY_BATCH_SIZE || '10';

const STEPS = [
    { name: 'ATS Detection', cmd: 'node', args: ['scripts/run-ats-detection.js', '--all', '--concurrency', '10'] },
    { name: 'Custom Crawler', cmd: 'node', args: ['src/crawlers/custom-crawler-queue.js'] },
    { name: 'Personio Crawler', cmd: 'node', args: ['scripts/run-personio.js'] },
    { name: 'Softgarden Crawler', cmd: 'node', args: ['scripts/run-softgarden.js'] },
    { name: 'Workday Crawler', cmd: 'node', args: ['scripts/run-workday.js'] },
    { name: 'Workwise Crawler', cmd: 'node', args: ['scripts/run-workwise.js'] },
    { name: 'SuccessFactors Crawler', cmd: 'node', args: ['scripts/run-successfactors.js'] },
    { name: 'TeamTailor Crawler', cmd: 'node', args: ['scripts/run-teamtailor.js'] },
    { name: 'OnApply Crawler', cmd: 'node', args: ['scripts/run-onapply.js'] },
    { name: 'Concludis Crawler', cmd: 'node', args: ['scripts/run-concludis.js'] },
    { name: 'Recruitee Crawler', cmd: 'node', args: ['scripts/run-recruitee.js'] },
    { name: 'Rexx Crawler', cmd: 'node', args: ['scripts/run-rexx.js'] },
    { name: 'SmartRecruiters Crawler', cmd: 'node', args: ['scripts/run-smartrecruiters.js'] },
    { name: 'Umantis Crawler', cmd: 'node', args: ['scripts/run-umantis.js'] },
    { name: 'Job Structuring', cmd: 'node', args: ['scripts/run-job-structuring.js'] },
    { name: 'Geocoding Backfill', cmd: 'node', args: ['scripts/geocode-jobs.js'] },
    { name: 'Voyage Embeddings Backfill', cmd: 'node', args: ['scripts/run-embeddings-queue.js'] }
];

function runStep(step) {
    return new Promise((resolve, reject) => {
        console.log(`\n🚀 [PIPELINE] ${step.name}`);
        const child = spawn(step.cmd, step.args, {
            cwd: ROOT,
            stdio: 'inherit',
            env: {
                ...process.env,
                CRAWLER_COMPANY_BATCH_SIZE: COMPANY_BATCH_SIZE
            }
        });

        child.on('close', code => {
            if (code === 0) resolve();
            else reject(new Error(`${step.name} exited with code ${code}`));
        });

        child.on('error', reject);
    });
}

async function main() {
    console.log(`\n════════════════════════════════════════════════`);
    console.log(`🚚 Production pipeline start`);
    console.log(`   Batch size: ${COMPANY_BATCH_SIZE}`);
    console.log(`   Workload  : 10 companies in parallel`);
    console.log(`════════════════════════════════════════════════`);

    for (const step of STEPS) {
        await runStep(step);
    }

    console.log(`\n✅ Production pipeline complete`);
}

main().catch(error => {
    console.error(`\n❌ Pipeline failed: ${error.message}`);
    process.exit(1);
});
