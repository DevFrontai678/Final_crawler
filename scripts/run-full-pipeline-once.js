#!/usr/bin/env node

/**
 * ONE‑TIME FULL PIPELINE SCHEDULER (Development)
 * Runs:
 * 1. ATS Detection
 * 2. All Crawlers (enqueue jobs)
 * 3. Job Structuring
 * 4. Geocoding
 * 5. Voyage Embeddings
 */

const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');
const dotenv = require('dotenv');

const envFile = process.env.NODE_ENV === 'production' ? '.env.production' : '.env.development';
dotenv.config({ path: path.resolve(process.cwd(), envFile) });

const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 5000;
const LOG_FILE = path.join(__dirname, 'pipeline-run.log');

function execCommand(cmd, stepName) {
    return new Promise((resolve, reject) => {
        const logStream = fs.createWriteStream(LOG_FILE, { flags: 'a' });
        logStream.write(`\n[${new Date().toISOString()}] Starting: ${stepName}\n`);
        logStream.write(`Command: ${cmd}\n`);

        const child = exec(cmd, { env: { ...process.env, NODE_ENV: 'development' } });

        child.stdout.pipe(logStream);
        child.stderr.pipe(logStream);

        child.on('close', (code) => {
            logStream.write(`[${new Date().toISOString()}] Finished with code ${code}\n`);
            logStream.end();
            if (code === 0) resolve();
            else reject(new Error(`Exit code ${code}`));
        });
        child.on('error', (err) => {
            logStream.write(`Error: ${err.message}\n`);
            logStream.end();
            reject(err);
        });
    });
}

async function runWithRetries(cmd, stepName, retries = MAX_RETRIES) {
    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            console.log(`\n🚀 Running "${stepName}" (attempt ${attempt}/${retries})...`);
            await execCommand(cmd, stepName);
            console.log(`✅ "${stepName}" completed.`);
            return;
        } catch (error) {
            console.error(`❌ Attempt ${attempt} failed: ${error.message}`);
            if (attempt < retries) {
                const delay = RETRY_DELAY_MS * Math.pow(2, attempt - 1);
                console.log(`⏳ Retrying in ${delay/1000}s...`);
                await new Promise(resolve => setTimeout(resolve, delay));
            } else {
                throw new Error(`Step "${stepName}" failed after ${retries} attempts.`);
            }
        }
    }
}

async function runPipeline() {
    const startTime = Date.now();
    console.log(`\n${'='.repeat(60)}`);
    console.log(`🔄 Starting FULL PIPELINE at ${new Date().toISOString()}`);
    console.log(`📁 Log file: ${LOG_FILE}`);
    console.log(`${'='.repeat(60)}\n`);

    process.chdir('/home/customer-matching-crawler');

    const steps = [
        { name: 'ATS Detection', cmd: 'node scripts/run-ats-detection.js' }
    ];

    const crawlers = [
        'run-concludes.js',
        'run-google-jobs.js',
        'run-onapply.js',
        'run-personio.js',
        'run-recruitee.js',
        'run-rexx.js',
        'run-smartrecruiters.js',
        'run-softgarden.js',
        'run-successfactors.js',
        'run-teamailor.js',
        'run-umantis.js',
        'run-workday.js',
        'run-workwise.js'
    ];

    crawlers.forEach(crawler => {
        const name = crawler.replace('run-', '').replace('.js', '').toUpperCase();
        steps.push({ name: `Crawler: ${name}`, cmd: `node scripts/${crawler}` });
    });

    steps.push(
        { name: 'Job Structuring (Queue)', cmd: 'node scripts/run-job-structuring-queue.js' },
        { name: 'Job Structuring (Worker)', cmd: 'node scripts/run-job-structuring-worker.js' },
        { name: 'Job Structuring (Main)', cmd: 'node scripts/run-job-structuring.js' },
        { name: 'Geocoding Jobs', cmd: 'node scripts/geocode-jobs.js' },
        { name: 'Voyage Embeddings (Queue)', cmd: 'node scripts/run-embeddings-queue.js' }
    );

    const availableSteps = steps.filter(step => {
        const scriptPath = path.join(process.cwd(), step.cmd.split(' ')[1]);
        const exists = fs.existsSync(scriptPath);
        if (!exists) console.warn(`⚠️ Skipping "${step.name}" – script not found: ${scriptPath}`);
        return exists;
    });

    for (const step of availableSteps) {
        try {
            await runWithRetries(step.cmd, step.name);
        } catch (error) {
            console.error(`\n💥 FATAL: Pipeline stopped at "${step.name}" after retries.`);
            console.error(`   Error: ${error.message}`);
            console.error(`   Check log: ${LOG_FILE}`);
            process.exit(1);
        }
    }

    const duration = ((Date.now() - startTime) / 1000 / 60).toFixed(2);
    console.log(`\n✅ PIPELINE COMPLETED in ${duration} minutes.`);
    console.log(`📁 Log: ${LOG_FILE}`);
}

runPipeline().catch(err => {
    console.error('Unhandled error:', err);
    process.exit(1);
});
