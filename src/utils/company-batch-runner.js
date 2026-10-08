require('dotenv').config();
const fs = require('fs');

const CHECKPOINT_LOCK_STALE_MS = 60 * 1000;

function checkpointLockFile(checkpointFile) {
    return `${checkpointFile}.lock`;
}

function acquireCheckpointLock(checkpointFile) {
    const lockFile = checkpointLockFile(checkpointFile);
    const waitBuffer = new Int32Array(new SharedArrayBuffer(4));

    for (let attempt = 0; attempt < 200; attempt++) {
        try {
            const descriptor = fs.openSync(lockFile, 'wx');
            fs.writeFileSync(descriptor, `${process.pid}\n`);
            return () => {
                try { fs.closeSync(descriptor); } catch (_) {}
                try { fs.unlinkSync(lockFile); } catch (_) {}
            };
        } catch (error) {
            if (error.code !== 'EEXIST') throw error;

            let removeStaleLock = false;
            try {
                const lockStat = fs.statSync(lockFile);
                const lockPid = Number.parseInt(fs.readFileSync(lockFile, 'utf8').trim(), 10);
                const ownerAlive = Number.isInteger(lockPid) && lockPid > 0
                    ? (() => {
                        try { process.kill(lockPid, 0); return true; } catch (_) { return false; }
                    })()
                    : false;
                removeStaleLock = !ownerAlive || Date.now() - lockStat.mtimeMs > CHECKPOINT_LOCK_STALE_MS;
            } catch (_) {
                removeStaleLock = true;
            }

            if (removeStaleLock) {
                try { fs.unlinkSync(lockFile); } catch (_) {}
            } else {
                Atomics.wait(waitBuffer, 0, 0, 5);
            }
        }
    }

    throw new Error(`Checkpoint lock timeout: ${lockFile}`);
}

function toLabel(company, index) {
    return company?.Name || company?.name || company?.Id || `#${index}`;
}

function chunk(items, size) {
    const output = [];
    for (let i = 0; i < items.length; i += size) {
        output.push(items.slice(i, i + size));
    }
    return output;
}

function getRecoverySkipIds() {
    return new Set(
        String(process.env.CRAWLER_SKIP_COMPANY_IDS || '')
            .split(',')
            .map(value => value.trim())
            .filter(Boolean)
    );
}

function getRecoveryEligibleIds() {
    const inlineIds = String(process.env.CRAWLER_RECOVERY_ELIGIBLE_COMPANY_IDS || '')
        .split(',')
        .map(value => value.trim())
        .filter(Boolean);
    if (inlineIds.length > 0) return new Set(inlineIds);

    const checkpointFile = process.env.PRODUCTION_RUN_CHECKPOINT_FILE;
    const runId = process.env.PRODUCTION_RUN_ID;
    if (!checkpointFile || !runId) return new Set();
    try {
        const checkpoint = JSON.parse(fs.readFileSync(checkpointFile, 'utf8'));
        if (checkpoint.runId !== runId) return new Set();
        return new Set((checkpoint.recoveryEligibleCompanyIds || []).map(String));
    } catch (_) {
        return new Set();
    }
}

async function updateCompanyCrawlStatusOrThrow(supabase, companyId, status) {
    const { error } = await supabase
        .from('companies')
        .update({ crawl_status: status })
        .eq('Id', companyId);
    if (error) throw error;
}

function filterRecoveryCompanies(companies) {
    if (process.env.CRAWLER_RECOVERY_MODE !== '1') {
        return { eligibleCompanies: companies, skipped: 0 };
    }

    const skipIds = getRecoverySkipIds();
    const recoveryEligibleIds = getRecoveryEligibleIds();
    const scopedCompanies = recoveryEligibleIds.size === 0
        ? []
        : companies.filter(company => recoveryEligibleIds.has(String(company?.Id ?? company?.id ?? '')));
    const eligibleCompanies = scopedCompanies.filter(company =>
        !skipIds.has(String(company?.Id ?? company?.id ?? ''))
    );
    return {
        eligibleCompanies,
        skipped: companies.length - eligibleCompanies.length
    };
}

function recordProductionRunCompanyCompleted(companyId) {
    const checkpointFile = process.env.PRODUCTION_RUN_CHECKPOINT_FILE;
    const runId = process.env.PRODUCTION_RUN_ID;
    if (!checkpointFile || !runId || companyId === undefined || companyId === null) return;

    let release;
    try {
        release = acquireCheckpointLock(checkpointFile);
        const checkpoint = JSON.parse(fs.readFileSync(checkpointFile, 'utf8'));
        if (checkpoint.runId !== runId) return;
        const completed = new Set((checkpoint.completedCompanyIds || []).map(String));
        completed.add(String(companyId));
        checkpoint.completedCompanyIds = [...completed];
        const tempFile = `${checkpointFile}.${process.pid}.tmp`;
        fs.writeFileSync(tempFile, JSON.stringify(checkpoint, null, 2));
        fs.renameSync(tempFile, checkpointFile);
    } catch (error) {
        console.warn(`   ⚠️ Run checkpoint update failed: ${error.message}`);
    } finally {
        release?.();
    }
}

async function runCompaniesInBatches(companies, {
    batchSize = 10,
    label = 'CRAWLER',
    handler
} = {}) {
    if (!Array.isArray(companies) || companies.length === 0) {
        return { processed: 0, batches: 0, jobs: 0 };
    }

    const { eligibleCompanies, skipped } = filterRecoveryCompanies(companies);

    if (skipped > 0) {
        console.log(`   ⏭️ [${label}] Recovery skip applied: ${skipped} companies`);
    }

    if (eligibleCompanies.length === 0) {
        return { processed: 0, batches: 0, jobs: 0, skipped };
    }

    const safeBatchSize = Number.isFinite(batchSize) && batchSize > 0 ? batchSize : 10;
    const batches = chunk(eligibleCompanies, safeBatchSize);
    let processed = 0;
    let jobs = 0;

    for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
        const batch = batches[batchIndex];
        const batchStart = processed + 1;
        const batchEnd = processed + batch.length;
        const batchNames = batch.map((company, offset) => `${batchStart + offset}. ${toLabel(company, batchStart + offset)}`).join(' | ');

        console.log(`\n🚚 [${label}] Batch ${batchIndex + 1}/${batches.length} | companies ${batchStart}-${batchEnd}/${eligibleCompanies.length}`);
        console.log(`   🧾 ${batchNames}`);

        const results = await Promise.all(batch.map(async (company, offset) => {
            const absoluteIndex = batchStart + offset;
            const companyLabel = toLabel(company, absoluteIndex);
            const startedAt = Date.now();
            console.log(`   ▶️ [${label}] ${absoluteIndex}/${companies.length} ${companyLabel} started`);

            try {
                const result = await handler(company, {
                    batchIndex: batchIndex + 1,
                    batchTotal: batches.length,
                    companyIndex: absoluteIndex,
                    companyTotal: companies.length
                });
                recordProductionRunCompanyCompleted(company?.Id ?? company?.id);

                const jobCount = Array.isArray(result?.jobs)
                    ? result.jobs.length
                    : Number.isFinite(result?.jobsFound)
                        ? result.jobsFound
                        : 0;
                jobs += jobCount;
                const status = result?.status || (jobCount > 0 ? 'completed' : 'no_jobs');
                console.log(`   ✅ [${label}] ${absoluteIndex}/${eligibleCompanies.length} ${companyLabel} done | status=${status} | jobs=${jobCount} | ${Date.now() - startedAt}ms`);
                return { company, result };
            } catch (error) {
                console.log(`   ❌ [${label}] ${absoluteIndex}/${eligibleCompanies.length} ${companyLabel} failed | ${error.message}`);
                return { company, error };
            }
        }));

        processed += results.length;
        console.log(`   📦 [${label}] Batch ${batchIndex + 1}/${batches.length} complete | processed=${processed}/${eligibleCompanies.length} | jobs=${jobs}`);
    }

    return { processed, batches: batches.length, jobs, skipped };
}

module.exports = {
    filterRecoveryCompanies,
    getRecoverySkipIds,
    getRecoveryEligibleIds,
    updateCompanyCrawlStatusOrThrow,
    recordProductionRunCompanyCompleted,
    runCompaniesInBatches
};
