require('dotenv').config();

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

async function runCompaniesInBatches(companies, {
    batchSize = 10,
    label = 'CRAWLER',
    handler
} = {}) {
    if (!Array.isArray(companies) || companies.length === 0) {
        return { processed: 0, batches: 0, jobs: 0 };
    }

    const safeBatchSize = Number.isFinite(batchSize) && batchSize > 0 ? batchSize : 10;
    const batches = chunk(companies, safeBatchSize);
    let processed = 0;
    let jobs = 0;

    for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
        const batch = batches[batchIndex];
        const batchStart = processed + 1;
        const batchEnd = processed + batch.length;
        const batchNames = batch.map((company, offset) => `${batchStart + offset}. ${toLabel(company, batchStart + offset)}`).join(' | ');

        console.log(`\n🚚 [${label}] Batch ${batchIndex + 1}/${batches.length} | companies ${batchStart}-${batchEnd}/${companies.length}`);
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

                const jobCount = Array.isArray(result?.jobs)
                    ? result.jobs.length
                    : Number.isFinite(result?.jobsFound)
                        ? result.jobsFound
                        : 0;
                jobs += jobCount;
                const status = result?.status || (jobCount > 0 ? 'completed' : 'no_jobs');
                console.log(`   ✅ [${label}] ${absoluteIndex}/${companies.length} ${companyLabel} done | status=${status} | jobs=${jobCount} | ${Date.now() - startedAt}ms`);
                return { company, result };
            } catch (error) {
                console.log(`   ❌ [${label}] ${absoluteIndex}/${companies.length} ${companyLabel} failed | ${error.message}`);
                return { company, error };
            }
        }));

        processed += results.length;
        console.log(`   📦 [${label}] Batch ${batchIndex + 1}/${batches.length} complete | processed=${processed}/${companies.length} | jobs=${jobs}`);
    }

    return { processed, batches: batches.length, jobs };
}

module.exports = {
    runCompaniesInBatches
};
