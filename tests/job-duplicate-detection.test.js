'use strict';

const assert = require('assert');
const {
    deterministicDuplicate,
    findDuplicateJob,
    resolveDuplicateIdentity,
    DUPLICATE_CONFIDENCE_THRESHOLD
} = require('../src/utils/job-duplicate-detection');
const {
    classifyJobDuplicateWithLLM,
    validateDuplicateClassification
} = require('../src/ai/job-classifier');

function row(overrides = {}) {
    return {
        company_id: 'company-1',
        external_job_id: 'existing-job-1',
        title: 'Application Consultant',
        raw_description: 'Implement and support enterprise application solutions for customers.',
        location: 'Berlin',
        employment_type: 'Full Time',
        apply_url: 'https://example.test/en/career/job/application-consultant.html',
        ats_source: 'custom',
        _dedupe_metadata: {
            source_url: 'https://example.test/en/career/job/application-consultant.html',
            canonical_url: null,
            alternate_urls: [],
            stable_provider_id: false,
            source_job_id: null,
            requisition_id: null
        },
        ...overrides
    };
}

function fakeSupabase(existingRows) {
    const state = { queried: false, limit: null };
    const supabase = {
        state,
        from: () => ({
            select: () => ({
                eq: () => ({
                    order: () => ({
                        limit: async count => {
                            state.queried = true;
                            state.limit = count;
                            return { data: existingRows.slice(0, count), error: null };
                        }
                    })
                })
            })
        })
    };
    return supabase;
}

async function run() {
    const english = row();
    const german = row({
        apply_url: 'https://example.test/de/karriere/job/application-consultant.html',
        _dedupe_metadata: {
            ...english._dedupe_metadata,
            source_url: 'https://example.test/de/karriere/job/application-consultant.html'
        }
    });
    assert.strictEqual(deterministicDuplicate(english, german), true);

    assert.strictEqual(deterministicDuplicate(english, row({ location: 'Munich' })), false);
    assert.strictEqual(deterministicDuplicate(english, row({
        title: 'Application Consultant',
        raw_description: 'Implement and support enterprise application solutions for customers.',
        apply_url: 'https://example.test/jobs/application-consultant-2.html',
        _dedupe_metadata: { ...english._dedupe_metadata, source_url: 'https://example.test/jobs/application-consultant-2.html' }
    })), false);

    const stable = row({
        _dedupe_metadata: { ...english._dedupe_metadata, stable_provider_id: true, source_job_id: 'ATS-123' }
    });
    const stableDb = fakeSupabase([english]);
    assert.strictEqual(await findDuplicateJob(stableDb, stable), null);
    assert.strictEqual(stableDb.state.queried, false);

    const beyondPreviousLimit = Array.from({ length: 51 }, (_, index) => row({
        external_job_id: `distractor-${index}`,
        title: `Different Vacancy ${index}`,
        location: 'Munich',
        apply_url: `https://example.test/jobs/different-vacancy-${index}.html`
    }));
    beyondPreviousLimit.push(english);
    const expandedCandidateDb = fakeSupabase(beyondPreviousLimit);
    const expandedCandidate = await findDuplicateJob(expandedCandidateDb, german);
    assert.strictEqual(expandedCandidate.external_job_id, english.external_job_id);
    assert.strictEqual(expandedCandidateDb.state.limit, 200);

    assert.strictEqual(await findDuplicateJob(fakeSupabase([row({ company_id: 'company-2' })]), german), null);

    const languages = [english, german, row({
        apply_url: 'https://example.test/fr/carriere/job/application-consultant.html',
        _dedupe_metadata: { ...english._dedupe_metadata, source_url: 'https://example.test/fr/carriere/job/application-consultant.html' }
    })];
    for (const language of languages.slice(1)) {
        const resolved = await resolveDuplicateIdentity(fakeSupabase([english]), language);
        assert.strictEqual(resolved.external_job_id, english.external_job_id);
    }

    const fallbackExisting = row({
        apply_url: 'https://example.test/jobs/application-consultant.html',
        _dedupe_metadata: { ...english._dedupe_metadata, source_url: 'https://example.test/jobs/application-consultant.html' }
    });
    let llmCalls = 0;
    const fallback = row({
        title: 'Berater für Anwendungen',
        apply_url: 'https://example.test/jobs/application-consultant-de.html',
        _dedupe_metadata: { ...english._dedupe_metadata, source_url: 'https://example.test/jobs/application-consultant-de.html' }
    });
    const highConfidence = await findDuplicateJob(fakeSupabase([fallbackExisting]), fallback, {
        classifier: async () => {
            llmCalls++;
            return { ok: true, data: { is_same_job: true, confidence: DUPLICATE_CONFIDENCE_THRESHOLD, reason: 'same vacancy' } };
        }
    });
    assert.strictEqual(highConfidence.external_job_id, fallbackExisting.external_job_id);
    assert.strictEqual(llmCalls, 1);

    const lowConfidence = await findDuplicateJob(fakeSupabase([fallbackExisting]), fallback, {
        classifier: async () => ({
            ok: true,
            data: { is_same_job: true, confidence: 0.7, reason: 'uncertain' }
        })
    });
    assert.strictEqual(lowConfidence, null);

    let duplicatePrompt = '';
    const llmResult = await classifyJobDuplicateWithLLM(
        { title: 'Application Consultant', raw_description: 'Job A', location: 'Berlin' },
        { title: 'Berater für Anwendungen', raw_description: 'Job B', location: 'Berlin' },
        {
            clientOverride: {
                chat: { completions: { create: async request => {
                    duplicatePrompt = request.messages[1].content;
                    return { choices: [{ message: { content: JSON.stringify({
                        is_same_job: true, confidence: 0.95, reason: 'same vacancy'
                    }) } }] };
                } } }
            }
        }
    );
    assert.strictEqual(llmResult.ok, true);
    assert.match(duplicatePrompt, /JOB A:/);
    assert.match(duplicatePrompt, /JOB B:/);
    assert.match(duplicatePrompt, /different locations/i);

    assert.throws(
        () => validateDuplicateClassification({ is_same_job: true, confidence: 1.1, reason: 'bad' }),
        /invalid duplicate confidence/
    );
    assert.strictEqual(validateDuplicateClassification({
        is_same_job: false, confidence: 0.99, reason: 'different location'
    }).is_same_job, false);

    console.log('job duplicate detection tests passed');
}

run().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
