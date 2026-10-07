'use strict';

const assert = require('assert');
const {
    buildClassificationPrompt,
    classifyJobWithLLM,
    validateClassification,
    CLASSIFICATION_RESPONSE_FORMAT,
    VALID_SENIORITY_LEVELS,
    VALID_SUPPORT_LEVELS,
    VALID_EMPLOYMENT_TYPES,
    buildSafeClassificationFallback
} = require('../src/ai/job-classifier');
const { preserveAuthoritativeFieldsForUpsert, resolveJobLocation } = require('../src/utils/job-enrichment');

function mockClient(payloads) {
    let index = 0;
    const defaults = {
        seniority_level: 'Mid Level',
        support_level: 'Not Applicable',
        employment_type: 'Unknown'
    };
    return {
        chat: {
            completions: {
                create: async () => ({
                    choices: [{
                        message: {
                            content: JSON.stringify({
                                ...defaults,
                                ...payloads[Math.min(index++, payloads.length - 1)]
                            })
                        }
                    }]
                })
            }
        }
    };
}

async function run() {
    const cases = [
        ['Fully remote Software Engineer position', 'remote', null],
        ['100% remote position', 'remote', null],
        ['Work from anywhere', 'remote', null],
        ['Remote within Germany', 'remote', null],
        ['Hybrid role in Berlin', 'hybrid', 'Berlin'],
        ['Hybrid role with 2 days home office', 'hybrid', null],
        ['2 days remote per week', 'hybrid', null],
        ['Office based with remote work possible 2 days per week', 'hybrid', null],
        ['3 days in office and 2 days remote', 'hybrid', null],
        ['Office + remote', 'hybrid', null],
        ['Remote hybrid possible with office days', 'hybrid', null],
        ['Remote work not available', 'onsite', null],
        ['Office based role', 'onsite', null],
        ['Remote monitoring tools', 'onsite', null],
        ['Remote access systems', 'onsite', null],
        ['Hybrid position based in Hamburg', 'hybrid', 'Hamburg'],
        ['2 days remote, 3 days in Frankfurt office', 'hybrid', 'Frankfurt'],
        ['Fully remote. Occasional team meetings in Munich.', 'remote', null],
        ['Headquarters in Munich. Position based in Berlin.', 'onsite', 'Berlin'],
        ['Customers are located in Berlin and Munich. Employee works from Frankfurt.', 'onsite', 'Frankfurt'],
        ['Fully remote within Germany. Headquarters are in Munich.', 'remote', null],
        ['Homeoffice 2 Tage pro Woche', 'hybrid', null],
        ['100 % Homeoffice', 'remote', null],
        ['Mobiles Arbeiten möglich', 'hybrid', null],
        ['Präsenz am Standort erforderlich', 'onsite', null]
        ,['Insufficient employment arrangement information', 'unknown', null]
    ];

    const prompt = buildClassificationPrompt({
        company_name: 'Example GmbH',
        title: 'Software Engineer',
        raw_description: cases[0][0],
        crawler_location: 'Berlin',
        company_hq: 'Munich'
    });
    assert.match(prompt, /actual location/i);
    assert.match(prompt, /technical terms/i);
    assert.match(prompt, /company headquarters/i);
    assert.match(prompt, /clean and normalize the supplied location evidence/i);
    assert.match(prompt, /working hours, weekly hours, benefits, contact information/i);
    assert.match(prompt, /multiple valid locations/i);
    assert.match(prompt, /seniority_level MUST be exactly one of/i);
    assert.match(prompt, /support_level MUST be exactly one of/i);
    assert.match(prompt, /complete title, description, responsibilities/i);
    assert.match(prompt, /financial reports, company policies, quality documents/i);
    assert.deepStrictEqual(
        CLASSIFICATION_RESPONSE_FORMAT.json_schema.schema.properties.seniority_level.enum,
        [...VALID_SENIORITY_LEVELS]
    );
    assert.deepStrictEqual(
        CLASSIFICATION_RESPONSE_FORMAT.json_schema.schema.properties.support_level.enum,
        [...VALID_SUPPORT_LEVELS]
    );
    assert.deepStrictEqual(
        CLASSIFICATION_RESPONSE_FORMAT.json_schema.schema.properties.employment_type.enum,
        [...VALID_EMPLOYMENT_TYPES]
    );

    assert.strictEqual(validateClassification({
        is_job: false,
        is_relevant: false,
        remote_type: 'unknown',
        seniority_level: 'Senior',
        support_level: 'Not Applicable',
        employment_type: 'Unknown'
    }).is_job, false);
    assert.throws(
        () => validateClassification({
            is_job: 'false',
            remote_type: 'unknown',
            seniority_level: 'Senior',
            support_level: 'Not Applicable',
            employment_type: 'Unknown'
        }),
        /invalid is_job/
    );

    const correctionPrompts = [];
    let correctionAttempt = 0;
    const corrected = await classifyJobWithLLM(
        { company_name: 'ATIS systems GmbH', external_job_id: '2733664', title: 'Systems Engineer', raw_description: 'A complete job posting.' },
        {
            clientOverride: {
                chat: { completions: { create: async request => {
                    correctionPrompts.push(request.messages[1].content);
                    correctionAttempt++;
                    const seniority = correctionAttempt === 1 ? 'Intermediate' : 'Mid Level';
                    return { choices: [{ message: { content: JSON.stringify({
                        is_job: true,
                        is_relevant: true,
                        seniority_level: seniority,
                        support_level: 'Not Applicable',
                        employment_type: 'Full Time',
                        remote_type: 'onsite',
                        job_location: null,
                        location_city: null,
                        location_country: null
                    }) } }] };
                } } }
            }
        }
    );
    assert.equal(corrected.ok, true);
    assert.equal(corrected.data.seniority_level, 'Mid Level');
    assert.equal(correctionAttempt, 2);
    assert.match(correctionPrompts[1], /invalid seniority_level/i);
    assert.match(correctionPrompts[1], /Mid Level/);

    let fallbackAttempts = 0;
    const classificationLogs = [];
    const originalConsoleError = console.error;
    const originalConsoleWarn = console.warn;
    console.error = (...args) => classificationLogs.push(args.join(' '));
    console.warn = (...args) => classificationLogs.push(args.join(' '));
    let fallback;
    try {
        fallback = await classifyJobWithLLM(
            { company_name: 'ATIS systems GmbH', external_job_id: '2733664', title: 'Systems Engineer', raw_description: 'A complete job posting.' },
            {
                clientOverride: {
                    chat: { completions: { create: async () => {
                        fallbackAttempts++;
                        return { choices: [{ message: { content: JSON.stringify({
                            is_job: true,
                            is_relevant: true,
                            seniority_level: 'Intermediate',
                            support_level: 'Not Applicable',
                            employment_type: 'Full Time',
                            remote_type: 'onsite',
                            job_location: null,
                            location_city: null,
                            location_country: null
                        }) } }] };
                    } } }
                }
            }
        );
    } finally {
        console.error = originalConsoleError;
        console.warn = originalConsoleWarn;
    }
    assert.equal(fallback.ok, true);
    assert.equal(fallback.fallback, true);
    assert.equal(fallback.data.is_job, true);
    assert.equal(fallback.data.seniority_level, null);
    assert.equal(fallback.data.employment_type, 'Full Time');
    assert.equal(fallbackAttempts, 3);
    assert.match(classificationLogs.join('\n'), /invalid_field=seniority_level/);
    assert.match(classificationLogs.join('\n'), /invalid_value="Intermediate"/);
    assert.match(classificationLogs.join('\n'), /attempts=3/);
    assert.match(classificationLogs.join('\n'), /LLM CLASSIFY FALLBACK/);

    const invalidSupportFallback = buildSafeClassificationFallback({
        is_job: true,
        is_relevant: true,
        seniority_level: 'Senior',
        support_level: 'L4',
        employment_type: 'Full Time',
        remote_type: 'unknown'
    });
    assert.equal(invalidSupportFallback.support_level, null);
    assert.equal(invalidSupportFallback.seniority_level, 'Senior');

    const invalidEmploymentFallback = buildSafeClassificationFallback({
        is_job: true,
        is_relevant: true,
        seniority_level: 'Senior',
        support_level: 'Not Applicable',
        employment_type: 'Permanent Employee',
        remote_type: 'unknown'
    });
    assert.equal(invalidEmploymentFallback.employment_type, 'Unknown');

    const missingFieldFallback = buildSafeClassificationFallback({
        is_job: true,
        is_relevant: true,
        remote_type: 'unknown'
    });
    assert.strictEqual(missingFieldFallback.seniority_level, null);
    assert.strictEqual(missingFieldFallback.support_level, null);
    assert.strictEqual(missingFieldFallback.employment_type, 'Unknown');

    for (const [description, remoteType, location] of cases) {
        const result = await classifyJobWithLLM(
            { company_name: 'Example GmbH', external_job_id: description, raw_description: description },
            {
                clientOverride: mockClient([{
                    remote_type: remoteType,
                    job_location: location,
                    location_city: location,
                    location_country: location ? 'Germany' : null
                }])
            }
        );
        assert.strictEqual(result.ok, true);
        assert.strictEqual(result.data.remote_type, remoteType);
        assert.strictEqual(result.data.job_location, location);
    }

    assert.throws(
        () => validateClassification({ remote_type: 'REMOTE' }),
        /invalid remote_type/
    );
    assert.strictEqual(validateClassification({
        remote_type: 'unknown', seniority_level: 'Senior',
        support_level: '3rd Level', employment_type: 'Full Time'
    }).support_level, '3rd Level');
    for (const value of ['Entry Level', 'Junior', 'Mid Level', 'Senior', 'Lead', 'Manager', 'Director', 'Executive']) {
        assert.strictEqual(validateClassification({
            remote_type: 'unknown', seniority_level: value,
            support_level: 'Not Applicable', employment_type: 'Unknown'
        }).seniority_level, value);
    }
    for (const value of ['1st Level', '2nd Level', '3rd Level', 'Multi Level', 'Not Applicable']) {
        assert.strictEqual(validateClassification({
            remote_type: 'unknown', seniority_level: 'Senior',
            support_level: value, employment_type: 'Unknown'
        }).support_level, value);
    }
    for (const value of ['Full Time', 'Part Time', 'Internship', 'Apprenticeship', 'Working Student', 'Contract', 'Temporary', 'Freelance', 'Other', 'Unknown']) {
        assert.strictEqual(validateClassification({
            remote_type: 'unknown', seniority_level: 'Senior',
            support_level: 'Not Applicable', employment_type: value
        }).employment_type, value);
    }
    assert.throws(
        () => validateClassification({
            remote_type: 'unknown', seniority_level: 'senior',
            support_level: 'Not Applicable', employment_type: 'Full Time'
        }),
        /invalid seniority_level/
    );

    const longTailCases = [
        [5000, '2 days remote per week', 'hybrid', null],
        [6000, 'Position based in Frankfurt', 'unknown', 'Frankfurt'],
        [8000, 'Position based in Berlin', 'onsite', 'Berlin']
    ];
    for (const [prefixLength, tail, remoteType, location] of longTailCases) {
        let capturedPrompt = '';
        const result = await classifyJobWithLLM(
            {
                company_name: 'Example GmbH',
                external_job_id: `long-tail-${prefixLength}`,
                classification_description: 'a'.repeat(prefixLength) + ` ${tail}`
            },
            {
                clientOverride: {
                    chat: { completions: { create: async request => {
                        capturedPrompt = request.messages[1].content;
                        return { choices: [{ message: { content: JSON.stringify({
                            seniority_level: 'Senior',
                            support_level: '3rd Level',
                            employment_type: 'Full Time',
                            remote_type: remoteType,
                            job_location: location
                        }) } }] };
                    } } }
                }
            }
        );
        assert.strictEqual(result.ok, true);
        assert.strictEqual(result.data.remote_type, remoteType);
        assert.strictEqual(result.data.job_location, location);
        assert.match(capturedPrompt, new RegExp(tail));
    }

    const retryResult = await classifyJobWithLLM(
        { company_name: 'Example GmbH', external_job_id: 'retry', raw_description: 'Hybrid role in Berlin' },
        {
            clientOverride: {
                chat: {
                    completions: {
                        create: async () => {
                            throw new Error('temporary failure');
                        }
                    }
                }
            }
        }
    );
    assert.strictEqual(retryResult.ok, false);
    assert.strictEqual(retryResult.attempts, 3);

    const existingJobClient = {
        from: () => ({
            select: () => ({
                eq: () => ({
                    eq: () => ({
                        maybeSingle: async () => ({
                            data: {
                                remote_type: 'hybrid',
                                location: 'Berlin',
                                location_lat: 52.52,
                                location_lng: 13.405
                            },
                            error: null
                        })
                    })
                })
            })
        })
    };
    const preserved = await preserveAuthoritativeFieldsForUpsert(existingJobClient, {
        company_id: 'company',
        external_job_id: 'job',
        remote_type: null,
        location: 'Munich',
        location_lat: null,
        location_lng: null,
        _classification_source: 'failed',
        _location_source: 'company_hq'
    });
    assert.strictEqual(preserved.remote_type, 'hybrid');
    assert.strictEqual(preserved.location, 'Berlin');
    assert.strictEqual(preserved.location_lat, 52.52);
    assert.strictEqual(preserved._classification_source, undefined);

    const unknownLocation = await resolveJobLocation({ location: null }, { companyHq: null });
    assert.strictEqual(unknownLocation.location, 'Unknown');

    console.log('job-classifier tests passed');
}

run().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
