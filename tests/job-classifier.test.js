'use strict';

const assert = require('assert');
const {
    buildClassificationPrompt,
    classifyJobWithLLM,
    validateClassification
} = require('../src/ai/job-classifier');
const { preserveAuthoritativeFieldsForUpsert, resolveJobLocation } = require('../src/utils/job-enrichment');

function mockClient(payloads) {
    let index = 0;
    return {
        chat: {
            completions: {
                create: async () => ({
                    choices: [{
                        message: {
                            content: JSON.stringify(payloads[Math.min(index++, payloads.length - 1)])
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
    assert.strictEqual(validateClassification({ remote_type: 'unknown' }).remote_type, 'unknown');

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
                        return { choices: [{ message: { content: JSON.stringify({ remote_type: remoteType, job_location: location }) } }] };
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
