'use strict';

const assert = require('assert');
const {
    classifyJobWithLLM,
    buildClassificationPrompt
} = require('../src/ai/job-classifier');
const {
    enrichJobRows,
    enrichJobForStorage,
    preserveAuthoritativeFieldsForUpsert
} = require('../src/utils/job-enrichment');

function classifierClient(payload, seen) {
    return {
        chat: {
            completions: {
                create: async request => {
                    seen.push(request.messages[1].content);
                    return { choices: [{ message: { content: JSON.stringify({
                        is_job: true,
                        is_relevant: true,
                        skills: ['systems administration'],
                        cleaned_title: 'Example role',
                        remote_type: 'unknown',
                        job_location: null,
                        location_city: null,
                        location_country: null,
                        ...payload
                    }) } }] };
                }
            }
        }
    };
}

async function run() {
    const cases = [
        ['Senior System Administrator', 'Senior', 'Not Applicable', 'Full Time'],
        ['Junior IT Support Specialist', 'Junior', '1st Level', 'Full Time'],
        ['Infrastructure Engineer', 'Mid Level', '2nd Level', 'Contract'],
        ['3rd Level Support Engineer', 'Senior', '3rd Level', 'Full Time'],
        ['IT Support Mitarbeiter 1st Level', 'Entry Level', '1st Level', 'Full Time'],
        ['Team Lead Infrastructure', 'Lead', 'Not Applicable', 'Full Time'],
        ['Platform ownership with complex systems and architecture responsibility', 'Senior', '3rd Level', 'Full Time'],
        ['Service desk first contact and password troubleshooting', 'Entry Level', '1st Level', 'Part Time'],
        ['Escalated incidents and deep technical troubleshooting', 'Mid Level', '2nd Level', 'Temporary'],
        ['Advanced infrastructure escalation and expert troubleshooting', 'Senior', '3rd Level', 'Freelance'],
        ['Support across first, second, and third line', 'Senior', 'Multi Level', 'Full Time'],
        ['Finance Analyst', 'Mid Level', 'Not Applicable', 'Working Student']
    ];

    for (const [title, seniority, support, employment] of cases) {
        const seen = [];
        const result = await classifyJobWithLLM({
            title,
            raw_description: `${title}\nResponsibilities, requirements, qualifications, skills, experience, and scope are supplied here.`,
            responsibilities: 'Own and operate the systems described in the posting.',
            requirements: 'Relevant professional experience and technical skills.',
            qualifications: 'Relevant qualifications.',
            skills: ['Linux', 'incident response'],
            experience_requirements: 'Several years where appropriate.',
            employment_metadata: employment
        }, { clientOverride: classifierClient({
            seniority_level: seniority,
            support_level: support,
            employment_type: employment
        }, seen) });
        assert.strictEqual(result.ok, true);
        assert.strictEqual(result.data.seniority_level, seniority);
        assert.strictEqual(result.data.support_level, support);
        assert.strictEqual(result.data.employment_type, employment);
        assert.strictEqual(seen.length, 1);
    }

    const contextPrompt = buildClassificationPrompt({
        title: 'Infrastructure Engineer',
        company_name: 'Example GmbH',
        company_website: 'https://example.test',
        raw_description: 'Full posting text',
        responsibilities: ['own production infrastructure', 'resolve escalations'],
        requirements: ['incident response'],
        qualifications: ['degree or equivalent'],
        skills: ['Linux', 'networking'],
        experience_requirements: '5+ years',
        employment_metadata: { type: 'Full Time' },
        metadata: { source: 'ATS', level: 'not stated' }
    });
    for (const field of ['Responsibilities:', 'Requirements:', 'Qualifications:', 'Skills:', 'Experience requirements:', 'Employment metadata:', 'Other job metadata:']) {
        assert.match(contextPrompt, new RegExp(field.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
    }
    assert.match(contextPrompt, /Company website: https:\/\/example\.test/);
    assert.match(contextPrompt, /PDF URL: none/);

    let llmCalls = 0;
    const rows = await enrichJobRows([
        { company_id: 'company', external_job_id: 'one', title: 'Senior Administrator', raw_description: 'A complete job posting.', posted_at: '2025-01-15T00:00:00.000Z' },
        { company_id: 'company', external_job_id: 'two', title: 'Support Specialist', raw_description: 'Another complete job posting.' }
    ], {
        clientOverride: classifierClient({
            seniority_level: 'Senior',
            support_level: '2nd Level',
            employment_type: 'Full Time'
        }, { push: () => { llmCalls++; } }),
        companyId: 'company',
        companyName: 'Example GmbH'
    });
    assert.strictEqual(rows.length, 2);
    assert.strictEqual(rows[0].seniority_level, 'Senior');
    assert.strictEqual(rows[0].support_level, '2nd Level');
    assert.strictEqual(rows[0].employment_type, 'Full Time');
    assert.strictEqual(rows[1].support_level, '2nd Level');

    const saved = [];
    const supabase = {
        from: () => ({
            select: () => ({
                eq: () => ({
                    eq: () => ({
                        maybeSingle: async () => ({ data: null, error: null })
                    })
                })
            })
        })
    };
    saved.push(await preserveAuthoritativeFieldsForUpsert(supabase, rows[0]));
    assert.strictEqual(saved[0].seniority_level, 'Senior');
    assert.strictEqual(saved[0].support_level, '2nd Level');
    assert.strictEqual(saved[0].employment_type, 'Full Time');
    assert.strictEqual(saved[0].posted_at, '2025-01-15T00:00:00.000Z');

    const fallbackRows = await enrichJobRows([
        {
            company_id: 'company',
            external_job_id: 'fallback',
            title: 'Existing role',
            raw_description: 'A complete job posting.',
            seniority_level: 'Senior',
            support_level: '2nd Level',
            employment_type: 'Full Time'
        }
    ], {
        clientOverride: {
            chat: { completions: { create: async () => ({ choices: [{ message: { content: JSON.stringify({
                is_job: true,
                is_relevant: true,
                seniority_level: 'Intermediate',
                support_level: 'Not Applicable',
                employment_type: 'Full Time',
                remote_type: 'unknown',
                job_location: null,
                location_city: null,
                location_country: null
            }) } }] }) } }
        },
        companyId: 'company',
        companyName: 'Example GmbH'
    });
    assert.strictEqual(fallbackRows[0].seniority_level, 'Senior');
    assert.strictEqual(fallbackRows[0].support_level, 'Not Applicable');
    assert.strictEqual(fallbackRows[0].employment_type, 'Full Time');

    let unexpectedSecondClassification = 0;
    const acceptedClassification = {
        ok: true,
        data: {
            is_job: true,
            is_relevant: true,
            seniority_level: 'Lead',
            support_level: '3rd Level',
            employment_type: 'Contract',
            remote_type: 'hybrid',
            job_location: 'Berlin',
            location_city: 'Berlin',
            location_country: 'Germany'
        }
    };
    const reused = await enrichJobForStorage({
        company_id: 'company',
        company_name: 'Example GmbH',
        external_job_id: 'reused-classification',
        title: 'Existing role',
        raw_description: 'A complete job posting.'
    }, {
        classification: acceptedClassification,
        clientOverride: {
            chat: { completions: { create: async () => {
                unexpectedSecondClassification++;
                throw new Error('classification must be reused');
            } } }
        }
    });
    assert.strictEqual(unexpectedSecondClassification, 0);
    assert.strictEqual(reused.seniority_level, 'Lead');
    assert.strictEqual(reused.support_level, '3rd Level');
    assert.strictEqual(reused.employment_type, 'Contract');

    console.log('job-enrichment classification tests passed');
}

run().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
