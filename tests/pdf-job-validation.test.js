'use strict';

const assert = require('node:assert/strict');
const { classifyJobWithLLM } = require('../src/ai/job-classifier');

process.env.CRAWLER_TEST_MODE = '1';
process.env.CRAWLER_NO_RUNTIME = '1';

const crawler = require('../src/crawlers/custom-crawler-queue');

assert.equal(crawler.extractRawJobFromPdf('', 'https://example.test/document.pdf').valid, false);
assert.ok(crawler.extractRawJobFromPdf('', 'https://example.test/document.pdf').reasons.includes('insufficient_pdf_content'));
assert.doesNotThrow(() => crawler.extractRawJobFromPdf(null, 'https://example.test/unreadable.pdf'));
assert.equal(crawler.extractRawJobFromPdf('Short but readable vacancy text', 'https://example.test/unreadable.pdf').rawDescription, 'Short but readable vacancy text');

const pdfRawJob = {
    title: 'Example vacancy',
    rawDescription: 'A complete vacancy description with responsibilities and requirements. '.repeat(20),
    score: 0
};

assert.equal(
    crawler.validateStructuredJob(
        { is_job: false, reason: 'financial report', is_relevant: true, cleaned_title: 'Annual report' },
        pdfRawJob,
        'Example GmbH',
        { isPdf: true }
    ).ok,
    false
);

assert.equal(
    crawler.validateStructuredJob(
        { is_job: true, is_relevant: true, cleaned_title: 'Example vacancy' },
        pdfRawJob,
        'Example GmbH',
        { isPdf: true }
    ).ok,
    true
);

assert.equal(
    crawler.validateStructuredJob(
        { is_job: true, is_relevant: false, cleaned_title: 'Example vacancy' },
        pdfRawJob,
        'Example GmbH',
        { isPdf: true }
    ).ok,
    true
);

console.log('pdf job validation tests passed');

function classifierClient(payload, prompts) {
    return {
        chat: {
            completions: {
                create: async request => {
                    prompts.push(request.messages[1].content);
                    return { choices: [{ message: { content: JSON.stringify({
                        is_job: payload.is_job,
                        is_relevant: payload.is_job,
                        seniority_level: 'Mid Level',
                        support_level: 'Not Applicable',
                        employment_type: 'Full Time',
                        remote_type: 'onsite',
                        job_location: null,
                        location_city: null,
                        location_country: null
                    }) } }] };
                }
            }
        }
    };
}

(async () => {
    const vacancyPrompts = [];
    const vacancy = await classifyJobWithLLM({
        company_name: 'Example GmbH',
        company_website: 'https://example.test',
        pdf_url: 'https://example.test/careers/platform-engineer.pdf',
        title: 'Platform Engineer',
        raw_description: 'We are hiring a Platform Engineer. Responsibilities include operating cloud infrastructure, improving deployment automation, and participating in on-call support. Requirements include production experience with Linux and Kubernetes.'
    }, { clientOverride: classifierClient({ is_job: true }, vacancyPrompts) });
    assert.equal(vacancy.ok, true);
    assert.equal(vacancy.data.is_job, true);
    assert.match(vacancyPrompts[0], /Platform Engineer/);
    assert.match(vacancyPrompts[0], /operating cloud infrastructure/);

    const nonJobPrompts = [];
    const nonJob = await classifyJobWithLLM({
        company_name: 'Example GmbH',
        company_website: 'https://example.test',
        pdf_url: 'https://example.test/company/annual-report.pdf',
        title: 'Annual Report',
        raw_description: 'This annual financial report describes revenue, expenses, assets, liabilities, and audited company results for the reporting year. It contains no employment vacancy or application instructions.'
    }, { clientOverride: classifierClient({ is_job: false }, nonJobPrompts) });
    assert.equal(nonJob.ok, true);
    assert.equal(nonJob.data.is_job, false);
    assert.match(nonJobPrompts[0], /Annual Report/);
    assert.match(nonJobPrompts[0], /financial report/);

    for (const documentType of ['company policy', 'brochure', 'certificate', 'recipe', 'technical document', 'informational document']) {
        const prompts = [];
        const result = await classifyJobWithLLM({
            company_name: 'Example GmbH',
            company_website: 'https://example.test',
            pdf_url: `https://example.test/docs/${documentType.replace(/ /g, '-')}.pdf`,
            title: documentType,
            raw_description: `This ${documentType} contains general company information and no specific employment vacancy.`
        }, { clientOverride: classifierClient({ is_job: false }, prompts) });
        assert.equal(result.data.is_job, false);
        assert.equal(prompts.length, 1);
    }

    const { structureJobWithGPT } = crawler;
    const responses = {
        is_job: true,
        is_relevant: true,
        cleaned_title: 'Platform Engineer',
        skills: ['Linux'],
        seniority_level: 'Mid Level',
        support_level: 'Not Applicable',
        employment_type: 'Full Time',
        remote_type: 'onsite',
        job_location: null,
        location_city: null,
        location_country: null
    };
    let htmlCalls = 0;
    const oneCallClient = {
        chat: { completions: { create: async () => {
            htmlCalls++;
            return { choices: [{ message: { content: JSON.stringify(responses) } }] };
        } } }
    };
    const htmlRaw = {
        title: 'Platform Engineer',
        rawDescription: 'Short extracted HTML content',
        companyName: 'Example GmbH',
        companyWebsiteUrl: 'https://example.test',
        url: 'https://example.test/careers/platform-engineer',
        companyHq: null
    };
    const htmlClassification = await structureJobWithGPT(htmlRaw, undefined, undefined, { clientOverride: oneCallClient });
    assert.equal(htmlCalls, 1);
    assert.equal(crawler.validateStructuredJob(htmlClassification, htmlRaw, 'Example GmbH', { isHtml: true }).ok, true);

    let pdfCalls = 0;
    const pdfClient = {
        chat: { completions: { create: async () => {
            pdfCalls++;
            return { choices: [{ message: { content: JSON.stringify(responses) } }] };
        } } }
    };
    const pdfRaw = {
        title: 'Platform Engineer',
        rawDescription: 'Short extracted PDF content',
        companyName: 'Example GmbH',
        companyWebsiteUrl: 'https://example.test',
        pdfUrl: 'https://example.test/careers/platform-engineer.pdf',
        url: 'https://example.test/careers/platform-engineer.pdf',
        companyHq: null
    };
    const pdfClassification = await structureJobWithGPT(pdfRaw, undefined, undefined, { clientOverride: pdfClient });
    assert.equal(pdfCalls, 1);
    assert.equal(crawler.validateStructuredJob(pdfClassification, pdfRaw, 'Example GmbH', { isPdf: true }).ok, true);

    console.log('pdf classifier decision tests passed');
})().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
