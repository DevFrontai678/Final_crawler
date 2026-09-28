const test = require('node:test');
const assert = require('node:assert/strict');

const detector = require('../scripts/run-ats-detection');

const heinrichDetail =
    'https://www.heinrich-schmid.com/karriere/jobs/projektassistenz-gebaeudetechnik-m-w-d/';

test('Heinrich Schmid job detail URL is rejected as a career entry', () => {
    assert.equal(detector.isIndividualCareerJobUrl(heinrichDetail), true);
    assert.equal(detector.isCareerListingUrl(heinrichDetail), false);
    const candidates = detector.collectHomepageCandidates(
        `<a href="${heinrichDetail}">Projektassistenz</a>`,
        'https://www.heinrich-schmid.com/'
    );
    assert.deepEqual(
        candidates.map(candidate => candidate.url).sort(),
        [
            'https://www.heinrich-schmid.com/karriere/',
            'https://www.heinrich-schmid.com/karriere/jobs/',
        ]
    );
    assert.equal(candidates.some(candidate => candidate.url === heinrichDetail), false);
});

test('career listing roots remain valid', () => {
    for (const url of [
        'https://www.heinrich-schmid.com/karriere/',
        'https://www.heinrich-schmid.com/karriere/jobs/',
        'https://www.heinrich-schmid.com/karriere/stellenangebote/',
        'https://example.com/careers/',
        'https://example.com/jobs/',
    ]) {
        assert.equal(detector.isIndividualCareerJobUrl(url), false, url);
        assert.equal(detector.isCareerListingUrl(url), true, url);
    }
});

test('single-level career job slugs are rejected', () => {
    assert.equal(
        detector.isIndividualCareerJobUrl(
            'https://www.heinrich-schmid.com/karriere/it-systemadministrator-m-w-d/'
        ),
        true
    );
});

test('canonical listing candidates prefer the Heinrich Schmid career root', () => {
    assert.deepEqual(
        detector.canonicalCareerListingCandidates(heinrichDetail),
        [
            'https://www.heinrich-schmid.com/karriere/',
            'https://www.heinrich-schmid.com/karriere/jobs/',
        ]
    );
});
