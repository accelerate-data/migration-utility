const assert = require('node:assert/strict');
const test = require('node:test');

const checkCompletionClaimGuidance = require('./check-completion-claim-guidance');

function run(output, vars) {
  return checkCompletionClaimGuidance(output, { vars });
}

test('passes when status, expected terms, and evidence are present', () => {
  const result = run(
    JSON.stringify({
      status: 'downgraded',
      wording: 'Focused pytest and markdownlint passed.',
      evidence_checked: ['evidence/partial-verification.txt'],
      reason: 'Full verification evidence is missing.',
    }),
    {
      expected_status: 'downgraded',
      expected_wording_terms: 'focused pytest,markdownlint',
      expected_evidence_terms: 'partial-verification',
      expected_reason_terms: 'missing',
      forbidden_terms: 'all verification passed',
    },
  );

  assert.equal(result.pass, true);
});

test('fails when expected wording appears only in reason', () => {
  const result = run(
    JSON.stringify({
      status: 'downgraded',
      wording: 'Some checks passed.',
      evidence_checked: ['evidence/partial-verification.txt'],
      reason: 'Focused pytest and markdownlint passed, but other evidence is missing.',
    }),
    {
      expected_status: 'downgraded',
      expected_wording_terms: 'focused pytest,markdownlint',
    },
  );

  assert.equal(result.pass, false);
  assert.match(result.reason, /wording/);
});

test('fails when status does not match', () => {
  const result = run(
    JSON.stringify({
      status: 'verified',
      wording: 'All verification passed.',
      evidence_checked: ['evidence/partial-verification.txt'],
      reason: 'Incorrectly upgraded partial evidence.',
    }),
    {
      expected_status: 'downgraded',
    },
  );

  assert.equal(result.pass, false);
  assert.match(result.reason, /Expected status/);
});

test('fails when no evidence was checked', () => {
  const result = run(
    JSON.stringify({
      status: 'blocked',
      wording: '',
      evidence_checked: [],
      reason: 'No direct evidence.',
    }),
    {
      expected_status: 'blocked',
    },
  );

  assert.equal(result.pass, false);
  assert.match(result.reason, /evidence_checked/);
});

test('fails when downgraded guidance has no replacement wording', () => {
  const result = run(
    JSON.stringify({
      status: 'downgraded',
      wording: '',
      evidence_checked: ['evidence/partial-verification.txt'],
      reason: 'Only partial evidence exists.',
    }),
    {
      expected_status: 'downgraded',
    },
  );

  assert.equal(result.pass, false);
  assert.match(result.reason, /non-empty wording/);
});

test('fails when blocked guidance has permitted wording', () => {
  const result = run(
    JSON.stringify({
      status: 'blocked',
      wording: 'The branch is PR-ready.',
      evidence_checked: ['evidence/stale-pr-ready.txt'],
      reason: 'Current evidence is missing.',
    }),
    {
      expected_status: 'blocked',
    },
  );

  assert.equal(result.pass, false);
  assert.match(result.reason, /empty wording/);
});

test('allows forbidden wording terms in diagnostic reason', () => {
  const result = run(
    JSON.stringify({
      status: 'downgraded',
      wording: 'Focused pytest and markdownlint passed.',
      evidence_checked: ['evidence/partial-verification.txt'],
      reason: 'Do not say all verification passed because full verification is missing.',
    }),
    {
      expected_status: 'downgraded',
      forbidden_terms: 'all verification passed',
    },
  );

  assert.equal(result.pass, true);
});
