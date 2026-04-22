const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const checkModelReview = require('./check-model-review');

function makeRunRoot(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'check-model-review-'));
  t.after(() => fs.rmSync(root, { force: true, recursive: true }));
  return root;
}

function writeReview(runRoot, table, review) {
  const dir = path.join(runRoot, 'model-review-results');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, `${table}.json`), JSON.stringify(review, null, 2));
}

test('check-model-review validates persisted review checks and feedback codes', (t) => {
  const runRoot = makeRunRoot(t);
  writeReview(runRoot, 'silver.dimcustomer', {
    status: 'approved_with_warnings',
    checks: {
      standards: { passed: true },
      correctness: { passed: false, issues: ['missing surrogate key'] },
      test_integration: { passed: true },
    },
    feedback_for_model_generator: [{
      code: 'MODEL_CORRECTNESS',
      severity: 'warning',
      ack_required: true,
      message: 'Add surrogate key handling.',
    }],
  });

  const result = checkModelReview('', {
    vars: {
      run_path: runRoot,
      target_table: 'silver.dimcustomer',
      expected_status: 'approved_with_warnings',
      expect_standards_passed: 'true',
      expect_correctness_passed: 'false',
      expect_test_integration_passed: 'true',
      expected_feedback_codes: 'MODEL_CORRECTNESS',
      expected_feedback_terms: 'surrogate key',
      expected_issue_terms: 'missing surrogate key',
    },
  });

  assert.equal(result.pass, true, result.reason);
});

test('check-model-review rejects feedback items that cannot be acknowledged correctly', (t) => {
  const runRoot = makeRunRoot(t);
  writeReview(runRoot, 'silver.dimcustomer', {
    status: 'changes_requested',
    checks: {},
    feedback_for_model_generator: [{
      code: 'INFO_ONLY',
      severity: 'info',
      ack_required: true,
    }],
  });

  const result = checkModelReview('', {
    vars: {
      run_path: runRoot,
      target_table: 'silver.dimcustomer',
    },
  });

  assert.equal(result.pass, false);
  assert.match(result.reason, /severity='info' must have ack_required=false/);
});
