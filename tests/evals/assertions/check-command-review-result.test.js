const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const checkCommandReviewResult = require('./check-command-review-result');

const repoRoot = path.resolve(__dirname, '..', '..', '..');

test('check-command-review-result falls back to latest iteration artifact', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'command-review-'));
  const runPath = path.relative(repoRoot, tmp);
  const reviewDir = path.join(tmp, 'test-review-results');
  fs.mkdirSync(reviewDir, { recursive: true });

  fs.writeFileSync(
    path.join(reviewDir, 'silver.dimcustomer.iteration-1.json'),
    JSON.stringify({
      item_id: 'silver.dimcustomer',
      status: 'needs_revision',
      warnings: [],
    }),
  );
  fs.writeFileSync(
    path.join(reviewDir, 'silver.dimcustomer.iteration-2.json'),
    JSON.stringify({
      item_id: 'silver.dimcustomer',
      status: 'approved_with_warnings',
      warnings: [
        {
          code: 'ITERATION_2_APPROVAL',
          message: 'Approved after second iteration.',
        },
      ],
    }),
  );

  try {
    const result = checkCommandReviewResult('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.DimCustomer',
        expected_command_review_verdict: 'approved_with_warnings',
        expected_warning_terms: 'iteration_2_approval',
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

test('check-command-review-result prefers latest iteration artifact over generic review file', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'command-review-'));
  const runPath = path.relative(repoRoot, tmp);
  const reviewDir = path.join(tmp, 'test-review-results');
  fs.mkdirSync(reviewDir, { recursive: true });

  fs.writeFileSync(
    path.join(reviewDir, 'silver.dimcustomer.json'),
    JSON.stringify({
      item_id: 'silver.dimcustomer',
      status: 'revision_requested',
      warnings: [],
    }),
  );
  fs.writeFileSync(
    path.join(reviewDir, 'silver.dimcustomer.iteration-2.json'),
    JSON.stringify({
      item_id: 'silver.dimcustomer',
      status: 'approved_with_warnings',
      warnings: [
        {
          code: 'COVERAGE_PARTIAL',
          message: 'Approved with partial coverage.',
        },
      ],
    }),
  );

  try {
    const result = checkCommandReviewResult('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.DimCustomer',
        expected_command_review_verdict: 'approved_with_warnings',
        expected_warning_terms: 'coverage_partial',
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

test('check-command-review-result accepts fixture quality issue warnings', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'command-review-'));
  const runPath = path.relative(repoRoot, tmp);
  const reviewDir = path.join(tmp, 'test-review-results');
  fs.mkdirSync(reviewDir, { recursive: true });

  fs.writeFileSync(
    path.join(reviewDir, 'silver.dimcustomer.json'),
    JSON.stringify({
      item_id: 'silver.dimcustomer',
      status: 'approved_with_warnings',
      fixture_quality: {
        issues: [
          {
            severity: 'warning',
            issue: 'Coverage partial because static NULL mappings are accepted for this fixture.',
          },
        ],
      },
    }),
  );

  try {
    const result = checkCommandReviewResult('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.DimCustomer',
        expected_command_review_verdict: 'approved_with_warnings',
        expected_warning_terms: 'static null',
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

test('check-command-review-result accepts approved string verdict', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'command-review-'));
  const runPath = path.relative(repoRoot, tmp);
  const reviewDir = path.join(tmp, 'test-review-results');
  fs.mkdirSync(reviewDir, { recursive: true });

  fs.writeFileSync(
    path.join(reviewDir, 'silver.dimcustomer.json'),
    JSON.stringify({
      item_id: 'silver.dimcustomer',
      approved: 'approved_with_warnings',
      warnings: [],
    }),
  );

  try {
    const result = checkCommandReviewResult('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.DimCustomer',
        expected_command_review_verdict: 'approved_with_warnings',
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

test('check-command-review-result accepts final verdict', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'command-review-'));
  const runPath = path.relative(repoRoot, tmp);
  const reviewDir = path.join(tmp, 'test-review-results');
  fs.mkdirSync(reviewDir, { recursive: true });

  fs.writeFileSync(
    path.join(reviewDir, 'silver.dimcustomer.json'),
    JSON.stringify({
      item_id: 'silver.dimcustomer',
      final_verdict: 'approved_with_warnings',
      warnings: [],
    }),
  );

  try {
    const result = checkCommandReviewResult('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.DimCustomer',
        expected_command_review_verdict: 'approved_with_warnings',
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});
