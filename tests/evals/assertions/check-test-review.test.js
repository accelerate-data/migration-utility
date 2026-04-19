const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const checkTestReview = require('./check-test-review');

const repoRoot = path.resolve(__dirname, '..', '..', '..');

test('check-test-review accepts top-level covered branch review shape', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'test-review-'));
  const runPath = path.relative(repoRoot, tmp);
  const reviewDir = path.join(tmp, 'test-review-results');
  fs.mkdirSync(reviewDir, { recursive: true });

  fs.writeFileSync(
    path.join(reviewDir, 'silver.ifelsetarget.json'),
    JSON.stringify({
      item_id: 'silver.ifelsetarget',
      status: 'approved',
      reviewer_branch_manifest: [
        { id: 'if_premium_path' },
        { id: 'else_standard_path' },
      ],
      coverage: {
        total_branches: 2,
        covered_branches: 2,
      },
      covered_branches: [
        { id: 'if_premium_path', covered_by: ['test_premium'] },
        { id: 'else_standard_path', covered_by: ['test_standard'] },
      ],
      uncovered_branches: [],
      untestable_branches: [],
    }),
  );

  try {
    const result = checkTestReview('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.IfElseTarget',
        min_covered_branches: 2,
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

test('check-test-review accepts reviewer manifest is_covered shape', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'test-review-'));
  const runPath = path.relative(repoRoot, tmp);
  const reviewDir = path.join(tmp, 'test-review-results');
  fs.mkdirSync(reviewDir, { recursive: true });

  fs.writeFileSync(
    path.join(reviewDir, 'silver.ifelsetarget.json'),
    JSON.stringify({
      item_id: 'silver.ifelsetarget',
      status: 'success',
      verdict: 'approved_with_warnings',
      reviewer_branch_manifest: [
        { id: 'if_premium_path', is_covered: true },
        { id: 'else_standard_path', is_covered: true },
      ],
      coverage: {
        total_branches: 2,
        covered_branches: 2,
        uncovered: [],
        untestable: [],
      },
    }),
  );

  try {
    const result = checkTestReview('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.IfElseTarget',
        min_covered_branches: 2,
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

test('check-test-review accepts coverage_analysis branch review shape', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'test-review-'));
  const runPath = path.relative(repoRoot, tmp);
  const reviewDir = path.join(tmp, 'test-review-results');
  fs.mkdirSync(reviewDir, { recursive: true });

  fs.writeFileSync(
    path.join(reviewDir, 'silver.ifelsetarget.json'),
    JSON.stringify({
      item_id: 'silver.ifelsetarget',
      status: 'approved',
      coverage_analysis: {
        total_branches: 2,
        covered_branches: 2,
        branch_coverage: [
          { branch_id: 'if_premium_path', covered: true },
          { branch_id: 'else_standard_path', covered: true },
        ],
      },
    }),
  );

  try {
    const result = checkTestReview('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.IfElseTarget',
        min_covered_branches: 2,
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

test('check-test-review accepts keyed coverage_analysis review shape', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'test-review-'));
  const runPath = path.relative(repoRoot, tmp);
  const reviewDir = path.join(tmp, 'test-review-results');
  fs.mkdirSync(reviewDir, { recursive: true });

  fs.writeFileSync(
    path.join(reviewDir, 'silver.ifelsetarget.json'),
    JSON.stringify({
      item_id: 'silver.ifelsetarget',
      approved: true,
      coverage_analysis: {
        if_premium_path: { covered: true, scenario_count: 1 },
        else_standard_path: { covered: true, scenario_count: 1 },
      },
    }),
  );

  try {
    const result = checkTestReview('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.IfElseTarget',
        min_covered_branches: 2,
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

test('check-test-review accepts branch_coverage object review shape', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'test-review-'));
  const runPath = path.relative(repoRoot, tmp);
  const reviewDir = path.join(tmp, 'test-review-results');
  fs.mkdirSync(reviewDir, { recursive: true });

  fs.writeFileSync(
    path.join(reviewDir, 'silver.ifelsetarget.json'),
    JSON.stringify({
      item_id: 'silver.ifelsetarget',
      status: 'approved',
      branch_coverage: {
        total: 2,
        covered: 2,
      },
      scenarios: {
        test_premium: { branch_id: 'if_premium_path', status: 'approved' },
        test_standard: { branch_id: 'else_standard_path', status: 'approved' },
      },
    }),
  );

  try {
    const result = checkTestReview('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.IfElseTarget',
        min_covered_branches: 2,
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

test('check-test-review accepts keyed branch_coverage review shape', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'test-review-'));
  const runPath = path.relative(repoRoot, tmp);
  const reviewDir = path.join(tmp, 'test-review-results');
  fs.mkdirSync(reviewDir, { recursive: true });

  fs.writeFileSync(
    path.join(reviewDir, 'silver.ifelsetarget.json'),
    JSON.stringify({
      item_id: 'silver.ifelsetarget',
      verdict: 'approved',
      branch_coverage: {
        if_premium_path: {
          covered: true,
          status: 'complete',
          scenarios: ['test_premium_path_high_avg'],
        },
        else_standard_path: {
          covered: true,
          status: 'complete',
          scenarios: ['test_standard_path_all_products'],
        },
      },
      metadata: {
        total_branches: 2,
        covered_branches: 2,
      },
    }),
  );

  try {
    const result = checkTestReview('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.IfElseTarget',
        min_covered_branches: 2,
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

test('check-test-review accepts branch_coverage array review shape', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'test-review-'));
  const runPath = path.relative(repoRoot, tmp);
  const reviewDir = path.join(tmp, 'test-review-results');
  fs.mkdirSync(reviewDir, { recursive: true });

  fs.writeFileSync(
    path.join(reviewDir, 'silver.ifelsetarget.json'),
    JSON.stringify({
      item_id: 'silver.ifelsetarget',
      verdict: 'approved',
      coverage_assessment: 'complete',
      branch_coverage: [
        { branch_id: 'if_premium_path', status: 'covered' },
        { branch_id: 'else_standard_path', status: 'covered' },
      ],
    }),
  );

  try {
    const result = checkTestReview('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.IfElseTarget',
        min_covered_branches: 2,
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

test('check-test-review accepts command review branch coverage summary shape', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'test-review-'));
  const runPath = path.relative(repoRoot, tmp);
  const reviewDir = path.join(tmp, 'test-review-results');
  fs.mkdirSync(reviewDir, { recursive: true });

  fs.writeFileSync(
    path.join(reviewDir, 'silver.ifelsetarget.json'),
    JSON.stringify({
      item_id: 'silver.ifelsetarget',
      status: 'ok',
      branch_coverage: {
        total_branches: 2,
        covered_branches: 2,
      },
      scenarios_reviewed: [
        { branch_id: 'if_premium_path', status: 'ok' },
        { branch_id: 'else_standard_path', status: 'ok' },
      ],
    }),
  );

  try {
    const result = checkTestReview('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.IfElseTarget',
        min_covered_branches: 2,
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

test('check-test-review accepts checklist details branch review shape', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'test-review-'));
  const runPath = path.relative(repoRoot, tmp);
  const reviewDir = path.join(tmp, 'test-review-results');
  fs.mkdirSync(reviewDir, { recursive: true });

  fs.writeFileSync(
    path.join(reviewDir, 'silver.ifelsetarget.json'),
    JSON.stringify({
      item_id: 'silver.ifelsetarget',
      status: 'passed',
      checks: [
        {
          check_id: 'branch_coverage',
          status: 'passed',
          details: {
            total_branches: 2,
            covered_branches: 2,
            branches: [
              { id: 'if_premium_path', status: 'covered' },
              { id: 'else_standard_path', status: 'covered' },
            ],
          },
        },
      ],
    }),
  );

  try {
    const result = checkTestReview('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.IfElseTarget',
        min_covered_branches: 2,
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

test('check-test-review accepts branch_reviews shape', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'test-review-'));
  const runPath = path.relative(repoRoot, tmp);
  const reviewDir = path.join(tmp, 'test-review-results');
  fs.mkdirSync(reviewDir, { recursive: true });

  fs.writeFileSync(
    path.join(reviewDir, 'silver.ifelsetarget.json'),
    JSON.stringify({
      item_id: 'silver.ifelsetarget',
      verdict: 'approved',
      branch_reviews: [
        { id: 'if_premium_path', status: 'covered' },
        { id: 'else_standard_path', status: 'covered' },
      ],
    }),
  );

  try {
    const result = checkTestReview('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.IfElseTarget',
        min_covered_branches: 2,
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

test('check-test-review accepts branch reviews with approval status', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'test-review-'));
  const runPath = path.relative(repoRoot, tmp);
  const reviewDir = path.join(tmp, 'test-review-results');
  fs.mkdirSync(reviewDir, { recursive: true });

  fs.writeFileSync(
    path.join(reviewDir, 'silver.ifelsetarget.json'),
    JSON.stringify({
      item_id: 'silver.ifelsetarget',
      review_status: 'approved',
      branch_reviews: [
        { branch_id: 'if_premium_path', approval_status: 'approved' },
        { branch_id: 'else_standard_path', approval_status: 'approved' },
      ],
    }),
  );

  try {
    const result = checkTestReview('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.IfElseTarget',
        min_covered_branches: 2,
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

test('check-test-review accepts coverage_assessment object shape', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'test-review-'));
  const runPath = path.relative(repoRoot, tmp);
  const reviewDir = path.join(tmp, 'test-review-results');
  fs.mkdirSync(reviewDir, { recursive: true });

  fs.writeFileSync(
    path.join(reviewDir, 'silver.ifelsetarget.json'),
    JSON.stringify({
      item_id: 'silver.ifelsetarget',
      verdict: 'approved',
      coverage_assessment: {
        total_branches: 2,
        covered_branches: 2,
        branch_details: [
          { id: 'if_premium_path', status: 'covered' },
          { id: 'else_standard_path', status: 'covered' },
        ],
      },
    }),
  );

  try {
    const result = checkTestReview('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.IfElseTarget',
        min_covered_branches: 2,
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

test('check-test-review prefers latest iteration artifact over generic review file', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'test-review-'));
  const runPath = path.relative(repoRoot, tmp);
  const reviewDir = path.join(tmp, 'test-review-results');
  fs.mkdirSync(reviewDir, { recursive: true });

  fs.writeFileSync(
    path.join(reviewDir, 'silver.ifelsetarget.json'),
    JSON.stringify({
      item_id: 'silver.ifelsetarget',
      status: 'revision_requested',
      coverage: { total_branches: 2, covered_branches: 0 },
      reviewer_branch_manifest: [
        { id: 'if_premium_path', covered: false },
        { id: 'else_standard_path', covered: false },
      ],
    }),
  );
  fs.writeFileSync(
    path.join(reviewDir, 'silver.ifelsetarget.iteration-2.json'),
    JSON.stringify({
      item_id: 'silver.ifelsetarget',
      status: 'approved',
      coverage: { total_branches: 2, covered_branches: 2 },
      reviewer_branch_manifest: [
        { id: 'if_premium_path', covered: true },
        { id: 'else_standard_path', covered: true },
      ],
    }),
  );

  try {
    const result = checkTestReview('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.IfElseTarget',
        min_covered_branches: 2,
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

test('check-test-review accepts complete summary review shape without branch manifest', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'test-review-'));
  const runPath = path.relative(repoRoot, tmp);
  const reviewDir = path.join(tmp, 'test-review-results');
  fs.mkdirSync(reviewDir, { recursive: true });

  fs.writeFileSync(
    path.join(reviewDir, 'silver.ifelsetarget.json'),
    JSON.stringify({
      item_id: 'silver.ifelsetarget',
      status: 'approved',
      branch_count: 2,
      scenario_count: 2,
      coverage_assessment: 'complete',
    }),
  );

  try {
    const result = checkTestReview('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.IfElseTarget',
        min_covered_branches: 2,
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});
