const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const checkCommandSummary = require('./check-command-summary');

const repoRoot = path.resolve(__dirname, '..', '..', '..');

test('check-command-summary prefers final per-item artifact for reviewed runs', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cmd-summary-'));
  const runPath = path.relative(repoRoot, tmp);
  const migrationsDir = path.join(tmp, '.migration-runs');
  fs.mkdirSync(migrationsDir, { recursive: true });

  const runId = '1234-abcd';
  fs.writeFileSync(
    path.join(migrationsDir, `summary.${runId}.json`),
    JSON.stringify({ total: 1, ok: 1, error: 0, run_id: runId }),
  );
  fs.writeFileSync(
    path.join(migrationsDir, `silver.ifelsetarget.${runId}.json`),
    JSON.stringify({
      status: 'ok',
      output: { review_iterations: 0 },
    }),
  );
  fs.writeFileSync(
    path.join(migrationsDir, `silver.ifelsetarget.${runId}.final.json`),
    JSON.stringify({
      status: 'ok',
      output: { review_iterations: 2, review_verdict: 'approved' },
    }),
  );

  try {
    const result = checkCommandSummary('', {
      vars: {
        run_path: runPath,
        expected_item_statuses: '{"silver.IfElseTarget": "ok"}',
        expected_item_review_iterations: '{"silver.IfElseTarget": 2}',
        expected_item_review_verdicts: '{"silver.IfElseTarget": "approved"}',
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

test('check-command-summary accepts multiple expected review iteration counts', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cmd-summary-'));
  const runPath = path.relative(repoRoot, tmp);
  const migrationsDir = path.join(tmp, '.migration-runs');
  fs.mkdirSync(migrationsDir, { recursive: true });

  const runId = '1234-abcd';
  fs.writeFileSync(
    path.join(migrationsDir, `summary.${runId}.json`),
    JSON.stringify({ total: 1, ok: 1, error: 0, run_id: runId }),
  );
  fs.writeFileSync(
    path.join(migrationsDir, `silver.ifelsetarget.${runId}.json`),
    JSON.stringify({
      status: 'ok',
      output: { review_iterations: 1, review_verdict: 'approved' },
    }),
  );

  try {
    const result = checkCommandSummary('', {
      vars: {
        run_path: runPath,
        expected_item_statuses: '{"silver.IfElseTarget": "ok"}',
        expected_item_review_iterations: '{"silver.IfElseTarget": "1,2"}',
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

test('check-command-summary accepts singular top-level review fields', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cmd-summary-'));
  const runPath = path.relative(repoRoot, tmp);
  const migrationsDir = path.join(tmp, '.migration-runs');
  fs.mkdirSync(migrationsDir, { recursive: true });

  const runId = '1234-abcd';
  fs.writeFileSync(
    path.join(migrationsDir, `summary.${runId}.json`),
    JSON.stringify({ total: 1, ok: 1, error: 0, run_id: runId }),
  );
  fs.writeFileSync(
    path.join(migrationsDir, `silver.ifelsetarget.${runId}.json`),
    JSON.stringify({
      status: 'ok',
      review_iteration: 1,
      review_verdict: 'approved',
    }),
  );

  try {
    const result = checkCommandSummary('', {
      vars: {
        run_path: runPath,
        expected_item_statuses: '{"silver.IfElseTarget": "ok"}',
        expected_item_review_iterations: '{"silver.IfElseTarget": 1}',
        expected_item_review_verdicts: '{"silver.IfElseTarget": "approved"}',
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

test('check-command-summary accepts completed artifact with complete result as ok', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cmd-summary-'));
  const runPath = path.relative(repoRoot, tmp);
  const migrationsDir = path.join(tmp, '.migration-runs');
  fs.mkdirSync(migrationsDir, { recursive: true });

  const runId = '1234-abcd';
  fs.writeFileSync(
    path.join(migrationsDir, `summary.${runId}.json`),
    JSON.stringify({ total: 1, ok: 1, error: 0, run_id: runId }),
  );
  fs.writeFileSync(
    path.join(migrationsDir, `silver.ifelsetarget.${runId}.json`),
    JSON.stringify({
      status: 'completed',
      result: {
        status: 'complete',
        coverage: 'complete',
      },
    }),
  );

  try {
    const result = checkCommandSummary('', {
      vars: {
        run_path: runPath,
        expected_item_statuses: '{"silver.IfElseTarget": "ok"}',
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});
