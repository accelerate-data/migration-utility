const assert = require('node:assert/strict');
const test = require('node:test');

const {
  ALLOWED_ARTIFACT_PREFIXES,
  detectCleanupViolations,
} = require('./run-promptfoo-with-guard');

test('detectCleanupViolations ignores new files under approved eval artifact directories', () => {
  const before = {
    tracked: new Set(),
    untracked: new Set(),
  };
  const after = {
    tracked: new Set(),
    untracked: new Set([
      'tests/evals/output/runs/listing-objects/run-1/transcript.txt',
      'tests/evals/results/logs/promptfoo.log',
      'tests/evals/.tmp/trace.json',
      'tests/evals/.cache/promptfoo/cache.db',
      'tests/evals/.promptfoo/promptfoo.db',
    ]),
  };

  const violations = detectCleanupViolations(before, after);

  assert.deepEqual(violations, []);
});

test('detectCleanupViolations reports newly dirtied tracked files outside approved artifact directories', () => {
  const before = {
    tracked: new Set(['tests/evals/package.json']),
    untracked: new Set(),
  };
  const after = {
    tracked: new Set([
      'tests/evals/package.json',
      'tests/evals/fixtures/analyzing-table/truncate-insert/catalog/tables/silver.dimcustomer.json',
      'tests/evals/package-lock.json',
    ]),
    untracked: new Set(),
  };

  const violations = detectCleanupViolations(before, after);

  assert.deepEqual(violations, [
    'tests/evals/fixtures/analyzing-table/truncate-insert/catalog/tables/silver.dimcustomer.json',
    'tests/evals/package-lock.json',
  ]);
});

test('detectCleanupViolations reports new untracked files outside approved artifact directories', () => {
  const before = {
    tracked: new Set(),
    untracked: new Set(['tests/evals/results/logs/already-there.log']),
  };
  const after = {
    tracked: new Set(),
    untracked: new Set([
      'tests/evals/results/logs/already-there.log',
      'tests/evals/eval-dimcustomer.log',
      'tests/evals/tests/evals/eval_output.log',
    ]),
  };

  const violations = detectCleanupViolations(before, after);

  assert.deepEqual(violations, [
    'tests/evals/eval-dimcustomer.log',
    'tests/evals/tests/evals/eval_output.log',
  ]);
});

test('allowed artifact prefixes stay limited to the dedicated eval output roots', () => {
  assert.deepEqual(ALLOWED_ARTIFACT_PREFIXES, [
    'tests/evals/.cache/',
    'tests/evals/.promptfoo/',
    'tests/evals/.tmp/',
    'tests/evals/output/',
    'tests/evals/results/',
  ]);
});
