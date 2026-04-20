const assert = require('node:assert/strict');
const test = require('node:test');

const { containsDelimitedTerm, resolveProjectPath } = require('./schema-helpers');

test('resolveProjectPath returns run_path when it is set', () => {
  assert.equal(
    resolveProjectPath({
      vars: {
        fixture_path: 'tests/evals/fixtures/cmd-scope/happy-path',
        run_path: 'tests/evals/output/runs/suite/test',
      },
    }),
    'tests/evals/output/runs/suite/test',
  );
});

test('resolveProjectPath throws when run_path is not set', () => {
  assert.throws(
    () => resolveProjectPath({
      vars: {
        fixture_path: 'tests/evals/fixtures/cmd-scope/happy-path',
      },
    }),
    /run_path was not set/,
  );
});

test('containsDelimitedTerm requires token boundaries', () => {
  assert.equal(containsDelimitedTerm('PR: https://example.test/1', 'PR'), true);
  assert.equal(containsDelimitedTerm('Branch: feature/x', 'branch'), true);
  assert.equal(containsDelimitedTerm('Worktree=/tmp/work', 'worktree'), true);
  assert.equal(containsDelimitedTerm('prompt prepared without handoff', 'PR'), false);
});
