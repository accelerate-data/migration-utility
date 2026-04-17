const assert = require('node:assert/strict');
const test = require('node:test');

const { resolveProjectPath } = require('./schema-helpers');

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
