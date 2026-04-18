const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const checkGuardStop = require('./check-guard-stop');

function writeJson(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2) + '\n', 'utf8');
}

test('check-guard-stop accepts injected catalog errors without other mutations', () => {
  const fixtureRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'guard-fixture-'));
  const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'guard-run-'));

  try {
    fs.mkdirSync(path.join(fixtureRoot, 'dbt', 'models'), { recursive: true });
    fs.writeFileSync(path.join(fixtureRoot, 'dbt', 'models', 'model.sql'), 'select 1\n', 'utf8');
    fs.cpSync(fixtureRoot, runRoot, { recursive: true });
    writeJson(path.join(fixtureRoot, 'catalog', 'tables', 'silver.dimproduct.json'), {
      name: 'DimProduct',
      profile: { status: 'ok' },
    });
    writeJson(path.join(runRoot, 'catalog', 'tables', 'silver.dimproduct.json'), {
      name: 'DimProduct',
      profile: { status: 'ok' },
      errors: [{ code: 'TYPE_MAPPING_UNSUPPORTED', severity: 'error' }],
    });

    const result = checkGuardStop('CATALOG_ERRORS_UNRESOLVED', {
      vars: {
        fixture_path: fixtureRoot,
        run_path: runRoot,
        expected_final_output_terms: 'CATALOG_ERRORS_UNRESOLVED',
        unchanged_paths: 'dbt/models',
        unchanged_catalog_object: 'silver.DimProduct',
      },
    });

    assert.equal(result.pass, true);
  } finally {
    fs.rmSync(fixtureRoot, { recursive: true, force: true });
    fs.rmSync(runRoot, { recursive: true, force: true });
  }
});

test('check-guard-stop fails when guarded artifacts are mutated', () => {
  const fixtureRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'guard-fixture-'));
  const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'guard-run-'));

  try {
    fs.mkdirSync(path.join(fixtureRoot, 'dbt', 'models'), { recursive: true });
    fs.mkdirSync(path.join(runRoot, 'dbt', 'models'), { recursive: true });
    fs.writeFileSync(path.join(fixtureRoot, 'dbt', 'models', 'model.sql'), 'select 1\n', 'utf8');
    fs.writeFileSync(path.join(runRoot, 'dbt', 'models', 'model.sql'), 'select 2\n', 'utf8');

    const result = checkGuardStop('CATALOG_ERRORS_UNRESOLVED', {
      vars: {
        fixture_path: fixtureRoot,
        run_path: runRoot,
        unchanged_paths: 'dbt/models',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /dbt\/models/);
  } finally {
    fs.rmSync(fixtureRoot, { recursive: true, force: true });
    fs.rmSync(runRoot, { recursive: true, force: true });
  }
});
