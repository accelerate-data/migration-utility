const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const checkTableProfile = require('./check-table-profile');

function makeRunRoot(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'check-table-profile-'));
  t.after(() => fs.rmSync(root, { force: true, recursive: true }));
  return root;
}

function writeCatalog(runRoot, table, catalog) {
  const tableDir = path.join(runRoot, 'catalog', 'tables');
  fs.mkdirSync(tableDir, { recursive: true });
  fs.writeFileSync(path.join(tableDir, `${table}.json`), JSON.stringify(catalog, null, 2));
}

function context(runRoot, vars = {}) {
  return {
    vars: {
      run_path: runRoot,
      target_table: 'silver.dimcustomer',
      ...vars,
    },
  };
}

test('check-table-profile validates persisted classification and expected profile fields', (t) => {
  const runRoot = makeRunRoot(t);
  writeCatalog(runRoot, 'silver.dimcustomer', {
    profile: {
      status: 'ok',
      classification: {
        resolved_kind: 'dimension',
        source: 'catalog',
      },
      pii_actions: [{ column: 'email' }],
      foreign_keys: [{ fk_type: 'natural_key' }],
      watermark: { column: 'updated_at' },
      warnings: [{ code: 'PROFILE_LOW_CONFIDENCE' }],
    },
  });

  const result = checkTableProfile('profile complete', context(runRoot, {
    expected_status: 'ok',
    expected_kind: 'dimension',
    expected_source: 'catalog',
    expected_output_terms: 'profile complete',
    expected_pii_columns: 'email',
    expected_fk_type: 'natural_key',
    expected_watermark_column: 'updated_at',
    expected_warning_codes: 'PROFILE_LOW_CONFIDENCE',
  }));

  assert.equal(result.pass, true);
});

test('check-table-profile rejects missing profile sections', (t) => {
  const runRoot = makeRunRoot(t);
  writeCatalog(runRoot, 'silver.dimcustomer', {});

  const result = checkTableProfile('', context(runRoot));

  assert.equal(result.pass, false);
  assert.match(result.reason, /No profile section/);
});

test('check-table-profile rejects missing classification source', (t) => {
  const runRoot = makeRunRoot(t);
  writeCatalog(runRoot, 'silver.dimcustomer', {
    profile: {
      status: 'ok',
      classification: {
        resolved_kind: 'dimension',
      },
    },
  });

  const result = checkTableProfile('', context(runRoot));

  assert.equal(result.pass, false);
  assert.match(result.reason, /missing classification\.source/);
});
