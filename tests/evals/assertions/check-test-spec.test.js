const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const checkTestSpec = require('./check-test-spec');

function makeRunRoot(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'check-test-spec-'));
  t.after(() => fs.rmSync(root, { force: true, recursive: true }));
  return root;
}

function writeSpec(runRoot, table, spec) {
  const dir = path.join(runRoot, 'test-specs');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, `${table}.json`), JSON.stringify(spec, null, 2));
}

test('check-test-spec validates persisted branch and scenario coverage', (t) => {
  const runRoot = makeRunRoot(t);
  writeSpec(runRoot, 'silver.dimcustomer', {
    item_id: 'silver.dimcustomer',
    status: 'ok',
    coverage: 'complete',
    branch_manifest: [{ id: 'branch-1', scenarios: ['loads_new_customer'] }],
    unit_tests: [{ name: 'loads_new_customer' }],
    warnings: [{ message: 'source row count estimated' }],
  });

  const result = checkTestSpec('', {
    vars: {
      run_path: runRoot,
      target_table: 'silver.dimcustomer',
      expected_status: 'ok',
      expected_coverage: 'complete',
      min_branch_count: '1',
      min_scenario_count: '1',
      expected_branch_ids: 'branch-1',
      expected_unit_test_names: 'loads_new_customer',
      expected_warning_terms: 'row count',
    },
  });

  assert.equal(result.pass, true, result.reason);
});

test('check-test-spec rejects branch scenarios that are not present in unit_tests', (t) => {
  const runRoot = makeRunRoot(t);
  writeSpec(runRoot, 'silver.dimcustomer', {
    item_id: 'silver.dimcustomer',
    status: 'ok',
    coverage: 'partial',
    branch_manifest: [{ id: 'branch-1', scenarios: ['missing_test'] }],
    unit_tests: [{ name: 'loads_new_customer' }],
  });

  const result = checkTestSpec('', {
    vars: {
      run_path: runRoot,
      target_table: 'silver.dimcustomer',
    },
  });

  assert.equal(result.pass, false);
  assert.match(result.reason, /references scenario 'missing_test' not found/);
});
