const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const checkViewCatalog = require('./check-view-catalog');

function makeRunRoot(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'check-view-catalog-'));
  t.after(() => fs.rmSync(root, { force: true, recursive: true }));
  return root;
}

function writeView(runRoot, name, catalog) {
  const dir = path.join(runRoot, 'catalog', 'views');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, `${name}.json`), JSON.stringify(catalog, null, 2));
}

test('check-view-catalog validates scoping SQL element types', (t) => {
  const runRoot = makeRunRoot(t);
  writeView(runRoot, 'silver.vw_customer', {
    scoping: {
      status: 'analyzed',
      sql_elements: [{ type: 'join' }, { type: 'aggregation' }],
    },
  });

  const result = checkViewCatalog('', {
    vars: {
      run_path: runRoot,
      target_view: 'silver.vw_customer',
      check_type: 'scoping',
      expected_sql_element_types: 'join, aggregation',
    },
  });

  assert.equal(result.pass, true, result.reason);
});

test('check-view-catalog rejects invalid profile classification', (t) => {
  const runRoot = makeRunRoot(t);
  writeView(runRoot, 'silver.vw_customer', {
    profile: {
      classification: 'ods',
      status: 'ok',
      source: 'llm',
    },
  });

  const result = checkViewCatalog('', {
    vars: {
      run_path: runRoot,
      target_view: 'silver.vw_customer',
      check_type: 'profile',
    },
  });

  assert.equal(result.pass, false);
  assert.match(result.reason, /classification must be "stg" or "mart"/);
});
