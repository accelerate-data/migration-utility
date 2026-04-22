const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const checkProcedureCatalog = require('./check-procedure-catalog');

function makeRunRoot(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'check-procedure-catalog-'));
  t.after(() => fs.rmSync(root, { force: true, recursive: true }));
  return root;
}

function writeCatalog(runRoot, name, catalog) {
  const dir = path.join(runRoot, 'catalog', 'procedures');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, `${name}.json`), JSON.stringify(catalog, null, 2));
}

test('check-procedure-catalog validates resolved migrate statements and output terms', (t) => {
  const runRoot = makeRunRoot(t);
  writeCatalog(runRoot, 'silver.loadcustomer', {
    statements: [{
      action: 'migrate',
      source: 'llm',
      sql: 'insert into silver.dimcustomer select * from bronze.customer',
      rationale: 'loads customer dimension',
    }],
  });

  const result = checkProcedureCatalog('Scoping complete.', {
    vars: {
      run_path: runRoot,
      target_procedure: 'silver.LoadCustomer',
      expected_action: 'migrate',
      expected_source: 'llm',
      expected_statement_terms: 'dimcustomer',
      expected_rationale_terms: 'customer dimension',
      expected_output_terms: 'scoping complete',
    },
  });

  assert.equal(result.pass, true, result.reason);
});

test('check-procedure-catalog rejects unresolved needs_llm statements', (t) => {
  const runRoot = makeRunRoot(t);
  writeCatalog(runRoot, 'silver.loadcustomer', {
    statements: [{ action: 'needs_llm' }],
  });

  const result = checkProcedureCatalog('', {
    vars: {
      run_path: runRoot,
      target_procedure: 'silver.LoadCustomer',
      allow_zero_migrate: 'true',
    },
  });

  assert.equal(result.pass, false);
  assert.match(result.reason, /unresolved 'needs_llm'/);
});
