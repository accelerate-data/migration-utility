const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const checkRefactoredSql = require('./check-refactored-sql');

function makeRunRoot(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'check-refactored-sql-'));
  t.after(() => fs.rmSync(root, { force: true, recursive: true }));
  return root;
}

function writeJson(filePath, payload) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(payload, null, 2), 'utf8');
}

function validRefactor(overrides = {}) {
  return {
    status: 'ok',
    extracted_sql: 'select productalternatekey from bronze.product',
    refactored_sql: 'with final as (select productalternatekey from source) select * from final',
    semantic_review: {
      passed: true,
      checks: {
        source_tables: { passed: true },
        output_columns: { passed: true },
        joins: { passed: true },
        filters: { passed: true },
        aggregation_grain: { passed: true },
      },
    },
    compare_sql: { passed: true },
    ...overrides,
  };
}

test('check-refactored-sql validates persisted CTE refactor artifacts', (t) => {
  const runRoot = makeRunRoot(t);
  writeJson(path.join(runRoot, 'catalog', 'tables', 'silver.dimproduct.json'), {
    item_id: 'silver.DimProduct',
    refactor: validRefactor(),
  });

  const result = checkRefactoredSql('', {
    vars: {
      run_path: runRoot,
      target_table: 'silver.DimProduct',
      expected_status: 'ok',
      expected_refactored_terms: 'with,final',
      expected_extracted_terms: 'productalternatekey',
    },
  });

  assert.equal(result.pass, true, result.reason);
});

test('check-refactored-sql rejects write keywords in extracted SQL', (t) => {
  const runRoot = makeRunRoot(t);
  writeJson(path.join(runRoot, 'catalog', 'tables', 'silver.dimproduct.json'), {
    item_id: 'silver.DimProduct',
    refactor: validRefactor({
      extracted_sql: 'insert into silver.dimproduct select * from bronze.product',
    }),
  });

  const result = checkRefactoredSql('', {
    vars: {
      run_path: runRoot,
      target_table: 'silver.DimProduct',
      expected_status: 'ok',
    },
  });

  assert.equal(result.pass, false);
  assert.match(result.reason, /extracted_sql contains write keyword/);
});
