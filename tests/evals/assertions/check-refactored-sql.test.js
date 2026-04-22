const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const checkRefactoredSql = require('./check-refactored-sql');

function makeRunPath() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'check-refactored-sql-'));
}

function writeJson(filePath, payload) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(payload, null, 2), 'utf8');
}

function validRefactor(status = 'ok') {
  return {
    status,
    extracted_sql: 'select productalternatekey from bronze.product',
    refactored_sql: 'with final as (select productalternatekey from source) select * from final',
    semantic_review: {
      passed: true,
      checks: {
        source_tables: { passed: true, summary: 'Source tables match.' },
        output_columns: { passed: true, summary: 'Output columns match.' },
        joins: { passed: true, summary: 'Join shape matches.' },
        filters: { passed: true, summary: 'Filters match.' },
        aggregation_grain: { passed: true, summary: 'Aggregation grain matches.' },
      },
    },
    compare_sql: { required: false, executed: false, passed: false },
  };
}

test('check-refactored-sql rejects text-only output when catalog was not written', () => {
  const runPath = makeRunPath();
  try {
    const result = checkRefactoredSql(
      'Extracted SQL: select productalternatekey. Refactored SQL: with final as (select 1) select * from final',
      {
        vars: {
          run_path: runPath,
          target_table: 'silver.DimProduct',
          expected_refactored_terms: 'with,final',
        },
      },
    );

    assert.equal(result.pass, false);
    assert.match(result.reason, /Catalog file not found/);
  } finally {
    fs.rmSync(runPath, { recursive: true, force: true });
  }
});

test('check-refactored-sql rejects missing persisted refactor section', () => {
  const runPath = makeRunPath();
  try {
    writeJson(path.join(runPath, 'catalog', 'tables', 'silver.dimproduct.json'), {
      item_id: 'silver.DimProduct',
    });

    const result = checkRefactoredSql(
      'Extracted SQL: select productalternatekey. Refactored SQL: with final as (select 1) select * from final',
      {
        vars: {
          run_path: runPath,
          target_table: 'silver.DimProduct',
          expected_refactored_terms: 'with,final',
        },
      },
    );

    assert.equal(result.pass, false);
    assert.match(result.reason, /No refactor section/);
  } finally {
    fs.rmSync(runPath, { recursive: true, force: true });
  }
});

test('check-refactored-sql defaults to ok status', () => {
  const runPath = makeRunPath();
  try {
    writeJson(path.join(runPath, 'catalog', 'tables', 'silver.dimproduct.json'), {
      item_id: 'silver.DimProduct',
      refactor: validRefactor('partial'),
    });

    const result = checkRefactoredSql('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.DimProduct',
        expected_refactored_terms: 'with,final',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /Unexpected refactor status 'partial'/);
  } finally {
    fs.rmSync(runPath, { recursive: true, force: true });
  }
});

test('check-refactored-sql allows explicit graceful partials', () => {
  const runPath = makeRunPath();
  try {
    writeJson(path.join(runPath, 'catalog', 'tables', 'silver.dimproduct.json'), {
      item_id: 'silver.DimProduct',
      refactor: validRefactor('partial'),
    });

    const result = checkRefactoredSql('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.DimProduct',
        graceful_partial: 'true',
        expected_refactored_terms: 'with,final',
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(runPath, { recursive: true, force: true });
  }
});

test('check-refactored-sql rejects no-compare partials with failed semantic review', () => {
  const runPath = makeRunPath();
  try {
    const refactor = validRefactor('partial');
    refactor.semantic_review.passed = false;
    writeJson(path.join(runPath, 'catalog', 'tables', 'silver.dimproduct.json'), {
      item_id: 'silver.DimProduct',
      refactor,
    });

    const result = checkRefactoredSql('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.DimProduct',
        graceful_partial: 'true',
        expected_refactored_terms: 'with,final',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /semantic_review\.passed must be true/);
  } finally {
    fs.rmSync(runPath, { recursive: true, force: true });
  }
});

test('check-refactored-sql rejects semantic checks without evidence summaries', () => {
  const runPath = makeRunPath();
  try {
    const refactor = validRefactor('partial');
    refactor.semantic_review.checks.source_tables.summary = '';
    writeJson(path.join(runPath, 'catalog', 'tables', 'silver.dimproduct.json'), {
      item_id: 'silver.DimProduct',
      refactor,
    });

    const result = checkRefactoredSql('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.DimProduct',
        graceful_partial: 'true',
        expected_refactored_terms: 'with,final',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /source_tables\.summary must be non-empty/);
  } finally {
    fs.rmSync(runPath, { recursive: true, force: true });
  }
});
