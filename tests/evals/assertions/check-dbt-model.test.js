const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const checkDbtModel = require('./check-dbt-model');

function makeRunPath() {
  const runsRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'unit-check-dbt-model-root-'));
  return fs.mkdtempSync(path.join(runsRoot, 'unit-check-dbt-model-'));
}

test('check-dbt-model accepts alternate expected model paths', () => {
  const runPath = makeRunPath();
  try {
    const martsDir = path.join(runPath, 'dbt', 'models', 'marts');
    fs.mkdirSync(martsDir, { recursive: true });
    fs.writeFileSync(
      path.join(martsDir, 'fact_sales.sql'),
      "with final as (select * from {{ ref('stg_bronze__salesorder') }}) select * from final\n",
      'utf8',
    );

    const result = checkDbtModel('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.FactSales',
        expected_model_path: 'models/marts/factsales.sql,models/marts/fact_sales.sql',
        expected_model_terms: "ref('stg_bronze__salesorder')",
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(runPath, { recursive: true, force: true });
  }
});
