const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const check = require('./check-refactor-mart-stg-execution');

function writeFile(root, relativePath, content) {
  const filePath = path.join(root, relativePath);
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, content, 'utf8');
}

function withRunProject(callback) {
  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'refactor-mart-stg-'));
  const relativeRunPath = path.relative(repoRoot, tempRoot);
  try {
    callback(tempRoot, relativeRunPath);
  } finally {
    fs.rmSync(tempRoot, { recursive: true, force: true });
  }
}

test('passes when statuses, validation details, and consumer refs match', () => {
  withRunProject((root, runPath) => {
    writeFile(root, 'docs/design/plan.md', `# Refactor Mart Plan

## Candidate: STG-001

- [x] Approve: yes
- Type: stg
- Output: dbt/models/staging/stg_bronze__orders.sql
- Depends on: none
- Validation: dbt build --select stg_bronze__orders int_sales_orders fct_sales
- Execution status: applied
- Validation result: dbt build --select stg_bronze__orders int_sales_orders fct_sales

## Candidate: INT-001

- [x] Approve: yes
- Type: int
- Output: dbt/models/intermediate/int_sales_orders.sql
- Depends on: STG-001
- Validation: dbt build --select int_sales_orders
- Execution status: planned
`);
    writeFile(root, 'dbt/models/intermediate/int_sales_orders.sql', "select * from {{ ref('stg_bronze__orders') }}\n");
    writeFile(root, 'dbt/models/marts/fct_sales.sql', "select * from {{ ref('stg_bronze__orders') }}\n");
    writeFile(root, 'dbt/models/staging/stg_bronze__orders.sql', "select * from {{ source('bronze', 'orders') }}\n");

    const result = check('applied stg_bronze__orders', {
      vars: {
        run_path: runPath,
        plan_file: 'docs/design/plan.md',
        expected_candidate_statuses: 'STG-001:applied,INT-001:planned',
        expected_output_terms: 'applied,stg_bronze__orders',
        expected_consumer_refs: 'int_sales_orders:stg_bronze__orders,fct_sales:stg_bronze__orders',
        expected_validation_result_details: 'STG-001=dbt build --select stg_bronze__orders int_sales_orders fct_sales',
      },
    });

    assert.equal(result.pass, true, result.reason);
  });
});

test('fails when an applied staging candidate output model is missing', () => {
  withRunProject((root, runPath) => {
    writeFile(root, 'docs/design/plan.md', `# Refactor Mart Plan

## Candidate: STG-001

- [x] Approve: yes
- Type: stg
- Output: dbt/models/staging/stg_bronze__orders.sql
- Depends on: none
- Validation: dbt build --select stg_bronze__orders int_sales_orders
- Execution status: applied
- Validation result: dbt build --select stg_bronze__orders int_sales_orders
`);

    const result = check('applied stg_bronze__orders', {
      vars: {
        run_path: runPath,
        plan_file: 'docs/design/plan.md',
        expected_candidate_statuses: 'STG-001:applied',
        expected_output_terms: 'applied,stg_bronze__orders',
        expected_validation_result_details: 'STG-001=dbt build --select stg_bronze__orders int_sales_orders',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /Output file not found/);
  });
});

test('fails when an applied candidate omits validation result detail', () => {
  withRunProject((root, runPath) => {
    writeFile(root, 'docs/design/plan.md', `# Refactor Mart Plan

## Candidate: STG-001

- [x] Approve: yes
- Type: stg
- Output: dbt/models/staging/stg_bronze__orders.sql
- Depends on: none
- Validation: dbt build --select stg_bronze__orders int_sales_orders
- Execution status: applied
`);
    writeFile(root, 'dbt/models/staging/stg_bronze__orders.sql', "select * from {{ source('bronze', 'orders') }}\n");

    const result = check('applied stg_bronze__orders', {
      vars: {
        run_path: runPath,
        plan_file: 'docs/design/plan.md',
        expected_candidate_statuses: 'STG-001:applied',
        expected_output_terms: 'applied,stg_bronze__orders',
        expected_validation_result_details: 'STG-001=dbt build --select stg_bronze__orders int_sales_orders',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /Validation result/);
  });
});

test('fails when a declared consumer is not rewired to the staging model', () => {
  withRunProject((root, runPath) => {
    writeFile(root, 'docs/design/plan.md', `# Refactor Mart Plan

## Candidate: STG-001

- [x] Approve: yes
- Type: stg
- Output: dbt/models/staging/stg_bronze__orders.sql
- Depends on: none
- Validation: dbt build --select stg_bronze__orders int_sales_orders
- Execution status: applied
- Validation result: dbt build --select stg_bronze__orders int_sales_orders
`);
    writeFile(root, 'dbt/models/intermediate/int_sales_orders.sql', "select * from {{ source('bronze', 'orders') }}\n");
    writeFile(root, 'dbt/models/staging/stg_bronze__orders.sql', "select * from {{ source('bronze', 'orders') }}\n");

    const result = check('applied stg_bronze__orders', {
      vars: {
        run_path: runPath,
        plan_file: 'docs/design/plan.md',
        expected_candidate_statuses: 'STG-001:applied',
        expected_output_terms: 'applied,stg_bronze__orders',
        expected_consumer_refs: 'int_sales_orders:stg_bronze__orders',
        expected_validation_result_details: 'STG-001=dbt build --select stg_bronze__orders int_sales_orders',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /int_sales_orders/);
  });
});

test('passes when blocked candidates record a blocked reason', () => {
  withRunProject((root, runPath) => {
    writeFile(root, 'docs/design/plan.md', `# Refactor Mart Plan

## Candidate: STG-003

- [x] Approve: yes
- Type: stg
- Output: missing
- Depends on: none
- Validation: dbt build --select stg_missing
- Execution status: blocked
- Blocked reason: missing staging output path
`);

    const result = check('blocked', {
      vars: {
        run_path: runPath,
        plan_file: 'docs/design/plan.md',
        expected_candidate_statuses: 'STG-003:blocked',
        expected_output_terms: 'blocked',
        expected_blocked_reason_details: 'STG-003=missing staging output path',
      },
    });

    assert.equal(result.pass, true, result.reason);
  });
});
