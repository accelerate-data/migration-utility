const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const check = require('./check-refactor-mart-mart-execution');

function writeFile(root, relativePath, content) {
  const filePath = path.join(root, relativePath);
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, content, 'utf8');
}

function withRunProject(callback) {
  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'refactor-mart-int-'));
  const relativeRunPath = path.relative(repoRoot, tempRoot);
  try {
    callback(tempRoot, relativeRunPath);
  } finally {
    fs.rmSync(tempRoot, { recursive: true, force: true });
  }
}

function writeHappyPathPlan(root, overrides = {}) {
  writeFile(root, 'docs/design/plan.md', `# Refactor Mart Plan

## Candidate: STG-001

- [x] Approve: yes
- Type: stg
- Output: dbt/models/staging/stg_bronze__orders.sql
- Depends on: none
- Validation: dbt build --select stg_bronze__orders
- Execution status: ${overrides.stgStatus || 'applied'}

## Candidate: INT-001

- [x] Approve: yes
- Type: int
- Output: ${overrides.intOutput || 'dbt/models/intermediate/int_sales_orders.sql'}
- Depends on: STG-001
- Validation: dbt build --select int_sales_orders
- Execution status: ${overrides.intStatus || 'applied'}
${overrides.omitIntValidation ? '' : '- Validation result: dbt build --select int_sales_orders\n'}
## Candidate: MART-001

- [x] Approve: yes
- Type: mart
- Output: ${overrides.martOutput || 'dbt/models/marts/fct_sales.sql'}
- Depends on: INT-001
- Validation: dbt build --select fct_sales
- Execution status: ${overrides.martStatus || 'applied'}
- Validation result: dbt build --select fct_sales
`);
}

test('passes when higher-layer statuses, validation details, and refs match', () => {
  withRunProject((root, runPath) => {
    writeHappyPathPlan(root);
    writeFile(root, 'dbt/models/staging/stg_bronze__orders.sql', "select * from {{ source('bronze', 'orders') }}\n");
    writeFile(root, 'dbt/models/intermediate/int_sales_orders.sql', "select * from {{ ref('stg_bronze__orders') }}\n");
    writeFile(root, 'dbt/models/marts/fct_sales.sql', 'select * from {{ ref("int_sales_orders") }}\n');

    const result = check('applied int_sales_orders fct_sales', {
      vars: {
        run_path: runPath,
        plan_file: 'docs/design/plan.md',
        expected_candidate_statuses: 'STG-001:applied,INT-001:applied,MART-001:applied',
        expected_output_terms: 'applied,int_sales_orders,fct_sales',
        expected_model_refs: 'int_sales_orders:stg_bronze__orders,fct_sales:int_sales_orders',
        expected_validation_results: 'INT-001,MART-001',
      },
    });

    assert.equal(result.pass, true, result.reason);
  });
});

test('passes when blocked higher-layer candidates record blocked reasons', () => {
  withRunProject((root, runPath) => {
    writeFile(root, 'docs/design/plan.md', `# Refactor Mart Plan

## Candidate: STG-001

- [x] Approve: yes
- Type: stg
- Output: dbt/models/staging/stg_bronze__orders.sql
- Depends on: none
- Validation: dbt build --select stg_bronze__orders
- Execution status: planned

## Candidate: INT-001

- [x] Approve: yes
- Type: int
- Output: dbt/models/intermediate/int_sales_orders.sql
- Depends on: STG-001
- Validation: dbt build --select int_sales_orders
- Execution status: blocked
- Blocked reason: dependency STG-001 is planned
`);

    const result = check('blocked skipped', {
      vars: {
        run_path: runPath,
        plan_file: 'docs/design/plan.md',
        expected_candidate_statuses: 'STG-001:planned,INT-001:blocked',
        expected_output_terms: 'blocked,skipped',
        expected_blocked_reasons: 'INT-001',
      },
    });

    assert.equal(result.pass, true, result.reason);
  });
});

test('fails when a staging candidate status changes during int mode', () => {
  withRunProject((root, runPath) => {
    writeHappyPathPlan(root, { stgStatus: 'applied' });
    writeFile(root, 'dbt/models/intermediate/int_sales_orders.sql', "select * from {{ ref('stg_bronze__orders') }}\n");
    writeFile(root, 'dbt/models/marts/fct_sales.sql', "select * from {{ ref('int_sales_orders') }}\n");

    const result = check('applied int_sales_orders fct_sales', {
      vars: {
        run_path: runPath,
        plan_file: 'docs/design/plan.md',
        expected_candidate_statuses: 'STG-001:planned,INT-001:applied,MART-001:applied',
        expected_validation_results: 'INT-001,MART-001',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /STG-001 expected status 'planned'/);
  });
});

test('fails when applied higher-layer candidate omits validation result', () => {
  withRunProject((root, runPath) => {
    writeHappyPathPlan(root, { omitIntValidation: true });
    writeFile(root, 'dbt/models/intermediate/int_sales_orders.sql', "select * from {{ ref('stg_bronze__orders') }}\n");
    writeFile(root, 'dbt/models/marts/fct_sales.sql', "select * from {{ ref('int_sales_orders') }}\n");

    const result = check('applied int_sales_orders fct_sales', {
      vars: {
        run_path: runPath,
        plan_file: 'docs/design/plan.md',
        expected_candidate_statuses: 'STG-001:applied,INT-001:applied,MART-001:applied',
        expected_validation_results: 'INT-001,MART-001',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /INT-001 missing Validation result/);
  });
});

test('fails when declared consumer does not reference expected model', () => {
  withRunProject((root, runPath) => {
    writeHappyPathPlan(root);
    writeFile(root, 'dbt/models/intermediate/int_sales_orders.sql', "select * from {{ ref('stg_bronze__orders') }}\n");
    writeFile(root, 'dbt/models/marts/fct_sales.sql', "select * from {{ source('gold', 'sales') }}\n");

    const result = check('applied int_sales_orders fct_sales', {
      vars: {
        run_path: runPath,
        plan_file: 'docs/design/plan.md',
        expected_candidate_statuses: 'STG-001:applied,INT-001:applied,MART-001:applied',
        expected_model_refs: 'fct_sales:int_sales_orders',
        expected_validation_results: 'INT-001,MART-001',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /Expected consumer fct_sales to reference int_sales_orders/);
  });
});

test('fails when a consumer contains an expected absent model ref', () => {
  withRunProject((root, runPath) => {
    writeHappyPathPlan(root);
    writeFile(root, 'dbt/models/intermediate/int_sales_orders.sql', "select * from {{ ref('stg_bronze__orders') }}\n");
    writeFile(root, 'dbt/models/marts/fct_sales.sql', "select * from {{ ref('int_sales_orders') }}\n");

    const result = check('blocked', {
      vars: {
        run_path: runPath,
        plan_file: 'docs/design/plan.md',
        expected_candidate_statuses: 'STG-001:applied,INT-001:applied,MART-001:applied',
        expected_absent_model_refs: 'fct_sales:int_sales_orders',
        expected_validation_results: 'INT-001,MART-001',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /Expected consumer fct_sales not to reference int_sales_orders/);
  });
});

test('fails when a listed unchanged model content differs from fixture', () => {
  withRunProject((root, runPath) => {
    writeHappyPathPlan(root);
    writeFile(root, 'dbt/models/intermediate/int_sales_orders.sql', "select * from {{ source('bronze', 'orders') }}\n");
    writeFile(root, 'dbt/models/marts/fct_sales.sql', "select * from {{ source('silver', 'sales_orders') }}\n");

    const fixtureRoot = path.join(root, 'fixture');
    writeFile(fixtureRoot, 'dbt/models/intermediate/int_sales_orders.sql', "select * from {{ source('bronze', 'orders') }}\n");
    writeFile(fixtureRoot, 'dbt/models/marts/fct_sales.sql', "select * from {{ source('silver', 'sales_orders') }}\n");

    writeFile(root, 'dbt/models/marts/fct_sales.sql', "select * from {{ ref('int_sales_orders') }}\n");

    const result = check('blocked skipped', {
      vars: {
        fixture_path: path.relative(path.resolve(__dirname, '..', '..', '..'), fixtureRoot),
        run_path: runPath,
        plan_file: 'docs/design/plan.md',
        expected_candidate_statuses: 'STG-001:applied,INT-001:applied,MART-001:applied',
        expected_unchanged_models: 'int_sales_orders,fct_sales',
        expected_validation_results: 'INT-001,MART-001',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /Expected model fct_sales to remain unchanged/);
  });
});

test('fails when expected ref appears only in a SQL comment', () => {
  withRunProject((root, runPath) => {
    writeHappyPathPlan(root);
    writeFile(root, 'dbt/models/intermediate/int_sales_orders.sql', "-- {{ ref('stg_bronze__orders') }}\nselect * from {{ source('bronze', 'orders') }}\n");
    writeFile(root, 'dbt/models/marts/fct_sales.sql', "-- {{ ref('int_sales_orders') }}\nselect * from {{ source('silver', 'sales_orders') }}\n");

    const result = check('applied int_sales_orders fct_sales', {
      vars: {
        run_path: runPath,
        plan_file: 'docs/design/plan.md',
        expected_candidate_statuses: 'STG-001:applied,INT-001:applied,MART-001:applied',
        expected_model_refs: 'fct_sales:int_sales_orders',
        expected_validation_results: 'INT-001,MART-001',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /Expected consumer fct_sales to reference int_sales_orders/);
  });
});

test('fails when the plan contains duplicate candidate IDs', () => {
  withRunProject((root, runPath) => {
    writeFile(root, 'docs/design/plan.md', `# Refactor Mart Plan

## Candidate: INT-001

- [x] Approve: yes
- Type: int
- Output: dbt/models/intermediate/int_sales_orders.sql
- Depends on: STG-001
- Validation: dbt build --select int_sales_orders
- Execution status: blocked
- Blocked reason: dependency STG-001 is planned

## Candidate: INT-001

- [x] Approve: yes
- Type: int
- Output: dbt/models/intermediate/int_sales_orders.sql
- Depends on: STG-001
- Validation: dbt build --select int_sales_orders
- Execution status: blocked
- Blocked reason: dependency STG-001 is planned
`);

    const result = check('blocked skipped', {
      vars: {
        run_path: runPath,
        plan_file: 'docs/design/plan.md',
        expected_candidate_statuses: 'INT-001:blocked',
        expected_blocked_reasons: 'INT-001',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /Duplicate candidate ID found: INT-001/);
  });
});

test('fails when the plan contains an unexpected candidate ID', () => {
  withRunProject((root, runPath) => {
    writeHappyPathPlan(root);

    const result = check('applied int_sales_orders fct_sales', {
      vars: {
        run_path: runPath,
        plan_file: 'docs/design/plan.md',
        expected_candidate_statuses: 'STG-001:applied,INT-001:applied',
        expected_validation_results: 'INT-001',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /Unexpected candidate ID found: MART-001/);
  });
});

test('fails when an applied higher-layer output is missing', () => {
  withRunProject((root, runPath) => {
    writeHappyPathPlan(root);
    writeFile(root, 'dbt/models/marts/fct_sales.sql', "select * from {{ ref('int_sales_orders') }}\n");

    const result = check('applied int_sales_orders fct_sales', {
      vars: {
        run_path: runPath,
        plan_file: 'docs/design/plan.md',
        expected_candidate_statuses: 'STG-001:applied,INT-001:applied,MART-001:applied',
        expected_validation_results: 'INT-001,MART-001',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /Output file not found/);
  });
});

test('fails when applied INT output is under intermediate but lacks int prefix', () => {
  withRunProject((root, runPath) => {
    writeHappyPathPlan(root, { intOutput: 'dbt/models/intermediate/orders.sql' });
    writeFile(root, 'dbt/models/intermediate/orders.sql', "select * from {{ ref('stg_bronze__orders') }}\n");
    writeFile(root, 'dbt/models/marts/fct_sales.sql', "select * from {{ ref('int_sales_orders') }}\n");

    const result = check('applied int_sales_orders fct_sales', {
      vars: {
        run_path: runPath,
        plan_file: 'docs/design/plan.md',
        expected_candidate_statuses: 'STG-001:applied,INT-001:applied,MART-001:applied',
        expected_validation_results: 'INT-001,MART-001',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /not an intermediate model/);
  });
});

test('fails when applied INT output has int prefix but is outside intermediate path', () => {
  withRunProject((root, runPath) => {
    writeHappyPathPlan(root, { intOutput: 'dbt/models/marts/int_orders.sql' });
    writeFile(root, 'dbt/models/marts/int_orders.sql', "select * from {{ ref('stg_bronze__orders') }}\n");
    writeFile(root, 'dbt/models/marts/fct_sales.sql', "select * from {{ ref('int_orders') }}\n");

    const result = check('applied int_orders fct_sales', {
      vars: {
        run_path: runPath,
        plan_file: 'docs/design/plan.md',
        expected_candidate_statuses: 'STG-001:applied,INT-001:applied,MART-001:applied',
        expected_validation_results: 'INT-001,MART-001',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /not an intermediate model/);
  });
});

test('fails when applied MART output is under marts but lacks mart prefix', () => {
  withRunProject((root, runPath) => {
    writeHappyPathPlan(root, { martOutput: 'dbt/models/marts/sales.sql' });
    writeFile(root, 'dbt/models/intermediate/int_sales_orders.sql', "select * from {{ ref('stg_bronze__orders') }}\n");
    writeFile(root, 'dbt/models/marts/sales.sql', "select * from {{ ref('int_sales_orders') }}\n");

    const result = check('applied int_sales_orders fct_sales', {
      vars: {
        run_path: runPath,
        plan_file: 'docs/design/plan.md',
        expected_candidate_statuses: 'STG-001:applied,INT-001:applied,MART-001:applied',
        expected_validation_results: 'INT-001,MART-001',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /not a mart model/);
  });
});

test('fails when applied MART output has mart prefix but is outside marts path', () => {
  withRunProject((root, runPath) => {
    writeHappyPathPlan(root, { martOutput: 'dbt/models/intermediate/fct_sales.sql' });
    writeFile(root, 'dbt/models/intermediate/int_sales_orders.sql', "select * from {{ ref('stg_bronze__orders') }}\n");
    writeFile(root, 'dbt/models/intermediate/fct_sales.sql', "select * from {{ ref('int_sales_orders') }}\n");

    const result = check('applied int_sales_orders fct_sales', {
      vars: {
        run_path: runPath,
        plan_file: 'docs/design/plan.md',
        expected_candidate_statuses: 'STG-001:applied,INT-001:applied,MART-001:applied',
        expected_validation_results: 'INT-001,MART-001',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /not a mart model/);
  });
});
