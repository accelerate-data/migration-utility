const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const checkDbtRefs = require('./check-dbt-refs');

function makeRunPath() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'check-dbt-refs-'));
}

function writeFile(filePath, content) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, content, 'utf8');
}

test('check-dbt-refs fails when dbt directory is missing by default', () => {
  const runPath = makeRunPath();
  try {
    const result = checkDbtRefs('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.DimProduct',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /dbt directory not found/);
  } finally {
    fs.rmSync(runPath, { recursive: true, force: true });
  }
});

test('check-dbt-refs fails when target model is missing by default', () => {
  const runPath = makeRunPath();
  try {
    fs.mkdirSync(path.join(runPath, 'dbt', 'models', 'marts'), { recursive: true });

    const result = checkDbtRefs('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.DimProduct',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /No model file found for 'dimproduct'/);
  } finally {
    fs.rmSync(runPath, { recursive: true, force: true });
  }
});

test('check-dbt-refs allows missing model when explicitly expected', () => {
  const runPath = makeRunPath();
  try {
    fs.mkdirSync(path.join(runPath, 'dbt', 'models', 'marts'), { recursive: true });

    const result = checkDbtRefs('', {
      vars: {
        run_path: runPath,
        target_table: 'bronze.Product',
        expect_no_generated_model: 'true',
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(runPath, { recursive: true, force: true });
  }
});

test('check-dbt-refs fails when a model is generated despite skip expectation', () => {
  const runPath = makeRunPath();
  try {
    writeFile(
      path.join(runPath, 'dbt', 'models', 'marts', 'product.sql'),
      "select * from {{ source('bronze', 'product') }}\n",
    );
    writeFile(
      path.join(runPath, 'dbt', 'models', 'staging', '_staging__sources.yml'),
      [
        'version: 2',
        'sources:',
        '  - name: bronze',
        '    tables:',
        '      - name: product',
        '',
      ].join('\n'),
    );

    const result = checkDbtRefs('', {
      vars: {
        run_path: runPath,
        target_table: 'bronze.Product',
        expect_no_generated_model: 'true',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /Expected no generated model/);
  } finally {
    fs.rmSync(runPath, { recursive: true, force: true });
  }
});

test('check-dbt-refs validates source calls for generated models', () => {
  const runPath = makeRunPath();
  try {
    writeFile(
      path.join(runPath, 'dbt', 'models', 'marts', 'dimproduct.sql'),
      "select * from {{ source('bronze', 'product') }}\n",
    );
    writeFile(
      path.join(runPath, 'dbt', 'models', 'staging', '_staging__sources.yml'),
      [
        'version: 2',
        'sources:',
        '  - name: bronze',
        '    tables:',
        '      - name: product',
        '',
      ].join('\n'),
    );

    const result = checkDbtRefs('', {
      vars: {
        run_path: runPath,
        target_table: 'silver.DimProduct',
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(runPath, { recursive: true, force: true });
  }
});
