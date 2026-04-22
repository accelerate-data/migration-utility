const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const checkDbtRefs = require('./check-dbt-refs');

function makeRunRoot(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'check-dbt-refs-'));
  t.after(() => fs.rmSync(root, { force: true, recursive: true }));
  return root;
}

function writeFile(filePath, content) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, content, 'utf8');
}

test('check-dbt-refs skips when the command has not generated a dbt directory', (t) => {
  const runRoot = makeRunRoot(t);

  const result = checkDbtRefs('', {
    vars: {
      run_path: runRoot,
      target_table: 'silver.DimProduct',
    },
  });

  assert.equal(result.pass, true, result.reason);
  assert.match(result.reason, /No dbt directory/);
});

test('check-dbt-refs validates source calls for generated models', (t) => {
  const runRoot = makeRunRoot(t);
  writeFile(
    path.join(runRoot, 'dbt', 'models', 'marts', 'dimproduct.sql'),
    "select * from {{ source('bronze', 'product') }}\n",
  );
  writeFile(
    path.join(runRoot, 'dbt', 'models', 'staging', '_staging__sources.yml'),
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
      run_path: runRoot,
      target_table: 'silver.DimProduct',
    },
  });

  assert.equal(result.pass, true, result.reason);
});

test('check-dbt-refs rejects generated models with undeclared source calls', (t) => {
  const runRoot = makeRunRoot(t);
  writeFile(
    path.join(runRoot, 'dbt', 'models', 'marts', 'dimproduct.sql'),
    "select * from {{ source('bronze', 'product') }}\n",
  );
  writeFile(
    path.join(runRoot, 'dbt', 'models', 'staging', '_staging__sources.yml'),
    'version: 2\nsources: []\n',
  );

  const result = checkDbtRefs('', {
    vars: {
      run_path: runRoot,
      target_table: 'silver.DimProduct',
    },
  });

  assert.equal(result.pass, false);
  assert.match(result.reason, /not in _staging__sources.yml/);
});
