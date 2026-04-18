const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..');
const ROOT_GITIGNORE = path.join(REPO_ROOT, '.gitignore');
const EVAL_PACKAGE_JSON = path.join(REPO_ROOT, 'tests', 'evals', 'package.json');
const EVAL_PACKAGES_DIR = path.join(REPO_ROOT, 'tests', 'evals', 'packages');
const EXTENSION_MODULE_PATH = require.resolve('./run-workspace-extension');
const LIVE_PACKAGE_CONFIGS = [
  'oracle-live/promptfooconfig.yaml',
  'mssql-live/promptfooconfig.yaml',
];

function makeRunDir(root, relativePath) {
  const target = path.join(root, relativePath);
  fs.mkdirSync(target, { recursive: true });
  return target;
}

function buildContext(testDescription = 'Creates a workspace copy') {
  return {
    suite: { description: 'Listing Objects' },
    test: {
      description: testDescription,
      vars: {
        fixture_path: 'tests/evals/fixtures/cmd-scope/happy-path',
      },
    },
  };
}

function makePruneState() {
  return { hasPrunedRuns: false };
}

function loadExtensionModule() {
  delete require.cache[EXTENSION_MODULE_PATH];
  return require('./run-workspace-extension');
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function readText(filePath) {
  return fs.readFileSync(filePath, 'utf8');
}

function hasWhitespaceSeparatedTokens(command, tokens) {
  const parts = command.trim().split(/\s+/);
  return tokens.every((token) => parts.includes(token));
}

function hasMaxConcurrencyOne(command) {
  return hasWhitespaceSeparatedTokens(command, ['--max-concurrency', '1']);
}

function readEvalPackageConfigs() {
  return fs.readdirSync(EVAL_PACKAGES_DIR, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .flatMap((entry) => {
      const packageDir = path.join(EVAL_PACKAGES_DIR, entry.name);
      return fs.readdirSync(packageDir, { withFileTypes: true })
        .filter((file) => file.isFile() && file.name.endsWith('.yaml'))
        .map((file) => path.posix.join('packages', entry.name, file.name));
    })
    .sort();
}

function extractEvalConfigs(command) {
  const configs = [];
  const configPattern = /(?:^|\s)-c\s+([^\s]+)/g;
  let match;

  while ((match = configPattern.exec(command)) !== null) {
    configs.push(match[1]);
  }

  return configs;
}

function touchMtime(target, mtimeMs) {
  const mtime = new Date(mtimeMs);
  fs.utimesSync(target, mtime, mtime);
}

function countSmokeDescriptions(packageConfigPath) {
  const relativePath = packageConfigPath.startsWith('packages/')
    ? packageConfigPath.slice('packages/'.length)
    : packageConfigPath;
  const configText = readText(path.join(EVAL_PACKAGES_DIR, relativePath));
  const matches = configText.match(/^\s*-\s+description:\s*["']?\[smoke\]/gm);
  return matches === null ? 0 : matches.length;
}

test('workspace extension entrypoint is wired into eval scripts and ignores run output', () => {
  const packageJson = readJson(EVAL_PACKAGE_JSON);
  const gitignoreEntries = readText(ROOT_GITIGNORE)
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  const workspaceExtensionScript = packageJson.scripts['test:workspace-extension'];

  assert.equal(typeof workspaceExtensionScript, 'string');
  assert.equal(Boolean(workspaceExtensionScript), true);
  assert.equal(hasWhitespaceSeparatedTokens(workspaceExtensionScript, ['node', '--test']), true);
  assert.equal(
    workspaceExtensionScript.includes('scripts/run-workspace-extension.test.js'),
    true,
  );
  assert.equal(gitignoreEntries.includes('tests/evals/output/runs/'), true);
});

test('smoke eval script exists and live suites remain standalone', () => {
  const packageJson = readJson(EVAL_PACKAGE_JSON);
  const scripts = packageJson.scripts;

  assert.equal(Boolean(scripts['eval:smoke']), true);
  assert.equal(Boolean(scripts['eval:skills']), false);
  assert.equal(Boolean(scripts['eval:commands']), false);

  assert.equal(
    hasWhitespaceSeparatedTokens(scripts['eval:smoke'], ['./scripts/promptfoo.sh', 'eval', '--no-cache']),
    true,
  );
  assert.equal(
    scripts['eval:smoke'].includes("--filter-pattern '^\\[smoke\\]'"),
    true,
  );
  assert.deepEqual(extractEvalConfigs(scripts['eval:smoke']), readEvalPackageConfigs());
  assert.equal(Boolean(scripts['eval:full']), false);
  assert.equal(Boolean(scripts['eval:oracle-regression']), false);
  assert.equal(Boolean(scripts['eval:oracle-live']), true);
  assert.equal(Boolean(scripts['eval:mssql-live']), true);
  assert.deepEqual(extractEvalConfigs(scripts['eval:oracle-live']), ['oracle-live/promptfooconfig.yaml']);
  assert.deepEqual(extractEvalConfigs(scripts['eval:mssql-live']), ['mssql-live/promptfooconfig.yaml']);

  const smokeConfigs = extractEvalConfigs(scripts['eval:smoke']);
  for (const liveConfig of LIVE_PACKAGE_CONFIGS) {
    assert.equal(smokeConfigs.includes(liveConfig), false);
  }
});

test('each eval package config has exactly one smoke scenario', () => {
  const packageJson = readJson(EVAL_PACKAGE_JSON);
  const smokePackageConfigs = extractEvalConfigs(packageJson.scripts['eval:smoke']);

  for (const packageConfigPath of smokePackageConfigs) {
    assert.equal(countSmokeDescriptions(packageConfigPath), 1, packageConfigPath);
  }
});

test('pruneOldRuns removes run directories older than the cutoff', () => {
  const { pruneOldRuns } = loadExtensionModule();
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'eval-runs-'));
  try {
    const staleRun = makeRunDir(tempRoot, 'suite-a/stale-run');
    const freshRun = makeRunDir(tempRoot, 'suite-a/fresh-run');
    const now = Date.now();

    touchMtime(staleRun, now - (26 * 60 * 60 * 1000));
    touchMtime(freshRun, now - (2 * 60 * 60 * 1000));

    pruneOldRuns(tempRoot, { cutoffMs: now - (24 * 60 * 60 * 1000) });

    assert.equal(fs.existsSync(staleRun), false);
    assert.equal(fs.existsSync(freshRun), true);
  } finally {
    fs.rmSync(tempRoot, { recursive: true, force: true });
  }
});

test('pruneOldRuns is a no-op when the runs root is missing', () => {
  const { pruneOldRuns } = loadExtensionModule();
  const missingRoot = path.join(os.tmpdir(), `missing-${Date.now()}`);

  assert.doesNotThrow(() => {
    pruneOldRuns(missingRoot, { cutoffMs: Date.now() - 1 });
  });
});

test('pruneOldRuns retains run directories exactly at the cutoff boundary', () => {
  const { pruneOldRuns } = loadExtensionModule();
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'eval-runs-'));
  try {
    const boundaryRun = makeRunDir(tempRoot, 'suite-a/boundary-run');
    const now = Date.now();

    touchMtime(boundaryRun, now - (24 * 60 * 60 * 1000));

    pruneOldRuns(tempRoot, { cutoffMs: now - (24 * 60 * 60 * 1000) });

    assert.equal(fs.existsSync(boundaryRun), true);
  } finally {
    fs.rmSync(tempRoot, { recursive: true, force: true });
  }
});

test('pruneOldRuns retains runs with recent nested activity even when the run root mtime is old', () => {
  const { pruneOldRuns } = loadExtensionModule();
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'eval-runs-'));
  try {
    const activeRun = makeRunDir(tempRoot, 'suite-a/active-run');
    const nestedDir = makeRunDir(tempRoot, 'suite-a/active-run/logs');
    const nestedFile = path.join(nestedDir, 'heartbeat.log');
    const now = Date.now();

    fs.writeFileSync(nestedFile, 'still running\n', 'utf8');
    touchMtime(activeRun, now - (26 * 60 * 60 * 1000));
    touchMtime(nestedDir, now - (5 * 60 * 1000));
    touchMtime(nestedFile, now - (5 * 60 * 1000));

    pruneOldRuns(tempRoot, { cutoffMs: now - (24 * 60 * 60 * 1000) });

    assert.equal(fs.existsSync(activeRun), true);
  } finally {
    fs.rmSync(tempRoot, { recursive: true, force: true });
  }
});

test('extensionHook creates a fresh run_path after pruning stale runs', async () => {
  const { extensionHook } = loadExtensionModule();
  const tempRunsRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'eval-runs-'));
  const staleRun = makeRunDir(tempRunsRoot, 'task1-cleanup/stale-run');
  const now = Date.now();
  touchMtime(staleRun, now - (26 * 60 * 60 * 1000));

  try {
    const context = {
      suite: { description: 'Listing Objects' },
      test: {
        description: 'Creates a workspace copy',
        vars: {
          fixture_path: 'tests/evals/fixtures/cmd-scope/happy-path',
        },
      },
    };

    const nextContext = await extensionHook('beforeEach', context, {
      nowMs: now,
      runsRoot: tempRunsRoot,
      pruneState: makePruneState(),
    });
    const runPath = path.resolve(REPO_ROOT, nextContext.test.vars.run_path);

    assert.equal(fs.existsSync(staleRun), false);
    assert.equal(fs.existsSync(runPath), true);
    assert.equal(typeof nextContext.test.vars.run_path, 'string');
    assert.notEqual(nextContext.test.vars.run_path.length, 0);
  } finally {
    fs.rmSync(tempRunsRoot, { recursive: true, force: true });
  }
});

test('extensionHook prunes once per process across runs roots', async () => {
  const { extensionHook } = loadExtensionModule();
  const now = Date.now();
  const firstRunsRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'eval-runs-a-'));
  const secondRunsRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'eval-runs-b-'));
  const pruneState = makePruneState();

  try {
    const firstStaleRun = makeRunDir(firstRunsRoot, 'listing-objects/stale-run-1');
    touchMtime(firstStaleRun, now - (26 * 60 * 60 * 1000));

    const firstContext = await extensionHook('beforeEach', buildContext('First run'), {
      nowMs: now,
      runsRoot: firstRunsRoot,
      pruneState,
    });

    assert.equal(fs.existsSync(firstStaleRun), false);
    assert.equal(typeof firstContext.test.vars.run_path, 'string');
    assert.notEqual(firstContext.test.vars.run_path.length, 0);

    const secondStaleRun = makeRunDir(firstRunsRoot, 'listing-objects/stale-run-2');
    touchMtime(secondStaleRun, now - (26 * 60 * 60 * 1000));

    const secondContext = await extensionHook('beforeEach', buildContext('Second run'), {
      nowMs: now + 1000,
      runsRoot: firstRunsRoot,
      pruneState,
    });

    assert.equal(fs.existsSync(secondStaleRun), true);
    assert.equal(typeof secondContext.test.vars.run_path, 'string');
    assert.notEqual(secondContext.test.vars.run_path.length, 0);

    const thirdStaleRun = makeRunDir(secondRunsRoot, 'listing-objects/stale-run-3');
    touchMtime(thirdStaleRun, now - (26 * 60 * 60 * 1000));

    const thirdContext = await extensionHook('beforeEach', buildContext('Third run'), {
      nowMs: now + 2000,
      runsRoot: secondRunsRoot,
      pruneState,
    });

    assert.equal(fs.existsSync(thirdStaleRun), true);
    assert.equal(typeof thirdContext.test.vars.run_path, 'string');
    assert.notEqual(thirdContext.test.vars.run_path.length, 0);
  } finally {
    fs.rmSync(firstRunsRoot, { recursive: true, force: true });
    fs.rmSync(secondRunsRoot, { recursive: true, force: true });
  }
});

test('extensionHook rejects an empty runsRoot override', async () => {
  const { extensionHook } = loadExtensionModule();
  await assert.rejects(
    () => extensionHook('beforeEach', buildContext('Invalid root'), {
      nowMs: Date.now(),
      runsRoot: '',
      pruneState: makePruneState(),
    }),
    /runsRoot must be a non-empty string when provided/,
  );
});

test('extensionHook rejects a missing fixture_path target', async () => {
  const { extensionHook } = loadExtensionModule();
  const tempRunsRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'eval-runs-'));

  try {
    await assert.rejects(
      () => extensionHook('beforeEach', {
        suite: { description: 'Missing fixture' },
        test: {
          description: 'Missing fixture path',
          vars: {
            fixture_path: 'tests/evals/fixtures/does-not-exist',
          },
        },
      }, {
        nowMs: Date.now(),
        runsRoot: tempRunsRoot,
        pruneState: makePruneState(),
      }),
      /fixture_path does not exist:/,
    );
  } finally {
    fs.rmSync(tempRunsRoot, { recursive: true, force: true });
  }
});

test('pruneOldRuns tolerates ENOENT when a nested child disappears during recursive traversal', () => {
  const extensionModule = loadExtensionModule();
  const { pruneOldRuns } = extensionModule;
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'eval-runs-'));
  const originalStatSync = fs.statSync;
  const statCalls = [];

  try {
    const vanishingRun = makeRunDir(tempRoot, 'suite-a/vanishing-run');
    const missingChild = makeRunDir(vanishingRun, 'logs');
    const steadyRun = makeRunDir(tempRoot, 'suite-a/steady-run');
    const cutoffMs = Date.now() - (24 * 60 * 60 * 1000);

    fs.statSync = (targetPath, ...args) => {
      statCalls.push(targetPath);
      if (targetPath === missingChild) {
        const error = new Error(`ENOENT: no such file or directory, stat '${targetPath}'`);
        error.code = 'ENOENT';
        throw error;
      }
      return originalStatSync.call(fs, targetPath, ...args);
    };

    assert.doesNotThrow(() => {
      pruneOldRuns(tempRoot, { cutoffMs });
    });
    assert.equal(statCalls.includes(vanishingRun), true);
    assert.equal(statCalls.includes(missingChild), true);
    assert.equal(fs.existsSync(steadyRun), true);
  } finally {
    fs.statSync = originalStatSync;
    fs.rmSync(tempRoot, { recursive: true, force: true });
  }
});

test('extensionHook excludes volatile paths from the run copy', async () => {
  const { extensionHook } = loadExtensionModule();
  const tempRunsRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'eval-runs-'));
  const tempFixture = fs.mkdtempSync(path.join(os.tmpdir(), 'eval-fixture-'));

  try {
    // Build a minimal valid fixture
    const manifest = {
      technology: 'sql_server',
      dialect: 'tsql',
      runtime: {
        source: { technology: 'sql_server', dialect: 'tsql', connection: { database: 'TestDB' } },
      },
    };
    fs.writeFileSync(path.join(tempFixture, 'manifest.json'), JSON.stringify(manifest), 'utf8');
    fs.mkdirSync(path.join(tempFixture, 'ddl'), { recursive: true });
    fs.writeFileSync(path.join(tempFixture, 'ddl', 'tables.sql'), 'CREATE TABLE t(id INT);\n', 'utf8');
    fs.mkdirSync(path.join(tempFixture, 'catalog'), { recursive: true });
    fs.writeFileSync(path.join(tempFixture, 'catalog', 'tables.json'), '{}', 'utf8');

    // Plant volatile artifacts that should be excluded
    fs.mkdirSync(path.join(tempFixture, 'dbt', 'target'), { recursive: true });
    fs.writeFileSync(path.join(tempFixture, 'dbt', 'target', 'manifest.json'), '{}', 'utf8');
    fs.mkdirSync(path.join(tempFixture, 'dbt', 'logs'), { recursive: true });
    fs.writeFileSync(path.join(tempFixture, 'dbt', 'logs', 'dbt.log'), 'log content\n', 'utf8');
    fs.mkdirSync(path.join(tempFixture, '.migration-runs'), { recursive: true });
    fs.writeFileSync(path.join(tempFixture, '.migration-runs', 'run.json'), '{}', 'utf8');
    fs.mkdirSync(path.join(tempFixture, 'model-review-results'), { recursive: true });
    fs.writeFileSync(path.join(tempFixture, 'model-review-results', 'result.json'), '{}', 'utf8');
    fs.mkdirSync(path.join(tempFixture, 'test-review-results'), { recursive: true });
    fs.writeFileSync(path.join(tempFixture, 'test-review-results', 'result.json'), '{}', 'utf8');
    fs.writeFileSync(path.join(tempFixture, 'context.json'), '{}', 'utf8');

    const fixturePath = path.relative(REPO_ROOT, tempFixture).split(path.sep).join(path.posix.sep);
    const context = {
      suite: { description: 'Filter test' },
      test: { description: 'Excludes volatile paths', vars: { fixture_path: fixturePath } },
    };

    const nextContext = await extensionHook('beforeEach', context, {
      nowMs: Date.now(),
      runsRoot: tempRunsRoot,
      pruneState: makePruneState(),
    });
    const runPath = path.resolve(REPO_ROOT, nextContext.test.vars.run_path);

    // Valid content must be present
    assert.equal(fs.existsSync(path.join(runPath, 'manifest.json')), true);
    assert.equal(fs.existsSync(path.join(runPath, 'ddl', 'tables.sql')), true);
    assert.equal(fs.existsSync(path.join(runPath, 'catalog', 'tables.json')), true);

    // Volatile content must be absent
    assert.equal(fs.existsSync(path.join(runPath, 'dbt', 'target')), false);
    assert.equal(fs.existsSync(path.join(runPath, 'dbt', 'logs')), false);
    assert.equal(fs.existsSync(path.join(runPath, '.migration-runs')), false);
    assert.equal(fs.existsSync(path.join(runPath, 'model-review-results')), false);
    assert.equal(fs.existsSync(path.join(runPath, 'test-review-results')), false);
    assert.equal(fs.existsSync(path.join(runPath, 'context.json')), false);
  } finally {
    fs.rmSync(tempRunsRoot, { recursive: true, force: true });
    fs.rmSync(tempFixture, { recursive: true, force: true });
  }
});

test('extensionHook can inject a top-level catalog error into the run copy', async () => {
  const { extensionHook } = loadExtensionModule();
  const tempRunsRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'eval-runs-'));

  try {
    const context = buildContext('Injects catalog error');
    context.test.vars.catalog_error_object = 'silver.DimProduct';
    context.test.vars.catalog_error_code = 'TYPE_MAPPING_UNSUPPORTED';
    context.test.vars.catalog_error_message = 'Unsupported type in eval fixture.';

    const nextContext = await extensionHook('beforeEach', context, {
      nowMs: Date.now(),
      runsRoot: tempRunsRoot,
      pruneState: makePruneState(),
    });
    const runPath = path.resolve(REPO_ROOT, nextContext.test.vars.run_path);
    const runCatalog = readJson(path.join(runPath, 'catalog', 'tables', 'silver.dimproduct.json'));
    const fixtureCatalog = readJson(
      path.join(REPO_ROOT, 'tests/evals/fixtures/cmd-scope/happy-path/catalog/tables/silver.dimproduct.json'),
    );

    assert.deepEqual(runCatalog.errors, [{
      code: 'TYPE_MAPPING_UNSUPPORTED',
      message: 'Unsupported type in eval fixture.',
      severity: 'error',
    }]);
    assert.equal(fixtureCatalog.errors, undefined);
  } finally {
    fs.rmSync(tempRunsRoot, { recursive: true, force: true });
  }
});

test('extensionHook allows oracle dbt fixtures without rewriting profiles.yml', async () => {
  const { extensionHook } = loadExtensionModule();
  const tempRunsRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'eval-runs-'));
  const tempFixture = fs.mkdtempSync(path.join(os.tmpdir(), 'eval-fixture-'));

  try {
    const manifest = {
      technology: 'oracle',
      dialect: 'oracle',
      runtime: {
        source: {
          technology: 'oracle',
          dialect: 'oracle',
          connection: { service: 'FREEPDB1' },
        },
        target: {
          technology: 'oracle',
          dialect: 'oracle',
          connection: { service: 'FREEPDB1' },
        },
      },
    };
    const profileText = [
      'oracle_regression_test:',
      '  target: dev',
      '  outputs:',
      '    dev:',
      '      type: oracle',
      '      service: "{{ env_var(\'ORACLE_SERVICE\', \'FREEPDB1\') }}"',
      '      schema: "{{ env_var(\'DBT_SCHEMA\', \'sh\') }}"',
      '',
    ].join('\n');

    fs.writeFileSync(path.join(tempFixture, 'manifest.json'), JSON.stringify(manifest), 'utf8');
    fs.mkdirSync(path.join(tempFixture, 'ddl'), { recursive: true });
    fs.writeFileSync(path.join(tempFixture, 'ddl', 'procedures.sql'), 'BEGIN NULL; END;\n', 'utf8');
    fs.mkdirSync(path.join(tempFixture, 'catalog'), { recursive: true });
    fs.writeFileSync(path.join(tempFixture, 'catalog', 'tables.json'), '{}', 'utf8');
    fs.mkdirSync(path.join(tempFixture, 'dbt'), { recursive: true });
    fs.writeFileSync(path.join(tempFixture, 'dbt', 'profiles.yml'), profileText, 'utf8');

    const fixturePath = path.relative(REPO_ROOT, tempFixture).split(path.sep).join(path.posix.sep);
    const context = {
      suite: { description: 'Oracle regression' },
      test: { description: 'Oracle fixture', vars: { fixture_path: fixturePath } },
    };

    const nextContext = await extensionHook('beforeEach', context, {
      nowMs: Date.now(),
      runsRoot: tempRunsRoot,
      pruneState: makePruneState(),
    });
    const runPath = path.resolve(REPO_ROOT, nextContext.test.vars.run_path);
    const copiedProfile = fs.readFileSync(path.join(runPath, 'dbt', 'profiles.yml'), 'utf8');

    assert.equal(copiedProfile, profileText);
    assert.equal(fs.existsSync(path.join(runPath, 'manifest.json')), true);
  } finally {
    fs.rmSync(tempRunsRoot, { recursive: true, force: true });
    fs.rmSync(tempFixture, { recursive: true, force: true });
  }
});

test('extensionHook uses the default module prune state when pruneState is not provided', async () => {
  const firstModule = loadExtensionModule();
  const now = Date.now();
  const firstRunsRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'eval-runs-a-'));
  const secondRunsRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'eval-runs-b-'));

  try {
    const firstStaleRun = makeRunDir(firstRunsRoot, 'listing-objects/stale-run-1');
    touchMtime(firstStaleRun, now - (26 * 60 * 60 * 1000));

    await firstModule.extensionHook('beforeEach', buildContext('Default state first run'), {
      nowMs: now,
      runsRoot: firstRunsRoot,
    });

    assert.equal(fs.existsSync(firstStaleRun), false);

    const secondStaleRun = makeRunDir(secondRunsRoot, 'listing-objects/stale-run-2');
    touchMtime(secondStaleRun, now - (26 * 60 * 60 * 1000));

    await firstModule.extensionHook('beforeEach', buildContext('Default state second run'), {
      nowMs: now + 1000,
      runsRoot: secondRunsRoot,
    });

    assert.equal(fs.existsSync(secondStaleRun), true);
  } finally {
    fs.rmSync(firstRunsRoot, { recursive: true, force: true });
    fs.rmSync(secondRunsRoot, { recursive: true, force: true });
  }
});
