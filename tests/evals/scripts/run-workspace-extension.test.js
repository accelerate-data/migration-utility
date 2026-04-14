const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const { extensionHook, pruneOldRuns } = require('./run-workspace-extension');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..');
const ROOT_GITIGNORE = path.join(REPO_ROOT, '.gitignore');
const EVAL_PACKAGE_JSON = path.join(REPO_ROOT, 'tests', 'evals', 'package.json');

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

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function readText(filePath) {
  return fs.readFileSync(filePath, 'utf8');
}

function touchMtime(target, mtimeMs) {
  const mtime = new Date(mtimeMs);
  fs.utimesSync(target, mtime, mtime);
}

test('workspace extension entrypoint is wired into eval scripts and ignores run output', () => {
  const packageJson = readJson(EVAL_PACKAGE_JSON);
  const gitignore = readText(ROOT_GITIGNORE);

  assert.equal(
    packageJson.scripts['test:workspace-extension'],
    'node --test scripts/run-workspace-extension.test.js',
  );
  assert.match(gitignore, /^tests\/evals\/output\/runs\/$/m);
});

test('pruneOldRuns removes run directories older than the cutoff', () => {
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
  const missingRoot = path.join(os.tmpdir(), `missing-${Date.now()}`);

  assert.doesNotThrow(() => {
    pruneOldRuns(missingRoot, { cutoffMs: Date.now() - 1 });
  });
});

test('extensionHook creates a fresh run_path after pruning stale runs', async () => {
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
  await assert.rejects(
    () => extensionHook('beforeEach', buildContext('Invalid root'), {
      nowMs: Date.now(),
      runsRoot: '',
      pruneState: makePruneState(),
    }),
    /runsRoot must be a non-empty string when provided/,
  );
});
