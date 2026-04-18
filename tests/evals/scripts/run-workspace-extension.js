const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..');
const RUNS_ROOT = path.join(REPO_ROOT, 'tests', 'evals', 'output', 'runs');
const RUN_RETENTION_MS = 24 * 60 * 60 * 1000;
const modulePruneState = { hasPrunedRuns: false };
const VOLATILE_PATHS = [
  '.migration-runs',
  'model-review-results',
  'test-review-results',
  'context.json',
  path.join('dbt', 'logs'),
  path.join('dbt', 'target'),
];
const FIXTURE_DB_NAME = 'MigrationTest';
const LEGACY_MANIFEST_KEYS = [
  'source_database',
  'source_host',
  'source_port',
  'extracted_schemas',
  'extracted_at',
  'sandbox',
];

function sanitizeSegment(value, fallback) {
  const normalized = String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80);
  return normalized || fallback;
}

function clearVolatileArtifacts(projectRoot) {
  for (const relativePath of VOLATILE_PATHS) {
    fs.rmSync(path.join(projectRoot, relativePath), { force: true, recursive: true });
  }
}

function loadManifest(projectRoot) {
  const manifestPath = path.join(projectRoot, 'manifest.json');
  if (!fs.existsSync(manifestPath)) {
    throw new Error(`manifest.json not found in ${projectRoot}`);
  }
  return JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
}

function validateManifest(manifest, projectRoot) {
  const legacyKeys = LEGACY_MANIFEST_KEYS.filter((key) => Object.prototype.hasOwnProperty.call(manifest, key));
  if (legacyKeys.length > 0) {
    throw new Error(
      `Fixture manifest at ${projectRoot} uses legacy keys: ${legacyKeys.join(', ')}. ` +
      'Port the fixture to runtime.* and extraction.* first.',
    );
  }

  if (!manifest.runtime || !manifest.runtime.source) {
    throw new Error(`Fixture manifest at ${projectRoot} is missing runtime.source`);
  }

  const hasDbtProject = fs.existsSync(path.join(projectRoot, 'dbt', 'profiles.yml'));
  if (hasDbtProject && !manifest.runtime.target) {
    throw new Error(
      `Fixture manifest at ${projectRoot} has dbt/ but is missing runtime.target`,
    );
  }
}

function targetDatabaseFromManifest(manifest) {
  const target = manifest?.runtime?.target;
  if (!target) {
    return FIXTURE_DB_NAME;
  }

  if (target.technology !== 'sql_server') {
    throw new Error(
      `Eval dbt target technology ${target.technology} is not supported by run-workspace-extension`,
    );
  }

  const database = target.connection?.database;
  if (!database) {
    throw new Error('runtime.target.connection.database is required for dbt eval fixtures');
  }
  return database;
}

function pinFixtureDatabase(projectRoot, manifest) {
  const profilesPath = path.join(projectRoot, 'dbt', 'profiles.yml');
  if (!fs.existsSync(profilesPath)) {
    return;
  }

  const target = manifest?.runtime?.target;
  if (target?.technology === 'oracle') {
    return;
  }

  const targetDatabase = targetDatabaseFromManifest(manifest);
  const original = fs.readFileSync(profilesPath, 'utf8');
  const pinned = original.replace(
    /^(\s*database:\s*).+$/m,
    `database: "${targetDatabase}"`,
  );

  if (pinned !== original) {
    fs.writeFileSync(profilesPath, pinned, 'utf8');
  }
}

function normalizeFqn(value) {
  return String(value || '').trim().toLowerCase();
}

function injectCatalogError(projectRoot, vars) {
  const objectFqn = normalizeFqn(vars?.catalog_error_object);
  if (!objectFqn) {
    return;
  }

  const catalogRoot = path.join(projectRoot, 'catalog');
  const catalogFile = ['tables', 'views']
    .map((bucket) => path.join(catalogRoot, bucket, `${objectFqn}.json`))
    .find((candidate) => fs.existsSync(candidate));

  if (!catalogFile) {
    throw new Error(`catalog_error_object not found in run workspace: ${objectFqn}`);
  }

  const catalog = JSON.parse(fs.readFileSync(catalogFile, 'utf8'));
  catalog.errors = [
    ...(Array.isArray(catalog.errors) ? catalog.errors : []),
    {
    code: vars.catalog_error_code || 'CATALOG_ERROR',
    message: vars.catalog_error_message || 'Injected catalog error for readiness guard eval.',
    severity: 'error',
    },
  ];
  fs.writeFileSync(catalogFile, JSON.stringify(catalog, null, 2) + '\n', 'utf8');
}

function isMissingPathError(error) {
  return error && typeof error === 'object' && error.code === 'ENOENT';
}

function readDirectoryEntries(dirPath) {
  try {
    return fs.readdirSync(dirPath, { withFileTypes: true });
  } catch (error) {
    if (isMissingPathError(error)) {
      return [];
    }
    throw error;
  }
}

function latestActivityMs(targetPath) {
  let stats;
  try {
    stats = fs.statSync(targetPath);
  } catch (error) {
    if (isMissingPathError(error)) {
      return null;
    }
    throw error;
  }

  let latestMs = stats.mtimeMs;
  if (!stats.isDirectory()) {
    return latestMs;
  }

  for (const entry of readDirectoryEntries(targetPath)) {
    const childActivityMs = latestActivityMs(path.join(targetPath, entry.name));
    if (childActivityMs !== null && childActivityMs > latestMs) {
      latestMs = childActivityMs;
    }
  }

  return latestMs;
}

function pruneOldRuns(root, { cutoffMs }) {
  if (!fs.existsSync(root)) {
    return;
  }

  for (const suiteEntry of readDirectoryEntries(root)) {
    if (!suiteEntry.isDirectory()) {
      continue;
    }

    const suiteRoot = path.join(root, suiteEntry.name);
    for (const runEntry of readDirectoryEntries(suiteRoot)) {
      if (!runEntry.isDirectory()) {
        continue;
      }

      const runRoot = path.join(suiteRoot, runEntry.name);
      const runActivityMs = latestActivityMs(runRoot);
      if (runActivityMs !== null && runActivityMs < cutoffMs) {
        fs.rmSync(runRoot, { force: true, recursive: true });
      }
    }
  }
}

function resolveRunsRoot(runsRoot) {
  if (runsRoot === undefined || runsRoot === null) {
    return RUNS_ROOT;
  }

  if (typeof runsRoot !== 'string' || runsRoot.trim().length === 0) {
    throw new Error('runsRoot must be a non-empty string when provided');
  }

  return runsRoot;
}

function resolvePruneState(pruneState) {
  return pruneState ?? modulePruneState;
}

async function extensionHook(hookName, context, options = {}) {
  if (hookName !== 'beforeEach') {
    return context;
  }

  const fixturePath = context?.test?.vars?.fixture_path;
  if (!fixturePath || context.test.vars.run_path) {
    return context;
  }

  const fixtureRoot = path.resolve(REPO_ROOT, fixturePath);
  if (!fs.existsSync(fixtureRoot)) {
    throw new Error(`fixture_path does not exist: ${fixtureRoot}`);
  }

  const runsRoot = resolveRunsRoot(options.runsRoot);
  const nowMs = options.nowMs ?? Date.now();
  const pruneState = resolvePruneState(options.pruneState);

  if (!pruneState.hasPrunedRuns) {
    pruneOldRuns(runsRoot, { cutoffMs: nowMs - RUN_RETENTION_MS });
    pruneState.hasPrunedRuns = true;
  }

  const suiteSlug = sanitizeSegment(context?.suite?.description, 'eval');
  const testSlug = sanitizeSegment(context?.test?.description, 'test');
  const fixtureSlug = sanitizeSegment(path.basename(fixtureRoot), 'fixture');
  const uniqueId = crypto.randomUUID().slice(0, 8);
  const runRoot = path.join(runsRoot, suiteSlug, `${fixtureSlug}-${testSlug}-${uniqueId}`);

  fs.mkdirSync(path.dirname(runRoot), { recursive: true });
  fs.rmSync(runRoot, { force: true, recursive: true });
  fs.cpSync(fixtureRoot, runRoot, {
    recursive: true,
    filter: (src) => {
      const rel = path.relative(fixtureRoot, src);
      return !VOLATILE_PATHS.some(
        (v) => rel === v || rel.startsWith(v + path.sep),
      );
    },
  });
  clearVolatileArtifacts(runRoot); // defensive: filter already skips these, but guard against drift
  const manifest = loadManifest(runRoot);
  validateManifest(manifest, runRoot);
  pinFixtureDatabase(runRoot, manifest);
  injectCatalogError(runRoot, context.test.vars);

  context.test.vars.run_path = runRoot;
  context.test.vars.repo_root = REPO_ROOT;
  return context;
}

module.exports = extensionHook;
module.exports.extensionHook = extensionHook;
module.exports.pruneOldRuns = pruneOldRuns;
