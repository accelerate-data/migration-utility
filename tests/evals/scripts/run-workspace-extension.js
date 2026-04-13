const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..');
const RUNS_ROOT = path.join(REPO_ROOT, 'tests', 'evals', 'output', 'runs');
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

function relativePosixPath(targetPath) {
  return path.relative(REPO_ROOT, targetPath).split(path.sep).join(path.posix.sep);
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

async function extensionHook(hookName, context) {
  if (hookName !== 'beforeEach') {
    return context;
  }

  const fixturePath = context?.test?.vars?.fixture_path;
  if (!fixturePath || context.test.vars.run_path) {
    return context;
  }

  const fixtureRoot = path.resolve(REPO_ROOT, fixturePath);
  if (!fs.existsSync(fixtureRoot)) {
    return context;
  }

  const suiteSlug = sanitizeSegment(context?.suite?.description, 'eval');
  const testSlug = sanitizeSegment(context?.test?.description, 'test');
  const fixtureSlug = sanitizeSegment(path.basename(fixtureRoot), 'fixture');
  const uniqueId = crypto.randomUUID().slice(0, 8);
  const runRoot = path.join(RUNS_ROOT, suiteSlug, `${fixtureSlug}-${testSlug}-${uniqueId}`);

  fs.mkdirSync(path.dirname(runRoot), { recursive: true });
  fs.rmSync(runRoot, { force: true, recursive: true });
  fs.cpSync(fixtureRoot, runRoot, { recursive: true });
  clearVolatileArtifacts(runRoot);
  const manifest = loadManifest(runRoot);
  validateManifest(manifest, runRoot);
  pinFixtureDatabase(runRoot, manifest);

  context.test.vars.run_path = relativePosixPath(runRoot);
  return context;
}

module.exports = extensionHook;
