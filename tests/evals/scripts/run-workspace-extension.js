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

function pinFixtureDatabase(projectRoot) {
  const profilesPath = path.join(projectRoot, 'dbt', 'profiles.yml');
  if (!fs.existsSync(profilesPath)) {
    return;
  }

  const original = fs.readFileSync(profilesPath, 'utf8');
  const pinned = original.replace(
    /database:\s*"\{\{\s*env_var\('MSSQL_DB',\s*'MigrationTest'\)\s*\}\}"/g,
    `database: "${FIXTURE_DB_NAME}"`,
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
  pinFixtureDatabase(runRoot);

  context.test.vars.run_path = relativePosixPath(runRoot);
  return context;
}

module.exports = extensionHook;
