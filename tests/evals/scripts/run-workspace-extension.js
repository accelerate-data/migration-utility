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

  context.test.vars.run_path = relativePosixPath(runRoot);
  return context;
}

module.exports = extensionHook;
