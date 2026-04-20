// Validates that a guard failure stopped before mutating workflow artifacts.
// Expects context.vars:
// {
//   run_path,
//   fixture_path,
//   expected_final_output_terms?,
//   expected_any_final_output_terms?,
//   unchanged_paths?,          -- comma-separated paths relative to project root
//   unchanged_catalog_object?, -- object FQN whose catalog JSON should be unchanged except errors[]
//   expect_only_gitkeep_dirs?  -- comma-separated directories relative to project root
// }
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { normalizeTerms, resolveProjectPath } = require('./schema-helpers');

function fail(reason) {
  return { pass: false, score: 0, reason };
}

function fileDigest(filePath) {
  return crypto.createHash('sha256').update(fs.readFileSync(filePath)).digest('hex');
}

function listFiles(root) {
  if (!fs.existsSync(root)) {
    return [];
  }

  const files = [];
  const visit = (dir) => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const fullPath = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        visit(fullPath);
      } else if (entry.isFile()) {
        files.push(path.relative(root, fullPath).split(path.sep).join('/'));
      }
    }
  };

  visit(root);
  return files.sort();
}

function snapshotPath(root) {
  if (!fs.existsSync(root)) {
    return null;
  }
  const stat = fs.statSync(root);
  if (stat.isFile()) {
    return { __file: fileDigest(root) };
  }
  if (!stat.isDirectory()) {
    return { __other: String(stat.mode) };
  }
  return Object.fromEntries(
    listFiles(root).map((relativePath) => [
      relativePath,
      fileDigest(path.join(root, relativePath)),
    ]),
  );
}

function sameSnapshot(left, right) {
  return JSON.stringify(left) === JSON.stringify(right);
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function catalogFile(root, objectFqn) {
  const normalized = String(objectFqn || '').trim().toLowerCase();
  for (const bucket of ['tables', 'views']) {
    const candidate = path.join(root, 'catalog', bucket, `${normalized}.json`);
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }
  return null;
}

function catalogWithoutInjectedErrors(root, objectFqn) {
  const filePath = catalogFile(root, objectFqn);
  if (!filePath) {
    return null;
  }
  const catalog = readJson(filePath);
  delete catalog.errors;
  return catalog;
}

module.exports = (output, context) => {
  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const runRoot = path.resolve(repoRoot, resolveProjectPath(context));
  const fixtureRoot = path.resolve(
    repoRoot,
    context.vars.canonical_fixture_path || context.vars.fixture_path || '',
  );
  const outputStr = String(output || '').toLowerCase();

  for (const term of normalizeTerms(context.vars.expected_final_output_terms)) {
    if (!outputStr.includes(term)) {
      return fail(`Expected output term '${term}' not found in final response`);
    }
  }

  const anyTerms = normalizeTerms(context.vars.expected_any_final_output_terms);
  if (anyTerms.length > 0 && !anyTerms.some((term) => outputStr.includes(term))) {
    return fail(`Expected at least one output term not found in final response: ${anyTerms.join(', ')}`);
  }

  for (const relativePath of normalizeTerms(context.vars.unchanged_paths)) {
    const expected = snapshotPath(path.join(fixtureRoot, relativePath));
    const actual = snapshotPath(path.join(runRoot, relativePath));
    if (!sameSnapshot(expected, actual)) {
      return fail(`Guard mutated '${relativePath}'`);
    }
  }

  const unchangedCatalogObject = String(context.vars.unchanged_catalog_object || '').trim();
  if (unchangedCatalogObject) {
    const expected = catalogWithoutInjectedErrors(fixtureRoot, unchangedCatalogObject);
    const actual = catalogWithoutInjectedErrors(runRoot, unchangedCatalogObject);
    if (!sameSnapshot(expected, actual)) {
      return fail(`Guard mutated catalog object '${unchangedCatalogObject}' beyond injected errors`);
    }
  }

  for (const relativeDir of normalizeTerms(context.vars.expect_only_gitkeep_dirs)) {
    const dirPath = path.join(runRoot, relativeDir);
    const unexpectedFiles = listFiles(dirPath).filter((file) => file !== '.gitkeep');
    if (unexpectedFiles.length > 0) {
      return fail(`Guard wrote unexpected files under '${relativeDir}': ${unexpectedFiles.join(', ')}`);
    }
  }

  return { pass: true, score: 1, reason: 'Guard stopped before mutating workflow artifacts' };
};
