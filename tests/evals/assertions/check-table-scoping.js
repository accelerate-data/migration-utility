// Validates that a scoping section was written to the table catalog file.
// Usage: type: javascript, value: file://../../assertions/check-table-scoping.js
// Expects context.vars:
// {
//   fixture_path,
//   target_table,
//   expected_status,
//   expected_writer?,
//   expected_output_terms?,
//   expected_rationale_terms?
// }
const fs = require('fs');
const path = require('path');
const { normalizeTerms, resolveProjectPath } = require('./schema-helpers');

function findLatestItemResult(repoRoot, fixturePath, table) {
  const migrationsDir = path.resolve(repoRoot, fixturePath, '.migration-runs');
  if (!fs.existsSync(migrationsDir)) return null;
  const prefix = `${table.toLowerCase()}.`;
  const files = fs.readdirSync(migrationsDir)
    .filter(file => file.toLowerCase().startsWith(prefix) && file.endsWith('.json'))
    .sort((a, b) => {
      const aPath = path.join(migrationsDir, a);
      const bPath = path.join(migrationsDir, b);
      const mtimeA = fs.statSync(aPath).mtimeMs;
      const mtimeB = fs.statSync(bPath).mtimeMs;
      if (mtimeA !== mtimeB) return mtimeA - mtimeB;
      return a.localeCompare(b);
    });
  if (files.length === 0) return null;
  try {
    return JSON.parse(fs.readFileSync(path.join(migrationsDir, files[files.length - 1]), 'utf8'));
  } catch (_error) {
    return null;
  }
}

module.exports = (output, context) => {
  const fixturePath = resolveProjectPath(context);
  const table = context.vars.target_table;
  const view = context.vars.target_view;
  const expectedStatuses = normalizeTerms(context.vars.expected_status);
  const expectedWriter = context.vars.expected_writer;
  const expectedOutputTerms = normalizeTerms(context.vars.expected_output_terms);
  const expectedRationaleTerms = normalizeTerms(context.vars.expected_rationale_terms);

  if (!table) {
    if (view) {
      return {
        pass: true,
        score: 1,
        reason: `Skipped table scoping assertion for view target '${view}'`
      };
    }
    return {
      pass: false,
      score: 0,
      reason: 'target_table is required unless this is a view scenario with target_view'
    };
  }

  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const catalogFile = path.resolve(repoRoot, fixturePath, 'catalog', 'tables', `${table.toLowerCase()}.json`);

  if (!fs.existsSync(catalogFile)) {
    return { pass: false, score: 0, reason: `Table catalog file not found: ${catalogFile}` };
  }

  let catalog;
  try {
    catalog = JSON.parse(fs.readFileSync(catalogFile, 'utf8'));
  } catch (e) {
    return { pass: false, score: 0, reason: `Failed to parse catalog: ${e.message}` };
  }

  // Check scoping section exists
  if (!catalog.scoping) {
    return { pass: false, score: 0, reason: 'No scoping section in table catalog' };
  }

  const scoping = catalog.scoping;
  const latestResult = findLatestItemResult(repoRoot, fixturePath, table);

  // Check scoping.status is in expected list
  if (expectedStatuses.length > 0) {
    const statusValue = scoping.status || latestResult?.status || '';
    const actualStatus = statusValue.toLowerCase();
    if (!expectedStatuses.includes(actualStatus)) {
      return {
        pass: false,
        score: 0,
        reason: `Expected scoping.status in [${expectedStatuses.join(', ')}], got '${statusValue}'`
      };
    }
  }

  // Check selected_writer contains expected substring
  if (expectedWriter) {
    const actualWriter = (scoping.selected_writer || '').toLowerCase();
    if (!actualWriter.includes(expectedWriter.toLowerCase())) {
      return {
        pass: false,
        score: 0,
        reason: `Expected selected_writer to contain '${expectedWriter}', got '${scoping.selected_writer}'`
      };
    }
  }

  // Check rationale terms
  if (expectedRationaleTerms.length > 0) {
    const rationale = (scoping.selected_writer_rationale || '').toLowerCase();
    for (const term of expectedRationaleTerms) {
      if (!rationale.includes(term)) {
        return {
          pass: false,
          score: 0,
          reason: `Expected rationale term '${term}' not found in scoping rationale`
        };
      }
    }
  }

  // Check output terms
  if (expectedOutputTerms.length > 0) {
    const outputStr = String(output || '').toLowerCase();
    for (const term of expectedOutputTerms) {
      if (!outputStr.includes(term)) {
        return {
          pass: false,
          score: 0,
          reason: `Expected output term '${term}' not found in final response`
        };
      }
    }
  }

  return {
    pass: true,
    score: 1,
    reason: `Scoping written: status=${scoping.status}, writer=${scoping.selected_writer || 'none'}`
  };
};
