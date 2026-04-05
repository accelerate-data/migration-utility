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
const { validateSection, normalizeTerms } = require('./schema-helpers');

module.exports = (output, context) => {
  const fixturePath = context.vars.fixture_path;
  const table = context.vars.target_table;
  const expectedStatuses = normalizeTerms(context.vars.expected_status);
  const expectedWriter = context.vars.expected_writer;
  const expectedOutputTerms = normalizeTerms(context.vars.expected_output_terms);
  const expectedRationaleTerms = normalizeTerms(context.vars.expected_rationale_terms);

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

  // Schema validation of the scoping section
  const schemaResult = validateSection(scoping, 'table_catalog.json', 'properties/scoping');
  if (!schemaResult.valid) {
    return { pass: false, score: 0, reason: `Scoping section schema validation failed: ${schemaResult.errors}` };
  }

  // Check scoping.status is in expected list
  if (expectedStatuses.length > 0) {
    const actualStatus = (scoping.status || '').toLowerCase();
    if (!expectedStatuses.includes(actualStatus)) {
      return {
        pass: false,
        score: 0,
        reason: `Expected scoping.status in [${expectedStatuses.join(', ')}], got '${scoping.status}'`
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
