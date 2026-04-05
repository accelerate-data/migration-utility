// Validates that a profile was written to the table catalog file.
// Usage: type: javascript, value: file://../../assertions/check-table-profile.js
// Expects context.vars:
// {
//   fixture_path,
//   target_table,
//   expected_kind?,
//   expected_status?,
//   expected_source?,
//   expected_output_terms?
// }
const fs = require('fs');
const path = require('path');
const { validateSection, normalizeTerms } = require('./schema-helpers');

module.exports = (output, context) => {
  const fixturePath = context.vars.fixture_path;
  const table = context.vars.target_table;
  const expectedKind = context.vars.expected_kind;
  const expectedStatus = context.vars.expected_status;
  const expectedSource = context.vars.expected_source;
  const expectedOutputTerms = normalizeTerms(context.vars.expected_output_terms);

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

  if (!catalog.profile) {
    return { pass: false, score: 0, reason: 'No profile section in table catalog' };
  }

  const profile = catalog.profile;

  // Schema validation of the profile section (validates enums, required fields, and structure)
  const schemaResult = validateSection(profile, 'table_catalog.json', 'profile_section');
  if (!schemaResult.valid) {
    return { pass: false, score: 0, reason: `Profile section schema validation failed: ${schemaResult.errors}` };
  }

  if (expectedStatus) {
    const validStatuses = normalizeTerms(expectedStatus);
    if (!validStatuses.includes((profile.status || '').toLowerCase())) {
      return { pass: false, score: 0, reason: `Expected profile.status in [${validStatuses.join(', ')}], got '${profile.status}'` };
    }
  }

  // Check classification exists (enum validity is enforced by schema)
  if (!profile.classification || !profile.classification.resolved_kind) {
    return { pass: false, score: 0, reason: 'Profile missing classification.resolved_kind' };
  }

  if (!profile.classification.source) {
    return { pass: false, score: 0, reason: 'Profile missing classification.source' };
  }

  if (expectedKind && profile.classification.resolved_kind !== expectedKind) {
    return {
      pass: false,
      score: 0,
      reason: `Expected classification kind '${expectedKind}', got '${profile.classification.resolved_kind}'`
    };
  }

  if (expectedSource && profile.classification.source !== expectedSource) {
    return {
      pass: false,
      score: 0,
      reason: `Expected classification source '${expectedSource}', got '${profile.classification.source}'`
    };
  }

  // Cross-artifact consistency: scoping.selected_writer should match profile.writer
  if (catalog.scoping && catalog.scoping.selected_writer && profile.writer) {
    const scopingWriter = catalog.scoping.selected_writer.toLowerCase();
    const profileWriter = profile.writer.toLowerCase();
    if (!scopingWriter.includes(profileWriter) && !profileWriter.includes(scopingWriter)) {
      return {
        pass: false,
        score: 0,
        reason: `Cross-artifact mismatch: scoping.selected_writer='${catalog.scoping.selected_writer}' vs profile.writer='${profile.writer}'`
      };
    }
  }

  if (expectedOutputTerms.length > 0) {
    const outputStr = String(output || '').toLowerCase();
    for (const term of expectedOutputTerms) {
      if (!outputStr.includes(term)) {
        return { pass: false, score: 0, reason: `Expected output term '${term}' not found in final response` };
      }
    }
  }

  return {
    pass: true,
    score: 1,
    reason: `Profile written: kind=${profile.classification.resolved_kind}, source=${profile.classification.source}`
  };
};
