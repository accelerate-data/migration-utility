// Validates that a profile was written to the table catalog file.
// Usage: type: javascript, value: file://../../assertions/check-table-profile.js
// Expects context.vars:
// {
//   fixture_path,
//   target_table,
//   expected_kind?,
//   expected_status?,
//   expected_source?,
//   expected_output_terms?,
//   expected_pii_columns?,
//   expected_fk_type?,
//   expected_watermark_column?,
//   expected_warning_codes?,
//   expected_error_codes?
// }
const fs = require('fs');
const path = require('path');
const { normalizeTerms } = require('./schema-helpers');

module.exports = (output, context) => {
  const fixturePath = context.vars.fixture_path;
  const table = context.vars.target_table;
  const targetView = context.vars.target_view;
  const expectedKind = context.vars.expected_kind;
  const expectedStatus = context.vars.expected_status;
  const expectedSource = context.vars.expected_source;
  const expectedOutputTerms = normalizeTerms(context.vars.expected_output_terms);
  const expectedWarningCodes = normalizeTerms(context.vars.expected_warning_codes);
  const expectedErrorCodes = normalizeTerms(context.vars.expected_error_codes);

  if (!table && targetView) {
    return { pass: true, score: 1, reason: 'Skipping table profile assertion for view-targeted scenario' };
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

  if (!catalog.profile) {
    return { pass: false, score: 0, reason: 'No profile section in table catalog' };
  }

  const profile = catalog.profile;

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

  if (expectedOutputTerms.length > 0) {
    const outputStr = String(output || '').toLowerCase();
    for (const term of expectedOutputTerms) {
      if (!outputStr.includes(term)) {
        return { pass: false, score: 0, reason: `Expected output term '${term}' not found in final response` };
      }
    }
  }

  // Check expected PII columns appear in profile.pii_actions[].column
  const expectedPiiColumns = normalizeTerms(context.vars.expected_pii_columns);
  if (expectedPiiColumns.length > 0) {
    const piiActions = Array.isArray(profile.pii_actions) ? profile.pii_actions : [];
    const actualPiiColumns = piiActions.map(a => (a.column || '').toLowerCase());
    for (const col of expectedPiiColumns) {
      if (!actualPiiColumns.includes(col)) {
        return { pass: false, score: 0, reason: `Expected PII column '${col}' not found in profile.pii_actions` };
      }
    }
  }

  // Check expected foreign key type appears in profile.foreign_keys[].fk_type
  const expectedFkType = context.vars.expected_fk_type;
  if (expectedFkType) {
    const foreignKeys = Array.isArray(profile.foreign_keys) ? profile.foreign_keys : [];
    const found = foreignKeys.some(fk => (fk.fk_type || '').toLowerCase() === expectedFkType.toLowerCase());
    if (!found) {
      return { pass: false, score: 0, reason: `Expected fk_type '${expectedFkType}' not found in profile.foreign_keys` };
    }
  }

  // Check expected watermark column matches profile.watermark.column
  const expectedWatermarkColumn = context.vars.expected_watermark_column;
  if (expectedWatermarkColumn) {
    const actualWatermark = (profile.watermark && profile.watermark.column) || '';
    if (actualWatermark.toLowerCase() !== expectedWatermarkColumn.toLowerCase()) {
      return { pass: false, score: 0, reason: `Expected watermark column '${expectedWatermarkColumn}', got '${actualWatermark}'` };
    }
  }

  if (expectedWarningCodes.length > 0) {
    const actualWarningCodes = Array.isArray(profile.warnings)
      ? profile.warnings.map((entry) => (entry.code || '').toLowerCase())
      : [];
    for (const code of expectedWarningCodes) {
      if (!actualWarningCodes.includes(code)) {
        return { pass: false, score: 0, reason: `Expected warning code '${code}' not found in profile.warnings` };
      }
    }
  }

  if (expectedErrorCodes.length > 0) {
    const actualErrorCodes = Array.isArray(profile.errors)
      ? profile.errors.map((entry) => (entry.code || '').toLowerCase())
      : [];
    for (const code of expectedErrorCodes) {
      if (!actualErrorCodes.includes(code)) {
        return { pass: false, score: 0, reason: `Expected error code '${code}' not found in profile.errors` };
      }
    }
  }

  return {
    pass: true,
    score: 1,
    reason: `Profile written: kind=${profile.classification.resolved_kind}, source=${profile.classification.source}`
  };
};
