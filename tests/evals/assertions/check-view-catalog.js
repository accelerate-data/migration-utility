// Validates the view catalog JSON written by analyzing-table (scoping) or
// profiling-table (profile) skill runs against a view.
//
// Usage: type: javascript, value: file://../../assertions/check-view-catalog.js
//
// Expects context.vars:
// {
//   fixture_path,        — path to fixture root, e.g. "tests/evals/fixtures/migration-test"
//   target_view,         — fully-qualified view name, e.g. "silver.vw_currency_ref"
//   check_type,          — "scoping" | "profile"
//   expected_sql_element_types?,  — comma-sep types expected in scoping.sql_elements[].type
//   expected_classification?,     — "stg" | "mart" (for check_type: "profile")
// }
//
// output (LLM text) is intentionally ignored — assertions are purely on the catalog file.

const fs = require('fs');
const path = require('path');
const { validateSection, normalizeTerms } = require('./schema-helpers');

module.exports = (_output, context) => {
  const fixturePath = context.vars.fixture_path;
  const targetView = context.vars.target_view;
  const checkType = context.vars.check_type;

  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const catalogFile = path.resolve(repoRoot, fixturePath, 'catalog', 'views', `${targetView.toLowerCase()}.json`);

  if (!fs.existsSync(catalogFile)) {
    return { pass: false, score: 0, reason: `View catalog file not found: ${catalogFile}` };
  }

  let catalog;
  try {
    catalog = JSON.parse(fs.readFileSync(catalogFile, 'utf8'));
  } catch (e) {
    return { pass: false, score: 0, reason: `Failed to parse view catalog: ${e.message}` };
  }

  if (checkType === 'scoping') {
    return checkScoping(catalog, context);
  }
  if (checkType === 'profile') {
    return checkProfile(catalog, context);
  }
  return { pass: false, score: 0, reason: `Unknown check_type: '${checkType}'. Use "scoping" or "profile".` };
};

function checkScoping(catalog, context) {
  if (!catalog.scoping) {
    return { pass: false, score: 0, reason: 'No scoping section in view catalog' };
  }

  const { scoping } = catalog;

  if (scoping.status !== 'analyzed') {
    return { pass: false, score: 0, reason: `Expected scoping.status "analyzed", got "${scoping.status}"` };
  }

  const expectedTypes = normalizeTerms(context.vars.expected_sql_element_types);
  if (expectedTypes.length > 0) {
    const actualTypes = (scoping.sql_elements || []).map((e) => (e.type || '').toLowerCase());
    for (const t of expectedTypes) {
      if (!actualTypes.includes(t)) {
        return { pass: false, score: 0, reason: `Expected sql_element type "${t}" not found in scoping.sql_elements` };
      }
    }
  }

  return { pass: true, score: 1, reason: `scoping.status=analyzed, sql_elements present` };
}

function checkProfile(catalog, context) {
  if (!catalog.profile) {
    return { pass: false, score: 0, reason: 'No profile section in view catalog' };
  }

  const { profile } = catalog;

  const schemaResult = validateSection(profile, 'view_catalog.json', 'properties/profile');
  if (!schemaResult.valid) {
    return { pass: false, score: 0, reason: `Profile schema validation failed: ${schemaResult.errors}` };
  }

  if (!['stg', 'mart'].includes(profile.classification)) {
    return { pass: false, score: 0, reason: `profile.classification must be "stg" or "mart", got "${profile.classification}"` };
  }

  if (!['ok', 'partial'].includes(profile.status)) {
    return { pass: false, score: 0, reason: `profile.status must be "ok" or "partial", got "${profile.status}"` };
  }

  if (profile.source !== 'llm') {
    return { pass: false, score: 0, reason: `profile.source must be "llm", got "${profile.source}"` };
  }

  const expectedClassification = context.vars.expected_classification;
  if (expectedClassification && profile.classification !== expectedClassification) {
    return {
      pass: false,
      score: 0,
      reason: `Expected profile.classification "${expectedClassification}", got "${profile.classification}"`
    };
  }

  return { pass: true, score: 1, reason: `profile written: classification=${profile.classification}, status=${profile.status}` };
}
