// Validates that a profile was written to the table catalog file.
// Usage: type: javascript, value: file://../../assertions/check-table-profile.js
// Expects context.vars: { fixture_path, target_table }
const fs = require('fs');
const path = require('path');

const VALID_KINDS = [
  'fact_transaction', 'fact_periodic_snapshot', 'fact_accumulating_snapshot',
  'dim_full_reload', 'dim_non_scd', 'dim_scd1', 'dim_scd2', 'dim_junk',
  'fact_aggregate'
];

module.exports = (output, context) => {
  const fixturePath = context.vars.fixture_path;
  const table = context.vars.target_table;

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

  // Check classification exists and is valid
  if (!profile.classification || !profile.classification.resolved_kind) {
    return { pass: false, score: 0, reason: 'Profile missing classification.resolved_kind' };
  }

  if (!VALID_KINDS.includes(profile.classification.resolved_kind)) {
    return { pass: false, score: 0, reason: `Invalid classification kind: ${profile.classification.resolved_kind}` };
  }

  // Check source field
  if (!profile.classification.source || !['catalog', 'llm', 'catalog+llm'].includes(profile.classification.source)) {
    return { pass: false, score: 0, reason: `Invalid or missing classification source: ${profile.classification?.source}` };
  }

  return {
    pass: true,
    score: 1,
    reason: `Profile written: kind=${profile.classification.resolved_kind}, source=${profile.classification.source}`
  };
};
