// Validates that a procedure catalog file was written with resolved statements.
// Usage: type: javascript, value: file://../../assertions/check-procedure-catalog.js
// Expects context.vars: { fixture_path, target_table, expected_action?, expected_content? }
const fs = require('fs');
const path = require('path');

module.exports = (output, context) => {
  const fixturePath = context.vars.fixture_path;
  const proc = context.vars.target_table;
  const expectedAction = context.vars.expected_action;
  const expectedContent = context.vars.expected_content;

  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  // Procedure catalog files use lowercase names
  const catalogFile = path.resolve(repoRoot, fixturePath, 'catalog', 'procedures', `${proc.toLowerCase()}.json`);

  if (!fs.existsSync(catalogFile)) {
    return { pass: false, score: 0, reason: `Procedure catalog file not found: ${catalogFile}` };
  }

  let catalog;
  try {
    catalog = JSON.parse(fs.readFileSync(catalogFile, 'utf8'));
  } catch (e) {
    return { pass: false, score: 0, reason: `Failed to parse catalog: ${e.message}` };
  }

  // Check statements exist
  if (!catalog.statements || !Array.isArray(catalog.statements) || catalog.statements.length === 0) {
    return { pass: false, score: 0, reason: 'No statements found in procedure catalog' };
  }

  // Check all statements have been resolved (no "claude" actions remaining)
  const unresolvedCount = catalog.statements.filter(s => s.action === 'claude').length;
  if (unresolvedCount > 0) {
    return { pass: false, score: 0, reason: `${unresolvedCount} unresolved 'claude' actions remain in statements` };
  }

  // Check at least one migrate statement exists
  const migrateCount = catalog.statements.filter(s => s.action === 'migrate').length;
  if (migrateCount === 0) {
    return { pass: false, score: 0, reason: 'No migrate statements found — expected at least one' };
  }

  // If expected_action is specified, verify it exists
  if (expectedAction) {
    const hasAction = catalog.statements.some(s => s.action === expectedAction);
    if (!hasAction) {
      return { pass: false, score: 0, reason: `Expected action '${expectedAction}' not found in statements` };
    }
  }

  // If expected_content is specified (comma-separated), check raw catalog JSON contains each
  if (expectedContent) {
    const catalogStr = JSON.stringify(catalog).toLowerCase();
    for (const term of expectedContent.split(',').map(t => t.trim().toLowerCase())) {
      if (!catalogStr.includes(term)) {
        return { pass: false, score: 0, reason: `Expected content '${term}' not found in procedure catalog` };
      }
    }
  }

  return {
    pass: true,
    score: 1,
    reason: `Procedure catalog has ${catalog.statements.length} statements (${migrateCount} migrate, ${catalog.statements.length - migrateCount} skip)`
  };
};
