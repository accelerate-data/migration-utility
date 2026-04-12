// Validates that a procedure catalog file was written with resolved statements.
// Usage: type: javascript, value: file://../../assertions/check-procedure-catalog.js
// Expects context.vars:
// {
//   fixture_path,
//   target_procedure? — preferred; falls back to target_table,
//   target_table,
//   expected_action?,
//   expected_content?,
//   expected_output_terms?,
//   expected_statement_terms?,
//   expected_rationale_terms?,
//   allow_zero_migrate?,
//   expected_source?
// }
const fs = require('fs');
const path = require('path');
const { normalizeTerms, resolveProjectPath } = require('./schema-helpers');

module.exports = (output, context) => {
  const fixturePath = resolveProjectPath(context);
  const proc = context.vars.target_procedure || context.vars.target_table;
  const view = context.vars.target_view;
  const expectedAction = context.vars.expected_action;
  const expectedContent = context.vars.expected_content;
  const expectedOutputTerms = normalizeTerms(context.vars.expected_output_terms);
  const expectedStatementTerms = normalizeTerms(context.vars.expected_statement_terms);
  const expectedRationaleTerms = normalizeTerms(context.vars.expected_rationale_terms);
  const allowZeroMigrate = String(context.vars.allow_zero_migrate || '').toLowerCase() === 'true';
  const expectedSource = context.vars.expected_source;

  if (!proc) {
    if (view) {
      return {
        pass: true,
        score: 1,
        reason: `Skipped procedure catalog assertion for view target '${view}'`
      };
    }
    return {
      pass: false,
      score: 0,
      reason: 'target_procedure or target_table is required unless this is a view scenario with target_view'
    };
  }

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

  // Check all statements have been resolved (no "needs_llm" actions remaining)
  const unresolvedCount = catalog.statements.filter(s => s.action === 'needs_llm').length;
  if (unresolvedCount > 0) {
    return { pass: false, score: 0, reason: `${unresolvedCount} unresolved 'needs_llm' actions remain in statements` };
  }

  // Check at least one migrate statement exists
  const migrateCount = catalog.statements.filter(s => s.action === 'migrate').length;
  if (!allowZeroMigrate && migrateCount === 0) {
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

  if (expectedSource) {
    const hasSource = catalog.statements.some(s => s.source === expectedSource);
    if (!hasSource) {
      return { pass: false, score: 0, reason: `Expected source '${expectedSource}' not found in statements` };
    }
  }

  if (expectedStatementTerms.length > 0) {
    const statementStr = JSON.stringify(catalog.statements).toLowerCase();
    for (const term of expectedStatementTerms) {
      if (!statementStr.includes(term)) {
        return { pass: false, score: 0, reason: `Expected statement term '${term}' not found in resolved statements` };
      }
    }
  }

  if (expectedRationaleTerms.length > 0) {
    const rationaleText = catalog.statements.map((statement) => statement.rationale || '').join(' ').toLowerCase();
    for (const term of expectedRationaleTerms) {
      if (!rationaleText.includes(term)) {
        return { pass: false, score: 0, reason: `Expected rationale term '${term}' not found in resolved statements` };
      }
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
    reason: `Procedure catalog has ${catalog.statements.length} statements (${migrateCount} migrate, ${catalog.statements.length - migrateCount} skip)`
  };
};
