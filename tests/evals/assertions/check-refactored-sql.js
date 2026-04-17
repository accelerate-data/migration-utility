// Validates refactoring-sql skill output: extracted SQL + refactored CTE SQL in catalog.
// Usage: type: javascript, value: file://../../assertions/check-refactored-sql.js
// Expects context.vars:
// {
//   fixture_path,
//   target_table?,
//   target_view?,
//   expected_extracted_terms?,   -- comma-separated terms expected in extracted SQL
//   forbidden_extracted_terms?,  -- comma-separated terms that must NOT appear in extracted SQL
//   expected_refactored_terms?,  -- comma-separated terms expected in refactored CTE SQL
//   forbidden_refactored_terms?, -- comma-separated terms that must NOT appear in refactored SQL
//   expected_status?,            -- comma-separated acceptable statuses (default: "partial")
//   graceful_partial?            -- "true" if partial status is acceptable
// }
const fs = require('fs');
const path = require('path');
const { normalizeTerms, resolveProjectPath } = require('./schema-helpers');

function findWriteKeyword(sqlText) {
  const patterns = [
    /\binsert\b\s+/i,
    /\bupdate\b\s+/i,
    /\bdelete\b\s+/i,
    /\bmerge\b\s+/i,
    /\bexec(?:ute)?\b\s+/i,
    /\bcreate\b\s+/i,
    /\balter\b\s+/i,
    /\bdrop\b\s+/i,
  ];
  for (const pattern of patterns) {
    const match = sqlText.match(pattern);
    if (match) {
      return match[0].trim().toLowerCase();
    }
  }
  return null;
}

module.exports = (output, context) => {
  const fixturePath = resolveProjectPath(context);
  const table = context.vars.target_table;
  const targetView = context.vars.target_view;
  const expectedExtractedTerms = normalizeTerms(context.vars.expected_extracted_terms);
  const forbiddenExtractedTerms = normalizeTerms(context.vars.forbidden_extracted_terms);
  const expectedRefactoredTerms = normalizeTerms(context.vars.expected_refactored_terms);
  const forbiddenRefactoredTerms = normalizeTerms(context.vars.forbidden_refactored_terms);
  const expectedStatuses = normalizeTerms(context.vars.expected_status || 'partial');
  const gracefulPartial = String(context.vars.graceful_partial || '').toLowerCase() === 'true';

  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const tableCatalogDir = path.resolve(repoRoot, fixturePath, 'catalog', 'tables');
  const viewCatalogDir = path.resolve(repoRoot, fixturePath, 'catalog', 'views');

  if (!table && !targetView) {
    return { pass: false, score: 0, reason: 'Expected one of target_table or target_view in test vars' };
  }

  const objectFqn = (table || targetView).toLowerCase().replace(/\[|\]/g, '');
  const tableCatalogPath = path.resolve(tableCatalogDir, `${objectFqn}.json`);
  const viewCatalogPath = path.resolve(viewCatalogDir, `${objectFqn}.json`);

  // Helper: check if the LLM output text contains both SQL blocks as fallback
  const outputStr = String(output || '').toLowerCase();
  const outputHasExtracted = outputStr.includes('extracted') && outputStr.includes('select');
  const outputHasRefactored = outputStr.includes('with') && outputStr.includes('select') && (outputStr.includes('final') || outputStr.includes('cte'));
  const outputFallback = outputHasExtracted && outputHasRefactored;

  const primaryCatalogPath = table ? tableCatalogPath : viewCatalogPath;
  if (!fs.existsSync(primaryCatalogPath)) {
    if (outputFallback) {
      // Agent produced both SQLs in text but didn't persist to catalog (e.g. ran out of turns)
      for (const term of expectedRefactoredTerms) {
        if (!outputStr.includes(term)) {
          return { pass: false, score: 0, reason: `Expected refactored term '${term}' not found in output text (catalog not written)` };
        }
      }
      return { pass: true, score: 0.7, reason: 'Both SQL blocks found in output text (catalog not written — likely ran out of turns)' };
    }
    return { pass: false, score: 0, reason: `Catalog file not found: ${primaryCatalogPath}` };
  }

  let tableCatalog;
  try {
    tableCatalog = JSON.parse(fs.readFileSync(primaryCatalogPath, 'utf8'));
  } catch (e) {
    return { pass: false, score: 0, reason: `Cannot parse catalog: ${e.message}` };
  }

  let catalog = tableCatalog;
  let refactor = catalog.refactor;
  if (table) {
    const writer = tableCatalog?.scoping?.selected_writer;
    if (writer) {
      const procedureCatalogPath = path.resolve(repoRoot, fixturePath, 'catalog', 'procedures', `${writer.toLowerCase()}.json`);
      if (fs.existsSync(procedureCatalogPath)) {
        try {
          const procedureCatalog = JSON.parse(fs.readFileSync(procedureCatalogPath, 'utf8'));
          const procedureRefactor = procedureCatalog.refactor;
          const procedureHasProof =
            !!procedureRefactor?.semantic_review ||
            !!procedureRefactor?.compare_sql;
          if (!refactor || procedureHasProof) {
            catalog = procedureCatalog;
            refactor = procedureRefactor;
          }
        } catch (e) {
          return { pass: false, score: 0, reason: `Cannot parse procedure catalog: ${e.message}` };
        }
      }
    }
  }
  if (!refactor) {
    if (outputFallback) {
      return { pass: true, score: 0.7, reason: 'Both SQL blocks found in output text (refactor section not written to catalog)' };
    }
    return { pass: false, score: 0, reason: 'No refactor section in catalog' };
  }

  if (!refactor.semantic_review) {
    return { pass: false, score: 0, reason: 'No semantic_review section in refactor catalog payload' };
  }
  if (typeof refactor.semantic_review.passed !== 'boolean') {
    return { pass: false, score: 0, reason: 'semantic_review.passed must be a boolean' };
  }
  if (!refactor.semantic_review.checks) {
    return { pass: false, score: 0, reason: 'semantic_review.checks missing from refactor catalog payload' };
  }

  const semanticChecks = ['source_tables', 'output_columns', 'joins', 'filters', 'aggregation_grain'];
  for (const checkName of semanticChecks) {
    const check = refactor.semantic_review.checks[checkName];
    if (!check) {
      return { pass: false, score: 0, reason: `semantic_review.checks.${checkName} missing` };
    }
    if (typeof check.passed !== 'boolean') {
      return { pass: false, score: 0, reason: `semantic_review.checks.${checkName}.passed must be boolean` };
    }
  }

  if (!refactor.compare_sql) {
    return { pass: false, score: 0, reason: 'No compare_sql summary in refactor catalog payload' };
  }

  // Status check
  const status = (refactor.status || '').toLowerCase();
  if (!expectedStatuses.includes(status)) {
    if (gracefulPartial && status === 'partial') {
      // Acceptable partial — continue with reduced score
    } else {
      return { pass: false, score: 0, reason: `Unexpected refactor status '${status}', expected one of: ${expectedStatuses.join(', ')}` };
    }
  }

  // Extracted SQL checks
  const extractedSql = (refactor.extracted_sql || '').toLowerCase();
  if (status === 'ok' && !extractedSql) {
    return { pass: false, score: 0, reason: 'Status is ok but extracted_sql is empty' };
  }

  // Extracted SQL must be a pure SELECT (no DML write keywords)
  const extractedWriteKeyword = findWriteKeyword(extractedSql);
  if (extractedWriteKeyword) {
    return { pass: false, score: 0, reason: `extracted_sql contains write keyword '${extractedWriteKeyword}'` };
  }

  for (const term of expectedExtractedTerms) {
    if (!extractedSql.includes(term)) {
      return { pass: false, score: 0, reason: `Expected term '${term}' not found in extracted_sql` };
    }
  }

  for (const term of forbiddenExtractedTerms) {
    if (extractedSql.includes(term)) {
      return { pass: false, score: 0, reason: `Forbidden term '${term}' found in extracted_sql` };
    }
  }

  // Refactored SQL checks
  const refactoredSql = (refactor.refactored_sql || '').toLowerCase();
  if (status === 'ok' && !refactoredSql) {
    return { pass: false, score: 0, reason: 'Status is ok but refactored_sql is empty' };
  }

  // Refactored SQL must have CTE structure
  if (status === 'ok' && !refactoredSql.includes('with')) {
    return { pass: false, score: 0, reason: 'refactored_sql missing WITH clause (expected CTE structure)' };
  }

  // Refactored SQL must be a pure SELECT
  const refactoredWriteKeyword = findWriteKeyword(refactoredSql);
  if (refactoredWriteKeyword) {
    return { pass: false, score: 0, reason: `refactored_sql contains write keyword '${refactoredWriteKeyword}'` };
  }

  for (const term of expectedRefactoredTerms) {
    if (!refactoredSql.includes(term)) {
      return { pass: false, score: 0, reason: `Expected term '${term}' not found in refactored_sql` };
    }
  }

  for (const term of forbiddenRefactoredTerms) {
    if (refactoredSql.includes(term)) {
      return { pass: false, score: 0, reason: `Forbidden term '${term}' found in refactored_sql` };
    }
  }

  const score = status === 'ok' ? 1 : 0.5;
  return { pass: true, score, reason: `Refactor section valid: status=${status}, extracted_sql present, refactored_sql has CTE structure` };
};
