// Validates refactoring-sql skill output: extracted SQL + refactored CTE SQL in catalog.
// Usage: type: javascript, value: file://../../assertions/check-refactored-sql.js
// Expects context.vars:
// {
//   fixture_path,
//   target_table,
//   expected_extracted_terms?,   -- comma-separated terms expected in extracted SQL
//   expected_refactored_terms?,  -- comma-separated terms expected in refactored CTE SQL
//   forbidden_refactored_terms?, -- comma-separated terms that must NOT appear in refactored SQL
//   expected_status?,            -- comma-separated acceptable statuses (default: "ok")
//   graceful_partial?            -- "true" if partial status is acceptable
// }
const fs = require('fs');
const path = require('path');
const { normalizeTerms, validateSection } = require('./schema-helpers');

module.exports = (output, context) => {
  const fixturePath = context.vars.fixture_path;
  const table = context.vars.target_table;
  const expectedExtractedTerms = normalizeTerms(context.vars.expected_extracted_terms);
  const expectedRefactoredTerms = normalizeTerms(context.vars.expected_refactored_terms);
  const forbiddenRefactoredTerms = normalizeTerms(context.vars.forbidden_refactored_terms);
  const expectedStatuses = normalizeTerms(context.vars.expected_status || 'ok');
  const gracefulPartial = String(context.vars.graceful_partial || '').toLowerCase() === 'true';

  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const catalogDir = path.resolve(repoRoot, fixturePath, 'catalog', 'tables');

  // Normalize table name for file lookup
  const tableLower = table.toLowerCase().replace(/\[|\]/g, '');
  const catalogPath = path.resolve(catalogDir, `${tableLower}.json`);

  // Helper: check if the LLM output text contains both SQL blocks as fallback
  const outputStr = String(output || '').toLowerCase();
  const outputHasExtracted = outputStr.includes('extracted') && outputStr.includes('select');
  const outputHasRefactored = outputStr.includes('with') && outputStr.includes('select') && (outputStr.includes('final') || outputStr.includes('cte'));
  const outputFallback = outputHasExtracted && outputHasRefactored;

  if (!fs.existsSync(catalogPath)) {
    if (outputFallback) {
      // Agent produced both SQLs in text but didn't persist to catalog (e.g. ran out of turns)
      for (const term of expectedRefactoredTerms) {
        if (!outputStr.includes(term)) {
          return { pass: false, score: 0, reason: `Expected refactored term '${term}' not found in output text (catalog not written)` };
        }
      }
      return { pass: true, score: 0.7, reason: 'Both SQL blocks found in output text (catalog not written — likely ran out of turns)' };
    }
    return { pass: false, score: 0, reason: `Catalog file not found: ${catalogPath}` };
  }

  let catalog;
  try {
    catalog = JSON.parse(fs.readFileSync(catalogPath, 'utf8'));
  } catch (e) {
    return { pass: false, score: 0, reason: `Cannot parse catalog: ${e.message}` };
  }

  const refactor = catalog.refactor;
  if (!refactor) {
    if (outputFallback) {
      return { pass: true, score: 0.7, reason: 'Both SQL blocks found in output text (refactor section not written to catalog)' };
    }
    return { pass: false, score: 0, reason: 'No refactor section in catalog' };
  }

  // Schema validation of the refactor section
  const schemaResult = validateSection(refactor, 'table_catalog.json', 'properties/refactor');
  if (!schemaResult.valid) {
    return { pass: false, score: 0, reason: `Refactor section schema validation failed: ${schemaResult.errors}` };
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
  const writeKeywords = ['insert ', 'update ', 'delete ', 'merge ', 'exec ', 'create ', 'alter ', 'drop '];
  for (const kw of writeKeywords) {
    if (extractedSql.includes(kw)) {
      return { pass: false, score: 0, reason: `extracted_sql contains write keyword '${kw.trim()}'` };
    }
  }

  for (const term of expectedExtractedTerms) {
    if (!extractedSql.includes(term)) {
      return { pass: false, score: 0, reason: `Expected term '${term}' not found in extracted_sql` };
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
  for (const kw of writeKeywords) {
    if (refactoredSql.includes(kw)) {
      return { pass: false, score: 0, reason: `refactored_sql contains write keyword '${kw.trim()}'` };
    }
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

  // Hash checks
  if (status === 'ok') {
    if (!refactor.extracted_sql_hash) {
      return { pass: false, score: 0, reason: 'extracted_sql_hash is missing' };
    }
    if (!refactor.refactored_sql_hash) {
      return { pass: false, score: 0, reason: 'refactored_sql_hash is missing' };
    }
  }

  const score = status === 'ok' ? 1 : 0.5;
  return { pass: true, score, reason: `Refactor section valid: status=${status}, extracted_sql present, refactored_sql has CTE structure` };
};
