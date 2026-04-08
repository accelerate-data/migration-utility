// Validates dbt-aware refactoring: checks that refactored CTE SQL in the catalog
// aligns with existing staging model column names on disk.
//
// Usage: type: javascript, value: file://../../assertions/check-dbt-aware-refactored-sql.js
// Expects context.vars:
// {
//   fixture_path,
//   target_table,
//   stg_model_file,            -- relative path to staging model under dbt/models/staging/ (e.g. "stg_dimproduct.sql")
//   expected_stg_columns,      -- comma-separated column names from the staging model that should appear in refactored SQL
//   expected_status?,           -- acceptable statuses (default: "ok")
//   graceful_partial?           -- "true" if partial is acceptable
// }
//
// Checks (all against catalog JSON, not agent output text):
// 1. Procedure catalog has a refactor section with acceptable status
// 2. refactored_sql contains CTE structure (WITH ... final)
// 3. refactored_sql references the staging model column names (from expected_stg_columns)
// 4. Import CTEs in refactored_sql do NOT use bare "select *" (since staging model defines explicit columns)

const fs = require('fs');
const path = require('path');
const { normalizeTerms } = require('./schema-helpers');

module.exports = (output, context) => {
  const fixturePath = context.vars.fixture_path;
  const table = context.vars.target_table;
  const stgModelFile = context.vars.stg_model_file;
  const expectedStgColumns = normalizeTerms(context.vars.expected_stg_columns);
  const expectedStatuses = normalizeTerms(context.vars.expected_status || 'ok');
  const gracefulPartial = String(context.vars.graceful_partial || '').toLowerCase() === 'true';

  const repoRoot = path.resolve(__dirname, '..', '..', '..');

  // 1. Read procedure catalog to find the writer
  const tableLower = table.toLowerCase().replace(/\[|\]/g, '');
  const tableCatalogPath = path.resolve(repoRoot, fixturePath, 'catalog', 'tables', `${tableLower}.json`);

  if (!fs.existsSync(tableCatalogPath)) {
    return { pass: false, score: 0, reason: `Table catalog not found: ${tableCatalogPath}` };
  }

  let tableCatalog;
  try {
    tableCatalog = JSON.parse(fs.readFileSync(tableCatalogPath, 'utf8'));
  } catch (e) {
    return { pass: false, score: 0, reason: `Cannot parse table catalog: ${e.message}` };
  }

  const writer = (tableCatalog.scoping && tableCatalog.scoping.selected_writer || '').toLowerCase();
  if (!writer) {
    return { pass: false, score: 0, reason: 'No selected_writer in table catalog scoping' };
  }

  // Read writer procedure catalog
  const procCatalogPath = path.resolve(repoRoot, fixturePath, 'catalog', 'procedures', `${writer}.json`);
  if (!fs.existsSync(procCatalogPath)) {
    return { pass: false, score: 0, reason: `Procedure catalog not found: ${procCatalogPath}` };
  }

  let procCatalog;
  try {
    procCatalog = JSON.parse(fs.readFileSync(procCatalogPath, 'utf8'));
  } catch (e) {
    return { pass: false, score: 0, reason: `Cannot parse procedure catalog: ${e.message}` };
  }

  const refactor = procCatalog.refactor;
  if (!refactor) {
    return { pass: false, score: 0, reason: 'No refactor section in procedure catalog' };
  }

  // 2. Check status
  const status = (refactor.status || '').toLowerCase();
  if (!expectedStatuses.includes(status)) {
    if (gracefulPartial && status === 'partial') {
      // continue with reduced score
    } else {
      return { pass: false, score: 0, reason: `Unexpected refactor status '${status}', expected: ${expectedStatuses.join(', ')}` };
    }
  }

  const refactoredSql = (refactor.refactored_sql || '').toLowerCase();
  if (!refactoredSql) {
    return { pass: false, score: 0, reason: 'refactored_sql is empty' };
  }

  // 3. Check CTE structure
  if (!refactoredSql.includes('with')) {
    return { pass: false, score: 0, reason: 'refactored_sql missing WITH clause' };
  }

  // 4. Verify staging model exists on disk
  const stgModelPath = path.resolve(repoRoot, fixturePath, 'dbt', 'models', 'staging', stgModelFile);
  if (!fs.existsSync(stgModelPath)) {
    return { pass: false, score: 0, reason: `Staging model not found: ${stgModelPath}` };
  }

  // 5. Check that refactored SQL references staging model column names
  const missingColumns = [];
  for (const col of expectedStgColumns) {
    if (!refactoredSql.includes(col)) {
      missingColumns.push(col);
    }
  }

  if (missingColumns.length > 0) {
    return {
      pass: false,
      score: 0,
      reason: `Refactored SQL missing staging model column names: ${missingColumns.join(', ')}. Expected alignment with ${stgModelFile}.`
    };
  }

  // 6. Check that import CTEs don't use bare "select *" (dbt-aware should use explicit columns)
  // Extract import CTEs: text between "with" and the first logical/final CTE
  // A bare "select * from" in the first CTE section indicates non-dbt-aware behavior
  const firstCtePortion = refactoredSql.split(/\)\s*,?\s*(?:final|joined|prepared|filtered|transformed)/i)[0] || '';
  const importSelectStarMatches = firstCtePortion.match(/select\s+\*\s+from/gi) || [];

  // Allow "select * from final" at the end but not in import CTEs
  // If we find select * in the import portion AND we have a staging model, flag it
  if (importSelectStarMatches.length > 0) {
    return {
      pass: false,
      score: 0.3,
      reason: `Import CTE uses "SELECT *" (found ${importSelectStarMatches.length} occurrence(s)) but staging model ${stgModelFile} defines explicit columns. dbt-aware refactoring should use explicit column selection.`
    };
  }

  const score = status === 'ok' ? 1 : 0.5;
  return {
    pass: true,
    score,
    reason: `dbt-aware refactoring verified: status=${status}, staging model columns present (${expectedStgColumns.join(', ')}), no bare SELECT * in import CTEs`
  };
};
