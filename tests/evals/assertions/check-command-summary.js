// Validates that the command orchestration produced expected per-item artifacts.
// Checks .migration-runs/ JSON artifacts first, falls back to output text.
//
// Usage: type: javascript, value: file://../../assertions/check-command-summary.js
// Expects context.vars:
// {
//   fixture_path,
//   expected_total?,          — expected total item count
//   expected_ok_count?,       — expected ok/resolved count
//   expected_error_count?,    — expected error count
//   expected_item_statuses?,  — JSON string: {"silver.DimProduct": "ok", "silver.DimDate": "error"}
//                               Multi-status: {"silver.DimCurrency": "ok,partial,error"} (any is acceptable)
//   expected_output_terms?,   — comma-separated terms that must appear in output text
//   expected_error_codes?     — comma-separated error codes that must appear in per-item artifacts or output text
//   expected_item_review_iterations?, — JSON string: {"silver.Table": 2}
//   expected_item_review_verdicts?    — JSON string: {"silver.Table": "approved"}
// }
const fs = require('fs');
const path = require('path');
const { normalizeTerms } = require('./schema-helpers');

/**
 * Find all per-item result JSON files in .migration-runs/ matching a table FQN.
 * Files follow the pattern: <schema.table>.<run_id>.json where run_id is unique per command run.
 */
function findItemResults(migrationsDir, tableFqn) {
  if (!fs.existsSync(migrationsDir)) return [];
  const prefix = tableFqn.toLowerCase() + '.';
  return fs.readdirSync(migrationsDir)
    .filter(f => f.toLowerCase().startsWith(prefix) && f.endsWith('.json') && !f.startsWith('summary'))
    .sort((a, b) => {
      const aPath = path.join(migrationsDir, a);
      const bPath = path.join(migrationsDir, b);
      const mtimeA = fs.statSync(aPath).mtimeMs;
      const mtimeB = fs.statSync(bPath).mtimeMs;
      if (mtimeA !== mtimeB) return mtimeA - mtimeB;
      return a.localeCompare(b);
    })
    .map(f => {
      try {
        return JSON.parse(fs.readFileSync(path.join(migrationsDir, f), 'utf8'));
      } catch (_e) {
        return null;
      }
    })
    .filter(Boolean);
}

/**
 * Find the most recent summary.<run_id>.json file.
 */
function findSummary(migrationsDir) {
  if (!fs.existsSync(migrationsDir)) return null;
  const summaryFiles = fs.readdirSync(migrationsDir)
    .filter(f => f.startsWith('summary') && f.endsWith('.json'))
    .sort((a, b) => {
      const aPath = path.join(migrationsDir, a);
      const bPath = path.join(migrationsDir, b);
      const mtimeA = fs.statSync(aPath).mtimeMs;
      const mtimeB = fs.statSync(bPath).mtimeMs;
      if (mtimeA !== mtimeB) return mtimeB - mtimeA;
      return b.localeCompare(a);
    });
  if (summaryFiles.length === 0) return null;
  try {
    return JSON.parse(fs.readFileSync(path.join(migrationsDir, summaryFiles[0]), 'utf8'));
  } catch (_e) {
    return null;
  }
}

module.exports = (output, context) => {
  const fixturePath = context.vars.fixture_path;
  const outputStr = String(output || '').toLowerCase();
  const expectedTotal =
    context.vars.expected_total !== undefined
      ? Number(context.vars.expected_total)
      : null;
  const expectedOk =
    context.vars.expected_ok_count !== undefined
      ? Number(context.vars.expected_ok_count)
      : null;
  const expectedError =
    context.vars.expected_error_count !== undefined
      ? Number(context.vars.expected_error_count)
      : null;
  const expectedOutputTerms = normalizeTerms(context.vars.expected_output_terms);
  const expectedErrorCodes = normalizeTerms(context.vars.expected_error_codes);
  let expectedItemReviewIterations = {};
  let expectedItemReviewVerdicts = {};

  // Parse expected_item_statuses if provided
  let expectedItemStatuses = {};
  if (context.vars.expected_item_statuses) {
    try {
      expectedItemStatuses = JSON.parse(context.vars.expected_item_statuses);
    } catch (e) {
      return {
        pass: false,
        score: 0,
        reason: `Failed to parse expected_item_statuses: ${e.message}`,
      };
    }
  }

  if (context.vars.expected_item_review_iterations) {
    try {
      expectedItemReviewIterations = JSON.parse(context.vars.expected_item_review_iterations);
    } catch (e) {
      return {
        pass: false,
        score: 0,
        reason: `Failed to parse expected_item_review_iterations: ${e.message}`,
      };
    }
  }

  if (context.vars.expected_item_review_verdicts) {
    try {
      expectedItemReviewVerdicts = JSON.parse(context.vars.expected_item_review_verdicts);
    } catch (e) {
      return {
        pass: false,
        score: 0,
        reason: `Failed to parse expected_item_review_verdicts: ${e.message}`,
      };
    }
  }

  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const migrationsDir = path.resolve(repoRoot, fixturePath, '.migration-runs');

  // Try to read summary file (matches summary.json or summary.<epoch>.json)
  let summary = findSummary(migrationsDir);

  // Summary count checks (best-effort)
  if (summary) {
    if (expectedTotal !== null && summary.total !== expectedTotal) {
      return { pass: false, score: 0, reason: `Expected summary.total=${expectedTotal}, got ${summary.total}` };
    }
    if (expectedOk !== null) {
      const actual = summary.ok ?? 0;
      if (actual !== expectedOk) {
        return { pass: false, score: 0, reason: `Expected ok count=${expectedOk}, got ${actual}` };
      }
    }
    if (expectedError !== null) {
      const actualError = summary.error ?? 0;
      if (actualError !== expectedError) {
        return { pass: false, score: 0, reason: `Expected error count=${expectedError}, got ${actualError}` };
      }
    }
  }

  // Check per-item statuses — prefer artifact JSON, fall back to output text
  for (const [table, statusSpec] of Object.entries(expectedItemStatuses)) {
    const tableLower = table.toLowerCase();
    const acceptableStatuses = statusSpec.toLowerCase().split(',').map(s => s.trim());

    // Try artifact-based check first
    const itemResults = findItemResults(migrationsDir, tableLower);
    if (itemResults.length > 0) {
      const latestResult = itemResults[itemResults.length - 1];
      const actualStatus = (latestResult.status || '').toLowerCase();
      if (!acceptableStatuses.includes(actualStatus)) {
        return {
          pass: false,
          score: 0,
          reason: `Item '${table}': artifact status='${actualStatus}', expected one of [${acceptableStatuses.join(', ')}]`,
        };
      }

      if (expectedItemReviewIterations[table] !== undefined) {
        const actualIterations = latestResult.output?.review_iterations;
        if (actualIterations !== expectedItemReviewIterations[table]) {
          return {
            pass: false,
            score: 0,
            reason: `Item '${table}': review_iterations=${actualIterations}, expected ${expectedItemReviewIterations[table]}`,
          };
        }
      }

      if (expectedItemReviewVerdicts[table] !== undefined) {
        const acceptableVerdicts = String(expectedItemReviewVerdicts[table]).toLowerCase().split(',').map(s => s.trim());
        const actualVerdict = String(latestResult.output?.review_verdict || '').toLowerCase();
        if (!acceptableVerdicts.includes(actualVerdict)) {
          return {
            pass: false,
            score: 0,
            reason: `Item '${table}': review_verdict='${actualVerdict}', expected one of [${acceptableVerdicts.join(', ')}]`,
          };
        }
      }

      continue; // Artifact found and status matches — skip text fallback
    }

    // Fall back to output text check
    const tableShort = tableLower.split('.').pop();
    if (!outputStr.includes(tableLower) && !outputStr.includes(tableShort)) {
      return {
        pass: false,
        score: 0,
        reason: `Table '${table}' not mentioned in output and no artifact found in .migration-runs/`,
      };
    }
    // Check if any acceptable status appears in output text
    const statusFound = acceptableStatuses.some(s => outputStr.includes(s));
    if (!statusFound) {
      return {
        pass: false,
        score: 0,
        reason: `No acceptable status [${acceptableStatuses.join(', ')}] for '${table}' found in output text`,
      };
    }
  }

  // Check error codes — in per-item artifacts or output text
  for (const code of expectedErrorCodes) {
    // Check artifacts first
    const allResults = fs.existsSync(migrationsDir)
      ? fs.readdirSync(migrationsDir)
          .filter(f => f.endsWith('.json'))
          .map(f => { try { return fs.readFileSync(path.join(migrationsDir, f), 'utf8').toLowerCase(); } catch(_e) { return ''; } })
          .join(' ')
      : '';
    if (allResults.includes(code)) continue;
    // Fall back to output text
    if (!outputStr.includes(code)) {
      return {
        pass: false,
        score: 0,
        reason: `Expected error code '${code}' not found in artifacts or output`,
      };
    }
  }

  // Check expected output terms in text (these are intentionally text-based)
  for (const term of expectedOutputTerms) {
    if (!outputStr.includes(term)) {
      return {
        pass: false,
        score: 0,
        reason: `Expected output term '${term}' not found in summary`,
      };
    }
  }

  return { pass: true, score: 1, reason: 'Command summary validated' };
};
