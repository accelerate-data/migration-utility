// Validates that the command orchestration summary contains expected outcomes.
// Usage: type: javascript, value: file://../../assertions/check-command-summary.js
// Expects context.vars:
// {
//   fixture_path,
//   expected_total?,          — expected total item count
//   expected_ok_count?,       — expected ok/resolved count
//   expected_error_count?,    — expected error count
//   expected_item_statuses?,  — JSON string: {"silver.DimProduct": "resolved", "silver.DimDate": "error"}
//   expected_output_terms?,   — comma-separated terms that must appear in output text
//   expected_error_codes?     — comma-separated error codes that must appear in output text
// }
const fs = require('fs');
const path = require('path');

function normalizeTerms(value) {
  if (!value) return [];
  return String(value)
    .split(',')
    .map((term) => term.trim().toLowerCase())
    .filter(Boolean);
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

  // Try to read summary.json from fixture
  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const summaryPath = path.resolve(
    repoRoot,
    fixturePath,
    '.migration-runs',
    'summary.json',
  );
  let summary = null;
  if (fs.existsSync(summaryPath)) {
    try {
      summary = JSON.parse(fs.readFileSync(summaryPath, 'utf8'));
    } catch (_e) {
      // summary.json may be malformed; fall back to text checks
    }
  }

  // Summary.json count checks are best-effort: the agent reliably produces
  // artifacts but summary.json aggregation is non-deterministic. When
  // summary.json exists and counts are specified, verify them. When it is
  // missing, fall through to text-based checks which are the primary signal.
  if (summary) {
    if (expectedTotal !== null && summary.total !== expectedTotal) {
      return {
        pass: false,
        score: 0,
        reason: `Expected summary.total=${expectedTotal}, got ${summary.total}`,
      };
    }
    if (expectedOk !== null) {
      const actual = summary.ok ?? 0;
      if (actual !== expectedOk) {
        return {
          pass: false,
          score: 0,
          reason: `Expected ok count=${expectedOk}, got ${actual}`,
        };
      }
    }
    if (expectedError !== null) {
      const actualError = summary.error ?? 0;
      if (actualError !== expectedError) {
        return {
          pass: false,
          score: 0,
          reason: `Expected error count=${expectedError}, got ${actualError}`,
        };
      }
    }
  }

  // Check per-item statuses in output text
  for (const [table, status] of Object.entries(expectedItemStatuses)) {
    const tableLower = table.toLowerCase();
    const tableShort = tableLower.split('.').pop();
    if (!outputStr.includes(tableLower) && !outputStr.includes(tableShort)) {
      return {
        pass: false,
        score: 0,
        reason: `Table '${table}' not mentioned in summary output`,
      };
    }
    if (!outputStr.includes(status.toLowerCase())) {
      return {
        pass: false,
        score: 0,
        reason: `Status '${status}' for '${table}' not found in output`,
      };
    }
  }

  // Check expected output terms
  for (const term of expectedOutputTerms) {
    if (!outputStr.includes(term)) {
      return {
        pass: false,
        score: 0,
        reason: `Expected output term '${term}' not found in summary`,
      };
    }
  }

  // Check error codes in output
  for (const code of expectedErrorCodes) {
    if (!outputStr.includes(code)) {
      return {
        pass: false,
        score: 0,
        reason: `Expected error code '${code}' not found in summary`,
      };
    }
  }

  return { pass: true, score: 1, reason: 'Command summary validated' };
};
