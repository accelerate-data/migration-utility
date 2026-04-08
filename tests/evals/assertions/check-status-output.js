// Validates that the /status command output contains expected stage statuses
// and recommendations.
// Usage: type: javascript, value: file://../../assertions/check-status-output.js
// Expects context.vars:
// {
//   fixture_path,
//   target_table?,              — single table name (omit for all-tables mode)
//   expected_stage_statuses?,   — JSON string: {"scope": "resolved", "profile": "ok", "test-gen": "pending", "migrate": "blocked"}
//   expected_output_terms?,     — comma-separated terms that must appear in output text
//   unexpected_output_terms?,   — comma-separated terms that must NOT appear in output text
//   expected_blocked_stage?,    — stage name that should be reported as blocked/pending
//   expected_recommendation?,   — term that should appear in the recommendation
//   expected_na_object?,        — FQN that should appear with N/A status
//   expected_view_objects?,     — comma-separated view FQNs that should appear in output
// }

const { normalizeTerms } = require('./schema-helpers');

module.exports = (output, context) => {
  const outputStr = String(output || '').toLowerCase();

  // Parse expected stage statuses if provided
  if (context.vars.expected_stage_statuses) {
    let statuses;
    try {
      statuses = JSON.parse(context.vars.expected_stage_statuses);
    } catch (e) {
      return {
        pass: false,
        score: 0,
        reason: `Failed to parse expected_stage_statuses: ${e.message}`,
      };
    }
    for (const [stage, status] of Object.entries(statuses)) {
      if (!outputStr.includes(stage.toLowerCase())) {
        return {
          pass: false,
          score: 0,
          reason: `Stage '${stage}' not mentioned in status output`,
        };
      }
      if (!outputStr.includes(status.toLowerCase())) {
        return {
          pass: false,
          score: 0,
          reason: `Status '${status}' for stage '${stage}' not found in output`,
        };
      }
    }
  }

  // Check expected output terms
  const expectedOutputTerms = normalizeTerms(context.vars.expected_output_terms);
  for (const term of expectedOutputTerms) {
    if (!outputStr.includes(term)) {
      return {
        pass: false,
        score: 0,
        reason: `Expected output term '${term}' not found in status output`,
      };
    }
  }

  // Check unexpected output terms (must NOT appear)
  const unexpectedOutputTerms = normalizeTerms(context.vars.unexpected_output_terms);
  for (const term of unexpectedOutputTerms) {
    if (outputStr.includes(term)) {
      return {
        pass: false,
        score: 0,
        reason: `Unexpected output term '${term}' found in status output (should be absent)`,
      };
    }
  }

  // Check that blocked stage is mentioned as blocked or pending
  if (context.vars.expected_blocked_stage) {
    const stage = context.vars.expected_blocked_stage.toLowerCase();
    const hasBlocked = outputStr.includes('blocked') || outputStr.includes('pending');
    if (!hasBlocked) {
      return {
        pass: false,
        score: 0,
        reason: `Expected stage '${stage}' to be blocked/pending but neither term found`,
      };
    }
  }

  // Check recommendation term
  if (context.vars.expected_recommendation) {
    const rec = context.vars.expected_recommendation.toLowerCase();
    if (!outputStr.includes(rec)) {
      return {
        pass: false,
        score: 0,
        reason: `Expected recommendation containing '${rec}' not found in output`,
      };
    }
  }

  // Must contain some form of status report structure
  const hasTable =
    outputStr.includes('scope') &&
    (outputStr.includes('profile') || outputStr.includes('profil'));
  if (!hasTable) {
    return {
      pass: false,
      score: 0,
      reason: 'Output does not contain stage names (scope, profile) — expected a status report',
    };
  }

  return { pass: true, score: 1, reason: 'Status output validated' };
};
