// Validates that the /status command output contains expected stage statuses
// and recommendations.
// Usage: type: javascript, value: file://../../assertions/check-status-output.js
// Expects context.vars:
// {
//   fixture_path,
//   target_table?,              — single table name (omit for all-tables mode)
//   expected_stage_statuses?,   — JSON string: {"scope": "ok", "profile": "ok", "test-gen": "pending", "migrate": "blocked"}
//   expected_output_terms?,     — comma-separated terms that must appear in output text
//   unexpected_output_terms?,   — comma-separated terms that must NOT appear in output text
//   expected_blocked_stage?,    — stage name that should be reported as blocked/pending
//   expected_recommendation?,   — term that should appear in the recommendation
//   expected_first_command?,    — command that should appear first among actionable commands
//   expected_na_object?,        — FQN that should appear with N/A status
//   expected_view_objects?,     — comma-separated view FQNs that should appear in output
//   expected_reviewed_warnings_hidden?, — numeric hidden reviewed warning count
// }

const { normalizeTerms } = require('./schema-helpers');

const escapeRegExp = (value) => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

const actionableSection = (outputStr) => {
  const recommendationMatch = outputStr.match(
    /\b(what to do next|recommend(?:ed|ation)?|next action)\b/,
  );
  return recommendationMatch ? outputStr.slice(recommendationMatch.index) : outputStr;
};

module.exports = (output, context) => {
  const outputStr = String(output || '').toLowerCase();
  const stageSatisfied = (stage, status) => {
    const stageLower = stage.toLowerCase();
    const statusLower = status.toLowerCase();
    if (!outputStr.includes(stageLower)) return false;
    if (outputStr.includes(statusLower)) return true;

    // Single-item detailed status views often use symbols instead of repeating
    // the raw stage value. Accept those richer presentations when they imply
    // the same status.
    if (stageLower === 'scope' && statusLower === 'ok') {
      return outputStr.includes('scope ✓') || outputStr.includes('selected_writer:');
    }
    if (stageLower === 'profile' && statusLower === 'ok') {
      return outputStr.includes('profile ✓') || outputStr.includes('status: ok');
    }
    return false;
  };

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
      if (!stageSatisfied(stage, status)) {
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

  // Check unexpected output terms. Command-like terms are recommendation
  // constraints, so scope them to the action section instead of incidental
  // command examples, tool output, or fixture setup text.
  const unexpectedOutputTerms = normalizeTerms(context.vars.unexpected_output_terms);
  for (const term of unexpectedOutputTerms) {
    const searchTarget = /^[!/]/.test(term) ? actionableSection(outputStr) : outputStr;
    if (searchTarget.includes(term)) {
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
    const stageBlockedPattern = new RegExp(
      `${escapeRegExp(stage)}[\\s\\S]{0,160}(blocked|pending|not configured|not ready)`
    );
    const blockedStagePattern = new RegExp(
      `(blocked|pending|not configured|not ready)[\\s\\S]{0,160}${escapeRegExp(stage)}`
    );
    const hasBlocked = stageBlockedPattern.test(outputStr) || blockedStagePattern.test(outputStr);
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

  if (context.vars.expected_reviewed_warnings_hidden) {
    const count = String(context.vars.expected_reviewed_warnings_hidden).trim();
    const reviewedPattern = new RegExp(
      `${count}\\s+reviewed\\s+warnings?\\s+hidden\\b`
    );
    if (!reviewedPattern.test(outputStr)) {
      return {
        pass: false,
        score: 0,
        reason: `Expected reviewed warnings hidden count '${count}' not found in output`,
      };
    }
  }

  if (context.vars.expected_first_command) {
    const actionableOutput = actionableSection(outputStr);
    const commandCandidates = [
      '!ad-migration setup-target',
      '!ad-migration setup-sandbox',
      '!ad-migration add-source-table',
      '/scope-tables',
      '/profile-tables',
      '/generate-tests',
      '/refactor-query',
      '/generate-model',
    ];
    let firstCommand = null;
    let firstIndex = Number.POSITIVE_INFINITY;
    for (const candidate of commandCandidates) {
      const index = actionableOutput.indexOf(candidate);
      if (index !== -1 && index < firstIndex) {
        firstIndex = index;
        firstCommand = candidate;
      }
    }

    if (!firstCommand) {
      return {
        pass: false,
        score: 0,
        reason: 'No actionable command found in status output',
      };
    }

    const expectedFirstCommand = context.vars.expected_first_command.toLowerCase();
    if (firstCommand !== expectedFirstCommand) {
      return {
        pass: false,
        score: 0,
        reason: `Expected first command '${expectedFirstCommand}' but found '${firstCommand}'`,
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
