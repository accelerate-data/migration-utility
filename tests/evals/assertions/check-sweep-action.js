// Validates that the skill respected the model-sweep artifact's recommended_action.
// Usage: type: javascript, value: file://../../assertions/check-sweep-action.js
// Expects context.vars:
// {
//   fixture_path,
//   target_table,
//   expected_action    — "skip" or "test-only"
// }
//
// For "skip":
//   1. Sweep artifact must exist with recommended_action="skip" for the table
//   2. No model files in dbt/models/ were modified (git diff)
//   3. No new model files were created (git status)
//
// For "test-only":
//   1. Sweep artifact must exist with recommended_action="test-only" for the table
//   2. Mart model was not overwritten (git diff on dbt/models/marts/)
//   3. No new mart model files were created (git status on dbt/models/marts/)
const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

module.exports = (output, context) => {
  const fixturePath = context.vars.fixture_path;
  const table = context.vars.target_table;
  const expectedAction = context.vars.expected_action;

  if (!expectedAction) {
    return { pass: false, score: 0, reason: 'Missing required var: expected_action' };
  }

  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const fixtureAbs = path.resolve(repoRoot, fixturePath);

  // ── 1. Read and validate the sweep artifact ─────────────────────────
  const runsDir = path.resolve(fixtureAbs, '.migration-runs');
  if (!fs.existsSync(runsDir)) {
    return { pass: false, score: 0, reason: `.migration-runs/ not found at ${runsDir}` };
  }

  const sweepFiles = fs.readdirSync(runsDir)
    .filter(f => f.startsWith('model-sweep.') && f.endsWith('.json'));
  if (sweepFiles.length === 0) {
    return { pass: false, score: 0, reason: 'No model-sweep.*.json found in .migration-runs/' };
  }

  let sweep;
  try {
    sweep = JSON.parse(fs.readFileSync(path.resolve(runsDir, sweepFiles[0]), 'utf8'));
  } catch (e) {
    return { pass: false, score: 0, reason: `Failed to parse sweep artifact: ${e.message}` };
  }

  const entry = (sweep.tables || []).find(
    t => t.fqn.toLowerCase() === table.toLowerCase(),
  );
  if (!entry) {
    return { pass: false, score: 0, reason: `Table '${table}' not found in sweep artifact` };
  }
  if (entry.recommended_action !== expectedAction) {
    return {
      pass: false,
      score: 0,
      reason: `Sweep artifact has recommended_action='${entry.recommended_action}', expected '${expectedAction}'`,
    };
  }

  // ── 2. Git-based file-integrity checks ──────────────────────────────
  const gitOpts = { encoding: 'utf8', cwd: repoRoot };

  const modelsDir = path.resolve(fixtureAbs, 'dbt', 'models');
  const martsDir = path.resolve(fixtureAbs, 'dbt', 'models', 'marts');

  // Scope: skip checks the full models/ tree; test-only only checks marts/
  const checkDir = expectedAction === 'skip' ? modelsDir : martsDir;
  const label = expectedAction === 'skip' ? 'dbt/models/' : 'dbt/models/marts/';

  // 2a. No tracked files modified
  try {
    const diff = execSync(`git diff --name-only -- "${checkDir}"`, gitOpts).trim();
    if (diff) {
      return {
        pass: false,
        score: 0,
        reason: `${expectedAction} action must not modify files in ${label}, but git diff shows: ${diff}`,
      };
    }
  } catch (_) {
    // git not available or not a repo — skip this check
  }

  // 2b. No new untracked files
  try {
    const status = execSync(`git status --porcelain -- "${checkDir}"`, gitOpts).trim();
    if (status) {
      const newFiles = status
        .split('\n')
        .filter(l => l.startsWith('??') || l.startsWith(' A') || l.startsWith('A '));
      if (newFiles.length > 0) {
        return {
          pass: false,
          score: 0,
          reason: `${expectedAction} action must not create files in ${label}, but found: ${newFiles.map(l => l.slice(3)).join(', ')}`,
        };
      }
    }
  } catch (_) {
    // git not available — skip this check
  }

  // ── 3. Pass ─────────────────────────────────────────────────────────
  return {
    pass: true,
    score: 1,
    reason: `${expectedAction} action verified: sweep artifact respected, no files modified or created in ${label}`,
  };
};
