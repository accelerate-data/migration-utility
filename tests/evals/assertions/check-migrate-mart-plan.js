const fs = require('fs');
const path = require('path');
const { resolveProjectPath } = require('./schema-helpers');

function fail(reason) {
  return { pass: false, score: 0, reason };
}

function readIfExists(filePath) {
  if (!fs.existsSync(filePath)) {
    return '';
  }
  return fs.readFileSync(filePath, 'utf8');
}

function expectedTerms(value) {
  return String(value || '')
    .split(',')
    .map((term) => term.trim())
    .filter(Boolean);
}

module.exports = (output, context) => {
  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const runRoot = path.resolve(repoRoot, resolveProjectPath(context));
  const planFile = context.vars.plan_file;
  const planPath = planFile ? path.join(runRoot, planFile) : null;
  const outputText = String(output || '');
  const planText = planPath ? readIfExists(planPath) : '';
  const evidence = `${outputText}\n${planText}`.toLowerCase();
  const blockerTerms = expectedTerms(context.vars.expected_blocker_terms);

  if (blockerTerms.length > 0) {
    for (const term of blockerTerms) {
      if (!evidence.includes(term.toLowerCase())) {
        return fail(`Missing expected blocker term '${term}'`);
      }
    }
    return {
      pass: true,
      score: 1,
      reason: 'Migrate mart plan blocker contract validated',
    };
  }

  if (planPath && !fs.existsSync(planPath) && !outputText.trim()) {
    return fail(`Plan file not found: ${planPath}`);
  }

  const requiredHeadings = [
    '## Coordinator',
    '## Source Replication',
    '## Stage 010: Runtime Readiness',
    '## Stage 020: Scope',
    '## Stage 040: Profile',
    '## Stage 130: Final Status',
  ];
  for (const heading of requiredHeadings) {
    if (!evidence.includes(heading.toLowerCase())) {
      return fail(`Missing required heading '${heading}'`);
    }
  }

  const requiredTerms = [
    'Row limit',
    '10000',
    'Worktree name',
    'Base branch',
    'Invocation:',
  ];
  for (const term of requiredTerms) {
    if (!evidence.includes(term.toLowerCase())) {
      return fail(`Missing required plan term '${term}'`);
    }
  }

  if (!/branch[:\s|`]+feature\/migrate-mart-/i.test(evidence)) {
    return fail('Coordinator branch metadata was not written');
  }

  if (!/worktree path[:\s|`]+\.{2}\/worktrees\/feature\/migrate-mart-/i.test(evidence)) {
    return fail('Coordinator worktree path metadata was not written');
  }

  return {
    pass: true,
    score: 1,
    reason: 'Migrate mart plan contract validated',
  };
};
