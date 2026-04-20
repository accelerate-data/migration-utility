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
    '## Stage 020: Scope Validation',
    '## Stage 030: Catalog Ownership Check',
    '## Stage 040: Profile',
    '## Stage 050: Target Validation',
    '## Stage 060: Sandbox Validation',
    '## Stage 070: Generate Tests',
    '## Stage 080: Refactor Query',
    '## Stage 090: Replicate Source Tables',
    '## Stage 100: Generate Model',
    '## Stage 110: Refactor Mart Staging',
    '## Stage 120: Refactor Mart Higher',
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
    'scope_phase',
    'terminal scoping outcome',
    'is_source',
    'is_seed',
    'excluded',
    'dbt/dbt_project.yml',
    'test-harness sandbox-status',
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

  const forbiddenPlaceholders = [
    '<stage-id>',
    '<worktree-name>',
    '<base-branch>',
    '<migrate-mart-plan-file>',
  ];
  for (const placeholder of forbiddenPlaceholders) {
    if (planText.toLowerCase().includes(placeholder)) {
      return fail(`Generated plan still contains placeholder '${placeholder}'`);
    }
  }

  const forbiddenTerms = [
    '## Stage 050: Setup Target',
    '## Stage 060: Setup Sandbox',
    'If `scope_phase` has objects, run `/scope-tables`',
    'deterministic `ad-migration setup-target` stage subagent',
    'deterministic `ad-migration setup-sandbox --yes` stage subagent',
  ];
  for (const term of forbiddenTerms) {
    if (evidence.includes(term.toLowerCase())) {
      return fail(`Forbidden stale migrate-mart-plan wording found: '${term}'`);
    }
  }

  return {
    pass: true,
    score: 1,
    reason: 'Migrate mart plan contract validated',
  };
};
