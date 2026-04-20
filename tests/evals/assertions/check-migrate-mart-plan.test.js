const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const checkMigrateMartPlan = require('./check-migrate-mart-plan');

function section(text, heading) {
  const start = text.indexOf(`\n${heading}\n`);
  assert.notEqual(start, -1, `missing section ${heading}`);
  const next = text.indexOf('\n## ', start + heading.length + 2);
  return text.slice(start, next === -1 ? undefined : next);
}

test('migrate-mart command docs use validation stages and prerequisite-only scoping guards', () => {
  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const planCommand = fs.readFileSync(path.join(repoRoot, 'commands/migrate-mart-plan.md'), 'utf8');
  const migrateCommand = fs.readFileSync(path.join(repoRoot, 'commands/migrate-mart.md'), 'utf8');
  const targetValidationSection = section(planCommand, '## Stage 050: Target Validation');
  const sandboxValidationSection = section(planCommand, '## Stage 060: Sandbox Validation');

  assert.match(planCommand, /## Stage 020: Scope Validation/);
  assert.match(planCommand, /## Stage 050: Target Validation/);
  assert.match(planCommand, /## Stage 060: Sandbox Validation/);
  assert.match(planCommand, /SCOPING_REQUIRED/);
  assert.match(planCommand, /terminal scoping outcome/);
  assert.match(planCommand, /ad-migration doctor drivers --project-root <worktree-path> --json/);
  assert.match(planCommand, /test-harness sandbox-status/);
  assert.doesNotMatch(planCommand, /scopes when needed/i);
  assert.doesNotMatch(planCommand, /## Stage 050: Setup Target/);
  assert.doesNotMatch(planCommand, /## Stage 060: Setup Sandbox/);
  assert.doesNotMatch(planCommand, /If `scope_phase` has objects, run `\/scope-tables`/);
  assert.match(targetValidationSection, /Branch: `feature\/migrate-mart-<slug>`/);
  assert.match(targetValidationSection, /PR: `none`/);
  assert.doesNotMatch(targetValidationSection, /050-target-validation-<slug>/);
  assert.match(sandboxValidationSection, /Branch: `feature\/migrate-mart-<slug>`/);
  assert.match(sandboxValidationSection, /PR: `none`/);
  assert.doesNotMatch(sandboxValidationSection, /060-sandbox-validation-<slug>/);

  assert.match(
    migrateCommand,
    /recorded `test -f dbt\/dbt_project\.yml && ad-migration doctor drivers --project-root <worktree-path> --json` invocation/,
  );
  assert.match(
    migrateCommand,
    /recorded `uv run --project "\$\{CLAUDE_PLUGIN_ROOT\}\/packages\/ad-migration-internal" test-harness sandbox-status` invocation/,
  );
  assert.doesNotMatch(migrateCommand, /deterministic `ad-migration setup-target` stage subagent/);
  assert.doesNotMatch(migrateCommand, /deterministic `ad-migration setup-sandbox --yes` stage subagent/);
});

test('blocker scenarios fail if an executable plan is written', () => {
  const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'migrate-mart-plan-blocker-'));

  try {
    const planFile = 'docs/migration-plans/scoping-required/README.md';
    const planPath = path.join(runRoot, planFile);
    fs.mkdirSync(path.dirname(planPath), { recursive: true });
    fs.writeFileSync(
      planPath,
      [
        '# Plan That Should Not Exist',
        '',
        'SCOPING_REQUIRED: scope_phase contains silver.dimcustomer.',
        '',
        '## Coordinator',
        '',
        '## Stage 050: Target Validation',
      ].join('\n'),
      'utf8',
    );

    const result = checkMigrateMartPlan('SCOPING_REQUIRED scope_phase silver.dimcustomer /scope-tables', {
      vars: {
        run_path: runRoot,
        plan_file: planFile,
        expected_blocker_terms: 'SCOPING_REQUIRED,scope_phase,silver.dimcustomer,/scope-tables',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /wrote a plan file/);
  } finally {
    fs.rmSync(runRoot, { recursive: true, force: true });
  }
});
