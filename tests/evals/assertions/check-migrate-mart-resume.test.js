const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const checkMigrateMartResume = require('./check-migrate-mart-resume');

test('check-migrate-mart-resume reads bold metadata labels in plan stages', () => {
  const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'migrate-mart-resume-'));

  try {
    const planFile = 'docs/migration-plans/demo/README.md';
    const planPath = path.join(runRoot, planFile);
    fs.mkdirSync(path.dirname(planPath), { recursive: true });
    fs.writeFileSync(
      planPath,
      [
        '# Demo',
        '',
        '## Stage 010: Validate source schema',
        '',
        '**Status:** complete',
        '',
        '## Stage 040: Profile source tables',
        '',
        '**Status:** incomplete',
        '',
      ].join('\n'),
      'utf8',
    );

    const result = checkMigrateMartResume('First incomplete stage: Stage 040 Profile source tables', {
      vars: {
        run_path: runRoot,
        plan_file: planFile,
        expected_resume_stage: '040',
        expected_resume_stage_name: 'Profile',
      },
    });

    assert.equal(result.pass, true);
  } finally {
    fs.rmSync(runRoot, { recursive: true, force: true });
  }
});
