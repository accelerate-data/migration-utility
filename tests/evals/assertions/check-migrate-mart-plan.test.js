const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const checkMigrateMartPlan = require('./check-migrate-mart-plan');

test('migrate-mart command docs use validation stages and prerequisite-only scoping guards', () => {
  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const commandText = [
    fs.readFileSync(path.join(repoRoot, 'commands/migrate-mart-plan.md'), 'utf8'),
    fs.readFileSync(path.join(repoRoot, 'commands/migrate-mart.md'), 'utf8'),
  ].join('\n');

  const result = checkMigrateMartPlan(commandText, {
    vars: {
      run_path: '.',
    },
  });

  assert.equal(result.pass, true, result.reason);
});
