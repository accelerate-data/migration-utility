// Runs `dbt parse` in the fixture dbt project and fails if parsing fails.
// Expects context.vars.fixture_path.
const path = require('path');
const fs = require('fs');
const cp = require('child_process');

module.exports = (_output, context) => {
  const fixturePath = context.vars.fixture_path;
  if (!fixturePath) {
    return { pass: false, score: 0, reason: 'fixture_path must be set in test vars' };
  }

  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const dbtDir = path.resolve(repoRoot, fixturePath, 'dbt');
  if (!fs.existsSync(dbtDir)) {
    return { pass: false, score: 0, reason: `dbt directory not found at ${dbtDir}` };
  }

  try {
    cp.execFileSync('dbt', ['parse'], {
      cwd: dbtDir,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
      env: process.env,
    });
    return { pass: true, score: 1, reason: 'dbt parse passed' };
  } catch (error) {
    const stderr = String(error.stderr || '').trim();
    const stdout = String(error.stdout || '').trim();
    const detail = stderr || stdout || String(error.message || error);
    return { pass: false, score: 0, reason: `dbt parse failed: ${detail}` };
  }
};
