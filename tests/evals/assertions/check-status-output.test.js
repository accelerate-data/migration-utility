const assert = require('node:assert/strict');
const test = require('node:test');

const checkStatusOutput = require('./check-status-output');

test('check-status-output validates stage statuses and first actionable command', () => {
  const output = [
    'Status report',
    'scope ok',
    'profile ok',
    'test-gen pending',
    'What to do next',
    '!ad-migration setup-target',
    '/scope-tables',
  ].join('\n');

  const result = checkStatusOutput(output, {
    vars: {
      expected_stage_statuses: '{"scope": "ok", "profile": "ok"}',
      expected_blocked_stage: 'test-gen',
      expected_first_command: '!ad-migration setup-target',
    },
  });

  assert.equal(result.pass, true, result.reason);
});

test('check-status-output scopes command-like unexpected terms to the action section', () => {
  const output = [
    'Example history mentioned /generate-model earlier.',
    'scope ok',
    'profile ok',
    'Recommendation',
    '/generate-tests',
  ].join('\n');

  const result = checkStatusOutput(output, {
    vars: {
      unexpected_output_terms: '/generate-model',
    },
  });

  assert.equal(result.pass, true, result.reason);
});
