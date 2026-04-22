const assert = require('node:assert/strict');
const test = require('node:test');

const checkLinearSkillContract = require('./check-linear-skill-contract');

test('check-linear-skill-contract validates expected workflow booleans and required terms', () => {
  const output = JSON.stringify({
    issue_kind: 'implementation',
    searches_codebase_first: true,
    enters_plan_mode: true,
    asks_question_now: false,
    creates_pr: false,
    question_count: 0,
    has_distinct_paths: true,
    notes: ['worktree', 'checkpoint commit'],
  });

  const result = checkLinearSkillContract(output, {
    vars: {
      expected_issue_kind: 'implementation',
      expect_searches_codebase_first: 'true',
      expect_enters_plan_mode: 'true',
      expect_asks_question_now: 'false',
      expect_question_count: '0',
      expect_has_distinct_paths: 'true',
      required_terms: 'worktree, checkpoint',
      forbidden_terms: 'merged',
    },
  });

  assert.equal(result.pass, true, result.reason);
});

test('check-linear-skill-contract rejects malformed or missing JSON', () => {
  const result = checkLinearSkillContract('not json', { vars: {} });

  assert.equal(result.pass, false);
  assert.match(result.reason, /Failed to parse JSON output/);
});
