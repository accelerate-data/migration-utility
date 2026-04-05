const fs = require('fs');
const path = require('path');
const { validateSchema, extractJsonObject, normalizeTerms } = require('./schema-helpers');

module.exports = (output, context) => {
  const fixturePath = context.vars.fixture_path;
  const table = String(context.vars.target_table || '').toLowerCase();
  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const resultPath = path.resolve(repoRoot, fixturePath, 'model-review-results', `${table}.json`);

  let review;
  try {
    review = extractJsonObject(output);
  } catch (_) {
    // fall through to file-based lookup
  }

  if (fs.existsSync(resultPath)) {
    try {
      review = JSON.parse(fs.readFileSync(resultPath, 'utf8'));
    } catch (error) {
      return { pass: false, score: 0, reason: `Failed to parse review artifact: ${error.message}` };
    }
  }

  if (!review) {
    return { pass: false, score: 0, reason: 'No review JSON found in output or artifact file' };
  }

  // Schema validation gate
  const schemaResult = validateSchema(review, 'model_review_output.json');
  if (!schemaResult.valid) {
    return { pass: false, score: 0, reason: `Model review schema validation failed: ${schemaResult.errors}` };
  }

  const expectedStatuses = normalizeTerms(context.vars.expected_status);
  const expectStandardsPassed = String(context.vars.expect_standards_passed || '').toLowerCase();
  const expectCorrectnessPassed = String(context.vars.expect_correctness_passed || '').toLowerCase();
  const expectTestIntegrationPassed = String(context.vars.expect_test_integration_passed || '').toLowerCase();
  const expectedFeedbackTerms = normalizeTerms(context.vars.expected_feedback_terms);
  const expectedIssueTerms = normalizeTerms(context.vars.expected_issue_terms);

  if (expectedStatuses.length > 0 && !expectedStatuses.includes(String(review.status || '').toLowerCase())) {
    return {
      pass: false,
      score: 0,
      reason: `Expected status in [${expectedStatuses.join(', ')}], got '${review.status}'`,
    };
  }

  if (expectStandardsPassed) {
    const expected = expectStandardsPassed === 'true';
    if (Boolean(review.checks?.standards?.passed) !== expected) {
      return { pass: false, score: 0, reason: `Expected standards.passed=${expected}` };
    }
  }

  if (expectCorrectnessPassed) {
    const expected = expectCorrectnessPassed === 'true';
    if (Boolean(review.checks?.correctness?.passed) !== expected) {
      return { pass: false, score: 0, reason: `Expected correctness.passed=${expected}` };
    }
  }

  if (expectTestIntegrationPassed) {
    const expected = expectTestIntegrationPassed === 'true';
    if (Boolean(review.checks?.test_integration?.passed) !== expected) {
      return { pass: false, score: 0, reason: `Expected test_integration.passed=${expected}` };
    }
  }

  const feedbackText = JSON.stringify(review.feedback_for_model_generator || []).toLowerCase();
  for (const term of expectedFeedbackTerms) {
    if (!feedbackText.includes(term)) {
      return { pass: false, score: 0, reason: `Expected feedback term '${term}' not found in model-review feedback` };
    }
  }

  const issueText = JSON.stringify(review.checks || {}).toLowerCase();
  for (const term of expectedIssueTerms) {
    if (!issueText.includes(term)) {
      return { pass: false, score: 0, reason: `Expected issue term '${term}' not found in review checks` };
    }
  }

  return { pass: true, score: 1, reason: `Model review returned status ${review.status}` };
};
