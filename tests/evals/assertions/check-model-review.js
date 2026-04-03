function extractJsonObject(output) {
  const text = String(output || '').trim();
  const fencedMatches = Array.from(text.matchAll(/```json\s*([\s\S]*?)```/gi));
  if (fencedMatches.length > 0) {
    return JSON.parse(fencedMatches.at(-1)[1]);
  }
  const start = text.indexOf('{');
  const end = text.lastIndexOf('}');
  if (start === -1 || end === -1 || end <= start) {
    throw new Error('No JSON object found in output');
  }
  return JSON.parse(text.slice(start, end + 1));
}

function normalizeTerms(value) {
  if (!value) return [];
  return String(value)
    .split(',')
    .map((term) => term.trim().toLowerCase())
    .filter(Boolean);
}

module.exports = (output, context) => {
  let review;
  try {
    review = extractJsonObject(output);
  } catch (error) {
    return { pass: false, score: 0, reason: error.message };
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
