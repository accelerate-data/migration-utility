const fs = require('fs');
const path = require('path');
const { validateSchema, extractJsonObject, normalizeTerms } = require('./schema-helpers');

module.exports = (output, context) => {
  const fixturePath = context.vars.fixture_path;
  const table = String(context.vars.target_table || '').toLowerCase();
  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const resultPath = path.resolve(repoRoot, fixturePath, 'test-review-results', `${table}.json`);

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
  const schemaResult = validateSchema(review, 'test_review_output.json');
  if (!schemaResult.valid) {
    return { pass: false, score: 0, reason: `Test review schema validation failed: ${schemaResult.errors}` };
  }

  // Cross-artifact: item_id should match target_table
  if (review.item_id && table) {
    const reviewItem = review.item_id.toLowerCase();
    const tableNorm = table.replace(/^[^.]+\./, '');
    const reviewItemShort = reviewItem.replace(/^[^.]+\./, '');
    if (reviewItemShort !== tableNorm && reviewItem !== table) {
      return {
        pass: false,
        score: 0,
        reason: `Cross-artifact mismatch: review.item_id='${review.item_id}' vs target_table='${context.vars.target_table}'`
      };
    }
  }

  const expectedStatus = context.vars.expected_status;
  const minCoveredBranches = Number(context.vars.min_covered_branches || 0);
  const expectedFeedbackTerms = normalizeTerms(context.vars.expected_feedback_terms);
  const expectedIssueTerms = normalizeTerms(context.vars.expected_issue_terms);

  if (expectedStatus && review.status !== expectedStatus) {
    return { pass: false, score: 0, reason: `Expected status '${expectedStatus}', got '${review.status}'` };
  }

  const coveredBranches = Number(review.coverage?.covered_branches || 0);
  if (coveredBranches < minCoveredBranches) {
    return { pass: false, score: 0, reason: `Expected at least ${minCoveredBranches} covered branches, got ${coveredBranches}` };
  }

  const feedbackText = JSON.stringify(review.feedback_for_generator || {}).toLowerCase();
  for (const term of expectedFeedbackTerms) {
    if (!feedbackText.includes(term)) {
      return { pass: false, score: 0, reason: `Expected feedback term '${term}' not found in review feedback` };
    }
  }

  const issueText = JSON.stringify(review.quality_issues || []).toLowerCase();
  for (const term of expectedIssueTerms) {
    if (!issueText.includes(term)) {
      return { pass: false, score: 0, reason: `Expected quality issue term '${term}' not found in review output` };
    }
  }

  return { pass: true, score: 1, reason: `Review returned status ${review.status}` };
};
