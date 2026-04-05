const fs = require('fs');
const path = require('path');

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
