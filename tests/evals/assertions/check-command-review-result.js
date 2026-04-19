const fs = require('fs');
const path = require('path');
const { normalizeTerms, resolveProjectPath } = require('./schema-helpers');

module.exports = (_output, context) => {
  const fixturePath = resolveProjectPath(context);
  const table = String(context.vars.target_table || '').toLowerCase();
  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const resultPath = path.resolve(repoRoot, fixturePath, 'test-review-results', `${table}.json`);
  const resultDir = path.dirname(resultPath);

  let reviewPath = resultPath;
  if (fs.existsSync(resultDir)) {
    const prefix = `${table}.iteration-`;
    const iterationResults = fs.readdirSync(resultDir)
      .filter(file => file.startsWith(prefix) && file.endsWith('.json'))
      .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
    if (iterationResults.length > 0) {
      reviewPath = path.join(resultDir, iterationResults[iterationResults.length - 1]);
    }
  }

  if (!fs.existsSync(reviewPath)) {
    return { pass: false, score: 0, reason: `Missing review artifact: ${resultPath}` };
  }

  let review;
  try {
    review = JSON.parse(fs.readFileSync(reviewPath, 'utf8'));
  } catch (error) {
    return { pass: false, score: 0, reason: `Failed to parse review artifact: ${error.message}` };
  }

  const expectedVerdicts = normalizeTerms(context.vars.expected_command_review_verdict);
  const minBranchReviewCount = Number(context.vars.min_branch_review_count || 0);
  const minScenarioReviewCount = Number(context.vars.min_scenario_review_count || 0);
  const expectedWarningTerms = normalizeTerms(context.vars.expected_warning_terms);

  const actualVerdict = String(review.review_verdict || review.status || '').toLowerCase();
  if (expectedVerdicts.length > 0 && !expectedVerdicts.includes(actualVerdict)) {
    return {
      pass: false,
      score: 0,
      reason: `Expected command review verdict in [${expectedVerdicts.join(', ')}], got '${actualVerdict}'`,
    };
  }

  const branchReviewCount = Array.isArray(review.branch_review) ? review.branch_review.length : 0;
  if (branchReviewCount < minBranchReviewCount) {
    return {
      pass: false,
      score: 0,
      reason: `Expected at least ${minBranchReviewCount} branch_review entries, got ${branchReviewCount}`,
    };
  }

  const scenarioReviewCount = Array.isArray(review.scenario_review) ? review.scenario_review.length : 0;
  if (scenarioReviewCount < minScenarioReviewCount) {
    return {
      pass: false,
      score: 0,
      reason: `Expected at least ${minScenarioReviewCount} scenario_review entries, got ${scenarioReviewCount}`,
    };
  }

  const warningText = JSON.stringify(review.warnings || []).toLowerCase();
  for (const term of expectedWarningTerms) {
    if (!warningText.includes(term)) {
      return {
        pass: false,
        score: 0,
        reason: `Expected warning term '${term}' not found in command review artifact`,
      };
    }
  }

  return {
    pass: true,
    score: 1,
    reason: `Command review artifact validated with verdict ${actualVerdict}`,
  };
};
