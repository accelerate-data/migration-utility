const fs = require('fs');
const path = require('path');
const { extractJsonObject, normalizeTerms, resolveProjectPath } = require('./schema-helpers');

function latestIterationReviewPath(resultDir, table) {
  if (!fs.existsSync(resultDir)) return null;
  const prefix = `${table}.iteration-`;
  const iterationResults = fs.readdirSync(resultDir)
    .filter(file => file.startsWith(prefix) && file.endsWith('.json'))
    .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
  return iterationResults.length > 0
    ? path.join(resultDir, iterationResults[iterationResults.length - 1])
    : null;
}

function coveredStatus(branch) {
  return branch.status === 'covered' ||
    branch.status === 'ok' ||
    branch.status === 'approved' ||
    branch.approval_status === 'approved' ||
    branch.coverage === 'covered' ||
    branch.covered === true ||
    branch.is_covered === true;
}

function branchCoverageCheck(review) {
  if (!Array.isArray(review.checks)) return null;
  return review.checks.find(check =>
    check?.check_id === 'branch_coverage' ||
    Array.isArray(check?.details?.branches) ||
    check?.details?.total_branches !== undefined ||
    check?.details?.covered_branches !== undefined
  ) || null;
}

function keyedCoverageAnalysisBranches(review) {
  if (
    !review.coverage_analysis ||
    Array.isArray(review.coverage_analysis) ||
    typeof review.coverage_analysis !== 'object'
  ) {
    return [];
  }

  return Object.entries(review.coverage_analysis)
    .filter(([, value]) => value && typeof value === 'object' && !Array.isArray(value))
    .map(([id, value]) => ({
      id: value.id || value.branch_id || id,
      covered: coveredStatus(value),
      coverage: value.coverage,
      status: value.status,
    }));
}

function keyedBranchCoverageBranches(review) {
  if (
    !review.branch_coverage ||
    Array.isArray(review.branch_coverage) ||
    typeof review.branch_coverage !== 'object'
  ) {
    return [];
  }

  const summaryKeys = new Set(['total', 'covered', 'uncovered', 'total_branches', 'covered_branches']);
  return Object.entries(review.branch_coverage)
    .filter(([key, value]) =>
      !summaryKeys.has(key) &&
      value &&
      typeof value === 'object' &&
      !Array.isArray(value)
    )
    .map(([id, value]) => ({
      id: value.id || value.branch_id || id,
      covered: coveredStatus(value),
      coverage: value.coverage,
      status: value.status,
    }));
}

module.exports = (output, context) => {
  const fixturePath = resolveProjectPath(context);
  const table = String(context.vars.target_table || '').toLowerCase();
  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const resultPath = path.resolve(repoRoot, fixturePath, 'test-review-results', `${table}.json`);
  const resultDir = path.dirname(resultPath);

  let review;
  try {
    review = extractJsonObject(output);
  } catch (_) {
    // fall through to file-based lookup
  }

  const filePath = latestIterationReviewPath(resultDir, table) || resultPath;
  if (fs.existsSync(filePath)) {
    try {
      review = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    } catch (error) {
      return { pass: false, score: 0, reason: `Failed to parse review artifact: ${error.message}` };
    }
  }

  if (!review) {
    return { pass: false, score: 0, reason: 'No review JSON found in output or artifact file' };
  }

  // Schema validation is now handled by Pydantic TestReviewOutput at runtime.
  // This assertion focuses on cross-artifact consistency and behavioral checks.

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
  const expectedReviewerBranchIds = normalizeTerms(context.vars.expected_reviewer_branch_ids);
  const rejectedReviewerBranchIds = normalizeTerms(context.vars.rejected_reviewer_branch_ids);

  if (expectedStatus) {
    const allowedStatuses = expectedStatus.split(',').map(s => s.trim());
    if (!allowedStatuses.includes(review.status)) {
      return { pass: false, score: 0, reason: `Expected status '${expectedStatus}', got '${review.status}'` };
    }
  }

  const branchCheck = branchCoverageCheck(review);
  const keyedCoverageBranches = keyedCoverageAnalysisBranches(review);
  const keyedBranchCoverageBranchesResult = keyedBranchCoverageBranches(review);

  const coveredBranches = Number(
    review.coverage?.covered_branches ||
    review.coverage_analysis?.covered_branches ||
    (keyedCoverageBranches.length > 0
      ? keyedCoverageBranches.filter(branch => branch.covered).length
      : 0) ||
    (keyedBranchCoverageBranchesResult.length > 0
      ? keyedBranchCoverageBranchesResult.filter(branch => branch.covered).length
      : 0) ||
    review.metadata?.covered_branches ||
    branchCheck?.details?.covered_branches ||
    (Array.isArray(review.branch_coverage)
      ? review.branch_coverage.filter(coveredStatus).length
      : review.branch_coverage?.covered_branches) ||
    (Array.isArray(review.branch_coverage) ? 0 : review.branch_coverage?.covered) ||
    review.coverage_assessment?.covered_branches ||
    review.coverage_summary?.covered_branches ||
    (Array.isArray(review.branch_reviews)
      ? review.branch_reviews.filter(coveredStatus).length
      : 0) ||
    (review.coverage_assessment === 'complete' ? review.branch_count : 0) ||
    0
  );
  if (coveredBranches < minCoveredBranches) {
    return { pass: false, score: 0, reason: `Expected at least ${minCoveredBranches} covered branches, got ${coveredBranches}` };
  }

  const reviewerBranchManifest = Array.isArray(review.reviewer_branch_manifest)
    ? review.reviewer_branch_manifest
    : Array.isArray(review.coverage_analysis?.branch_coverage)
    ? review.coverage_analysis.branch_coverage.map(branch => ({
        id: branch.id || branch.branch_id,
        covered: branch.covered,
      }))
    : Array.isArray(review.branch_coverage)
    ? review.branch_coverage.map(branch => ({
        id: branch.id || branch.branch_id,
        covered: coveredStatus(branch),
        coverage: branch.coverage,
        status: branch.status,
      }))
    : Array.isArray(review.coverage_assessment?.branch_details)
    ? review.coverage_assessment.branch_details.map(branch => ({
        id: branch.id || branch.branch_id,
        covered: coveredStatus(branch),
        coverage: branch.coverage,
        status: branch.status,
      }))
    : keyedCoverageBranches.length > 0
    ? keyedCoverageBranches
    : keyedBranchCoverageBranchesResult.length > 0
    ? keyedBranchCoverageBranchesResult
    : Array.isArray(branchCheck?.details?.branches)
    ? branchCheck.details.branches.map(branch => ({
        id: branch.id || branch.branch_id,
        covered: coveredStatus(branch),
        coverage: branch.coverage,
        status: branch.status,
      }))
    : Array.isArray(review.branch_reviews)
    ? review.branch_reviews.map(branch => ({
      id: branch.id || branch.branch_id,
      covered: coveredStatus(branch),
      }))
    : Array.isArray(review.scenarios_reviewed)
    ? review.scenarios_reviewed.map(scenario => ({
        id: scenario.branch_id,
        covered: scenario.status === 'ok' || scenario.status === 'approved',
      }))
    : review.branch_coverage && typeof review.branch_coverage === 'object' && review.scenarios
    ? Object.values(review.scenarios).map(scenario => ({
        id: scenario.branch_id,
        covered: scenario.status === 'approved',
      }))
    : [];
  const reviewerBranchIds = reviewerBranchManifest.map(branch => String(branch.id || '').toLowerCase());
  const topLevelCoveredBranchIds = Array.isArray(review.covered_branches)
    ? review.covered_branches.map(branch => String(branch.id || branch || '').toLowerCase())
    : Array.isArray(review.covered)
    ? review.covered.map(branch => String(branch.id || branch || '').toLowerCase())
    : [];
  const coveredManifestBranches = reviewerBranchManifest.filter(
    branch =>
      coveredStatus(branch) ||
      topLevelCoveredBranchIds.includes(String(branch.id || '').toLowerCase())
  ).length;
  const uncoveredBranchIds = Array.isArray(review.coverage?.uncovered)
    ? review.coverage.uncovered.map(branch => String(branch.id || branch || '').toLowerCase())
    : Array.isArray(review.uncovered_branches)
    ? review.uncovered_branches.map(branch => String(branch.id || branch || '').toLowerCase())
    : [];
  const untestableBranchIds = Array.isArray(review.coverage?.untestable)
    ? review.coverage.untestable.map(branch => String(branch.id || branch || '').toLowerCase())
    : Array.isArray(review.untestable_branches)
    ? review.untestable_branches.map(branch => String(branch.id || branch || '').toLowerCase())
    : [];
  const feedbackUncoveredBranchIds = Array.isArray(review.feedback_for_generator?.uncovered_branches)
    ? review.feedback_for_generator.uncovered_branches.map(branchId => String(branchId || '').toLowerCase())
    : [];

  const totalBranches = Number(
    review.coverage?.total_branches ||
    review.coverage_analysis?.total_branches ||
    (keyedCoverageBranches.length > 0 ? keyedCoverageBranches.length : 0) ||
    (keyedBranchCoverageBranchesResult.length > 0 ? keyedBranchCoverageBranchesResult.length : 0) ||
    review.metadata?.total_branches ||
    branchCheck?.details?.total_branches ||
    (Array.isArray(review.branch_coverage)
      ? review.branch_coverage.length
      : review.branch_coverage?.total_branches) ||
    (Array.isArray(review.branch_coverage) ? 0 : review.branch_coverage?.total) ||
    review.coverage_assessment?.total_branches ||
    review.coverage_summary?.total_branches ||
    review.branch_reviews?.length ||
    review.branch_count ||
    0
  );
  if (reviewerBranchManifest.length > 0 && reviewerBranchManifest.length !== totalBranches) {
    return {
      pass: false,
      score: 0,
      reason: `Coverage total_branches=${totalBranches} does not match reviewer_branch_manifest length=${reviewerBranchManifest.length}`
    };
  }

  if (reviewerBranchManifest.length > 0 && coveredManifestBranches !== coveredBranches) {
    return {
      pass: false,
      score: 0,
      reason: `Coverage covered_branches=${coveredBranches} does not match reviewer_branch_manifest covered count=${coveredManifestBranches}`
    };
  }

  for (const branchId of uncoveredBranchIds) {
    if (!reviewerBranchIds.includes(branchId)) {
      return {
        pass: false,
        score: 0,
        reason: `Uncovered branch '${branchId}' is not present in reviewer_branch_manifest`
      };
    }
  }

  for (const branchId of untestableBranchIds) {
    if (!reviewerBranchIds.includes(branchId)) {
      return {
        pass: false,
        score: 0,
        reason: `Untestable branch '${branchId}' is not present in reviewer_branch_manifest`
      };
    }
  }

  for (const branchId of feedbackUncoveredBranchIds) {
    if (!uncoveredBranchIds.includes(branchId)) {
      return {
        pass: false,
        score: 0,
        reason: `Feedback uncovered branch '${branchId}' is not present in coverage.uncovered`
      };
    }
  }

  for (const branchId of expectedReviewerBranchIds) {
    if (!reviewerBranchIds.includes(branchId)) {
      return {
        pass: false,
        score: 0,
        reason: `Expected reviewer branch id '${branchId}' not found in reviewer_branch_manifest`
      };
    }
  }

  for (const branchId of rejectedReviewerBranchIds) {
    if (reviewerBranchIds.includes(branchId)) {
      return {
        pass: false,
        score: 0,
        reason: `Rejected reviewer branch id '${branchId}' unexpectedly found in reviewer_branch_manifest`
      };
    }
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
