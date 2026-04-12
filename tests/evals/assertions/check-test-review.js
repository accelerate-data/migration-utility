const fs = require('fs');
const path = require('path');
const { extractJsonObject, normalizeTerms, resolveProjectPath } = require('./schema-helpers');

module.exports = (output, context) => {
  const fixturePath = resolveProjectPath(context);
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

  const coveredBranches = Number(review.coverage?.covered_branches || 0);
  if (coveredBranches < minCoveredBranches) {
    return { pass: false, score: 0, reason: `Expected at least ${minCoveredBranches} covered branches, got ${coveredBranches}` };
  }

  const reviewerBranchManifest = Array.isArray(review.reviewer_branch_manifest)
    ? review.reviewer_branch_manifest
    : [];
  const reviewerBranchIds = reviewerBranchManifest.map(branch => String(branch.id || '').toLowerCase());
  const coveredManifestBranches = reviewerBranchManifest.filter(branch => branch.covered === true).length;
  const uncoveredBranchIds = Array.isArray(review.coverage?.uncovered)
    ? review.coverage.uncovered.map(branch => String(branch.id || '').toLowerCase())
    : [];
  const untestableBranchIds = Array.isArray(review.coverage?.untestable)
    ? review.coverage.untestable.map(branch => String(branch.id || '').toLowerCase())
    : [];
  const feedbackUncoveredBranchIds = Array.isArray(review.feedback_for_generator?.uncovered_branches)
    ? review.feedback_for_generator.uncovered_branches.map(branchId => String(branchId || '').toLowerCase())
    : [];

  const totalBranches = Number(review.coverage?.total_branches || 0);
  if (reviewerBranchManifest.length !== totalBranches) {
    return {
      pass: false,
      score: 0,
      reason: `Coverage total_branches=${totalBranches} does not match reviewer_branch_manifest length=${reviewerBranchManifest.length}`
    };
  }

  if (coveredManifestBranches !== coveredBranches) {
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
