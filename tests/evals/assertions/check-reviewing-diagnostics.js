// Validates /review-diagnostics skill behavior.
// Checks final response terms and, when requested, the reviewed warning artifact.

const fs = require('fs');
const path = require('path');

const { normalizeTerms } = require('./schema-helpers');

function fail(reason) {
  return { pass: false, score: 0, reason };
}

module.exports = (output, context) => {
  const outputStr = String(output || '').toLowerCase();

  for (const term of normalizeTerms(context.vars.expected_output_terms)) {
    if (!outputStr.includes(term)) {
      return fail(`Expected output term '${term}' not found`);
    }
  }

  for (const term of normalizeTerms(context.vars.unexpected_output_terms)) {
    if (outputStr.includes(term)) {
      return fail(`Unexpected output term '${term}' found`);
    }
  }

  const runPath = context.vars.run_path;
  if (String(context.vars.expect_no_review_artifact || '').toLowerCase() === 'true') {
    if (!runPath) {
      return fail('run_path was not provided by the workspace extension');
    }
    const artifactPath = path.join(runPath, 'catalog', 'diagnostic-reviews.json');
    if (fs.existsSync(artifactPath)) {
      return fail(`Did not expect reviewed diagnostic artifact at ${artifactPath}`);
    }
  }

  if (String(context.vars.expect_review_artifact || '').toLowerCase() !== 'true') {
    return { pass: true, score: 1, reason: 'Reviewing diagnostics output validated' };
  }

  if (!runPath) {
    return fail('run_path was not provided by the workspace extension');
  }

  const artifactPath = path.join(runPath, 'catalog', 'diagnostic-reviews.json');
  if (!fs.existsSync(artifactPath)) {
    return fail(`Expected reviewed diagnostic artifact at ${artifactPath}`);
  }

  let artifact;
  try {
    artifact = JSON.parse(fs.readFileSync(artifactPath, 'utf8'));
  } catch (error) {
    return fail(`Failed to parse reviewed diagnostic artifact: ${error.message}`);
  }

  const reviews = Array.isArray(artifact.reviews) ? artifact.reviews : [];
  const expectedFqn = String(context.vars.expected_artifact_fqn || '').toLowerCase();
  const expectedCode = String(context.vars.expected_artifact_code || '').toLowerCase();
  const review = reviews.find((item) => (
    String(item.fqn || '').toLowerCase() === expectedFqn &&
    String(item.code || '').toLowerCase() === expectedCode &&
    item.status === 'accepted'
  ));

  if (!review) {
    return fail(`No accepted review found for ${expectedFqn} ${expectedCode}`);
  }
  if (!String(review.message_hash || '').startsWith('sha256:')) {
    return fail('Review message_hash must start with sha256:');
  }
  if (!review.reason || String(review.reason).trim().length < 20) {
    return fail('Review reason must be specific');
  }
  if (!Array.isArray(review.evidence) || review.evidence.length === 0) {
    return fail('Review evidence must include at least one path');
  }

  return { pass: true, score: 1, reason: 'Reviewing diagnostics artifact validated' };
};
