const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const checkReviewingDiagnostics = require('./check-reviewing-diagnostics');

function makeRunRoot(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'check-reviewing-diagnostics-'));
  t.after(() => fs.rmSync(root, { force: true, recursive: true }));
  return root;
}

test('check-reviewing-diagnostics validates accepted review artifact evidence', (t) => {
  const runRoot = makeRunRoot(t);
  fs.mkdirSync(path.join(runRoot, 'catalog'), { recursive: true });
  fs.writeFileSync(
    path.join(runRoot, 'catalog', 'diagnostic-reviews.json'),
    JSON.stringify({
      reviews: [{
        fqn: 'silver.dimcustomer',
        code: 'MISSING_FK',
        status: 'accepted',
        message_hash: 'sha256:abc123',
        reason: 'The table is intentionally denormalized for reporting.',
        evidence: ['catalog/tables/silver.dimcustomer.json'],
      }],
    }),
  );

  const result = checkReviewingDiagnostics('Reviewed diagnostic accepted.', {
    vars: {
      run_path: runRoot,
      expected_output_terms: 'reviewed',
      expect_review_artifact: 'true',
      expected_artifact_fqn: 'silver.dimcustomer',
      expected_artifact_code: 'MISSING_FK',
    },
  });

  assert.equal(result.pass, true, result.reason);
});

test('check-reviewing-diagnostics rejects unexpected review artifacts', (t) => {
  const runRoot = makeRunRoot(t);
  fs.mkdirSync(path.join(runRoot, 'catalog'), { recursive: true });
  fs.writeFileSync(path.join(runRoot, 'catalog', 'diagnostic-reviews.json'), '{"reviews": []}');

  const result = checkReviewingDiagnostics('No review needed.', {
    vars: {
      run_path: runRoot,
      expect_no_review_artifact: 'true',
    },
  });

  assert.equal(result.pass, false);
  assert.match(result.reason, /Did not expect reviewed diagnostic artifact/);
});
