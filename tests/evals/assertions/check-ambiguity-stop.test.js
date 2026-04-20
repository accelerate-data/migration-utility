const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const checkAmbiguityStop = require('./check-ambiguity-stop');

test('check-ambiguity-stop requires human ownership decision wording', () => {
  const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'ambiguity-run-'));
  try {
    const result = checkAmbiguityStop(
      'This ownership is ambiguous and needs a human decision before persistence.',
      {
        vars: {
          run_path: runRoot,
          unchanged_paths: 'warehouse-catalog',
        },
      },
    );

    assert.equal(result.pass, true);
  } finally {
    fs.rmSync(runRoot, { recursive: true, force: true });
  }
});

test('check-ambiguity-stop rejects weak ownership wording', () => {
  const result = checkAmbiguityStop('I analyzed ownership and wrote nothing.', {
    vars: {
      run_path: fs.mkdtempSync(path.join(os.tmpdir(), 'ambiguity-run-')),
    },
  });

  assert.equal(result.pass, false);
  assert.match(result.reason, /human ownership decision/);
});
