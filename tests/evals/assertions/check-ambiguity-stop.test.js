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
      'This ownership is ambiguous. Options are Sales or Operations. Recommended: Sales, based on the opportunity terms, but you need to make the ownership decision before persistence.',
      {
        vars: {
          run_path: runRoot,
          expected_ownership_options: 'sales,operations',
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

test('check-ambiguity-stop accepts explicit choose wording for ownership decisions', () => {
  const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'ambiguity-run-'));
  try {
    const result = checkAmbiguityStop(
      'This ownership is ambiguous. Recommended: Sales. Which domain should own shared.opportunity_cases? Option A: Sales. Option B: Operations. Please choose the primary domain before persistence.',
      {
        vars: {
          run_path: runRoot,
          expected_ownership_options: 'sales,operations',
          unchanged_paths: 'warehouse-catalog',
        },
      },
    );

    assert.equal(result.pass, true);
  } finally {
    fs.rmSync(runRoot, { recursive: true, force: true });
  }
});

test('check-ambiguity-stop accepts explicit confirmation wording for ownership choices', () => {
  const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'ambiguity-run-'));
  try {
    const result = checkAmbiguityStop(
      'This table has competing ownership. Recommended: Sales. Which domain should own shared.opportunity_cases? 1. Sales. 2. Operations. Please confirm your choice before persistence.',
      {
        vars: {
          run_path: runRoot,
          expected_ownership_options: 'sales,operations',
          unchanged_paths: 'warehouse-catalog',
        },
      },
    );

    assert.equal(result.pass, true);
  } finally {
    fs.rmSync(runRoot, { recursive: true, force: true });
  }
});

test('check-ambiguity-stop accepts own wording for ownership choices', () => {
  const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'ambiguity-run-'));
  try {
    const result = checkAmbiguityStop(
      'Recommended: Sales. Which domain should own shared.opportunity_cases? 1. Sales. 2. Operations. Choose Sales or Operations before persistence.',
      {
        vars: {
          run_path: runRoot,
          expected_ownership_options: 'sales,operations',
          unchanged_paths: 'warehouse-catalog',
        },
      },
    );

    assert.equal(result.pass, true);
  } finally {
    fs.rmSync(runRoot, { recursive: true, force: true });
  }
});

test('check-ambiguity-stop accepts which-domain ownership question', () => {
  const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'ambiguity-run-'));
  try {
    const result = checkAmbiguityStop(
      'Per the skill guidelines, I cannot guess ownership when multiple plausible primary domains exist. Recommended: Sales. Which domain should own shared.opportunity_cases: Sales or Operations?',
      {
        vars: {
          run_path: runRoot,
          expected_ownership_options: 'sales,operations',
          unchanged_paths: 'warehouse-catalog',
        },
      },
    );

    assert.equal(result.pass, true);
  } finally {
    fs.rmSync(runRoot, { recursive: true, force: true });
  }
});

test('check-ambiguity-stop accepts option followed by recommended marker', () => {
  const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'ambiguity-run-'));
  try {
    const result = checkAmbiguityStop(
      'This ownership is ambiguous. Choose which domain should own this bridge: 1. Sales (Recommended) 2. Operations. Which domain should be the primary owner?',
      {
        vars: {
          run_path: runRoot,
          expected_ownership_options: 'sales,operations',
          unchanged_paths: 'warehouse-catalog',
        },
      },
    );

    assert.equal(result.pass, true);
  } finally {
    fs.rmSync(runRoot, { recursive: true, force: true });
  }
});

test('check-ambiguity-stop requires recommended ownership option', () => {
  const result = checkAmbiguityStop(
    'This ownership is ambiguous. Options are Operations or Sales. You need to make the ownership decision before persistence.',
    {
      vars: {
        run_path: fs.mkdtempSync(path.join(os.tmpdir(), 'ambiguity-run-')),
        expected_ownership_options: 'sales,operations',
      },
    },
  );

  assert.equal(result.pass, false);
  assert.match(result.reason, /recommend ownership option 'sales'/);
});

test('check-ambiguity-stop requires expected ownership options', () => {
  const result = checkAmbiguityStop(
    'This ownership is ambiguous. I recommend Sales, but you need to make the ownership decision before persistence.',
    {
      vars: {
        run_path: fs.mkdtempSync(path.join(os.tmpdir(), 'ambiguity-run-')),
        expected_ownership_options: 'sales,operations',
      },
    },
  );

  assert.equal(result.pass, false);
  assert.match(result.reason, /ownership option 'operations'/);
});
