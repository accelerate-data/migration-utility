const assert = require('node:assert/strict');
const test = require('node:test');

const checkOutputTerms = require('./check-output-terms');

test('check-output-terms prefers final-output terms when provided', () => {
  const result = checkOutputTerms('guard failed: CATALOG_ERRORS_UNRESOLVED', {
    vars: {
      expected_output_terms: 'term-only-present-in-artifact',
      expected_final_output_terms: 'CATALOG_ERRORS_UNRESOLVED',
    },
  });

  assert.equal(result.pass, true);
});

test('check-output-terms keeps expected_output_terms as a legacy fallback', () => {
  const result = checkOutputTerms('guard failed: CATALOG_ERRORS_UNRESOLVED', {
    vars: {
      expected_output_terms: 'CATALOG_ERRORS_UNRESOLVED',
    },
  });

  assert.equal(result.pass, true);
});

test('check-output-terms honors intentionally empty final-output terms', () => {
  const result = checkOutputTerms('no required final response terms', {
    vars: {
      expected_output_terms: 'term-only-present-in-artifact',
      expected_final_output_terms: '',
    },
  });

  assert.equal(result.pass, true);
});
