const assert = require('node:assert/strict');
const test = require('node:test');

const checkListingOutput = require('./check-listing-output');

test('check-listing-output rejects forbidden JSON keys', () => {
  const result = checkListingOutput(
    '```json\n{"objects":{"tables":[],"procedures":[]}}\n```',
    {
      vars: {
        expected_terms: 'objects',
        forbidden_json_keys: 'procedures,functions',
      },
    },
  );

  assert.equal(result.pass, false);
  assert.match(result.reason, /forbidden JSON key 'procedures'/);
});

test('check-listing-output permits prose mentions of forbidden keys', () => {
  const result = checkListingOutput(
    'Procedures are ignored. ```json\n{"objects":{"tables":[],"views":[]}}\n```',
    {
      vars: {
        expected_terms: 'tables,views',
        forbidden_json_keys: 'procedures,functions',
      },
    },
  );

  assert.equal(result.pass, true);
});
