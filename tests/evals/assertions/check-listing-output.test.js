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

test('check-listing-output validates object role pairs across markdown punctuation', () => {
  const result = checkListingOutput(
    [
      '**customer.dim_customer:** Dimension',
      '- **customer.customer_address_history:** Dimension',
    ].join('\n'),
    {
      vars: {
        expected_role_pairs:
          'customer.dim_customer=dimension;customer.customer_address_history=dimension',
      },
    },
  );

  assert.equal(result.pass, true);
});

test('check-listing-output rejects missing object role pairs', () => {
  const result = checkListingOutput('customer.customer_address_history: Unknown', {
    vars: {
      expected_role_pairs: 'customer.customer_address_history=dimension',
    },
  });

  assert.equal(result.pass, false);
  assert.match(
    result.reason,
    /Expected role 'dimension' for 'customer\.customer_address_history'/,
  );
});
