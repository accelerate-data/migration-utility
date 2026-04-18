// Validates that expected terms appear in the final response text.
// Usage: type: javascript, value: file://../../assertions/check-output-terms.js
// Expects context.vars:
// {
//   expected_final_output_terms? preferred for final-response-only checks
//   expected_output_terms? legacy fallback
// }
const { normalizeTerms } = require('./schema-helpers');

module.exports = (output, context) => {
  const expectedOutputTerms = normalizeTerms(
    context.vars.expected_final_output_terms ?? context.vars.expected_output_terms,
  );
  const outputStr = String(output || '').toLowerCase();

  for (const term of expectedOutputTerms) {
    if (!outputStr.includes(term)) {
      return {
        pass: false,
        score: 0,
        reason: `Expected output term '${term}' not found in final response`,
      };
    }
  }

  return {
    pass: true,
    score: 1,
    reason: 'Expected output terms found in final response',
  };
};
