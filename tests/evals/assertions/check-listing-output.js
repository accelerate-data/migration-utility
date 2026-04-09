const { normalizeTerms } = require('./schema-helpers');

module.exports = (output, context) => {
  const text = String(output || '').toLowerCase();
  const expectedTerms = normalizeTerms(context.vars.expected_terms);
  const unexpectedTerms = normalizeTerms(context.vars.unexpected_terms);

  for (const term of expectedTerms) {
    if (!text.includes(term)) {
      return {
        pass: false,
        score: 0,
        reason: `Output missing expected term '${term}'`
      };
    }
  }

  for (const term of unexpectedTerms) {
    if (text.includes(term)) {
      return {
        pass: false,
        score: 0,
        reason: `Output included unexpected term '${term}'`
      };
    }
  }

  return {
    pass: true,
    score: 1,
    reason: expectedTerms.length
      ? `Output included expected terms: ${expectedTerms.join(', ')}`
      : 'Output satisfied listing-objects expectations'
  };
};
