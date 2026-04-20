const checkGuardStop = require('./check-guard-stop');
const { normalizeTerms } = require('./schema-helpers');

function fail(reason) {
  return { pass: false, score: 0, reason };
}

module.exports = (output, context) => {
  const text = String(output || '').toLowerCase();
  const hasOwnership = text.includes('ownership') ||
    text.includes('owner') ||
    text.includes('own ');
  const hasDecision = text.includes('decision') ||
    text.includes('decide') ||
    text.includes('resolve') ||
    text.includes('choose') ||
    text.includes('choice') ||
    text.includes('confirm');
  const hasHuman = text.includes('human') ||
    text.includes('user') ||
    text.includes('you') ||
    text.includes('please choose') ||
    text.includes('which domain should own');

  if (!hasOwnership || !hasDecision || !hasHuman) {
    return fail('Expected response to request a human ownership decision');
  }

  const options = normalizeTerms(context.vars.expected_ownership_options);
  const optionIndexes = options.map((option) => [option, text.indexOf(option)]);
  for (const [option, index] of optionIndexes) {
    if (index === -1) {
      return fail(`Expected response to include ownership option '${option}'`);
    }
  }
  if (optionIndexes.length > 1) {
    const [recommendedOption, recommendedIndex] = optionIndexes[0];
    for (const [option, index] of optionIndexes.slice(1)) {
      if (index < recommendedIndex) {
        return fail(
          `Expected recommended ownership option '${recommendedOption}' to appear before '${option}'`,
        );
      }
    }
  }

  return checkGuardStop(output, context);
};
