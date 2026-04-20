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
    text.includes('confirm') ||
    text.includes('which domain should own');
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
  const recommendedOption = options[0];
  const recommendedBeforeOption = new RegExp(
    `recommend(?:ed|ation)?[^.\\n]{0,120}${recommendedOption}`,
  );
  const optionBeforeRecommended = new RegExp(
    `${recommendedOption}[^.\\n]{0,80}recommend(?:ed|ation)?`,
  );
  if (
    recommendedOption &&
    !recommendedBeforeOption.test(text) &&
    !optionBeforeRecommended.test(text)
  ) {
    return fail(`Expected response to recommend ownership option '${recommendedOption}'`);
  }

  return checkGuardStop(output, context);
};
