const fs = require('fs');
const path = require('path');
const { normalizeTerms } = require('./schema-helpers');

module.exports = (_output, context) => {
  const runPath = context.vars.run_path;
  if (!runPath) {
    return {
      pass: false,
      score: 0,
      reason: 'run_path is required for analyze-data-domains contract checks',
    };
  }

  for (const relativePath of normalizeTerms(context.vars.forbidden_paths)) {
    const targetPath = path.join(runPath, relativePath);
    if (fs.existsSync(targetPath)) {
      return {
        pass: false,
        score: 0,
        reason: `Forbidden path was created: ${relativePath}`,
      };
    }
  }

  return {
    pass: true,
    score: 1,
    reason: 'Analyze-data-domains filesystem contract satisfied',
  };
};
