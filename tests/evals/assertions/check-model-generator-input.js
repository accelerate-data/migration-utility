// Validates that the model-generator input manifest is well-formed JSON.
// Schema shape is enforced by ModelGeneratorInput Pydantic model in output_models.py.
// Usage: type: javascript, value: file://../../assertions/check-model-generator-input.js
// Expects context.vars:
// {
//   fixture_path
// }
const fs = require('fs');
const path = require('path');

module.exports = (output, context) => {
  const fixturePath = context.vars.fixture_path;
  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const runsDir = path.resolve(repoRoot, fixturePath, '.migration-runs');

  if (!fs.existsSync(runsDir)) {
    // No migration-runs directory — skip validation (command may not have produced one)
    return { pass: true, score: 1, reason: 'No .migration-runs directory — input manifest validation skipped' };
  }

  // Look for model-generator input manifests (model-generator-input*.json)
  const files = fs.readdirSync(runsDir).filter((f) => f.includes('model-generator-input') && f.endsWith('.json'));
  if (files.length === 0) {
    return { pass: true, score: 1, reason: 'No model-generator input manifest found — validation skipped' };
  }

  for (const file of files) {
    try {
      JSON.parse(fs.readFileSync(path.join(runsDir, file), 'utf8'));
    } catch (e) {
      return { pass: false, score: 0, reason: `Failed to parse ${file}: ${e.message}` };
    }
  }

  return { pass: true, score: 1, reason: `Validated ${files.length} model-generator input manifest(s)` };
};
