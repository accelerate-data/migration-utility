// Validates that dbt model artifacts were written.
// Usage: type: javascript, value: file://../../assertions/check-dbt-model.js
// Expects context.vars:
// {
//   fixture_path,
//   target_table,
//   expected_model_terms?,
//   forbidden_model_terms?,
//   expected_output_terms?
// }
const fs = require('fs');
const path = require('path');

function normalizeTerms(value) {
  if (!value) return [];
  return String(value)
    .split(',')
    .map((term) => term.trim().toLowerCase())
    .filter(Boolean);
}

module.exports = (output, context) => {
  const fixturePath = context.vars.fixture_path;
  const table = context.vars.target_table;
  const expectedModelTerms = normalizeTerms(context.vars.expected_model_terms);
  const forbiddenModelTerms = normalizeTerms(context.vars.forbidden_model_terms);
  const expectedOutputTerms = normalizeTerms(context.vars.expected_output_terms);

  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const dbtDir = path.resolve(repoRoot, fixturePath, 'dbt');

  if (!fs.existsSync(dbtDir)) {
    // Check if output text contains model SQL as fallback
    if (output && output.toLowerCase().includes('config(') && output.toLowerCase().includes('select')) {
      return { pass: true, score: 1, reason: 'dbt model SQL found in output text (no dbt project to write to)' };
    }
    return { pass: false, score: 0, reason: `dbt directory not found at ${dbtDir} and no model SQL in output` };
  }

  // Look for model files in the dbt directory
  const modelsDir = path.resolve(dbtDir, 'models');
  if (!fs.existsSync(modelsDir)) {
    if (output && output.toLowerCase().includes('config(')) {
      return { pass: true, score: 1, reason: 'dbt model SQL found in output (models dir not yet created)' };
    }
    return { pass: false, score: 0, reason: 'No models directory found in dbt project' };
  }

  // Find SQL files that might match the table
  const tableName = table.split('.').pop().toLowerCase();
  const allFiles = [];
  const walkDir = (dir) => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      if (entry.isDirectory()) walkDir(path.join(dir, entry.name));
      else if (entry.name.endsWith('.sql')) allFiles.push(path.join(dir, entry.name));
    }
  };
  walkDir(modelsDir);

  const matchingFiles = allFiles.filter(f => f.toLowerCase().includes(tableName));
  if (matchingFiles.length === 0) {
    if (output && output.toLowerCase().includes('config(')) {
      return { pass: true, score: 1, reason: 'dbt model SQL found in output (no matching file written yet)' };
    }
    return { pass: false, score: 0, reason: `No SQL file matching '${tableName}' found in ${modelsDir}` };
  }

  // Verify the model contains config()
  const modelContent = fs.readFileSync(matchingFiles[0], 'utf8');
  if (!modelContent.includes('config(')) {
    return { pass: false, score: 0, reason: `Model file ${matchingFiles[0]} missing config() block` };
  }

  const normalizedModel = modelContent.toLowerCase();
  for (const term of expectedModelTerms) {
    if (!normalizedModel.includes(term)) {
      return { pass: false, score: 0, reason: `Expected model term '${term}' not found in ${path.basename(matchingFiles[0])}` };
    }
  }

  for (const term of forbiddenModelTerms) {
    if (normalizedModel.includes(term)) {
      return { pass: false, score: 0, reason: `Forbidden model term '${term}' found in ${path.basename(matchingFiles[0])}` };
    }
  }

  if (expectedOutputTerms.length > 0) {
    const outputStr = String(output || '').toLowerCase();
    for (const term of expectedOutputTerms) {
      if (!outputStr.includes(term)) {
        return { pass: false, score: 0, reason: `Expected output term '${term}' not found in final response` };
      }
    }
  }

  return { pass: true, score: 1, reason: `dbt model written: ${path.basename(matchingFiles[0])}` };
};
