// Validates output against candidate_writers.json schema (draft 2020-12).
const { validateSchema, extractJsonObject } = require('./schema-helpers');

module.exports = (output, context) => {
  let data;
  try {
    data = extractJsonObject(output);
  } catch (e) {
    return { pass: false, score: 0, reason: e.message };
  }

  const result = validateSchema(data, 'candidate_writers.json');
  if (result.valid) {
    return { pass: true, score: 1, reason: 'Validates against candidate_writers.json' };
  }
  return { pass: false, score: 0, reason: `Schema validation failed: ${result.errors}` };
};
