// Validates output against candidate_writers.json schema (draft 2020-12).
const Ajv2020 = require('ajv/dist/2020').default;
const addFormats = require('ajv-formats');
const fs = require('fs');
const path = require('path');

const SCHEMA_PATH = path.resolve(__dirname, '..', '..', '..', 'lib', 'shared', 'schemas', 'candidate_writers.json');
const COMMON_PATH = path.resolve(__dirname, '..', '..', '..', 'lib', 'shared', 'schemas', 'common.json');

module.exports = (output, context) => {
  const schema = JSON.parse(fs.readFileSync(SCHEMA_PATH, 'utf8'));
  const ajv = new Ajv2020({ allErrors: true, strict: false });
  addFormats(ajv);

  if (fs.existsSync(COMMON_PATH)) {
    ajv.addSchema(JSON.parse(fs.readFileSync(COMMON_PATH, 'utf8')), 'common.json');
  }

  let data;
  try {
    data = JSON.parse(output);
  } catch {
    const match = output.match(/\{[\s\S]*\}/);
    if (!match) return { pass: false, score: 0, reason: 'No JSON found in output' };
    try { data = JSON.parse(match[0]); }
    catch (e) { return { pass: false, score: 0, reason: `JSON parse error: ${e.message}` }; }
  }

  const validate = ajv.compile(schema);
  if (validate(data)) return { pass: true, score: 1, reason: 'Validates against candidate_writers.json' };

  const errors = validate.errors.map(e => `${e.instancePath || '/'}: ${e.message}`).join('; ');
  return { pass: false, score: 0, reason: `Schema validation failed: ${errors}` };
};
