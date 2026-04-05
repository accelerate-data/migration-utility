// Shared helpers for eval assertion scripts.
// Provides AJV schema validation, JSON extraction, and term normalization.
const Ajv2020 = require('ajv/dist/2020').default;
const addFormats = require('ajv-formats');
const fs = require('fs');
const path = require('path');

const SCHEMA_DIR = path.resolve(__dirname, '..', '..', '..', 'plugin', 'lib', 'shared', 'schemas');
const COMMON_PATH = path.join(SCHEMA_DIR, 'common.json');

/** Cache compiled validators keyed by schema filename. */
const validatorCache = new Map();

/**
 * Compile and cache an AJV validator for the given schema file.
 * @param {string} schemaFileName — filename inside plugin/lib/shared/schemas/
 * @returns {import('ajv').ValidateFunction}
 */
function createValidator(schemaFileName) {
  if (validatorCache.has(schemaFileName)) {
    return validatorCache.get(schemaFileName);
  }

  const schemaPath = path.join(SCHEMA_DIR, schemaFileName);
  const schema = JSON.parse(fs.readFileSync(schemaPath, 'utf8'));

  const ajv = new Ajv2020({ allErrors: true, strict: false });
  addFormats(ajv);

  if (fs.existsSync(COMMON_PATH)) {
    ajv.addSchema(JSON.parse(fs.readFileSync(COMMON_PATH, 'utf8')), 'common.json');
  }

  const validate = ajv.compile(schema);
  validatorCache.set(schemaFileName, validate);
  return validate;
}

/**
 * Validate data against a named schema.
 * @param {object} data — parsed JSON to validate
 * @param {string} schemaFileName — filename inside plugin/lib/shared/schemas/
 * @returns {{ valid: boolean, errors: string }}
 */
function validateSchema(data, schemaFileName) {
  const validate = createValidator(schemaFileName);
  const valid = validate(data);
  if (valid) {
    return { valid: true, errors: '' };
  }
  const errors = validate.errors
    .map((e) => `${e.instancePath || '/'}: ${e.message}`)
    .join('; ');
  return { valid: false, errors };
}

/**
 * Extract a JSON object from LLM output text.
 * Tries fenced ```json blocks first, then raw braces.
 * @param {string} output — raw LLM output text
 * @returns {object}
 * @throws {Error} when no JSON object can be extracted
 */
function extractJsonObject(output) {
  const text = String(output || '').trim();
  const fencedMatches = Array.from(text.matchAll(/```json\s*([\s\S]*?)```/gi));
  if (fencedMatches.length > 0) {
    return JSON.parse(fencedMatches.at(-1)[1]);
  }
  const start = text.indexOf('{');
  const end = text.lastIndexOf('}');
  if (start === -1 || end === -1 || end <= start) {
    throw new Error('No JSON object found in output');
  }
  return JSON.parse(text.slice(start, end + 1));
}

/**
 * Split a comma-separated string into lowercase trimmed terms.
 * @param {string|undefined} value
 * @returns {string[]}
 */
function normalizeTerms(value) {
  if (!value) return [];
  return String(value)
    .split(',')
    .map((term) => term.trim().toLowerCase())
    .filter(Boolean);
}

/**
 * Validate a nested section of an object against a property or $defs entry.
 * Useful when the parent object has pre-existing fixture data that may not
 * fully validate, but the section under test (written by the agent) should.
 *
 * @param {object} sectionData — the nested section to validate
 * @param {string} schemaFileName — parent schema file
 * @param {string} sectionPath — either a $defs name (e.g. 'profile_section')
 *                                or a property path (e.g. 'properties/scoping')
 * @returns {{ valid: boolean, errors: string }}
 */
function validateSection(sectionData, schemaFileName, sectionPath) {
  const cacheKey = `${schemaFileName}#${sectionPath}`;
  if (validatorCache.has(cacheKey)) {
    const validate = validatorCache.get(cacheKey);
    const valid = validate(sectionData);
    if (valid) return { valid: true, errors: '' };
    const errors = validate.errors.map((e) => `${e.instancePath || '/'}: ${e.message}`).join('; ');
    return { valid: false, errors };
  }

  const schemaPath = path.join(SCHEMA_DIR, schemaFileName);
  const parentSchema = JSON.parse(fs.readFileSync(schemaPath, 'utf8'));

  const ajv = new Ajv2020({ allErrors: true, strict: false });
  addFormats(ajv);

  if (fs.existsSync(COMMON_PATH)) {
    ajv.addSchema(JSON.parse(fs.readFileSync(COMMON_PATH, 'utf8')), 'common.json');
  }

  // Register the parent schema so $defs and $ref are available
  ajv.addSchema(parentSchema, schemaFileName);

  // Resolve the section schema: try $defs first, then navigate property path
  let wrapperSchema;
  if (parentSchema.$defs && parentSchema.$defs[sectionPath]) {
    wrapperSchema = { $ref: `${schemaFileName}#/$defs/${sectionPath}` };
  } else {
    // Navigate the property path (e.g. 'properties/scoping')
    let node = parentSchema;
    for (const segment of sectionPath.split('/')) {
      node = node?.[segment];
    }
    if (!node) {
      return { valid: false, errors: `Section '${sectionPath}' not found in ${schemaFileName}` };
    }
    wrapperSchema = node;
  }

  const validate = ajv.compile(wrapperSchema);
  validatorCache.set(cacheKey, validate);

  const valid = validate(sectionData);
  if (valid) return { valid: true, errors: '' };
  const errors = validate.errors.map((e) => `${e.instancePath || '/'}: ${e.message}`).join('; ');
  return { valid: false, errors };
}

module.exports = { createValidator, validateSchema, validateSection, extractJsonObject, normalizeTerms };
