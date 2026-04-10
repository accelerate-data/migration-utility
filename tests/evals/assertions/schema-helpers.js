// Shared helpers for eval assertion scripts.
// Provides JSON extraction and term normalization.

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

module.exports = { normalizeTerms, extractJsonObject };
