const { normalizeTerms } = require('./schema-helpers');

function normalizeRolePairs(value) {
  if (!value) return [];
  return String(value)
    .split(';')
    .map((pair) => pair.trim())
    .filter(Boolean)
    .map((pair) => {
      const [objectName, role] = pair.split('=').map((part) => part.trim().toLowerCase());
      return { objectName, role };
    })
    .filter(({ objectName, role }) => objectName && role);
}

function hasObjectRole(text, objectName, role) {
  const escapedObject = objectName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const escapedRole = role.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const pattern = new RegExp(
    `${escapedObject}[\\s\`*_]*:?[^\\n]{0,80}\\b${escapedRole}\\b`,
    'i',
  );
  return pattern.test(text);
}

module.exports = (output, context) => {
  const text = String(output || '').toLowerCase();
  const expectedTerms = normalizeTerms(context.vars.expected_terms);
  const unexpectedTerms = normalizeTerms(context.vars.unexpected_terms);
  const forbiddenJsonKeys = normalizeTerms(context.vars.forbidden_json_keys);
  const expectedRolePairs = normalizeRolePairs(context.vars.expected_role_pairs);

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

  for (const key of forbiddenJsonKeys) {
    const escapedKey = key.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    if (new RegExp(`"${escapedKey}"\\s*:`).test(text)) {
      return {
        pass: false,
        score: 0,
        reason: `Output included forbidden JSON key '${key}'`
      };
    }
  }

  for (const { objectName, role } of expectedRolePairs) {
    if (!hasObjectRole(text, objectName, role)) {
      return {
        pass: false,
        score: 0,
        reason: `Expected role '${role}' for '${objectName}'`,
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
