const { extractJsonObject, normalizeTerms } = require('./schema-helpers');

function fail(reason) {
  return { pass: false, score: 0, reason };
}

function requireTerms(text, value, label) {
  for (const term of normalizeTerms(value)) {
    if (!String(text || '').toLowerCase().includes(term)) {
      return fail(`Expected term '${term}' not found in ${label}`);
    }
  }
  return null;
}

module.exports = (output, context) => {
  let payload;
  try {
    payload = extractJsonObject(output);
  } catch (error) {
    return fail(`Failed to parse JSON output: ${error.message}`);
  }

  const expectedStatus = String(context.vars.expected_status || '').trim().toLowerCase();
  const actualStatus = String(payload.status || '').trim().toLowerCase();
  if (expectedStatus && actualStatus !== expectedStatus) {
      return fail(`Expected status '${expectedStatus}', got '${actualStatus}'`);
  }

  const wording = String(payload.wording || '').toLowerCase();
  if ((actualStatus === 'verified' || actualStatus === 'downgraded') && !wording.trim()) {
    return fail(`Expected non-empty wording for '${actualStatus}' guidance`);
  }
  if (actualStatus === 'blocked' && wording.trim()) {
    return fail("Expected empty wording for 'blocked' guidance");
  }

  const evidenceText = Array.isArray(payload.evidence_checked)
    ? payload.evidence_checked.join('\n')
    : '';
  const reason = String(payload.reason || '');

  const termChecks = [
    requireTerms(wording, context.vars.expected_wording_terms, 'wording'),
    requireTerms(evidenceText, context.vars.expected_evidence_terms, 'evidence_checked'),
    requireTerms(reason, context.vars.expected_reason_terms, 'reason'),
  ];
  const failedTermCheck = termChecks.find(Boolean);
  if (failedTermCheck) {
    return failedTermCheck;
  }

  const forbiddenWordingTerms = normalizeTerms(
    context.vars.forbidden_wording_terms ?? context.vars.forbidden_terms,
  );
  for (const term of forbiddenWordingTerms) {
    if (wording.includes(term)) {
      return fail(`Forbidden term '${term}' found in permitted wording`);
    }
  }

  if (!Array.isArray(payload.evidence_checked) || payload.evidence_checked.length === 0) {
    return fail('Expected non-empty evidence_checked array');
  }

  return {
    pass: true,
    score: 1,
    reason: `Completion claim guidance returned '${actualStatus}' with inspected evidence`,
  };
};
