const fs = require('fs');
const path = require('path');
const { normalizeTerms, resolveProjectPath } = require('./schema-helpers');

function fail(reason) {
  return { pass: false, score: 0, reason };
}

function candidateSections(markdown) {
  const candidateHeading = /^## Candidate:\s+((STG|INT|MART)-\d+)\s*$/gm;
  const sections = [];
  let match;

  while ((match = candidateHeading.exec(markdown)) !== null) {
    const bodyStart = candidateHeading.lastIndex;
    const nextMatch = /^## Candidate:\s+(STG|INT|MART)-\d+\s*$/gm;
    nextMatch.lastIndex = bodyStart;
    const next = nextMatch.exec(markdown);
    sections.push({
      id: match[1],
      body: markdown.slice(bodyStart, next ? next.index : markdown.length).trim(),
    });
  }

  return sections;
}

function fieldValue(section, field) {
  const escapedField = field.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const match = section.body.match(new RegExp(`^-\\s+${escapedField}:\\s+(.+?)\\s*$`, 'm'));
  return match ? match[1].trim().toLowerCase() : null;
}

function parseExpectedStatuses(value) {
  return normalizeTerms(value).map((entry) => {
    const [candidateId, status] = entry.split(':');
    return { candidateId: candidateId.toUpperCase(), status };
  });
}

module.exports = (output, context) => {
  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const runRoot = path.resolve(repoRoot, resolveProjectPath(context));
  const planPath = path.join(runRoot, context.vars.plan_file);
  if (!fs.existsSync(planPath)) {
    return fail(`Plan file not found: ${planPath}`);
  }

  const outputText = String(output || '').toLowerCase();
  for (const term of normalizeTerms(context.vars.expected_output_terms)) {
    if (!outputText.includes(term)) {
      return fail(`Final output missing expected term '${term}'`);
    }
  }

  const markdown = fs.readFileSync(planPath, 'utf8');
  const sections = candidateSections(markdown);
  if (sections.length === 0) {
    return fail('No candidate sections found');
  }

  for (const expected of parseExpectedStatuses(context.vars.expected_candidate_statuses)) {
    const section = sections.find((candidate) => candidate.id === expected.candidateId);
    if (!section) {
      return fail(`Candidate ${expected.candidateId} not found`);
    }
    const actualStatus = fieldValue(section, 'Execution status');
    if (actualStatus !== expected.status) {
      return fail(
        `Candidate ${expected.candidateId} expected status '${expected.status}', found '${actualStatus}'`,
      );
    }
  }

  const nonStagingChanged = sections.some((section) => {
    const type = fieldValue(section, 'Type');
    const status = fieldValue(section, 'Execution status');
    return (type === 'int' || type === 'mart') && status !== 'planned';
  });
  if (nonStagingChanged) {
    return fail('Staging execution must not mutate non-staging candidate status');
  }

  return { pass: true, score: 1, reason: 'Staging execution plan status matched expectations' };
};
