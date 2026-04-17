// Validates that /planning-refactor-mart wrote the expected markdown contract.
// Expects context.vars:
// {
//   run_path,
//   plan_name,
//   expected_candidate_types,
//   expected_terms?,
//   expected_unapproved_terms?
// }
const fs = require('fs');
const path = require('path');
const { normalizeTerms, resolveProjectPath } = require('./schema-helpers');

function candidateSections(markdown) {
  return String(markdown || '')
    .split(/^#{2,3} Candidate:\s+/m)
    .slice(1)
    .map((section) => section.trim())
    .filter(Boolean);
}

module.exports = (_output, context) => {
  const projectPath = resolveProjectPath(context);
  const planName = context.vars.plan_name;
  if (!planName) {
    return { pass: false, score: 0, reason: 'plan_name var is required' };
  }

  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const planPath = path.resolve(repoRoot, projectPath, 'docs', 'design', `${planName}.md`);
  if (!fs.existsSync(planPath)) {
    return { pass: false, score: 0, reason: `Plan file not found: ${planPath}` };
  }

  const markdown = fs.readFileSync(planPath, 'utf8');
  const normalized = markdown.toLowerCase();
  const sections = candidateSections(markdown);
  if (sections.length === 0) {
    return { pass: false, score: 0, reason: 'No candidate sections found' };
  }

  const requiredFields = [
    'approve:',
    'type:',
    'output:',
    'depends on:',
    'validation:',
    'execution status:',
  ];
  for (const section of sections) {
    const sectionLower = section.toLowerCase();
    for (const field of requiredFields) {
      if (!sectionLower.includes(field)) {
        return {
          pass: false,
          score: 0,
          reason: `Candidate section missing field '${field}'`,
        };
      }
    }
  }

  const expectedTypes = normalizeTerms(context.vars.expected_candidate_types);
  for (const type of expectedTypes) {
    if (!normalized.includes(`type: ${type}`)) {
      return {
        pass: false,
        score: 0,
        reason: `Expected candidate type '${type}' not found`,
      };
    }
  }

  for (const term of normalizeTerms(context.vars.expected_terms)) {
    if (!normalized.includes(term)) {
      return { pass: false, score: 0, reason: `Expected term '${term}' not found` };
    }
  }

  for (const term of normalizeTerms(context.vars.expected_unapproved_terms)) {
    const matchingSection = sections.find((section) =>
      section.toLowerCase().includes(term),
    );
    if (!matchingSection) {
      return {
        pass: false,
        score: 0,
        reason: `Expected unapproved candidate term '${term}' not found`,
      };
    }
    if (!matchingSection.toLowerCase().includes('approve: no')) {
      return {
        pass: false,
        score: 0,
        reason: `Candidate term '${term}' was not left unapproved`,
      };
    }
  }

  if (normalized.includes('dbt/models') && normalized.includes('execution status: applied')) {
    return {
      pass: false,
      score: 0,
      reason: 'Planning output marked a candidate applied',
    };
  }

  return {
    pass: true,
    score: 1,
    reason: `Plan contract found with ${sections.length} candidate sections`,
  };
};
