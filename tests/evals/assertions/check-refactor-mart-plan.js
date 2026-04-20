// Validates that /planning-refactor-mart wrote the expected markdown contract.
// Expects context.vars:
// {
//   run_path,
//   plan_name,
//   target_tables?,
//   expected_candidate_types,
//   expected_higher_layer_candidate?,
//   expected_terms?,
//   expected_unapproved_terms?,
//   expect_no_plan?
// }
const fs = require('fs');
const crypto = require('crypto');
const path = require('path');
const { normalizeTerms, resolveProjectPath } = require('./schema-helpers');

function fail(reason) {
  return { pass: false, score: 0, reason };
}

function resolveUnderRepo(repoRoot, maybeRelativePath) {
  return path.resolve(repoRoot, maybeRelativePath || '');
}

function fileDigest(filePath) {
  return crypto.createHash('sha256').update(fs.readFileSync(filePath)).digest('hex');
}

function listFiles(root) {
  if (!fs.existsSync(root)) {
    return [];
  }

  const files = [];
  const visit = (dir) => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const fullPath = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        visit(fullPath);
        continue;
      }
      if (entry.isFile()) {
        files.push(path.relative(root, fullPath));
      }
    }
  };

  visit(root);
  return files.sort();
}

function snapshotDirectory(root) {
  return Object.fromEntries(
    listFiles(root).map((relativePath) => [
      relativePath,
      fileDigest(path.join(root, relativePath)),
    ]),
  );
}

function sameSnapshot(left, right) {
  const leftEntries = Object.entries(left);
  const rightEntries = Object.entries(right);
  if (leftEntries.length !== rightEntries.length) {
    return false;
  }

  return leftEntries.every(([relativePath, digest]) => right[relativePath] === digest);
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

function candidateType(section) {
  return fieldValue(section, 'Type');
}

function normalizeTargetTables(value) {
  if (!value) return [];
  return String(value)
    .split(/[\s,]+/)
    .map((term) => term.trim().toLowerCase())
    .filter(Boolean);
}

function hasSection(markdown, heading) {
  const escapedHeading = heading.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(`^##\\s+${escapedHeading}\\s*$`, 'mi').test(markdown);
}

function sectionBody(markdown, heading) {
  const escapedHeading = heading.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const headingMatch = new RegExp(`^##\\s+${escapedHeading}\\s*$`, 'mi').exec(markdown);
  if (!headingMatch) {
    return '';
  }

  const bodyStart = headingMatch.index + headingMatch[0].length;
  const nextHeading = /^##\s+/gm;
  nextHeading.lastIndex = bodyStart;
  const nextMatch = nextHeading.exec(markdown);
  return markdown.slice(bodyStart, nextMatch ? nextMatch.index : markdown.length);
}

module.exports = (output, context) => {
  const projectPath = resolveProjectPath(context);
  const fixturePath = context.vars.canonical_fixture_path || context.vars.fixture_path;
  const planName = context.vars.plan_name;
  if (!planName) {
    return fail('plan_name var is required');
  }

  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const runRoot = resolveUnderRepo(repoRoot, projectPath);
  const fixtureRoot = resolveUnderRepo(repoRoot, fixturePath);
  const planRelativePath = path.join('docs', 'design', `${planName}.md`);
  const planPath = path.join(runRoot, planRelativePath);
  if (String(context.vars.expect_no_plan || '').toLowerCase() === 'true') {
    if (fs.existsSync(planPath)) {
      return fail(`Plan file should not have been created: ${planPath}`);
    }
    const expectedModels = snapshotDirectory(path.join(fixtureRoot, 'dbt', 'models'));
    const actualModels = snapshotDirectory(path.join(runRoot, 'dbt', 'models'));
    if (!sameSnapshot(expectedModels, actualModels)) {
      return fail('Planning guard mutated files under dbt/models');
    }
    return {
      pass: true,
      score: 1,
      reason: 'Planning guard stopped before plan creation',
    };
  }
  if (!fs.existsSync(planPath)) {
    return fail(`Plan file not found: ${planPath}`);
  }

  const outputText = String(output || '');
  if (!outputText.includes(planRelativePath) && !outputText.includes(planPath)) {
    return fail(`Final response did not report plan location '${planRelativePath}'`);
  }

  const markdown = fs.readFileSync(planPath, 'utf8');
  const normalized = markdown.toLowerCase();
  if (/^##\s+Candidates\s*$/m.test(markdown)) {
    return fail('Do not wrap candidate sections in a separate ## Candidates section');
  }
  if (/^#{3,}\s+Candidate:/m.test(markdown)) {
    return fail('Candidate headings must use level-2 markdown: ## Candidate: <ID>');
  }
  if (!/^#\s+Refactor[ -]Mart\b.*\bPlan\b.*$/m.test(markdown)) {
    return fail('Plan title must be a top-level refactor-mart plan heading');
  }
  for (const heading of ['Targets', 'Assumptions', 'Non-Goals', 'Candidate Summary', 'Execution Order']) {
    if (!hasSection(markdown, heading)) {
      return fail(`Plan missing required top-level section '${heading}'`);
    }
  }
  const hasNoMutationStatement =
    /no dbt models\b[\s\S]{0,120}\b(were|have been) (created|edited|deleted|mutated|rewired)/i.test(markdown) ||
    /does not\b[\s\S]{0,80}\bmodify any dbt models/i.test(markdown) ||
    /does not\b[\s\S]{0,80}\b(create|edit|delete|mutate|rewire)[\s\S]{0,80}\bany dbt models/i.test(markdown) ||
    /did not\b[\s\S]{0,80}\bmutate any dbt models/i.test(markdown);
  if (!hasNoMutationStatement) {
    return fail('Plan missing explicit no-mutation statement');
  }

  const sections = candidateSections(markdown);
  if (sections.length === 0) {
    return fail('No candidate sections found');
  }

  const targetTables = normalizeTargetTables(context.vars.target_tables);
  if (targetTables.length > 0) {
    const targetsBody = sectionBody(markdown, 'Targets').toLowerCase();
    for (const target of targetTables) {
      if (!targetsBody.includes(target)) {
        return fail(`Targets section missing selected target '${target}'`);
      }
    }
  }

  const requiredFields = {
    approve: /^-\s+(?:\[[xX]\]\s+Approve:\s+yes|\[\s\]\s+Approve:\s+no)\s*$/m,
    type: /^-\s+Type:\s+(stg|int|mart)\s*$/m,
    output: /^-\s+Output:\s+\S.+$/m,
    dependsOn: /^-\s+Depends on:\s+.+$/m,
    validation: /^-\s+Validation:\s+.+$/m,
    executionStatus: /^-\s+Execution status:\s+planned\s*$/m,
  };
  for (const section of sections) {
    for (const [field, pattern] of Object.entries(requiredFields)) {
      if (!pattern.test(section.body)) {
        return fail(`Candidate ${section.id} missing or malformed field '${field}'`);
      }
    }
    const type = candidateType(section);
    const expectedPrefix = type && `${type.toUpperCase()}-`;
    if (!expectedPrefix || !section.id.startsWith(expectedPrefix)) {
      return fail(`Candidate ${section.id} prefix does not match Type: ${type}`);
    }
  }

  const expectedTypes = normalizeTerms(context.vars.expected_candidate_types);
  for (const type of expectedTypes) {
    if (!sections.some((section) => candidateType(section) === type)) {
      return fail(`Expected candidate type '${type}' not found`);
    }
  }

  if (targetTables.length > 0) {
    const martCount = sections.filter((section) => candidateType(section) === 'mart').length;
    if (martCount < targetTables.length) {
      return fail(`Expected at least one mart candidate per selected target (${targetTables.length}); found ${martCount}`);
    }
  }

  if (String(context.vars.expected_higher_layer_candidate || '').toLowerCase() === 'true') {
    const hasHigherLayer = sections.some((section) =>
      ['int', 'mart'].includes(candidateType(section)) &&
      /^-\s+Depends on:\s+.*\bSTG-\d+\b.*$/im.test(section.body),
    );
    if (!hasHigherLayer) {
      return fail('Expected higher-layer int or mart candidate depending on staging not found');
    }
  }

  for (const term of normalizeTerms(context.vars.expected_terms)) {
    if (!normalized.includes(term)) {
      return fail(`Expected term '${term}' not found`);
    }
  }

  for (const term of normalizeTerms(context.vars.expected_unapproved_terms)) {
    const matchingSection = sections.find((section) =>
      section.body.toLowerCase().includes(term),
    );
    if (!matchingSection) {
      return fail(`Expected unapproved candidate term '${term}' not found`);
    }
    if (!/^-\s+\[\s\]\s+Approve:\s+no\s*$/m.test(matchingSection.body)) {
      return fail(`Candidate term '${term}' was not left unapproved`);
    }
  }

  const expectedModels = snapshotDirectory(path.join(fixtureRoot, 'dbt', 'models'));
  const actualModels = snapshotDirectory(path.join(runRoot, 'dbt', 'models'));
  if (!sameSnapshot(expectedModels, actualModels)) {
    return fail('Planning workflow mutated files under dbt/models');
  }

  return {
    pass: true,
    score: 1,
    reason: `Plan contract found with ${sections.length} candidate sections`,
  };
};
