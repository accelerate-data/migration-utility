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
  const candidateHeading = /^## Candidate:\s+([A-Z]+-\d+)\s*$/gm;
  const sections = [];
  let match;

  while ((match = candidateHeading.exec(markdown)) !== null) {
    const bodyStart = candidateHeading.lastIndex;
    const nextMatch = /^## Candidate:\s+[A-Z]+-\d+\s*$/gm;
    nextMatch.lastIndex = bodyStart;
    const next = nextMatch.exec(markdown);
    sections.push({
      id: match[1],
      body: markdown.slice(bodyStart, next ? next.index : markdown.length).trim(),
    });
  }

  return sections;
}

module.exports = (output, context) => {
  const projectPath = resolveProjectPath(context);
  const fixturePath = context.vars.fixture_path;
  const planName = context.vars.plan_name;
  if (!planName) {
    return fail('plan_name var is required');
  }

  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const runRoot = resolveUnderRepo(repoRoot, projectPath);
  const fixtureRoot = resolveUnderRepo(repoRoot, fixturePath);
  const planRelativePath = path.join('docs', 'design', `${planName}.md`);
  const planPath = path.join(runRoot, planRelativePath);
  if (!fs.existsSync(planPath)) {
    return fail(`Plan file not found: ${planPath}`);
  }

  const outputText = String(output || '');
  if (!outputText.includes(planRelativePath) && !outputText.includes(planPath)) {
    return fail(`Final response did not report plan location '${planRelativePath}'`);
  }

  const markdown = fs.readFileSync(planPath, 'utf8');
  const normalized = markdown.toLowerCase();
  if (/^#{3,}\s+Candidate:/m.test(markdown)) {
    return fail('Candidate headings must use level-2 markdown: ## Candidate: <ID>');
  }

  const sections = candidateSections(markdown);
  if (sections.length === 0) {
    return fail('No candidate sections found');
  }

  const requiredFields = {
    approve: /^-\s+\[[ xX]\]\s+Approve:\s+(yes|no)\s*$/m,
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
  }

  const expectedTypes = normalizeTerms(context.vars.expected_candidate_types);
  for (const type of expectedTypes) {
    if (!normalized.includes(`type: ${type}`)) {
      return fail(`Expected candidate type '${type}' not found`);
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
