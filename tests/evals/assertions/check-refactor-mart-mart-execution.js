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
  return match ? match[1].trim() : null;
}

function normalizedFieldValue(section, field) {
  return fieldValue(section, field)?.toLowerCase() || null;
}

function parseExpectedDetailPairs(value) {
  if (!value) {
    return [];
  }

  return String(value)
    .split(/,\s*(?=(?:stg|int|mart)-\d+=)/i)
    .map((entry) => entry.trim().toLowerCase())
    .filter(Boolean)
    .map((entry) => {
      const separatorIndex = entry.indexOf('=');
      if (separatorIndex <= 0 || separatorIndex === entry.length - 1) {
        throw new Error(`Invalid expected candidate detail '${entry}'`);
      }

      const detailSpec = entry.slice(separatorIndex + 1).trim();
      return {
        candidateId: entry.slice(0, separatorIndex).toUpperCase(),
        detail: detailSpec,
        detailGroups: detailSpec.includes('|')
          ? detailSpec
              .split('||')
              .map((group) => group.trim())
              .filter(Boolean)
              .map((group) => group.split('|').map((term) => term.trim()).filter(Boolean))
          : null,
      };
    });
}

function parseExpectedStatuses(value) {
  return normalizeTerms(value).map((entry) => {
    const [candidateId, status, extra] = entry.split(':');
    if (!candidateId || !status || extra !== undefined) {
      throw new Error(`Invalid expected candidate status '${entry}'`);
    }
    return { candidateId: candidateId.toUpperCase(), status };
  });
}

function parseExpectedPairs(value) {
  return normalizeTerms(value).map((entry) => {
    const [consumer, model, extra] = entry.split(':');
    if (!consumer || !model || extra !== undefined) {
      throw new Error(`Invalid expected model ref '${entry}'`);
    }
    return { consumer, model };
  });
}

function listSqlFiles(root) {
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
      if (entry.isFile() && entry.name.endsWith('.sql')) {
        files.push(fullPath);
      }
    }
  };

  visit(root);
  return files;
}

function modelNameFromOutput(output) {
  const normalized = output.replace(/\\/g, '/');
  return path.basename(normalized, '.sql').toLowerCase();
}

function findModelFile(runRoot, modelName) {
  const expectedFileName = `${modelName.toLowerCase()}.sql`;
  return listSqlFiles(path.join(runRoot, 'dbt', 'models')).find(
    (filePath) => path.basename(filePath).toLowerCase() === expectedFileName,
  );
}

function modelOrPathFile(root, modelOrPath) {
  const normalized = modelOrPath.replace(/\\/g, '/');
  if (normalized.endsWith('.sql') || normalized.includes('/')) {
    return path.resolve(root, normalized);
  }
  return findModelFile(root, normalized.toLowerCase());
}

function outputFilePath(runRoot, output) {
  const normalized = output.replace(/\\/g, '/');
  if (normalized.endsWith('.sql')) {
    return path.join(runRoot, normalized);
  }
  return findModelFile(runRoot, normalized.toLowerCase());
}

function isUnder(relativePath, prefix) {
  const normalized = relativePath.replace(/\\/g, '/').toLowerCase();
  return normalized.startsWith(prefix);
}

function verifyHigherLayerOutput(runRoot, section, type) {
  const output = fieldValue(section, 'Output');
  if (!output || output.toLowerCase() === 'missing') {
    return fail(`Candidate ${section.id} missing ${type} Output`);
  }

  const outputFile = outputFilePath(runRoot, output);
  if (!outputFile || !fs.existsSync(outputFile)) {
    return fail(`Candidate ${section.id} Output file not found: ${output}`);
  }

  const modelName = modelNameFromOutput(output);
  const relativeOutput = path.relative(runRoot, outputFile);
  if (type === 'int') {
    const validIntermediate = modelName.startsWith('int_') &&
      isUnder(relativeOutput, 'dbt/models/intermediate/');
    if (!validIntermediate) {
      return fail(`Candidate ${section.id} Output is not an intermediate model: ${output}`);
    }
  }

  if (type === 'mart') {
    const validMart = ['fct_', 'dim_', 'mart_'].some((prefix) => modelName.startsWith(prefix)) &&
      isUnder(relativeOutput, 'dbt/models/marts/');
    if (!validMart) {
      return fail(`Candidate ${section.id} Output is not a mart model: ${output}`);
    }
  }

  return null;
}

function stripSqlComments(sql) {
  return sql
    .replace(/\/\*[\s\S]*?\*\//g, '')
    .replace(/--.*$/gm, '');
}

function hasRef(sql, modelName) {
  const escapedModel = modelName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(`ref\\(\\s*['"]${escapedModel}['"]\\s*\\)`, 'i').test(stripSqlComments(sql));
}

function countBullets(section, field) {
  const escapedField = field.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const matches = section.body.match(new RegExp(`^-\\s+${escapedField}:\\s+\\S.+$`, 'gm'));
  return matches ? matches.length : 0;
}

module.exports = (output, context) => {
  let expectedStatuses;
  let expectedRefs;
  let expectedAbsentRefs;
  let expectedUnchangedModels;
  let expectedValidationDetails;
  let expectedBlockedReasonDetails;
  try {
    expectedStatuses = parseExpectedStatuses(context.vars.expected_candidate_statuses);
    expectedRefs = parseExpectedPairs(context.vars.expected_model_refs);
    expectedAbsentRefs = parseExpectedPairs(context.vars.expected_absent_model_refs);
    expectedUnchangedModels = normalizeTerms(context.vars.expected_unchanged_models);
    expectedValidationDetails = parseExpectedDetailPairs(context.vars.expected_validation_result_details);
    expectedBlockedReasonDetails = parseExpectedDetailPairs(context.vars.expected_blocked_reason_details);
  } catch (error) {
    return fail(error.message);
  }

  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const runRoot = path.resolve(repoRoot, resolveProjectPath(context));
  const fixtureRoot = context.vars.fixture_path
    ? path.resolve(repoRoot, context.vars.fixture_path)
    : null;
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

  const expectedCandidateIds = new Set(expectedStatuses.map((expected) => expected.candidateId));
  const seenCandidateIds = new Set();
  for (const section of sections) {
    if (seenCandidateIds.has(section.id)) {
      return fail(`Duplicate candidate ID found: ${section.id}`);
    }
    seenCandidateIds.add(section.id);
    if (!expectedCandidateIds.has(section.id)) {
      return fail(`Unexpected candidate ID found: ${section.id}`);
    }
  }

  for (const expected of expectedStatuses) {
    const section = sections.find((candidate) => candidate.id === expected.candidateId);
    if (!section) {
      return fail(`Candidate ${expected.candidateId} not found`);
    }

    const actualStatus = normalizedFieldValue(section, 'Execution status');
    if (actualStatus !== expected.status) {
      return fail(
        `Candidate ${expected.candidateId} expected status '${expected.status}', found '${actualStatus}'`,
      );
    }

    const type = normalizedFieldValue(section, 'Type');
    if ((type === 'int' || type === 'mart') && actualStatus === 'applied') {
      const outputFailure = verifyHigherLayerOutput(runRoot, section, type);
      if (outputFailure) {
        return outputFailure;
      }
    }
  }

  for (const expected of expectedValidationDetails) {
    const section = sections.find((candidate) => candidate.id === expected.candidateId);
    if (!section) {
      return fail(`Candidate ${expected.candidateId} not found`);
    }
    if (countBullets(section, 'Validation result') !== 1) {
      return fail(`Candidate ${expected.candidateId} missing Validation result`);
    }
    const actualDetail = normalizedFieldValue(section, 'Validation result');
    const detailMatches = expected.detailGroups
      ? expected.detailGroups.some((group) => group.every((term) => actualDetail.includes(term)))
      : actualDetail === expected.detail;
    if (!detailMatches) {
      return fail(
        `Candidate ${expected.candidateId} expected Validation result '${expected.detail}', found '${actualDetail}'`,
      );
    }
  }

  for (const expected of expectedBlockedReasonDetails) {
    const section = sections.find((candidate) => candidate.id === expected.candidateId);
    if (!section) {
      return fail(`Candidate ${expected.candidateId} not found`);
    }
    if (countBullets(section, 'Blocked reason') !== 1) {
      return fail(`Candidate ${expected.candidateId} missing Blocked reason`);
    }
    const actualDetail = normalizedFieldValue(section, 'Blocked reason');
    const detailMatches = expected.detailGroups
      ? expected.detailGroups.some((group) => group.every((term) => actualDetail.includes(term)))
      : actualDetail === expected.detail;
    if (!detailMatches) {
      return fail(
        `Candidate ${expected.candidateId} expected Blocked reason '${expected.detail}', found '${actualDetail}'`,
      );
    }
  }

  for (const expected of expectedRefs) {
    const consumerFile = findModelFile(runRoot, expected.consumer);
    if (!consumerFile) {
      return fail(`Expected consumer model not found: ${expected.consumer}`);
    }
    const consumerSql = fs.readFileSync(consumerFile, 'utf8');
    if (!hasRef(consumerSql, expected.model)) {
      return fail(
        `Expected consumer ${expected.consumer} to reference ${expected.model}`,
      );
    }
  }

  for (const expected of expectedAbsentRefs) {
    const consumerFile = findModelFile(runRoot, expected.consumer);
    if (!consumerFile) {
      return fail(`Expected consumer model not found: ${expected.consumer}`);
    }
    const consumerSql = fs.readFileSync(consumerFile, 'utf8');
    if (hasRef(consumerSql, expected.model)) {
      return fail(
        `Expected consumer ${expected.consumer} not to reference ${expected.model}`,
      );
    }
  }

  for (const modelOrPath of expectedUnchangedModels) {
    if (!fixtureRoot) {
      return fail('expected_unchanged_models requires fixture_path');
    }
    const runFile = modelOrPathFile(runRoot, modelOrPath);
    if (!runFile || !fs.existsSync(runFile)) {
      return fail(`Expected unchanged run model not found: ${modelOrPath}`);
    }
    const fixtureFile = modelOrPathFile(fixtureRoot, modelOrPath);
    if (!fixtureFile || !fs.existsSync(fixtureFile)) {
      return fail(`Expected unchanged fixture model not found: ${modelOrPath}`);
    }
    if (fs.readFileSync(runFile, 'utf8') !== fs.readFileSync(fixtureFile, 'utf8')) {
      return fail(`Expected model ${modelOrPath} to remain unchanged`);
    }
  }

  return { pass: true, score: 1, reason: 'Higher-layer execution plan status matched expectations' };
};
