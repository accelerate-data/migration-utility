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

function parseExpectedPairs(value) {
  return normalizeTerms(value).map((entry) => {
    const [left, right] = entry.split(':');
    return { left, right };
  });
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

function findModelFile(runRoot, modelName) {
  const expectedFileName = `${modelName}.sql`;
  return listSqlFiles(path.join(runRoot, 'dbt', 'models')).find(
    (filePath) => path.basename(filePath).toLowerCase() === expectedFileName,
  );
}

function hasRef(sql, modelName) {
  const escapedModel = modelName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(`ref\\(\\s*['"]${escapedModel}['"]\\s*\\)`, 'i').test(sql);
}

function verifyStagingOutput(runRoot, section) {
  const outputPath = fieldValue(section, 'Output');
  if (!outputPath || outputPath === 'missing') {
    return fail(`Candidate ${section.id} missing staging Output path`);
  }

  const outputFile = path.join(runRoot, outputPath);
  const outputName = path.basename(outputPath);
  if (!/^stg_.*\.sql$/i.test(outputName)) {
    return fail(`Candidate ${section.id} Output is not a stg_ model: ${outputPath}`);
  }
  if (!fs.existsSync(outputFile)) {
    return fail(`Candidate ${section.id} Output file not found: ${outputPath}`);
  }

  return null;
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

  const expectedValidationDetails = parseExpectedDetailPairs(context.vars.expected_validation_result_details);
  const expectedBlockedReasonDetails = parseExpectedDetailPairs(context.vars.expected_blocked_reason_details);

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
    const type = fieldValue(section, 'Type');
    if (type === 'stg' && (actualStatus === 'applied' || actualStatus === 'failed')) {
      const outputFailure = verifyStagingOutput(runRoot, section);
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
    if (!/^- Validation result:\s+\S.+$/m.test(section.body)) {
      return fail(`Candidate ${expected.candidateId} missing Validation result`);
    }
    const actualDetail = fieldValue(section, 'Validation result');
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
    if (!/^- Blocked reason:\s+\S.+$/m.test(section.body)) {
      return fail(`Candidate ${expected.candidateId} missing Blocked reason`);
    }
    const actualDetail = fieldValue(section, 'Blocked reason');
    const detailMatches = expected.detailGroups
      ? expected.detailGroups.some((group) => group.every((term) => actualDetail.includes(term)))
      : actualDetail === expected.detail;
    if (!detailMatches) {
      return fail(
        `Candidate ${expected.candidateId} expected Blocked reason '${expected.detail}', found '${actualDetail}'`,
      );
    }
  }

  for (const expected of parseExpectedPairs(context.vars.expected_consumer_refs)) {
    const consumerFile = findModelFile(runRoot, expected.left);
    if (!consumerFile) {
      return fail(`Expected consumer model not found: ${expected.left}`);
    }
    const consumerSql = fs.readFileSync(consumerFile, 'utf8');
    if (!hasRef(consumerSql, expected.right)) {
      return fail(
        `Expected consumer ${expected.left} to reference ${expected.right}`,
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
