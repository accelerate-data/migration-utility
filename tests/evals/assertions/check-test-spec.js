const fs = require('fs');
const path = require('path');
const { validateSchema, extractJsonObject, normalizeTerms } = require('./schema-helpers');

module.exports = (output, context) => {
  const fixturePath = context.vars.fixture_path;
  const table = String(context.vars.target_table || '').toLowerCase();
  const expectedStatus = context.vars.expected_status;
  const expectedCoverage = context.vars.expected_coverage;
  const minBranchCount = Number(context.vars.min_branch_count || 0);
  const minScenarioCount = Number(context.vars.min_scenario_count || 0);
  const expectedWarnings = normalizeTerms(context.vars.expected_warning_terms);
  const expectedErrors = normalizeTerms(context.vars.expected_error_terms);
  const expectedOutputTerms = normalizeTerms(context.vars.expected_output_terms);

  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const specPath = path.resolve(repoRoot, fixturePath, 'test-specs', `${table}.json`);

  let spec;
  if (fs.existsSync(specPath)) {
    try {
      spec = JSON.parse(fs.readFileSync(specPath, 'utf8'));
    } catch (error) {
      return { pass: false, score: 0, reason: `Failed to parse test spec: ${error.message}` };
    }
  } else {
    try {
      spec = extractJsonObject(output);
    } catch (error) {
      return { pass: false, score: 0, reason: error.message };
    }
  }

  // Normalize common LLM output quirks before schema validation
  if (Array.isArray(spec.uncovered_branches)) {
    spec.uncovered_branches = spec.uncovered_branches.map((b) =>
      typeof b === 'object' && b !== null ? (b.id || b.branch_id || JSON.stringify(b)) : String(b)
    );
  }
  if (Array.isArray(spec.branch_manifest)) {
    for (const branch of spec.branch_manifest) {
      if (branch.statement_index !== undefined && typeof branch.statement_index !== 'number') {
        const parsed = Number(branch.statement_index);
        if (!isNaN(parsed)) branch.statement_index = Math.round(parsed);
      }
    }
  }

  // Schema validation gate
  const schemaResult = validateSchema(spec, 'test_spec.json');
  if (!schemaResult.valid) {
    return { pass: false, score: 0, reason: `Test spec schema validation failed: ${schemaResult.errors}` };
  }

  // Cross-artifact: item_id should match target_table
  if (spec.item_id && table) {
    const specItem = spec.item_id.toLowerCase();
    const tableNorm = table.replace(/^[^.]+\./, '');
    const specItemShort = specItem.replace(/^[^.]+\./, '');
    if (specItemShort !== tableNorm && specItem !== table) {
      return {
        pass: false,
        score: 0,
        reason: `Cross-artifact mismatch: spec.item_id='${spec.item_id}' vs target_table='${context.vars.target_table}'`
      };
    }
  }

  // Cross-artifact: branch_manifest scenarios should reference actual unit_test names
  if (Array.isArray(spec.branch_manifest) && Array.isArray(spec.unit_tests)) {
    const testNames = new Set(spec.unit_tests.map((t) => t.name));
    for (const branch of spec.branch_manifest) {
      for (const scenario of branch.scenarios || []) {
        if (!testNames.has(scenario)) {
          return {
            pass: false,
            score: 0,
            reason: `Cross-artifact mismatch: branch '${branch.id}' references scenario '${scenario}' not found in unit_tests`
          };
        }
      }
    }
  }

  if (expectedStatus) {
    const validStatuses = normalizeTerms(expectedStatus);
    if (!validStatuses.includes((spec.status || '').toLowerCase())) {
      return { pass: false, score: 0, reason: `Expected status in [${validStatuses.join(', ')}], got '${spec.status}'` };
    }
  }

  if (expectedCoverage) {
    const validCoverages = normalizeTerms(expectedCoverage);
    if (!validCoverages.includes((spec.coverage || '').toLowerCase())) {
      return { pass: false, score: 0, reason: `Expected coverage in [${validCoverages.join(', ')}], got '${spec.coverage}'` };
    }
  }

  const branchCount = Array.isArray(spec.branch_manifest) ? spec.branch_manifest.length : 0;
  if (branchCount < minBranchCount) {
    return { pass: false, score: 0, reason: `Expected at least ${minBranchCount} branches, got ${branchCount}` };
  }

  const scenarioCount = Array.isArray(spec.unit_tests) ? spec.unit_tests.length : 0;
  if (scenarioCount < minScenarioCount) {
    return { pass: false, score: 0, reason: `Expected at least ${minScenarioCount} scenarios, got ${scenarioCount}` };
  }

  const specText = JSON.stringify(spec).toLowerCase();

  // Search warnings[].message specifically to avoid synonym-variation false negatives
  const warningMessages = (spec.warnings || []).map((w) => (w.message || '').toLowerCase()).join(' ');
  const warningText = JSON.stringify(spec.warnings || []).toLowerCase();
  for (const term of expectedWarnings) {
    if (!warningMessages.includes(term) && !warningText.includes(term)) {
      return { pass: false, score: 0, reason: `Expected warning term '${term}' not found in spec warnings` };
    }
  }

  // Search errors[].message specifically
  const errorMessages = (spec.errors || []).map((e) => (e.message || '').toLowerCase()).join(' ');
  const errorText = JSON.stringify(spec.errors || []).toLowerCase();
  for (const term of expectedErrors) {
    if (!errorMessages.includes(term) && !errorText.includes(term)) {
      return { pass: false, score: 0, reason: `Expected error term '${term}' not found in spec errors` };
    }
  }

  for (const term of expectedOutputTerms) {
    if (!specText.includes(term)) {
      return { pass: false, score: 0, reason: `Expected term '${term}' not found in spec artifact` };
    }
  }

  return {
    pass: true,
    score: 1,
    reason: `Test spec validated with ${branchCount} branches and ${scenarioCount} scenarios`
  };
};
