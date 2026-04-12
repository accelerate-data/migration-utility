const fs = require('fs');
const path = require('path');
const { resolveProjectPath } = require('./schema-helpers');

module.exports = (_output, context) => {
  const fixturePath = resolveProjectPath(context);
  const table = String(context.vars.target_table || '').toLowerCase();
  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const specPath = path.resolve(repoRoot, fixturePath, 'test-specs', `${table}.json`);

  if (!fs.existsSync(specPath)) {
    return { pass: false, score: 0, reason: `Spec not found at ${specPath}` };
  }

  let spec;
  try {
    spec = JSON.parse(fs.readFileSync(specPath, 'utf8'));
  } catch (error) {
    return { pass: false, score: 0, reason: `Failed to parse spec: ${error.message}` };
  }

  const testsByName = new Map((spec.unit_tests || []).map((test) => [String(test.name || ''), test]));
  const branchesById = new Map((spec.branch_manifest || []).map((branch) => [String(branch.id || ''), branch]));

  const premiumBranch = branchesById.get('if_premium_path');
  const standardBranch = branchesById.get('else_standard_path');

  if (!premiumBranch || !standardBranch) {
    return { pass: false, score: 0, reason: 'Expected if_premium_path and else_standard_path in branch_manifest' };
  }

  const premiumScenarios = premiumBranch.scenarios || [];
  const standardScenarios = standardBranch.scenarios || [];

  if (premiumScenarios.length === 0 || standardScenarios.length === 0) {
    return { pass: false, score: 0, reason: 'Expected both branches to have at least one covering scenario' };
  }

  const getPrices = (scenarioName) => {
    const test = testsByName.get(String(scenarioName || ''));
    if (!test) return null;
    const givenRows = (test.given || []).flatMap((block) => block.rows || []);
    const prices = givenRows
      .map((row) => Number(row.ListPrice))
      .filter((value) => !Number.isNaN(value));
    const categories = (test.expect?.rows || []).map((row) => String(row.PriceCategory || ''));
    return { prices, categories };
  };

  for (const scenarioName of premiumScenarios) {
    const data = getPrices(scenarioName);
    if (!data) {
      return { pass: false, score: 0, reason: `Premium scenario '${scenarioName}' not found in unit_tests` };
    }
    if (data.prices.length === 0 || !data.prices.every((price) => price > 100)) {
      return {
        pass: false,
        score: 0,
        reason: `Premium scenario '${scenarioName}' must use only ListPrice values > 100`
      };
    }
    if (data.categories.length === 0 || !data.categories.every((category) => category === 'Premium')) {
      return {
        pass: false,
        score: 0,
        reason: `Premium scenario '${scenarioName}' must expect only PriceCategory='Premium'`
      };
    }
  }

  for (const scenarioName of standardScenarios) {
    const data = getPrices(scenarioName);
    if (!data) {
      return { pass: false, score: 0, reason: `Standard scenario '${scenarioName}' not found in unit_tests` };
    }
    if (data.prices.length === 0 || !data.prices.every((price) => price <= 100)) {
      return {
        pass: false,
        score: 0,
        reason: `Standard scenario '${scenarioName}' must use only ListPrice values <= 100`
      };
    }
    if (data.categories.length === 0 || !data.categories.every((category) => category === 'Standard')) {
      return {
        pass: false,
        score: 0,
        reason: `Standard scenario '${scenarioName}' must expect only PriceCategory='Standard'`
      };
    }
  }

  return { pass: true, score: 1, reason: 'IfElseTarget branch semantics validated' };
};
