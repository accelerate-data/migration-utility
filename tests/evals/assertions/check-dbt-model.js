// Validates that dbt model artifacts were written.
// Usage: type: javascript, value: file://../../assertions/check-dbt-model.js
// Expects context.vars:
// {
//   fixture_path,
//   target_table,
//   require_config?,
//   expected_model_path?,
//   expected_model_terms?,
//   forbidden_model_terms?,
//   expected_output_terms?,
//   expected_yaml_terms?,
//   forbidden_yaml_terms?,
//   expected_stg_files?,
//   expected_stg_terms?,
//   forbidden_stg_terms?,
//   graceful_no_model?,
//   expect_no_generated_model?
// }
const fs = require('fs');
const path = require('path');
const { normalizeTerms, resolveProjectPath } = require('./schema-helpers');

module.exports = (output, context) => {
  const fixturePath = resolveProjectPath(context);
  const table = context.vars.target_table;
  const requireConfig = String(context.vars.require_config || '').toLowerCase() === 'true';
  const expectedModelPath = String(context.vars.expected_model_path || '').trim();
  const expectedModelTerms = normalizeTerms(context.vars.expected_model_terms);
  const forbiddenModelTermsRaw = String(context.vars.forbidden_model_terms || '')
    .split(',')
    .map((term) => term.trim())
    .filter(Boolean);
  const expectedOutputTerms = normalizeTerms(context.vars.expected_output_terms);
  const expectedYamlTerms = normalizeTerms(context.vars.expected_yaml_terms);
  const forbiddenYamlTerms = normalizeTerms(context.vars.forbidden_yaml_terms);
  const gracefulNoModel = String(context.vars.graceful_no_model || '').toLowerCase() === 'true';
  const expectNoGeneratedModel = String(context.vars.expect_no_generated_model || '').toLowerCase() === 'true';
  const expectedStgFiles = normalizeTerms(context.vars.expected_stg_files);
  const expectedStgTerms = normalizeTerms(context.vars.expected_stg_terms);
  const forbiddenStgTerms = normalizeTerms(context.vars.forbidden_stg_terms);

  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const dbtDir = path.resolve(repoRoot, fixturePath, 'dbt');
  const outputStr = String(output || '').toLowerCase();

  const assertExpectedOutputTerms = () => {
    for (const term of expectedOutputTerms) {
      if (!outputStr.includes(term)) {
        return { pass: false, score: 0, reason: `Expected output term '${term}' not found in final response` };
      }
    }
    return null;
  };

  if (!fs.existsSync(dbtDir)) {
    if (gracefulNoModel || expectNoGeneratedModel) {
      const failure = assertExpectedOutputTerms();
      return failure || { pass: true, score: 1, reason: 'Graceful no-model response accepted (no dbt project present)' };
    }
    // Check if output text contains model SQL as fallback
    if (outputStr.includes('config(') && outputStr.includes('select')) {
      return { pass: true, score: 1, reason: 'dbt model SQL found in output text (no dbt project to write to)' };
    }
    return { pass: false, score: 0, reason: `dbt directory not found at ${dbtDir} and no model SQL in output` };
  }

  // Look for model files in the dbt directory
  const modelsDir = path.resolve(dbtDir, 'models');
  if (!fs.existsSync(modelsDir)) {
    if (gracefulNoModel || expectNoGeneratedModel) {
      const failure = assertExpectedOutputTerms();
      return failure || { pass: true, score: 1, reason: 'Graceful no-model response accepted (no models dir present)' };
    }
    if (outputStr.includes('config(')) {
      return { pass: true, score: 1, reason: 'dbt model SQL found in output (models dir not yet created)' };
    }
    return { pass: false, score: 0, reason: 'No models directory found in dbt project' };
  }

  // Find SQL files that might match the table
  const tableName = table.split('.').pop().toLowerCase();
  const allFiles = [];
  const walkDir = (dir) => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      if (entry.isDirectory()) walkDir(path.join(dir, entry.name));
      else if (entry.name.endsWith('.sql')) allFiles.push(path.join(dir, entry.name));
    }
  };
  walkDir(modelsDir);

  const snapshotsDir = path.resolve(dbtDir, 'snapshots');
  if (fs.existsSync(snapshotsDir)) walkDir(snapshotsDir);

  const tableNameNorm = tableName.replace(/_/g, '');
  const matchingFiles = allFiles.filter(f => {
    const fNorm = f.toLowerCase().replace(/_/g, '');
    return fNorm.includes(tableNameNorm);
  });

  const generatedTargetMatches = matchingFiles.filter(f => {
    const relativePath = path.relative(modelsDir, f).split(path.sep).join('/');
    return !relativePath.startsWith('staging/stg_bronze__');
  });

  if (expectNoGeneratedModel) {
    const failure = assertExpectedOutputTerms();
    if (failure) {
      return failure;
    }
    if (generatedTargetMatches.length > 0) {
      return {
        pass: false,
        score: 0,
        reason: `Expected no generated target model for '${tableName}', found ${generatedTargetMatches.map(f => path.basename(f)).join(', ')}`,
      };
    }
    return { pass: true, score: 1, reason: `No generated target model found for '${tableName}'` };
  }

  if (matchingFiles.length === 0) {
    if (gracefulNoModel) {
      const failure = assertExpectedOutputTerms();
      return failure || { pass: true, score: 1, reason: `Graceful no-model response accepted for '${tableName}'` };
    }
    if (outputStr.includes('config(')) {
      return { pass: true, score: 1, reason: 'dbt model SQL found in output (no matching file written yet)' };
    }
    return { pass: false, score: 0, reason: `No SQL file matching '${tableName}' found in ${modelsDir}` };
  }

  let modelFile = matchingFiles[0];
  if (expectedModelPath) {
    const expectedModelFile = path.resolve(dbtDir, expectedModelPath);
    if (!fs.existsSync(expectedModelFile)) {
      return { pass: false, score: 0, reason: `Expected model path '${expectedModelPath}' not found at ${expectedModelFile}` };
    }
    modelFile = expectedModelFile;
  } else {
    return {
      pass: false,
      score: 0,
      reason: "expected_model_path must be set for generated model checks",
    };
  }

  const modelContent = fs.readFileSync(modelFile, 'utf8');
  if (requireConfig && !modelContent.includes('config(')) {
    return { pass: false, score: 0, reason: `Model file ${modelFile} missing config() block` };
  }

  const rawModel = modelContent;
  const normalizedModel = modelContent.toLowerCase();
  for (const term of expectedModelTerms) {
    if (!normalizedModel.includes(term)) {
      return { pass: false, score: 0, reason: `Expected model term '${term}' not found in ${path.basename(modelFile)}` };
    }
  }

  for (const term of forbiddenModelTermsRaw) {
    const hasUppercase = term !== term.toLowerCase();
    const haystack = hasUppercase ? rawModel : normalizedModel;
    const needle = hasUppercase ? term : term.toLowerCase();
    const compactHaystack = haystack.replace(/\s+/g, '');
    const compactNeedle = needle.replace(/\s+/g, '');
    if (haystack.includes(needle) || (compactNeedle && compactHaystack.includes(compactNeedle))) {
      return { pass: false, score: 0, reason: `Forbidden model term '${term}' found in ${path.basename(modelFile)}` };
    }
  }

  if (expectedOutputTerms.length > 0) {
    const failure = assertExpectedOutputTerms();
    if (failure) {
      return failure;
    }
  }

  if (expectedYamlTerms.length > 0 || forbiddenYamlTerms.length > 0) {
    const allYamlFiles = [];
    const walkYamlDir = (dir) => {
      for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
        if (entry.isDirectory()) walkYamlDir(path.join(dir, entry.name));
        else if (entry.name.endsWith('.yml') || entry.name.endsWith('.yaml')) allYamlFiles.push(path.join(dir, entry.name));
      }
    };
    walkYamlDir(modelsDir);
    if (fs.existsSync(snapshotsDir)) walkYamlDir(snapshotsDir);

    const matchingYamlFiles = allYamlFiles.filter(f => {
      const fNorm = f.toLowerCase().replace(/_/g, '');
      if (fNorm.includes(tableNameNorm)) return true;
      const yamlContent = fs.readFileSync(f, 'utf8').toLowerCase().replace(/_/g, '');
      return yamlContent.includes(`name: ${tableNameNorm}`);
    });

    if (matchingYamlFiles.length === 0) {
      return { pass: false, score: 0, reason: `No schema YAML matching '${tableName}' found in ${modelsDir}` };
    }

    const yamlContent = fs.readFileSync(matchingYamlFiles[0], 'utf8').toLowerCase();
    for (const term of expectedYamlTerms) {
      if (!yamlContent.includes(term)) {
        return { pass: false, score: 0, reason: `Expected YAML term '${term}' not found in ${path.basename(matchingYamlFiles[0])}` };
      }
    }
    for (const term of forbiddenYamlTerms) {
      if (yamlContent.includes(term)) {
        return { pass: false, score: 0, reason: `Forbidden YAML term '${term}' found in ${path.basename(matchingYamlFiles[0])}` };
      }
    }
  }

  for (const stgName of expectedStgFiles) {
    const stgPath = path.resolve(dbtDir, 'models', 'staging', `${stgName}.sql`);
    if (!fs.existsSync(stgPath)) {
      return { pass: false, score: 0, reason: `Expected staging file '${stgName}.sql' not found at ${stgPath}` };
    }

    if (expectedStgTerms.length > 0 || forbiddenStgTerms.length > 0) {
      const stgContent = fs.readFileSync(stgPath, 'utf8').toLowerCase();
      for (const term of expectedStgTerms) {
        if (!stgContent.includes(term)) {
          return { pass: false, score: 0, reason: `Expected staging term '${term}' not found in ${stgName}.sql` };
        }
      }
      for (const term of forbiddenStgTerms) {
        if (stgContent.includes(term)) {
          return { pass: false, score: 0, reason: `Forbidden staging term '${term}' found in ${stgName}.sql` };
        }
      }
    }
  }

  return { pass: true, score: 1, reason: `dbt model written: ${path.basename(modelFile)}` };
};
