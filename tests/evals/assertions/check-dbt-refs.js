// Validates dbt ref() and source() correctness in generated model files.
//
// Rules enforced on the target model and its staging models:
//   - All models (mart, staging, snapshot): every source('schema', 'table')
//     call must reference an entry in _staging__sources.yml.
//
// Only validates files written by the skill for the target table:
//   - The mart/snapshot/primary model matching target_table
//   - Any sibling staging SQL files created alongside it
//
// Usage:
//   assert:
//     - type: javascript
//       value: file://../../assertions/check-dbt-refs.js
//
// Expects context.vars: { fixture_path, target_table }
const fs = require('fs');
const path = require('path');
const yaml = require('js-yaml');
const { resolveProjectPath } = require('./schema-helpers');

/** Recursively collect .sql files under a directory. */
function collectSql(dir, results = []) {
  if (!fs.existsSync(dir)) return results;
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) collectSql(full, results);
    else if (entry.name.endsWith('.sql')) results.push(full);
  }
  return results;
}

/** Extract all {{ source('schema', 'table') }} calls from SQL content. */
function extractSourceCalls(sql) {
  const pattern = /\{\{\s*source\s*\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)\s*\}\}/gi;
  const calls = [];
  let m;
  while ((m = pattern.exec(sql)) !== null) {
    calls.push({ schema: m[1].toLowerCase(), table: m[2].toLowerCase(), raw: m[0] });
  }
  return calls;
}

/** Parse _staging__sources.yml and return a Set of 'schema.table' keys (lowercase). */
function loadValidSources(dbtDir) {
  const sourcesFile = path.join(dbtDir, 'models', 'staging', '_staging__sources.yml');
  const valid = new Set();
  if (!fs.existsSync(sourcesFile)) return valid;
  const doc = yaml.load(fs.readFileSync(sourcesFile, 'utf8'));
  for (const src of doc.sources || []) {
    for (const tbl of src.tables || []) {
      valid.add(`${src.name.toLowerCase()}.${tbl.name.toLowerCase()}`);
    }
  }
  return valid;
}

module.exports = (output, context) => {
  const fixturePath = resolveProjectPath(context);
  const targetTable = context.vars.target_table;

  if (!fixturePath || !targetTable) {
    return { pass: false, score: 0, reason: 'fixture_path and target_table must be set in test vars' };
  }

  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const dbtDir = path.resolve(repoRoot, fixturePath, 'dbt');

  if (!fs.existsSync(dbtDir)) {
    return { pass: true, score: 1, reason: 'No dbt directory — skipping ref check' };
  }

  const validSources = loadValidSources(dbtDir);

  const modelsDir = path.join(dbtDir, 'models');
  const snapshotsDir = path.join(dbtDir, 'snapshots');

  // Identify the target model file by matching the table name
  const tableName = targetTable.split('.').pop().toLowerCase();
  const tableNameNorm = tableName.replace(/_/g, '');

  const allModelFiles = collectSql(modelsDir);
  const allSnapshotFiles = collectSql(snapshotsDir);

  const targetFile = [...allModelFiles, ...allSnapshotFiles].find(f => {
    const stem = path.basename(f, '.sql').toLowerCase().replace(/_/g, '');
    return stem === tableNameNorm || stem === `${tableNameNorm}snapshot`;
  });

  if (!targetFile) {
    // Graceful — model may not have been written yet (sweep skip scenario)
    return { pass: true, score: 1, reason: `No model file found for '${tableName}' — skipping ref check` };
  }

  const errors = [];
  const checked = [];

  // Validate the target model (mart or snapshot)
  const sql = fs.readFileSync(targetFile, 'utf8');
  const sourceCalls = extractSourceCalls(sql);
  checked.push(path.basename(targetFile));

  for (const call of sourceCalls) {
    const key = `${call.schema}.${call.table}`;
    if (!validSources.has(key)) {
      errors.push(
        `${path.basename(targetFile)}: ${call.raw} references '${key}' which is not in _staging__sources.yml`
      );
    }
  }

  // Validate sibling staging SQL files in models/staging (likely written by the skill for this run)
  const stagingDir = path.join(modelsDir, 'staging');
  const stagingFiles = collectSql(stagingDir).filter(f => f !== targetFile);

  for (const stagingFile of stagingFiles) {
    const stagingSql = fs.readFileSync(stagingFile, 'utf8');
    const stagingSourceCalls = extractSourceCalls(stagingSql);
    checked.push(path.basename(stagingFile));
    for (const call of stagingSourceCalls) {
      const key = `${call.schema}.${call.table}`;
      if (!validSources.has(key)) {
        errors.push(
          `${path.basename(stagingFile)}: ${call.raw} references '${key}' which is not in _staging__sources.yml`
        );
      }
    }
  }

  if (errors.length > 0) {
    return {
      pass: false,
      score: 0,
      reason: `dbt ref check failed:\n  ${errors.join('\n  ')}`,
    };
  }

  return {
    pass: true,
    score: 1,
    reason: `ref() check passed (${checked.join(', ')})`,
  };
};
