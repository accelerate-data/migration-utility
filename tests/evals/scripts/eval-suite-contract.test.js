const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const yaml = require('js-yaml');

const EVAL_ROOT = path.resolve(__dirname, '..');
const OPENCODE_CONFIG = 'opencode.json';
const PACKAGE_ROOT = path.join(EVAL_ROOT, 'packages');
const SCRIPT_ROOT = path.join(EVAL_ROOT, 'scripts');
const LIVE_CONFIGS = [
  'oracle-live/promptfooconfig.yaml',
  'mssql-live/promptfooconfig.yaml',
];
const ALLOWED_TIERS = new Set(['light', 'standard', 'high', 'x_high']);
const EXPECTED_AGENT_STEPS = {
  eval_light: 60,
  eval_standard: 100,
  eval_high: 120,
  eval_x_high: 200,
};
const FORBIDDEN_REFERENCES = [
  'file://../../providers/',
  'file://../providers/',
  'anthropic:claude-agent-sdk',
  'max_turns',
  'runtime.tools',
  'model_provider_id',
  'working_dir',
];

function collectPackageConfigs(rootDir) {
  const configs = [];

  for (const entry of fs.readdirSync(rootDir, { withFileTypes: true })) {
    const entryPath = path.join(rootDir, entry.name);
    if (entry.isDirectory()) {
      configs.push(...collectPackageConfigs(entryPath));
      continue;
    }

    if (entry.isFile() && entry.name.endsWith('.yaml')) {
      configs.push(path.relative(EVAL_ROOT, entryPath));
    }
  }

  return configs.sort();
}

function readYaml(relativePath) {
  return yaml.load(fs.readFileSync(path.join(EVAL_ROOT, relativePath), 'utf8'));
}

function readJson(relativePath) {
  return JSON.parse(fs.readFileSync(path.join(EVAL_ROOT, relativePath), 'utf8'));
}

function collectTextFiles(rootDir) {
  const textFiles = [];

  for (const entry of fs.readdirSync(rootDir, { withFileTypes: true })) {
    const entryPath = path.join(rootDir, entry.name);
    if (entry.isDirectory()) {
      if (entry.name === 'node_modules' || entry.name === '.tmp') {
        continue;
      }
      textFiles.push(...collectTextFiles(entryPath));
      continue;
    }

    if (entry.isFile()) {
      textFiles.push(path.relative(EVAL_ROOT, entryPath));
    }
  }

  return textFiles.sort();
}

function toUtf8(value) {
  return Buffer.from(value, 'utf8').toString('utf8');
}

test('all eval package and live configs declare a valid metadata.eval_tier', () => {
  const packageConfigs = collectPackageConfigs(PACKAGE_ROOT);
  const configPaths = [...packageConfigs, ...LIVE_CONFIGS];

  assert.ok(packageConfigs.length > 0, 'expected at least one package config');
  assert.deepEqual(
    LIVE_CONFIGS.filter((relativePath) => fs.existsSync(path.join(EVAL_ROOT, relativePath))),
    LIVE_CONFIGS,
  );

  for (const relativePath of configPaths) {
    const parsed = readYaml(relativePath);
    assert.ok(parsed?.metadata, `${relativePath} must define metadata`);
    assert.ok(parsed.metadata.eval_tier, `${relativePath} must define metadata.eval_tier`);
    assert.ok(
      ALLOWED_TIERS.has(parsed.metadata.eval_tier),
      `${relativePath} has unsupported eval tier: ${parsed.metadata.eval_tier}`,
    );
  }
});

test('eval suite no longer references provider files or the Claude agent sdk', () => {
  const textFiles = [
    ...collectTextFiles(PACKAGE_ROOT),
    ...LIVE_CONFIGS,
    path.relative(EVAL_ROOT, path.join(SCRIPT_ROOT, 'promptfoo.sh')),
    path.relative(EVAL_ROOT, path.join(SCRIPT_ROOT, 'run-promptfoo-with-guard.js')),
    path.relative(EVAL_ROOT, path.join(SCRIPT_ROOT, 'resolve-promptfoo-config.js')),
    'package.json',
  ].sort();

  for (const relativePath of textFiles) {
    const contents = fs.readFileSync(path.join(EVAL_ROOT, relativePath), 'utf8');
    for (const forbiddenReference of FORBIDDEN_REFERENCES) {
      assert.ok(
        !contents.includes(toUtf8(forbiddenReference)),
        `${relativePath} still references ${forbiddenReference}`,
      );
    }
  }
});

test('suite OpenCode config disables provider timeouts for long-running evals', () => {
  const config = readJson(OPENCODE_CONFIG);

  assert.equal(config.provider?.opencode?.options?.timeout, false);
  assert.equal('chunkTimeout' in config.provider.opencode.options, false);
});

test('suite OpenCode config defines primary eval agents with enforceable fields', () => {
  const config = readJson(OPENCODE_CONFIG);

  for (const [agentName, steps] of Object.entries(EXPECTED_AGENT_STEPS)) {
    const agent = config.agent?.[agentName];
    assert.ok(agent, `missing OpenCode eval agent: ${agentName}`);
    assert.equal(agent.description.length > 0, true);
    assert.equal(agent.mode, 'primary');
    assert.equal(agent.model, 'opencode/qwen3.6-plus');
    if (agent.temperature !== undefined) {
      assert.equal(agent.temperature, 0.1);
    }
    assert.equal(agent.steps, steps);
    assert.deepEqual(agent.permission, {
      read: 'allow',
      write: 'allow',
      edit: 'allow',
      bash: 'allow',
      grep: 'allow',
      glob: 'allow',
      list: 'allow',
      webfetch: 'deny',
    });
    assert.equal('tools' in agent, false, `${agentName} must use permission, not tools`);
  }
});
