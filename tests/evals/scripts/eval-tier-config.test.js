const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const {
  CONFIG_PATH,
  loadEvalTierConfig,
  resolveEvalTier,
} = require('./eval-tier-config');

test('loadEvalTierConfig returns required suite tiers', () => {
  const config = loadEvalTierConfig();

  assert.equal(config.runtime.providerId, 'opencode:sdk');
  assert.equal(config.runtime.model, 'qwen-3.6');
  assert.equal(config.runtime.baseUrl, 'http://127.0.0.1:4096');
  assert.equal(config.runtime.workingDir, path.resolve(path.dirname(CONFIG_PATH), '../..'));
  assert.deepEqual(config.runtime.tools, {
    read: true,
    write: true,
    edit: true,
    bash: true,
    grep: true,
    glob: true,
    list: true,
  });
  assert.deepEqual(
    Object.keys(config.tiers).sort(),
    ['high', 'light', 'standard', 'x_high'],
  );
  assert.deepEqual(config.tiers.light, { maxTurns: 60 });
  assert.deepEqual(config.tiers.standard, { maxTurns: 100 });
});

test('resolveEvalTier returns the expected max_turns', () => {
  const config = loadEvalTierConfig();

  assert.equal(resolveEvalTier(config, 'light').maxTurns, 60);
  assert.equal(resolveEvalTier(config, 'standard').maxTurns, 100);
  assert.equal(resolveEvalTier(config, 'high').maxTurns, 120);
  assert.equal(resolveEvalTier(config, 'x_high').maxTurns, 200);
});

test('resolveEvalTier rejects unknown tiers', () => {
  const config = loadEvalTierConfig();

  assert.throws(() => resolveEvalTier(config, 'missing'), /Unknown eval tier: missing/);
});

test('loadEvalTierConfig rejects missing runtime and tier fields', () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'eval-tier-config-'));
  try {
    const missingRuntimePath = path.join(tempRoot, 'missing-runtime.toml');
    fs.writeFileSync(missingRuntimePath, `
[runtime]
provider_id = "opencode:sdk"
model = "qwen-3.6"
base_url = "http://127.0.0.1:4096"

[tiers.light]
max_turns = 60

[tiers.standard]
max_turns = 100

[tiers.high]
max_turns = 120

[tiers.x_high]
max_turns = 200
`.trimStart(), 'utf8');

    assert.throws(
      () => loadEvalTierConfig(missingRuntimePath),
      /Missing required eval runtime field: working_dir/,
    );

    const missingTierPath = path.join(tempRoot, 'missing-tier.toml');
    fs.writeFileSync(missingTierPath, `
[runtime]
provider_id = "opencode:sdk"
model = "qwen-3.6"
base_url = "http://127.0.0.1:4096"
working_dir = "../.."

[runtime.tools]
read = true
write = true
edit = true
bash = true
grep = true
glob = true
list = true

[tiers.light]
max_turns = 60

[tiers.standard]
max_turns = 100

[tiers.high]
max_turns = 120
`.trimStart(), 'utf8');

    assert.throws(
      () => loadEvalTierConfig(missingTierPath),
      /Missing required eval tier: x_high/,
    );

    const malformedToolsPath = path.join(tempRoot, 'malformed-tools.toml');
    fs.writeFileSync(malformedToolsPath, `
[runtime]
provider_id = "opencode:sdk"
model = "qwen-3.6"
base_url = "http://127.0.0.1:4096"
working_dir = "../.."

[runtime.tools]
read = "yes"
write = true
edit = true
bash = true
grep = true
glob = true
list = true

[tiers.light]
max_turns = 60

[tiers.standard]
max_turns = 100

[tiers.high]
max_turns = 120

[tiers.x_high]
max_turns = 200
`.trimStart(), 'utf8');

    assert.throws(
      () => loadEvalTierConfig(malformedToolsPath),
      /Invalid eval runtime tools field: read/,
    );

    const missingToolPath = path.join(tempRoot, 'missing-tool.toml');
    fs.writeFileSync(missingToolPath, `
[runtime]
provider_id = "opencode:sdk"
model = "qwen-3.6"
base_url = "http://127.0.0.1:4096"
working_dir = "../.."

[runtime.tools]
read = true
write = true
edit = true
bash = true
grep = true
glob = true

[tiers.light]
max_turns = 60

[tiers.standard]
max_turns = 100

[tiers.high]
max_turns = 120

[tiers.x_high]
max_turns = 200
`.trimStart(), 'utf8');

    assert.throws(
      () => loadEvalTierConfig(missingToolPath),
      /Missing required eval runtime tools field: list/,
    );

    const extraTierPath = path.join(tempRoot, 'extra-tier.toml');
    fs.writeFileSync(extraTierPath, `
[runtime]
provider_id = "opencode:sdk"
model = "qwen-3.6"
base_url = "http://127.0.0.1:4096"
working_dir = "../.."

[runtime.tools]
read = true
write = true
edit = true
bash = true
grep = true
glob = true
list = true

[tiers.light]
max_turns = 60

[tiers.standard]
max_turns = 100

[tiers.high]
max_turns = 120

[tiers.x_high]
max_turns = 200

[tiers.overflow]
max_turns = "many"
`.trimStart(), 'utf8');

    assert.throws(
      () => loadEvalTierConfig(extraTierPath),
      /Invalid eval tier field: overflow/,
    );

    const negativeTurnsPath = path.join(tempRoot, 'negative-turns.toml');
    fs.writeFileSync(negativeTurnsPath, `
[runtime]
provider_id = "opencode:sdk"
model = "qwen-3.6"
base_url = "http://127.0.0.1:4096"
working_dir = "../.."

[runtime.tools]
read = true
write = true
edit = true
bash = true
grep = true
glob = true
list = true

[tiers.light]
max_turns = -1

[tiers.standard]
max_turns = 100

[tiers.high]
max_turns = 120

[tiers.x_high]
max_turns = 200
`.trimStart(), 'utf8');

    assert.throws(
      () => loadEvalTierConfig(negativeTurnsPath),
      /Invalid eval tier field: light/,
    );

    const fractionalTurnsPath = path.join(tempRoot, 'fractional-turns.toml');
    fs.writeFileSync(fractionalTurnsPath, `
[runtime]
provider_id = "opencode:sdk"
model = "qwen-3.6"
base_url = "http://127.0.0.1:4096"
working_dir = "../.."

[runtime.tools]
read = true
write = true
edit = true
bash = true
grep = true
glob = true
list = true

[tiers.light]
max_turns = 60.5

[tiers.standard]
max_turns = 100

[tiers.high]
max_turns = 120

[tiers.x_high]
max_turns = 200
`.trimStart(), 'utf8');

    assert.throws(
      () => loadEvalTierConfig(fractionalTurnsPath),
      /Invalid eval tier field: light/,
    );

    const extraToolPath = path.join(tempRoot, 'extra-tool.toml');
    fs.writeFileSync(extraToolPath, `
[runtime]
provider_id = "opencode:sdk"
model = "qwen-3.6"
base_url = "http://127.0.0.1:4096"
working_dir = "../.."

[runtime.tools]
read = true
write = true
edit = true
bash = true
grep = true
glob = true
list = true
execute = true

[tiers.light]
max_turns = 60

[tiers.standard]
max_turns = 100

[tiers.high]
max_turns = 120

[tiers.x_high]
max_turns = 200
`.trimStart(), 'utf8');

    assert.throws(
      () => loadEvalTierConfig(extraToolPath),
      /Unexpected eval runtime tools field: execute/,
    );
  } finally {
    fs.rmSync(tempRoot, { recursive: true, force: true });
  }
});
