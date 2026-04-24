const assert = require('node:assert/strict');
const test = require('node:test');

const {
  loadEvalTierConfig,
  resolveEvalTier,
} = require('./eval-tier-config');

test('loadEvalTierConfig returns required suite tiers', () => {
  const config = loadEvalTierConfig();

  assert.equal(config.runtime.providerId, 'opencode:sdk');
  assert.equal(config.runtime.model, 'qwen-3.6');
  assert.deepEqual(
    Object.keys(config.tiers).sort(),
    ['high', 'light', 'standard', 'x_high'],
  );
});

test('resolveEvalTier returns the expected max_turns', () => {
  const config = loadEvalTierConfig();

  assert.equal(resolveEvalTier(config, 'light').maxTurns, 60);
  assert.equal(resolveEvalTier(config, 'standard').maxTurns, 100);
  assert.equal(resolveEvalTier(config, 'high').maxTurns, 120);
  assert.equal(resolveEvalTier(config, 'x_high').maxTurns, 200);
});
