const fs = require('node:fs');
const path = require('node:path');
const { parse } = require('smol-toml');

const EVAL_ROOT = path.resolve(__dirname, '..');
const CONFIG_PATH = path.join(EVAL_ROOT, 'config', 'eval-tiers.toml');
const REQUIRED_TIERS = ['light', 'standard', 'high', 'x_high'];

function loadEvalTierConfig(configPath = CONFIG_PATH) {
  const parsed = parse(fs.readFileSync(configPath, 'utf8'));
  const runtime = parsed.runtime || {};
  const tiers = parsed.tiers || {};

  validateRuntime(runtime);

  for (const tier of REQUIRED_TIERS) {
    if (!tiers[tier] || typeof tiers[tier].max_turns !== 'number') {
      throw new Error(`Missing required eval tier: ${tier}`);
    }
  }

  return {
    runtime: {
      providerId: runtime.provider_id,
      model: runtime.model,
      baseUrl: runtime.base_url,
      workingDir: runtime.working_dir,
      tools: runtime.tools || {},
    },
    tiers: Object.fromEntries(
      Object.entries(tiers).map(([tierName, tier]) => [
        tierName,
        { tierName, maxTurns: tier.max_turns },
      ]),
    ),
  };
}

function resolveEvalTier(config, tierName) {
  const tier = config.tiers[tierName];
  if (!tier) {
    throw new Error(`Unknown eval tier: ${tierName}`);
  }

  return tier;
}

function validateRuntime(runtime) {
  for (const field of ['provider_id', 'model', 'base_url', 'working_dir']) {
    if (typeof runtime[field] !== 'string') {
      throw new Error(`Missing required eval runtime field: ${field}`);
    }
  }
}

module.exports = {
  CONFIG_PATH,
  REQUIRED_TIERS,
  loadEvalTierConfig,
  resolveEvalTier,
};
