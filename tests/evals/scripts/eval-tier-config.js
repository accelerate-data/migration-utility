const fs = require('node:fs');
const path = require('node:path');
const { parse } = require('smol-toml');

const EVAL_ROOT = path.resolve(__dirname, '..');
const CONFIG_PATH = path.join(EVAL_ROOT, 'config', 'eval-tiers.toml');
const REQUIRED_TIERS = ['light', 'standard', 'high', 'x_high'];
const REQUIRED_RUNTIME_TOOLS = ['read', 'write', 'edit', 'bash', 'grep', 'glob', 'list'];

function loadEvalTierConfig(configPath = CONFIG_PATH) {
  const parsed = parse(fs.readFileSync(configPath, 'utf8'));
  const runtime = parsed.runtime || {};
  const tiers = parsed.tiers || {};
  const configDir = path.dirname(configPath);

  validateRuntime(runtime);

  for (const tier of REQUIRED_TIERS) {
    if (!tiers[tier] || typeof tiers[tier].max_turns !== 'number') {
      throw new Error(`Missing required eval tier: ${tier}`);
    }
  }

  for (const [tierName, tier] of Object.entries(tiers)) {
    if (!Number.isInteger(tier.max_turns) || tier.max_turns <= 0) {
      throw new Error(`Invalid eval tier field: ${tierName}`);
    }
  }

  return {
    runtime: {
      providerId: runtime.provider_id,
      model: runtime.model,
      baseUrl: runtime.base_url,
      workingDir: path.resolve(configDir, runtime.working_dir),
      tools: normalizeTools(runtime.tools),
    },
    tiers: Object.fromEntries(
      Object.entries(tiers).map(([tierName, tier]) => [tierName, { maxTurns: tier.max_turns }]),
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

  validateRuntimeTools(runtime.tools);
}

function validateRuntimeTools(tools) {
  if (!isPlainObject(tools)) {
    throw new Error('Missing required eval runtime field: tools');
  }

  for (const toolName of REQUIRED_RUNTIME_TOOLS) {
    if (!Object.prototype.hasOwnProperty.call(tools, toolName)) {
      throw new Error(`Missing required eval runtime tools field: ${toolName}`);
    }
    if (typeof tools[toolName] !== 'boolean') {
      throw new Error(`Invalid eval runtime tools field: ${toolName}`);
    }
  }

  for (const [toolName, enabled] of Object.entries(tools)) {
    if (!REQUIRED_RUNTIME_TOOLS.includes(toolName)) {
      throw new Error(`Unexpected eval runtime tools field: ${toolName}`);
    }
    if (typeof enabled !== 'boolean') {
      throw new Error(`Invalid eval runtime tools field: ${toolName}`);
    }
  }
}

function normalizeTools(tools) {
  return { ...tools };
}

function isPlainObject(value) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    return false;
  }

  return Object.getPrototypeOf(value) === Object.prototype;
}

module.exports = {
  CONFIG_PATH,
  REQUIRED_TIERS,
  REQUIRED_RUNTIME_TOOLS,
  loadEvalTierConfig,
  resolveEvalTier,
};
