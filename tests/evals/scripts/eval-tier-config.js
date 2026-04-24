const fs = require('node:fs');
const path = require('node:path');

let parse;

try {
  ({ parse } = require('smol-toml'));
} catch (error) {
  parse = parseEvalTierToml;
}

const EVAL_ROOT = path.resolve(__dirname, '..');
const CONFIG_PATH = path.join(EVAL_ROOT, 'config', 'eval-tiers.toml');
const REQUIRED_TIERS = ['light', 'standard', 'high', 'x_high'];

function parseEvalTierToml(source) {
  const result = {};
  let currentSection = result;

  for (const rawLine of source.split(/\r?\n/)) {
    const line = rawLine.trim();

    if (!line || line.startsWith('#')) {
      continue;
    }

    const sectionMatch = line.match(/^\[([^\]]+)\]$/);
    if (sectionMatch) {
      currentSection = ensureSection(result, sectionMatch[1].split('.'));
      continue;
    }

    const keyValueMatch = line.match(/^([A-Za-z0-9_]+)\s*=\s*(.+)$/);
    if (!keyValueMatch) {
      throw new Error(`Unsupported TOML line: ${line}`);
    }

    const key = keyValueMatch[1];
    const value = parseTomlValue(keyValueMatch[2]);
    currentSection[key] = value;
  }

  return result;
}

function ensureSection(root, parts) {
  let cursor = root;

  for (const part of parts) {
    if (!cursor[part] || typeof cursor[part] !== 'object' || Array.isArray(cursor[part])) {
      cursor[part] = {};
    }

    cursor = cursor[part];
  }

  return cursor;
}

function parseTomlValue(rawValue) {
  const value = rawValue.trim();

  if (value === 'true') {
    return true;
  }

  if (value === 'false') {
    return false;
  }

  if (/^-?\d+$/.test(value)) {
    return Number(value);
  }

  const quotedMatch = value.match(/^"(.*)"$/);
  if (quotedMatch) {
    return quotedMatch[1];
  }

  throw new Error(`Unsupported TOML value: ${value}`);
}

function loadEvalTierConfig(configPath = CONFIG_PATH) {
  const parsed = parse(fs.readFileSync(configPath, 'utf8'));
  const runtime = parsed.runtime || {};
  const tiers = parsed.tiers || {};

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
    tiers,
  };
}

function resolveEvalTier(config, tierName) {
  const tier = config.tiers[tierName];
  if (!tier) {
    throw new Error(`Unknown eval tier: ${tierName}`);
  }

  return { tierName, maxTurns: tier.max_turns };
}

module.exports = {
  CONFIG_PATH,
  REQUIRED_TIERS,
  loadEvalTierConfig,
  resolveEvalTier,
};
