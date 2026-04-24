const fs = require('node:fs');
const path = require('node:path');
const yaml = require('js-yaml');

const { loadEvalTierConfig, resolveEvalTier } = require('./eval-tier-config');

const EVAL_ROOT = path.resolve(__dirname, '..');
const TMP_ROOT = path.join(EVAL_ROOT, '.tmp', 'resolved-configs');

function readYaml(relativePath) {
  return yaml.load(fs.readFileSync(path.join(EVAL_ROOT, relativePath), 'utf8'));
}

function resolveProviderBlock(evalTier) {
  const suiteConfig = loadEvalTierConfig();
  const resolvedTier = resolveEvalTier(suiteConfig, evalTier);

  return {
    id: suiteConfig.runtime.providerId,
    config: {
      model: suiteConfig.runtime.model,
      baseUrl: suiteConfig.runtime.baseUrl,
      apiKey: 'promptfoo-local-baseurl-placeholder',
      working_dir: suiteConfig.runtime.workingDir,
      max_turns: resolvedTier.maxTurns,
      tools: suiteConfig.runtime.tools,
    },
  };
}

function resolveConfigEvalTier(parsed, relativePath) {
  return parsed?.metadata?.eval_tier || inferEvalTierFromProviders(parsed, relativePath);
}

function inferEvalTierFromProviders(parsed, relativePath) {
  const providerEntry = Array.isArray(parsed?.providers) ? parsed.providers[0] : null;
  const maxTurns = readProviderMaxTurns(providerEntry, relativePath);
  if (!Number.isInteger(maxTurns)) {
    return null;
  }

  const suiteConfig = loadEvalTierConfig();
  return Object.entries(suiteConfig.tiers).find(([, tier]) => tier.maxTurns === maxTurns)?.[0] || null;
}

function readProviderMaxTurns(providerEntry, relativePath) {
  if (!providerEntry) {
    return null;
  }

  if (typeof providerEntry === 'string' && providerEntry.startsWith('file://')) {
    const configDirectory = path.dirname(path.join(EVAL_ROOT, relativePath));
    const providerPath = path.resolve(configDirectory, providerEntry.slice('file://'.length));
    const providerConfig = yaml.load(fs.readFileSync(providerPath, 'utf8'));
    return providerConfig?.config?.max_turns ?? null;
  }

  if (typeof providerEntry === 'object') {
    return providerEntry?.config?.max_turns ?? null;
  }

  return null;
}

function resolveConfigFile(relativePath) {
  const parsed = readYaml(relativePath);
  const evalTier = resolveConfigEvalTier(parsed, relativePath);
  if (!evalTier) {
    throw new Error(`${relativePath} is missing metadata.eval_tier`);
  }

  return {
    ...parsed,
    providers: [resolveProviderBlock(evalTier)],
  };
}

function writeResolvedConfig(
  relativePath,
  {
    fsImpl = fs,
    outputRoot = TMP_ROOT,
  } = {},
) {
  fsImpl.mkdirSync(outputRoot, { recursive: true });
  const resolved = resolveConfigFile(relativePath);
  const outputPath = path.join(outputRoot, relativePath);
  fsImpl.mkdirSync(path.dirname(outputPath), { recursive: true });
  fsImpl.writeFileSync(outputPath, yaml.dump(resolved), 'utf8');
  return path.relative(EVAL_ROOT, outputPath);
}

module.exports = {
  TMP_ROOT,
  resolveConfigFile,
  writeResolvedConfig,
};
