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

function resolveConfigFile(relativePath) {
  const parsed = readYaml(relativePath);
  const evalTier = parsed?.metadata?.eval_tier;
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
