const fs = require('node:fs');
const path = require('node:path');
const yaml = require('js-yaml');

const { loadEvalTierConfig, resolveEvalTier } = require('./eval-tier-config');

const EVAL_ROOT = path.resolve(__dirname, '..');
const TMP_ROOT = path.join(EVAL_ROOT, '.tmp', 'resolved-configs');

function readYaml(relativePath) {
  const normalizedPath = normalizeConfigPath(relativePath);
  return yaml.load(fs.readFileSync(path.join(EVAL_ROOT, normalizedPath), 'utf8'));
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
  const normalizedPath = normalizeConfigPath(relativePath);
  const parsed = readYaml(normalizedPath);
  const evalTier = parsed?.metadata?.eval_tier;
  if (!evalTier) {
    throw new Error(`${normalizedPath} is missing metadata.eval_tier`);
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
  const normalizedPath = normalizeConfigPath(relativePath);
  const normalizedOutputRoot = normalizeOutputRoot(outputRoot);

  fsImpl.mkdirSync(normalizedOutputRoot, { recursive: true });
  const resolved = resolveConfigFile(normalizedPath);
  const outputPath = resolveWithinRoot(
    normalizedOutputRoot,
    normalizedPath,
    `Refusing to write resolved config outside output root: ${normalizedPath}`,
  );
  fsImpl.mkdirSync(path.dirname(outputPath), { recursive: true });
  fsImpl.writeFileSync(outputPath, yaml.dump(resolved), 'utf8');
  return path.relative(EVAL_ROOT, outputPath);
}

function normalizeConfigPath(relativePath) {
  const resolvedPath = resolveWithinRoot(
    EVAL_ROOT,
    relativePath,
    `Refusing to access config outside eval root: ${relativePath}`,
  );
  return path.relative(EVAL_ROOT, resolvedPath);
}

function normalizeOutputRoot(outputRoot) {
  const resolvedRoot = path.resolve(outputRoot);
  ensureWithinRoot(
    resolvedRoot,
    TMP_ROOT,
    `Refusing to write resolved configs outside ${path.relative(EVAL_ROOT, TMP_ROOT)}`,
  );
  return resolvedRoot;
}

function resolveWithinRoot(root, candidatePath, errorMessage) {
  const resolvedPath = path.resolve(root, candidatePath);
  ensureWithinRoot(resolvedPath, root, errorMessage);
  return resolvedPath;
}

function ensureWithinRoot(candidatePath, root, errorMessage) {
  const normalizedRoot = path.resolve(root);
  const rootWithSeparator = `${normalizedRoot}${path.sep}`;
  if (candidatePath !== normalizedRoot && !candidatePath.startsWith(rootWithSeparator)) {
    throw new Error(errorMessage);
  }
}

module.exports = {
  TMP_ROOT,
  resolveConfigFile,
  writeResolvedConfig,
};
