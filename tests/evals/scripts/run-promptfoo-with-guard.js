const { execFileSync, spawnSync } = require('node:child_process');
const path = require('node:path');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..');
const EVAL_ROOT = path.join(REPO_ROOT, 'tests', 'evals');
const PROMPTFOO_ENTRYPOINT = path.join(
  EVAL_ROOT,
  'node_modules',
  'promptfoo',
  'dist',
  'src',
  'entrypoint.js',
);

const ALLOWED_ARTIFACT_PREFIXES = [
  'tests/evals/.cache/',
  'tests/evals/.promptfoo/',
  'tests/evals/.tmp/',
  'tests/evals/output/',
  'tests/evals/results/',
];

function runGitLines(args) {
  const output = execFileSync(
    'git',
    ['-C', REPO_ROOT, ...args],
    { encoding: 'utf8' },
  );

  return output
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
}

function collectGitSnapshot() {
  return {
    tracked: new Set(
      runGitLines(['diff', '--name-only', 'HEAD', '--', 'tests/evals']),
    ),
    untracked: new Set(
      runGitLines(['ls-files', '--others', '--exclude-standard', '--', 'tests/evals']),
    ),
  };
}

function isAllowedArtifactPath(filePath) {
  return ALLOWED_ARTIFACT_PREFIXES.some((prefix) => filePath.startsWith(prefix));
}

function collectNewPaths(beforeSet, afterSet) {
  return [...afterSet].filter((filePath) => !beforeSet.has(filePath));
}

function detectCleanupViolations(before, after) {
  const newTracked = collectNewPaths(before.tracked, after.tracked);
  const newUntracked = collectNewPaths(before.untracked, after.untracked);

  return [...newTracked, ...newUntracked]
    .filter((filePath) => !isAllowedArtifactPath(filePath))
    .sort();
}

function formatViolationMessage(paths) {
  return [
    'Eval run dirtied files outside approved artifact directories:',
    ...paths.map((filePath) => `- ${filePath}`),
    'Allowed artifact roots: tests/evals/.cache/, tests/evals/.promptfoo/, tests/evals/.tmp/, tests/evals/output/, tests/evals/results/',
  ].join('\n');
}

function splitPromptfooInvocations(argv) {
  const sharedArgs = [];
  const configArgs = [];

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === '-c') {
      const configPath = argv[index + 1];
      if (!configPath) {
        throw new Error('Missing config path after -c');
      }

      configArgs.push(configPath);
      index += 1;
      continue;
    }

    sharedArgs.push(token);
  }

  if (configArgs.length === 0) {
    return [sharedArgs];
  }

  return configArgs.map((configPath) => [...sharedArgs, '-c', configPath]);
}

function runPromptfooInvocation(argv) {
  const result = spawnSync(
    process.execPath,
    [PROMPTFOO_ENTRYPOINT, ...argv],
    {
      cwd: EVAL_ROOT,
      env: process.env,
      stdio: 'inherit',
    },
  );

  if (result.error) {
    throw result.error;
  }

  if (result.status !== null) {
    return result.status;
  }

  if (result.signal) {
    console.error(`promptfoo exited due to signal ${result.signal}`);
    return 1;
  }

  return 1;
}

function main(argv = process.argv.slice(2)) {
  const before = collectGitSnapshot();
  let status = 0;

  for (const invocation of splitPromptfooInvocations(argv)) {
    status = runPromptfooInvocation(invocation);
    if (status !== 0) {
      break;
    }
  }

  const after = collectGitSnapshot();
  const violations = detectCleanupViolations(before, after);

  if (violations.length > 0) {
    console.error(formatViolationMessage(violations));
    return 1;
  }

  return status;
}

if (require.main === module) {
  process.exitCode = main();
}

module.exports = {
  ALLOWED_ARTIFACT_PREFIXES,
  collectGitSnapshot,
  detectCleanupViolations,
  formatViolationMessage,
  isAllowedArtifactPath,
  main,
  runPromptfooInvocation,
  splitPromptfooInvocations,
};
