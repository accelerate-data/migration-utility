# Promptfoo OpenCode Evals Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move `tests/evals/` to OpenCode on Qwen 3.6 with suite-owned tier/runtime config, then simplify the package layout during the forced full-suite migration.

**Architecture:** Keep `tests/evals/package.json` and package YAMLs as the public suite surface, but stop executing package configs directly. A suite-owned resolver reads `metadata.eval_tier`, loads `tests/evals/config/eval-tiers.toml`, writes resolved Promptfoo configs into `tests/evals/.tmp/`, then the existing cleanliness guard runs Promptfoo only against those resolved configs after ensuring the OpenCode server is reachable. During the same cutover, remove the old provider-file layer and merge the standalone `analyzing-table-readiness` package into `analyzing-table`.

**Tech Stack:** Node.js scripts, TOML configuration, Promptfoo, `@opencode-ai/sdk`, shell wrapper scripts, Markdown/YAML.

---

## File Map

| Action | File |
|---|---|
| Create | `tests/evals/config/eval-tiers.toml` |
| Create | `tests/evals/scripts/eval-tier-config.js` |
| Create | `tests/evals/scripts/eval-tier-config.test.js` |
| Create | `tests/evals/scripts/resolve-promptfoo-config.js` |
| Create | `tests/evals/scripts/resolve-promptfoo-config.test.js` |
| Modify | `tests/evals/scripts/promptfoo.sh` |
| Modify | `tests/evals/scripts/run-promptfoo-with-guard.js` |
| Modify | `tests/evals/scripts/run-promptfoo-with-guard.test.js` |
| Modify | `tests/evals/scripts/run-workspace-extension.test.js` |
| Modify | `tests/evals/package.json` |
| Modify | `tests/evals/package-lock.json` |
| Delete or stop referencing | `tests/evals/providers/haiku-60.yaml`, `tests/evals/providers/haiku-100.yaml`, `tests/evals/providers/haiku-120.yaml`, `tests/evals/providers/sonnet-120.yaml`, `tests/evals/providers/sonnet-200.yaml` |
| Replace | `tests/evals/providers/provider-tools.test.js` with suite-level config contract coverage |
| Modify | every package YAML under `tests/evals/packages/` plus `tests/evals/oracle-live/promptfooconfig.yaml` and `tests/evals/mssql-live/promptfooconfig.yaml` |
| Modify | `docs/design/promptfoo-opencode-evals/README.md` only if implementation reveals a durable contract change beyond the approved design |

---

### Task 1: Add suite-level tier registry and config loader

**Files:**

- Create: `tests/evals/config/eval-tiers.toml`
- Create: `tests/evals/scripts/eval-tier-config.js`
- Test: `tests/evals/scripts/eval-tier-config.test.js`

- [ ] **Step 1: Write the failing config-loader test**

Create `tests/evals/scripts/eval-tier-config.test.js` with coverage for the four required tiers and runtime settings:

```js
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
```

- [ ] **Step 2: Run the new test to verify it fails**

Run:

```bash
cd tests/evals && node --test scripts/eval-tier-config.test.js
```

Expected: FAIL because `./eval-tier-config` does not exist yet.

- [ ] **Step 3: Add the suite-level TOML registry**

Create `tests/evals/config/eval-tiers.toml` with suite-owned OpenCode runtime and the four package-selectable tiers:

```toml
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
```

- [ ] **Step 4: Implement the loader**

Create `tests/evals/scripts/eval-tier-config.js` so suite code has one place to load and validate the tier registry:

```js
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
```

- [ ] **Step 5: Run the config-loader test to verify it passes**

Run:

```bash
cd tests/evals && node --test scripts/eval-tier-config.test.js
```

Expected: PASS with 2 passing tests.

- [ ] **Step 6: Commit**

```bash
git add tests/evals/config/eval-tiers.toml tests/evals/scripts/eval-tier-config.js tests/evals/scripts/eval-tier-config.test.js
git commit -m "VU-1132: add suite-level eval tier registry"
```

---

### Task 2: Resolve package tiers into OpenCode configs and bootstrap OpenCode in the suite wrapper

**Files:**

- Create: `tests/evals/scripts/resolve-promptfoo-config.js`
- Test: `tests/evals/scripts/resolve-promptfoo-config.test.js`
- Modify: `tests/evals/scripts/promptfoo.sh`
- Modify: `tests/evals/scripts/run-promptfoo-with-guard.js`
- Test: `tests/evals/scripts/run-promptfoo-with-guard.test.js`

- [ ] **Step 1: Write the failing resolver test**

Create `tests/evals/scripts/resolve-promptfoo-config.test.js` with one representative package fixture and one live-config fixture:

```js
const assert = require('node:assert/strict');
const test = require('node:test');

const {
  resolveConfigFile,
} = require('./resolve-promptfoo-config');

test('resolveConfigFile materializes an opencode provider from metadata.eval_tier', () => {
  const resolved = resolveConfigFile('packages/listing-objects/skill-listing-objects.yaml');

  assert.equal(resolved.providers[0].id, 'opencode:sdk');
  assert.equal(resolved.providers[0].config.model, 'qwen-3.6');
  assert.equal(resolved.providers[0].config.max_turns, 60);
});
```

- [ ] **Step 2: Run the resolver test to verify it fails**

Run:

```bash
cd tests/evals && node --test scripts/resolve-promptfoo-config.test.js
```

Expected: FAIL because `./resolve-promptfoo-config` does not exist yet.

- [ ] **Step 3: Implement config resolution**

Create `tests/evals/scripts/resolve-promptfoo-config.js` so package configs are never passed directly to Promptfoo:

```js
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

function writeResolvedConfig(relativePath) {
  fs.mkdirSync(TMP_ROOT, { recursive: true });
  const resolved = resolveConfigFile(relativePath);
  const outputPath = path.join(TMP_ROOT, relativePath);
  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  fs.writeFileSync(outputPath, yaml.dump(resolved), 'utf8');
  return path.relative(EVAL_ROOT, outputPath);
}

module.exports = {
  TMP_ROOT,
  resolveConfigFile,
  writeResolvedConfig,
};
```

- [ ] **Step 4: Extend the wrapper to manage OpenCode before Promptfoo starts**

Update `tests/evals/scripts/promptfoo.sh` so it preserves the existing suite artifact directories but adds OpenCode readiness and exports a suite-owned base URL:

```sh
OPENCODE_HOST="${PROMPTFOO_OPENCODE_HOST:-127.0.0.1}"
OPENCODE_PORT="${PROMPTFOO_OPENCODE_PORT:-4096}"
OPENCODE_BASE_URL="http://${OPENCODE_HOST}:${OPENCODE_PORT}"
OPENCODE_MANAGE="${PROMPTFOO_MANAGE_OPENCODE:-1}"
OPENCODE_LOG="$SCRIPT_DIR/.promptfoo/opencode-server.log"
export PROMPTFOO_OPENCODE_BASE_URL="$OPENCODE_BASE_URL"
```

Add a readiness check and conditional startup:

```sh
is_opencode_ready() {
  curl -fsS "${OPENCODE_BASE_URL}/" >/dev/null 2>&1
}

if [ "${OPENCODE_MANAGE}" = "1" ] && ! is_opencode_ready; then
  : >"$OPENCODE_LOG"
  opencode serve --hostname "$OPENCODE_HOST" --port "$OPENCODE_PORT" >>"$OPENCODE_LOG" 2>&1 &
  OPENCODE_PID=$!
  trap cleanup EXIT INT TERM HUP
  # poll until ready
fi
```

- [ ] **Step 5: Resolve configs inside the cleanliness guard path**

Update `tests/evals/scripts/run-promptfoo-with-guard.js` so `splitPromptfooInvocations()` still isolates each `-c`, but `runPromptfooInvocation()` first rewrites `-c packages/...yaml` into `-c .tmp/resolved-configs/packages/...yaml` by calling `writeResolvedConfig()` from the resolver.

The shape should be:

```js
const { writeResolvedConfig } = require('./resolve-promptfoo-config');

function materializeInvocation(argv) {
  const next = [];
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === '-c') {
      next.push('-c', writeResolvedConfig(argv[index + 1]));
      index += 1;
      continue;
    }
    next.push(token);
  }
  return next;
}
```

- [ ] **Step 6: Extend runtime tests**

Add coverage in `tests/evals/scripts/run-promptfoo-with-guard.test.js` for:

- resolved configs being created only under `tests/evals/.tmp/`
- unresolved package configs never being passed to Promptfoo
- pre-existing cleanliness rules still failing on dirty paths outside allowed roots

Run:

```bash
cd tests/evals && node --test scripts/resolve-promptfoo-config.test.js scripts/run-promptfoo-with-guard.test.js
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add tests/evals/scripts/resolve-promptfoo-config.js tests/evals/scripts/resolve-promptfoo-config.test.js tests/evals/scripts/promptfoo.sh tests/evals/scripts/run-promptfoo-with-guard.js tests/evals/scripts/run-promptfoo-with-guard.test.js
git commit -m "VU-1132: resolve package tiers through suite-owned opencode runtime"
```

---

### Task 3: Move package configs to `metadata.eval_tier` and remove the old provider-file layer

**Files:**

- Modify: every YAML under `tests/evals/packages/`
- Modify: `tests/evals/oracle-live/promptfooconfig.yaml`
- Modify: `tests/evals/mssql-live/promptfooconfig.yaml`
- Modify: `tests/evals/package.json`
- Modify: `tests/evals/package-lock.json`
- Delete or stop referencing: `tests/evals/providers/*.yaml`
- Replace test: `tests/evals/providers/provider-tools.test.js`

- [ ] **Step 1: Replace provider references in package YAMLs with package-owned tier metadata**

For each package config, remove:

```yaml
providers:
  - file://../../providers/haiku-60.yaml
```

or per-test provider overrides like:

```yaml
provider: file://../../providers/sonnet-120.yaml
```

Then add package metadata:

```yaml
metadata:
  eval_tier: light
```

Use these initial mappings:

- `haiku-60` -> `light`
- `haiku-100` -> `standard`
- `haiku-120` -> `high`
- `sonnet-120` -> `high`
- `sonnet-200` -> `x_high`

If a package currently mixes `sonnet-120` and `sonnet-200`, keep the package default at `high` and move the `x_high` case into its own package-level config only if the suite contract cannot express it another way. Prefer simplifying the package to one tier.

- [ ] **Step 2: Update live configs to the same package contract**

In `tests/evals/oracle-live/promptfooconfig.yaml` and `tests/evals/mssql-live/promptfooconfig.yaml`, replace file-based providers with:

```yaml
metadata:
  eval_tier: high
```

Leave the live-suite-specific prompts, fixtures, and assertions intact.

- [ ] **Step 3: Replace provider-layer tests with suite contract tests**

Delete the old provider parsing expectation in `tests/evals/providers/provider-tools.test.js` and replace it with a suite-level contract test, for example `tests/evals/scripts/eval-package-contract.test.js`, that:

- finds every package YAML and live config
- asserts `metadata.eval_tier` exists
- asserts the value is one of `light|standard|high|x_high`
- asserts no file still contains `anthropic:claude-agent-sdk` or `file://../../providers/`

Skeleton:

```js
test('all eval configs declare a valid suite eval tier', () => {
  for (const configPath of listEvalConfigs()) {
    const parsed = yaml.load(fs.readFileSync(configPath, 'utf8'));
    assert.match(parsed.metadata.eval_tier, /^(light|standard|high|x_high)$/);
  }
});
```

- [ ] **Step 4: Cut the Anthropics SDK dependency and add the OpenCode SDK**

Update `tests/evals/package.json` dependencies to remove `@anthropic-ai/claude-agent-sdk` and add `@opencode-ai/sdk`.

The expected dependency block becomes:

```json
"dependencies": {
  "@opencode-ai/sdk": "1.14.21",
  "promptfoo": "0.121.7"
}
```

Then refresh the lockfile:

```bash
cd tests/evals && npm install --package-lock-only
```

- [ ] **Step 5: Run suite contract tests**

Run:

```bash
cd tests/evals && node --test scripts/eval-tier-config.test.js scripts/resolve-promptfoo-config.test.js scripts/run-promptfoo-with-guard.test.js scripts/run-workspace-extension.test.js
```

Expected: PASS, with no provider-file references left in `tests/evals/`.

- [ ] **Step 6: Commit**

```bash
git add tests/evals/package.json tests/evals/package-lock.json tests/evals/packages tests/evals/oracle-live/promptfooconfig.yaml tests/evals/mssql-live/promptfooconfig.yaml tests/evals/scripts
git commit -m "VU-1132: move eval packages to suite tier selection"
```

---

### Task 4: Rationalize package layout during the cutover

**Files:**

- Modify: `tests/evals/packages/analyzing-table/skill-analyzing-table.yaml`
- Delete or stop scripting: `tests/evals/packages/analyzing-table/skill-analyzing-table-readiness.yaml`
- Modify: `tests/evals/package.json`
- Modify: `tests/evals/scripts/run-workspace-extension.test.js`

- [ ] **Step 1: Merge readiness coverage into the main analyzing-table package**

Move the readiness-only scenarios from `tests/evals/packages/analyzing-table/skill-analyzing-table-readiness.yaml` into `tests/evals/packages/analyzing-table/skill-analyzing-table.yaml`.

Keep one package-level tier:

```yaml
metadata:
  eval_tier: light
```

Preserve the existing `[smoke]` convention by keeping exactly one `[smoke]` case in the merged file.

- [ ] **Step 2: Remove the standalone readiness package from scripts**

In `tests/evals/package.json`:

- delete `eval:analyzing-table-readiness`
- remove the extra readiness `-c` entry from `eval:smoke`

The merged smoke command should still include `packages/analyzing-table/skill-analyzing-table.yaml` exactly once.

- [ ] **Step 3: Update structural tests for the new package inventory**

Adjust `tests/evals/scripts/run-workspace-extension.test.js` so its package discovery and smoke-config assertions expect the merged package set.

The key checks remain:

- smoke still enumerates all package configs returned by package discovery
- live configs stay outside smoke
- each remaining package config still has exactly one `[smoke]` case

- [ ] **Step 4: Verify the package cleanup**

Run:

```bash
cd tests/evals && node --test scripts/run-workspace-extension.test.js
```

Expected: PASS, and no script references remain for `eval:analyzing-table-readiness`.

- [ ] **Step 5: Commit**

```bash
git add tests/evals/packages/analyzing-table tests/evals/package.json tests/evals/scripts/run-workspace-extension.test.js
git commit -m "VU-1132: merge analyzing-table readiness coverage into the main package"
```

---

### Task 5: Run suite verification and document any durable contract changes

**Files:**

- Modify only if needed: `docs/design/promptfoo-opencode-evals/README.md`

- [ ] **Step 1: Run deterministic suite tests**

Run:

```bash
cd tests/evals && npm test
```

Expected: PASS for script, assertion, and suite-contract tests.

- [ ] **Step 2: Run the curated smoke pass on the OpenCode path**

Run:

```bash
cd tests/evals && npm run eval:smoke
```

Expected: PASS, with Promptfoo running through the suite resolver and OpenCode bootstrap path.

- [ ] **Step 3: Update the design doc only if implementation changed the durable contract**

If implementation changes the approved design, update:

```markdown
- package cleanup keeps `analyzing-table` as the merged package for readiness + analysis coverage
- package configs now require `metadata.eval_tier`
- the suite runtime resolves package configs under `tests/evals/.tmp/resolved-configs/`
```

If no durable contract changed, leave the design doc untouched.

- [ ] **Step 4: Record final verification state**

Capture the exact commands run and their result in your final handoff notes:

```text
cd tests/evals && npm test
cd tests/evals && npm run eval:smoke
```

- [ ] **Step 5: Commit**

```bash
git add docs/design/promptfoo-opencode-evals/README.md
git commit -m "VU-1132: finalize opencode eval suite verification"
```

Only run this commit if the design doc changed. If no doc changes were required, skip the commit and leave Task 4's commit as the final code commit.

---

## Spec Coverage Check

- Suite-level tier registry: Task 1
- Package-level tier selection: Task 3
- Suite-owned resolver/runtime/bootstrap: Task 2
- Cleanliness guards stay suite-owned: Task 2 + Task 5
- OpenCode + Qwen 3.6 cutover: Tasks 1–3
- Package reorganization during cutover: Task 4
- Remove or merge low-value eval coverage: Task 4

## Placeholder Scan

- No `TODO` or `TBD` steps remain.
- Every task names exact files and exact commands.
- Package cleanup is explicit: merge `analyzing-table-readiness` into `analyzing-table`.

## Type Consistency Check

- Tier names are consistent everywhere: `light`, `standard`, `high`, `x_high`.
- Runtime provider id is consistent everywhere: `opencode:sdk`.
- Model name is consistent everywhere: `qwen-3.6`.
