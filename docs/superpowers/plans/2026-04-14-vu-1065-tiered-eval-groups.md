# VU-1065 Tiered Eval Groups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add grouped Promptfoo package runs for smoke, skills, and commands, while folding Oracle post-extract regression coverage into the main package eval configs and leaving only live extract suites standalone.

**Architecture:** Keep `tests/evals/packages/` as the source of truth. Add grouped npm scripts in `tests/evals/package.json`, tag exactly one scenario per package with `[smoke]`, move Oracle regression scenarios into the existing package YAML files, and update harness docs and repo metadata to match the new layout. Verification stays in the Node-based eval harness plus targeted Promptfoo runs.

**Tech Stack:** Node.js, npm scripts, Promptfoo YAML configs, Markdown docs, `repo-map.json`

---

## Task 1: Create The Issue Worktree And Capture The Baseline

**Files:**

- Modify: none
- Check: `tests/evals/package.json`
- Check: `tests/evals/packages/*.yaml`
- Check: `docs/reference/eval-harness/README.md`

- [ ] **Step 1: Create or attach the canonical issue worktree**

```bash
./scripts/worktree.sh feature/vu-1065-add-tiered-eval-sets-for-smoke-skill-command-and-full-runs
```

Expected: the script prints the worktree path under `../worktrees/feature-vu-1065-add-tiered-eval-sets-for-smoke-skill-command-and-full-runs` and checks out the issue branch.

- [ ] **Step 2: Confirm the worktree is clean before editing**

```bash
git status --short
git branch --show-current
```

Expected: no output from `git status --short`, and the branch name is `feature/vu-1065-add-tiered-eval-sets-for-smoke-skill-command-and-full-runs`.

- [ ] **Step 3: Record the package config inventory used by the grouped runs**

```bash
find tests/evals/packages -maxdepth 2 -name '*.yaml' | sort
```

Expected: one sorted list of the package configs that will feed `eval:smoke`, `eval:skills`, and `eval:commands`.

- [ ] **Step 4: Record the current standalone live suites**

```bash
node -e "const pkg=require('./tests/evals/package.json'); console.log(pkg.scripts['eval:oracle-live']); console.log(pkg.scripts['eval:mssql-live']);"
```

Expected: two script lines, one for `oracle-live` and one for `mssql-live`. No grouped scripts should exist yet.

## Task 2: Add Grouped Eval Scripts And Guard Tests

**Files:**

- Modify: `tests/evals/package.json`
- Modify: `tests/evals/scripts/run-workspace-extension.test.js`

- [ ] **Step 1: Write the failing Node test for grouped script wiring**

Add a new test in `tests/evals/scripts/run-workspace-extension.test.js` that asserts the grouped scripts exist and point only at package configs:

```javascript
test('grouped eval scripts target package configs and keep live suites standalone', () => {
  const packageJson = readJson(EVAL_PACKAGE_JSON);

  assert.equal(
    packageJson.scripts['eval:smoke'].includes('--filter-pattern'),
    true,
  );
  assert.equal(
    packageJson.scripts['eval:skills'].includes('packages/'),
    true,
  );
  assert.equal(
    packageJson.scripts['eval:commands'].includes('packages/'),
    true,
  );
  assert.equal(
    packageJson.scripts['eval:skills'].includes('oracle-live'),
    false,
  );
  assert.equal(
    packageJson.scripts['eval:commands'].includes('mssql-live'),
    false,
  );
});
```

- [ ] **Step 2: Run the Node test to verify it fails before implementation**

```bash
cd tests/evals
node --test scripts/run-workspace-extension.test.js
```

Expected: FAIL because `eval:smoke`, `eval:skills`, and `eval:commands` do not exist yet.

- [ ] **Step 3: Add the grouped npm scripts in `tests/evals/package.json`**

Update the scripts block with concrete grouped runners that chain the package configs through `promptfoo.sh`:

```json
{
  "scripts": {
    "eval:smoke": "./scripts/promptfoo.sh eval --no-cache -c packages/listing-objects/skill-listing-objects.yaml -c packages/profiling-table/skill-profiling-table.yaml -c packages/generating-model/skill-generating-model.yaml -c packages/generating-tests/skill-generating-tests.yaml -c packages/reviewing-tests/skill-reviewing-tests.yaml -c packages/reviewing-model/skill-reviewing-model.yaml -c packages/analyzing-table/skill-analyzing-table.yaml -c packages/analyzing-table/skill-analyzing-table-readiness.yaml -c packages/cmd-scope/cmd-scope.yaml -c packages/cmd-profile/cmd-profile.yaml -c packages/cmd-generate-model/cmd-generate-model.yaml -c packages/cmd-generate-tests/cmd-generate-tests.yaml -c packages/cmd-refactor/cmd-refactor.yaml -c packages/cmd-status/cmd-status.yaml -c packages/cmd-reset-migration/cmd-reset-migration.yaml -c packages/cmd-commit-push-pr/cmd-commit-push-pr.yaml -c packages/refactoring-sql/skill-refactoring-sql.yaml --filter-pattern '^\\[smoke\\]'",
    "eval:skills": "./scripts/promptfoo.sh eval --no-cache -c packages/listing-objects/skill-listing-objects.yaml -c packages/profiling-table/skill-profiling-table.yaml -c packages/generating-model/skill-generating-model.yaml -c packages/generating-tests/skill-generating-tests.yaml -c packages/reviewing-tests/skill-reviewing-tests.yaml -c packages/reviewing-model/skill-reviewing-model.yaml -c packages/analyzing-table/skill-analyzing-table.yaml -c packages/analyzing-table/skill-analyzing-table-readiness.yaml -c packages/refactoring-sql/skill-refactoring-sql.yaml",
    "eval:commands": "./scripts/promptfoo.sh eval --no-cache -c packages/cmd-scope/cmd-scope.yaml -c packages/cmd-profile/cmd-profile.yaml -c packages/cmd-generate-model/cmd-generate-model.yaml -c packages/cmd-generate-tests/cmd-generate-tests.yaml -c packages/cmd-refactor/cmd-refactor.yaml -c packages/cmd-status/cmd-status.yaml -c packages/cmd-reset-migration/cmd-reset-migration.yaml -c packages/cmd-commit-push-pr/cmd-commit-push-pr.yaml"
  }
}
```

- [ ] **Step 4: Extend the guard test to check the smoke count and package coverage**

Add a second test in `tests/evals/scripts/run-workspace-extension.test.js` that verifies the grouped scripts cover the full current package inventory:

```javascript
test('grouped eval scripts cover every package config exactly once', () => {
  const packageJson = readJson(EVAL_PACKAGE_JSON);
  const packageConfigs = fs
    .readdirSync(path.join(REPO_ROOT, 'tests', 'evals', 'packages'), { recursive: true })
    .filter((entry) => entry.endsWith('.yaml'))
    .map((entry) => `packages/${entry.replaceAll(path.sep, '/')}`)
    .sort();

  for (const configPath of packageConfigs) {
    const groupedScripts = [
      packageJson.scripts['eval:smoke'],
      packageJson.scripts['eval:skills'],
      packageJson.scripts['eval:commands'],
    ].join('\n');
    assert.equal(groupedScripts.includes(configPath), true, configPath);
  }
});
```

- [ ] **Step 5: Run the Node test suite to verify the grouped script wiring passes**

```bash
cd tests/evals
node --test scripts/run-workspace-extension.test.js
```

Expected: PASS, including the new grouped-script assertions.

- [ ] **Step 6: Commit the grouped-script and harness-test slice**

```bash
git add tests/evals/package.json tests/evals/scripts/run-workspace-extension.test.js
git commit -m "VU-1065: add grouped eval scripts"
```

## Task 3: Tag One Smoke Scenario Per Package

**Files:**

- Modify: `tests/evals/packages/analyzing-table/skill-analyzing-table-readiness.yaml`
- Modify: `tests/evals/packages/analyzing-table/skill-analyzing-table.yaml`
- Modify: `tests/evals/packages/cmd-commit-push-pr/cmd-commit-push-pr.yaml`
- Modify: `tests/evals/packages/cmd-generate-model/cmd-generate-model.yaml`
- Modify: `tests/evals/packages/cmd-generate-tests/cmd-generate-tests.yaml`
- Modify: `tests/evals/packages/cmd-profile/cmd-profile.yaml`
- Modify: `tests/evals/packages/cmd-refactor/cmd-refactor.yaml`
- Modify: `tests/evals/packages/cmd-reset-migration/cmd-reset-migration.yaml`
- Modify: `tests/evals/packages/cmd-scope/cmd-scope.yaml`
- Modify: `tests/evals/packages/cmd-status/cmd-status.yaml`
- Modify: `tests/evals/packages/generating-model/skill-generating-model.yaml`
- Modify: `tests/evals/packages/generating-tests/skill-generating-tests.yaml`
- Modify: `tests/evals/packages/listing-objects/skill-listing-objects.yaml`
- Modify: `tests/evals/packages/profiling-table/skill-profiling-table.yaml`
- Modify: `tests/evals/packages/refactoring-sql/skill-refactoring-sql.yaml`
- Modify: `tests/evals/packages/reviewing-model/skill-reviewing-model.yaml`
- Modify: `tests/evals/packages/reviewing-tests/skill-reviewing-tests.yaml`
- Modify: `tests/evals/scripts/run-workspace-extension.test.js`

- [ ] **Step 1: Write the failing guard test for one smoke scenario per package**

Add a text-based guard in `tests/evals/scripts/run-workspace-extension.test.js` that reads each package YAML and checks for exactly one `[smoke]` description:

```javascript
test('each package config has exactly one smoke scenario', () => {
  const packageConfigs = fs
    .readdirSync(path.join(REPO_ROOT, 'tests', 'evals', 'packages'), { recursive: true })
    .filter((entry) => entry.endsWith('.yaml'))
    .map((entry) => path.join(REPO_ROOT, 'tests', 'evals', 'packages', entry));

  for (const configPath of packageConfigs) {
    const text = readText(configPath);
    const smokeMatches = text.match(/description:\\s*\"\\[smoke\\]/g) ?? [];
    assert.equal(smokeMatches.length, 1, configPath);
  }
});
```

- [ ] **Step 2: Run the Node test to verify it fails before smoke tagging**

```bash
cd tests/evals
node --test scripts/run-workspace-extension.test.js
```

Expected: FAIL because no package config currently has a `[smoke]` description.

- [ ] **Step 3: Prefix one representative scenario in each package config with `[smoke]`**

Use the existing first happy-path or baseline scenario in each file. The edit shape should look like this:

```yaml
tests:
  - description: "[smoke] list-tables — reads catalog object names"
    vars:
      fixture_path: "tests/evals/fixtures/listing-objects/source-table-guard"
      request_text: "list tables"
      expected_terms: "silver.dimsource"
```

Repeat that exact prefix style once per package config and leave the rest of the descriptions unchanged.

- [ ] **Step 4: Verify every package now has exactly one smoke scenario**

```bash
node - <<'NODE'
const fs = require('node:fs');
const path = require('node:path');
const root = path.join(process.cwd(), 'tests', 'evals', 'packages');
for (const dirent of fs.readdirSync(root, { recursive: true, withFileTypes: true })) {
  if (!dirent.isFile() || !dirent.name.endsWith('.yaml')) continue;
  const rel = dirent.parentPath.replace(`${root}/`, '');
  const full = path.join(dirent.parentPath, dirent.name);
  const text = fs.readFileSync(full, 'utf8');
  const count = (text.match(/description:\s*"\[smoke\]/g) ?? []).length;
  console.log(`${rel}/${dirent.name}: ${count}`);
  if (count !== 1) process.exitCode = 1;
}
NODE
```

Expected: one `: 1` line for every package config and exit code `0`.

- [ ] **Step 5: Re-run the Node harness tests**

```bash
cd tests/evals
node --test scripts/run-workspace-extension.test.js
```

Expected: PASS, including the smoke guard.

- [ ] **Step 6: Commit the smoke-tagging slice**

```bash
git add tests/evals/packages tests/evals/scripts/run-workspace-extension.test.js
git commit -m "VU-1065: tag smoke eval scenarios"
```

## Task 4: Move Oracle Regression Coverage Into The Main Packages

**Files:**

- Modify: `tests/evals/packages/cmd-scope/cmd-scope.yaml`
- Modify: `tests/evals/packages/cmd-profile/cmd-profile.yaml`
- Modify: `tests/evals/packages/cmd-generate-model/cmd-generate-model.yaml`
- Modify: `tests/evals/packages/cmd-generate-tests/cmd-generate-tests.yaml`
- Modify: `tests/evals/packages/cmd-refactor/cmd-refactor.yaml`
- Delete: `tests/evals/oracle-regression/promptfooconfig.yaml`
- Delete: `tests/evals/oracle-regression/fixtures/`

- [ ] **Step 1: Copy the Oracle regression scenarios into the owning command package configs**

For each command package, append the Oracle case from `tests/evals/oracle-regression/promptfooconfig.yaml` with package-local assertions. The copied scenario shape should stay explicit:

```yaml
- description: "oracle — scope — CHANNEL_SALES_SUMMARY resolves to SUMMARIZE_CHANNEL_SALES"
  prompts: ["cmd-scope"]
  vars:
    fixture_path: "tests/evals/oracle-regression/fixtures"
    target_tables: "sh.CHANNEL_SALES_SUMMARY"
    target_table: "sh.channel_sales_summary"
    target_procedure: "sh.summarize_channel_sales"
    expected_item_statuses: '{"sh.CHANNEL_SALES_SUMMARY": "resolved"}'
    expected_status: "resolved"
    expected_writer: "summarize_channel_sales"
    expected_output_terms: "resolved"
  assert:
    - type: javascript
      value: file://../../assertions/check-table-scoping.js
    - type: javascript
      value: file://../../assertions/check-procedure-catalog.js
```

Keep the fixture path pointed at the existing Oracle fixture directory until the package tests are green.

- [ ] **Step 2: Remove the standalone `oracle-regression` npm script**

Delete this script from `tests/evals/package.json`:

```json
"eval:oracle-regression": "./scripts/promptfoo.sh eval --no-cache -c oracle-regression/promptfooconfig.yaml"
```

- [ ] **Step 3: Run targeted package evals for the migrated Oracle scenarios**

```bash
cd tests/evals
npm run eval:cmd-scope -- --filter-pattern "oracle"
npm run eval:cmd-profile -- --filter-pattern "oracle"
npm run eval:cmd-generate-model -- --filter-pattern "oracle"
npm run eval:cmd-generate-tests -- --filter-pattern "oracle"
npm run eval:cmd-refactor -- --filter-pattern "oracle"
```

Expected: each command package runs its migrated Oracle scenario successfully, with no dependency on the removed standalone config.

- [ ] **Step 4: Delete the obsolete Oracle regression package root after package-level proof**

```bash
rm -rf tests/evals/oracle-regression
```

Expected: the standalone offline Oracle package is removed, while `oracle-live` remains.

- [ ] **Step 5: Re-run the Node harness tests and one grouped command pass**

```bash
cd tests/evals
node --test scripts/run-workspace-extension.test.js
npm run eval:commands
```

Expected: Node tests pass, and the grouped command run includes the migrated Oracle command coverage through the owning package configs.

- [ ] **Step 6: Commit the Oracle regression migration slice**

```bash
git add tests/evals/package.json tests/evals/packages tests/evals/oracle-regression
git commit -m "VU-1065: fold oracle regression into command packages"
```

## Task 5: Update Contributor Docs And Repo Metadata

**Files:**

- Modify: `docs/reference/eval-harness/README.md`
- Modify: `repo-map.json`

- [ ] **Step 1: Update the eval harness reference doc**

Change the operational guidance so it reflects the grouped runs and the live-only standalone suites. The resulting text should include concrete commands like:

```md
- smoke package pass: `npm run eval:smoke`
- all skill packages: `npm run eval:skills`
- all command packages: `npm run eval:commands`
- live SQL Server extract → scope → profile: `npm run eval:mssql-live`
- live Oracle extract → scope → profile: `npm run eval:oracle-live`
```

Remove references to `oracle-regression` and explain that Oracle post-extract command coverage now lives in the owning package configs.

- [ ] **Step 2: Update `repo-map.json` for the new eval structure and command inventory**

Edit the `tests_evals` description and `commands` section to remove `eval_oracle_regression` and add the new grouped commands:

```json
{
  "commands": {
    "eval_smoke": "cd tests/evals && npm run eval:smoke",
    "eval_skills": "cd tests/evals && npm run eval:skills",
    "eval_commands": "cd tests/evals && npm run eval:commands",
    "eval_oracle_live": "cd tests/evals && npm run eval:oracle-live",
    "eval_mssql_live": "cd tests/evals && npm run eval:mssql-live"
  }
}
```

- [ ] **Step 3: Lint the updated Markdown**

```bash
markdownlint docs/reference/eval-harness/README.md
```

Expected: no lint errors.

- [ ] **Step 4: Re-run the grouped smoke and skill passes**

```bash
cd tests/evals
npm run eval:smoke
npm run eval:skills
```

Expected: `eval:smoke` runs one tagged scenario per package, and `eval:skills` runs the full skill package set.

- [ ] **Step 5: Commit the doc and repo-map slice**

```bash
git add docs/reference/eval-harness/README.md repo-map.json
git commit -m "VU-1065: document tiered eval groups"
```

## Task 6: Final Verification And Handoff

**Files:**

- Modify: none

- [ ] **Step 1: Run the complete required local verification set**

```bash
cd tests/evals
node --test scripts/run-workspace-extension.test.js
npm run eval:smoke
npm run eval:skills
npm run eval:commands
```

Expected: all commands pass. `eval:smoke` should finish with one scenario per package, and the grouped runs must not invoke `oracle-live` or `mssql-live`.

- [ ] **Step 2: Record the standalone live suites that remain intentionally separate**

```bash
node -e "const pkg=require('./tests/evals/package.json'); console.log('oracle-live:', pkg.scripts['eval:oracle-live']); console.log('mssql-live:', pkg.scripts['eval:mssql-live']);"
```

Expected: both live extract scripts are still present and unchanged except for any formatting-only script block edits.

- [ ] **Step 3: Confirm the worktree is clean**

```bash
git status --short
```

Expected: no output.

- [ ] **Step 4: Create the final implementation commit if any verification-driven edits remain**

```bash
git add tests/evals/package.json tests/evals/packages tests/evals/scripts/run-workspace-extension.test.js docs/reference/eval-harness/README.md repo-map.json
git commit -m "VU-1065: add tiered eval package groups"
```

Expected: only run this step if there is remaining staged work after the prior checkpoint commits.

- [ ] **Step 5: Prepare the handoff note**

```text
Branch: feature/vu-1065-add-tiered-eval-sets-for-smoke-skill-command-and-full-runs
Verification:
- node --test scripts/run-workspace-extension.test.js
- npm run eval:smoke
- npm run eval:skills
- npm run eval:commands
Standalone live suites left separate:
- npm run eval:oracle-live
- npm run eval:mssql-live
```

Use that note when handing off to the PR workflow.
