# Refactor Mart Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the mart-driven two-wave refactor workflow and restructure generated dbt projects so their folders, YAML files, defaults, and model placement follow dbt’s published project-structure standards.

**Architecture:** This feature has two coupled parts. First, normalize the generated dbt project layout to the dbt best-practice structure: `models/staging/<source_system>/`, `models/intermediate/<domain>/`, `models/marts/<domain>/`, directory-scoped YAML/docs files, and directory-level defaults in `dbt_project.yml`. Second, add the new `refactor-mart` commands and skills that operate against that normalized layout using an LLM-readable markdown plan file rather than a Python-first schema.

**Tech Stack:** Claude Code slash command specs, migration skills, Markdown plan artifacts, Promptfoo evals, pytest repo-structure regression checks, `markdownlint`, `rg`

---

## File Structure

**Create new user-facing command specs**

- `commands/refactor-mart-plan.md` — analysis-only slash command that produces the mart refactor markdown plan
- `commands/refactor-mart.md` — execution slash command that consumes the plan file in `stg` or `int` mode

**Create new internal skills**

- `skills/planning-refactor-mart/SKILL.md` — analyzes one mart-domain unit and writes the regular markdown plan
- `skills/planning-refactor-mart/references/plan-file-contract.md` — compact reference for candidate section shape, dependency fields, and status updates
- `skills/applying-staging-candidate/SKILL.md` — executes one approved `stg` candidate and validates the affected scope
- `skills/applying-staging-candidate/references/staging-validation-contract.md` — reference for how to validate a staging candidate and update plan status
- `skills/applying-refactor-mart-candidate/SKILL.md` — executes one approved higher-layer candidate and validates the affected scope
- `skills/applying-refactor-mart-candidate/references/candidate-dependency-contract.md` — reference for dependency checks, blocked states, and per-candidate status updates

**Create new eval prompts and packages**

- `tests/evals/prompts/cmd-refactor-mart-plan.txt`
- `tests/evals/prompts/cmd-refactor-mart-stg.txt`
- `tests/evals/prompts/cmd-refactor-mart-int.txt`
- `tests/evals/prompts/skill-planning-refactor-mart.txt`
- `tests/evals/prompts/skill-applying-staging-candidate.txt`
- `tests/evals/prompts/skill-applying-refactor-mart-candidate.txt`
- `tests/evals/packages/cmd-refactor-mart/cmd-refactor-mart.yaml`
- `tests/evals/packages/planning-refactor-mart/skill-planning-refactor-mart.yaml`
- `tests/evals/packages/applying-staging-candidate/skill-applying-staging-candidate.yaml`
- `tests/evals/packages/applying-refactor-mart-candidate/skill-applying-refactor-mart-candidate.yaml`

**Create eval fixtures**

- `tests/evals/fixtures/cmd-refactor-mart/plan-happy-path/`
- `tests/evals/fixtures/cmd-refactor-mart/stg-happy-path/`
- `tests/evals/fixtures/cmd-refactor-mart/int-happy-path/`
- `tests/evals/fixtures/cmd-refactor-mart/int-blocked-dependency/`

**Modify existing regression and eval entrypoints**

- `tests/unit/repo_structure/test_python_package_layout.py` — assert the new command and skill docs exist and remain on the internal-project path convention
- `tests/evals/package.json` — add direct eval scripts and include the new command/skill packages in `eval:smoke` only if the fixtures are cheap enough for smoke coverage
- `repo-map.json` — update only if the command/skill layout or referenced command inventory is now stale after the new docs land

**Modify generated dbt project structure surfaces**

- `lib/shared/target_setup.py` — scaffold `dbt_project.yml` defaults and the canonical dbt folder tree for staging, intermediate, marts, macros, snapshots, tests, and utilities
- `lib/shared/generate_sources.py` — write staging source YAML into per-source-system directories using the directory-scoped YAML naming convention
- `lib/shared/migrate_support/artifacts.py` — resolve and write generated model artifacts into the new staging/intermediate/marts directory structure
- `tests/unit/target_setup/test_target_setup.py` — update scaffolded file expectations for the new dbt structure
- `tests/unit/generate_sources/test_generate_sources.py` — update source-YAML placement and naming expectations
- `tests/unit/migrate/test_migrate.py` — update model artifact path expectations and creation behavior for the new directory layout
- `tests/unit/output_models/test_model_generation_models.py` — update serialized artifact path expectations
- `skills/reviewing-model/SKILL.md` — change review guidance to follow the new `staging` and `marts` directory conventions by domain/source system
- `tests/evals/assertions/check-dbt-refs.js` and related dbt-aware assertions — update `_sources.yml` discovery to the new per-directory YAML convention
- existing dbt fixture trees under `tests/evals/fixtures/` — move models and YAML files to the new standard layout so command and skill evals reflect the real project structure

**Do not create**

- any Python parser for the plan file
- any JSON plan schema that competes with the markdown contract
- a standalone validation skill; validation stays inside the apply skills

### Task 1: Lock Repo Contracts Before Adding New Commands

**Files:**

- Modify: `lib/shared/target_setup.py`
- Modify: `lib/shared/generate_sources.py`
- Modify: `lib/shared/migrate_support/artifacts.py`
- Modify: `tests/unit/target_setup/test_target_setup.py`
- Modify: `tests/unit/generate_sources/test_generate_sources.py`
- Modify: `tests/unit/migrate/test_migrate.py`
- Modify: `tests/unit/output_models/test_model_generation_models.py`
- Modify: `skills/reviewing-model/SKILL.md`
- Modify: `tests/evals/assertions/check-dbt-refs.js`
- Modify: representative fixture trees under `tests/evals/fixtures/`

- [ ] **Step 1: Capture the current generated dbt layout and path assumptions**

Run:

```bash
rg -n "models/staging|models/marts|_sources.yml|_.*\\.yml|dbt_project.yml" \
  lib/shared \
  skills/reviewing-model/SKILL.md \
  tests/unit \
  tests/evals \
  -g '!**/.venv/**'
```

Expected:

- hits show the current flat `models/staging/` assumptions
- some fixtures and tests still expect non-standard folders like `models/silver/` or single-file staging YAML placement
- `target_setup.py` and `generate_sources.py` expose the current scaffold and source-YAML decisions that need to change

- [ ] **Step 2: Update unit tests first to codify the dbt-standard layout**

Apply these expectation changes before touching implementation:

```text
staging:
  dbt/models/staging/<source_system>/stg_<source_system>__<entity>.sql
  dbt/models/staging/<source_system>/_<source_system>__sources.yml
  dbt/models/staging/<source_system>/_<source_system>__models.yml

intermediate:
  dbt/models/intermediate/<domain>/int_<domain>__<purpose>.sql
  dbt/models/intermediate/<domain>/_int_<domain>__models.yml

marts:
  dbt/models/marts/<domain>/<entity>.sql
  dbt/models/marts/<domain>/_<domain>__models.yml
```

Implementation notes:

- in `tests/unit/target_setup/test_target_setup.py`, assert the scaffolded `dbt_project.yml` sets directory-level defaults for `staging`, `intermediate`, and `marts`
- in `tests/unit/generate_sources/test_generate_sources.py`, assert sources are written into per-source folders, not one flat `models/staging/sources.yml`
- in `tests/unit/migrate/test_migrate.py` and `tests/unit/output_models/test_model_generation_models.py`, replace old flat artifact paths with the new standard ones

- [ ] **Step 3: Run the targeted tests and verify they fail on the old layout**

Run:

```bash
cd lib && uv run pytest \
  ../tests/unit/target_setup/test_target_setup.py \
  ../tests/unit/generate_sources/test_generate_sources.py \
  ../tests/unit/migrate/test_migrate.py \
  ../tests/unit/output_models/test_model_generation_models.py -q
```

Expected:

- FAIL with path and scaffold mismatches
- failures are limited to the dbt layout assumptions you just changed

- [ ] **Step 4: Implement the dbt-standard scaffold and artifact paths**

Make these code changes:

```text
lib/shared/target_setup.py:
  - scaffold `models/staging/`, `models/intermediate/`, `models/marts/`, `models/utilities/`, `macros/`, `snapshots/`, and `tests/`
  - render `dbt_project.yml` with folder-level defaults:
      staging -> view
      intermediate -> ephemeral
      marts -> table

lib/shared/generate_sources.py:
  - write per-source-system `_...__sources.yml` files
  - keep YAML placement directory-scoped rather than one file per model

lib/shared/migrate_support/artifacts.py:
  - resolve staging, intermediate, and mart model destinations according to the new layout
```

Constraints:

- follow the dbt structure guidance from the five referenced docs, including directory-scoped YAML files and folder-based default configs
- do not introduce a compatibility shim that preserves the old flat paths unless a specific test proves it is required

- [ ] **Step 5: Update fixture trees and dbt-aware review/assertion docs**

Update:

```text
skills/reviewing-model/SKILL.md
tests/evals/assertions/check-dbt-refs.js
tests/evals/fixtures/**/dbt/models/**
```

Required outcomes:

- review guidance points at `staging/<source_system>/`, `intermediate/<domain>/`, and `marts/<domain>/`
- dbt ref assertions discover `_...__sources.yml` in per-directory locations
- representative fixtures in `generating-model`, `cmd-generate-model`, `refactoring-sql`, and `cmd-status` reflect the new standard layout

- [ ] **Step 6: Run the layout verification suite and commit**

Run:

```bash
markdownlint skills/reviewing-model/SKILL.md
cd lib && uv run pytest \
  ../tests/unit/target_setup/test_target_setup.py \
  ../tests/unit/generate_sources/test_generate_sources.py \
  ../tests/unit/migrate/test_migrate.py \
  ../tests/unit/output_models/test_model_generation_models.py -q
```

Expected:

- markdownlint passes
- the targeted unit tests pass with the new dbt structure

Run:

```bash
git add \
  lib/shared/target_setup.py \
  lib/shared/generate_sources.py \
  lib/shared/migrate_support/artifacts.py \
  tests/unit/target_setup/test_target_setup.py \
  tests/unit/generate_sources/test_generate_sources.py \
  tests/unit/migrate/test_migrate.py \
  tests/unit/output_models/test_model_generation_models.py \
  skills/reviewing-model/SKILL.md \
  tests/evals/assertions/check-dbt-refs.js \
  tests/evals/fixtures
git commit -m "feat: normalize generated dbt project structure"
```

Expected:

- commit succeeds with the structure-normalization slice staged

### Task 2: Lock Repo Contracts Before Adding New Commands

**Files:**

- Modify: `tests/unit/repo_structure/test_python_package_layout.py`
- Modify: `repo-map.json` (only if inspection shows the command/skill layout entry is stale)

- [ ] **Step 1: Inspect the current regression file for command and skill assertions**

Run:

```bash
rg -n "commands/|skills/|repo_map|internal_files" tests/unit/repo_structure/test_python_package_layout.py
```

Expected:

- the file already asserts the current command and skill paths
- no assertions mention `refactor-mart-plan`, `refactor-mart`, or the new gerund-named skills

- [ ] **Step 2: Add failing expectations for the new command and skill docs**

Extend the `internal_files` list with these paths:

```python
internal_files = [
    "commands/refactor-mart-plan.md",
    "commands/refactor-mart.md",
    "skills/planning-refactor-mart/SKILL.md",
    "skills/planning-refactor-mart/references/plan-file-contract.md",
    "skills/applying-staging-candidate/SKILL.md",
    "skills/applying-staging-candidate/references/staging-validation-contract.md",
    "skills/applying-refactor-mart-candidate/SKILL.md",
    "skills/applying-refactor-mart-candidate/references/candidate-dependency-contract.md",
]
```

Implementation notes:

- append the new items near the existing command and skill path checks rather than creating a second list
- keep the existing `ROOT_PLUGIN_PATH not in text` assertions intact so the new docs follow the split-project path convention
- only touch `repo-map.json` in this task if a specific entry is stale after reviewing the current file

- [ ] **Step 3: Run the targeted regression and verify it fails on missing files**

Run:

```bash
cd lib && uv run pytest ../tests/unit/repo_structure/test_python_package_layout.py -q
```

Expected:

- FAIL with missing-file assertions for the new command or skill paths
- no unrelated failures

- [ ] **Step 4: Commit the failing contract update**

Run:

```bash
git add tests/unit/repo_structure/test_python_package_layout.py repo-map.json
git commit -m "test: lock refactor-mart command and skill paths"
```

Expected:

- commit succeeds with the regression file staged
- `repo-map.json` is staged only if it was intentionally updated for stale command/skill layout metadata

### Task 3: Add The New Slash Command Specs

**Files:**

- Create: `commands/refactor-mart-plan.md`
- Create: `commands/refactor-mart.md`
- Test: `tests/unit/repo_structure/test_python_package_layout.py`

- [ ] **Step 1: Capture the existing command patterns from `generate-model` and `refactor`**

Run:

```bash
sed -n '1,220p' commands/generate-model.md
sed -n '1,240p' commands/refactor.md
```

Expected:

- both files show the expected frontmatter shape and command pipeline sections
- the new command specs can mirror guard/progress/setup/summary patterns without inventing a new style

- [ ] **Step 2: Write `commands/refactor-mart-plan.md` with the analysis-only contract**

Create the file with frontmatter and skeleton like:

```md
---
name: refactor-mart-plan
description: >
  Mart refactor planning command — analyzes one mart-domain unit, writes a
  markdown candidate plan, and does not apply code changes.
user-invocable: true
argument-hint: "<schema.table> [schema.table ...]"
---

# Refactor Mart Plan

Analyze one selected mart unit, derive staging and higher-layer candidates, and
write a markdown plan under `docs/design/`.
```

Required sections:

- guard for `manifest.json` and `dbt/` prerequisites only if the command truly needs them
- run slug and worktree guidance, following the repo’s git-checkpoints pattern
- explicit statement that this command does not mutate dbt models
- plan-file location and naming rule
- markdown candidate format with `Approve`, `Depends on`, `Validation`, and `Execution status`
- summary/output expectations for the created plan path

- [ ] **Step 3: Write `commands/refactor-mart.md` with the wave-mode execution contract**

Create the file with frontmatter and skeleton like:

```md
---
name: refactor-mart
description: >
  Mart refactor execution command — consumes a markdown plan file and applies
  approved staging or higher-layer candidates with validation.
user-invocable: true
argument-hint: "<plan-file> stg|int"
---
```

Required behavior:

- parse the two positional arguments from `$ARGUMENTS`
- `stg` mode executes only approved staging candidates
- `int` mode executes only approved non-staging candidates and enforces dependency satisfaction first
- blocked dependencies are reported per candidate and written back into plan status
- validation is part of both modes
- summary distinguishes `applied`, `failed`, and `blocked`

- [ ] **Step 4: Run markdownlint and the repo-structure test**

Run:

```bash
markdownlint commands/refactor-mart-plan.md commands/refactor-mart.md
cd lib && uv run pytest ../tests/unit/repo_structure/test_python_package_layout.py -q
```

Expected:

- markdownlint passes
- the repo-structure test still fails because the new skills do not exist yet

- [ ] **Step 5: Commit the new command docs**

Run:

```bash
git add commands/refactor-mart-plan.md commands/refactor-mart.md
git commit -m "feat: add refactor-mart command specs"
```

Expected:

- commit succeeds with only the two command spec files staged

### Task 4: Add The Planning Skill And Plan-File Reference

**Files:**

- Create: `skills/planning-refactor-mart/SKILL.md`
- Create: `skills/planning-refactor-mart/references/plan-file-contract.md`
- Modify: `tests/unit/repo_structure/test_python_package_layout.py`
- Test: `tests/evals/prompts/skill-planning-refactor-mart.txt`
- Test: `tests/evals/packages/planning-refactor-mart/skill-planning-refactor-mart.yaml`

- [ ] **Step 1: Draft the plan-file reference before the skill body**

Create `skills/planning-refactor-mart/references/plan-file-contract.md` with concise guidance like:

```md
# Plan File Contract

Each candidate section must use:
- `## Candidate: <ID>`
- `- [x] Approve: yes|no`
- `- Type: stg|int|mart`
- `- Output: <path>`
- `- Depends on: <ids>|none`
- `- Validation: <models>`
- `- Execution status: planned|applied|failed|blocked`
```

Include:

- stable ID prefixes such as `STG-`, `INT-`, and `MART-`
- rule that one candidate occupies one markdown section
- rule that the file is optimized for LLM interpretation, not Python parsing

- [ ] **Step 2: Write the planning skill using gerund naming and the existing skill template**

Create `skills/planning-refactor-mart/SKILL.md` with frontmatter like:

```md
---
name: planning-refactor-mart
description: Use when a selected mart unit must be analyzed into staging and higher-layer refactor candidates before any code changes
user-invocable: false
argument-hint: "<schema.table> [schema.table ...]"
---
```

Required sections:

- arguments and unit-of-work clarification
- explicit `do not apply code changes` rule
- analysis flow for `stg` versus `int`/mart candidates
- markdown plan writing instructions pointing at `references/plan-file-contract.md`
- preselect `[x]` only when confidence is high
- dependency declaration rules for non-staging candidates

- [ ] **Step 3: Add eval prompt and package for the planning skill**

Create:

```text
tests/evals/prompts/skill-planning-refactor-mart.txt
tests/evals/packages/planning-refactor-mart/skill-planning-refactor-mart.yaml
```

Use a prompt shell like:

```text
Run /planning-refactor-mart for these tables: `{{target_tables}}`.
Read `${CLAUDE_PLUGIN_ROOT}/skills/planning-refactor-mart/SKILL.md`.
Write the markdown plan to `{{run_path}}/docs/design/{{plan_name}}.md`.
```

Use at least these test scenarios in the YAML:

- one shared-source happy path that proposes `stg` plus `int`
- one split-source path that produces mostly staging candidates
- one dependency path where `int` depends on multiple `stg` candidates

- [ ] **Step 4: Run markdownlint, the repo-structure test, and the new planning eval**

Run:

```bash
markdownlint \
  skills/planning-refactor-mart/SKILL.md \
  skills/planning-refactor-mart/references/plan-file-contract.md
cd lib && uv run pytest ../tests/unit/repo_structure/test_python_package_layout.py -q
cd tests/evals && npm run eval:planning-refactor-mart
```

Expected:

- markdownlint passes
- the repo-structure test still fails because the apply skills are not present yet
- the new planning eval exercises the markdown-plan output shape, even if some assertions still need iteration

- [ ] **Step 5: Commit the planning skill slice**

Run:

```bash
git add \
  skills/planning-refactor-mart/SKILL.md \
  skills/planning-refactor-mart/references/plan-file-contract.md \
  tests/evals/prompts/skill-planning-refactor-mart.txt \
  tests/evals/packages/planning-refactor-mart/skill-planning-refactor-mart.yaml
git commit -m "feat: add planning-refactor-mart skill"
```

Expected:

- commit succeeds with only the planning-skill files staged

### Task 5: Add The Staging Apply Skill And `stg` Command Coverage

**Files:**

- Create: `skills/applying-staging-candidate/SKILL.md`
- Create: `skills/applying-staging-candidate/references/staging-validation-contract.md`
- Create: `tests/evals/prompts/skill-applying-staging-candidate.txt`
- Create: `tests/evals/packages/applying-staging-candidate/skill-applying-staging-candidate.yaml`
- Create: `tests/evals/prompts/cmd-refactor-mart-stg.txt`
- Modify: `tests/evals/packages/cmd-refactor-mart/cmd-refactor-mart.yaml`

- [ ] **Step 1: Write the staging validation contract reference**

Create `skills/applying-staging-candidate/references/staging-validation-contract.md` with the exact status vocabulary:

```md
# Staging Validation Contract

- validate the changed `stg_*` model
- validate each downstream consumer listed in `Validation:`
- write back one of `applied`, `failed`, or `blocked`
- do not invalidate unrelated approved candidates
```

Include:

- how the skill should update `Execution status:` in the plan
- the rule that one `stg` candidate is scoped to one source table but may fan out to many consumers

- [ ] **Step 2: Write the staging apply skill**

Create `skills/applying-staging-candidate/SKILL.md` with frontmatter like:

```md
---
name: applying-staging-candidate
description: Use when one approved staging candidate from a refactor-mart plan must be applied and validated across all of its downstream consumers
user-invocable: false
argument-hint: "<plan-file> <candidate-id>"
---
```

Required behavior:

- read exactly one candidate section from the plan
- reject candidate types other than `stg`
- create or update the `stg_*` model
- rewire every declared consumer
- validate the changed scope
- update only that candidate section in the plan

- [ ] **Step 3: Add the staging skill eval and the command eval prompt**

Create:

```text
tests/evals/prompts/skill-applying-staging-candidate.txt
tests/evals/prompts/cmd-refactor-mart-stg.txt
tests/evals/packages/applying-staging-candidate/skill-applying-staging-candidate.yaml
```

The `cmd-refactor-mart-stg.txt` prompt should instruct:

```text
Run `/refactor-mart {{plan_file}} stg`.
Read `${CLAUDE_PLUGIN_ROOT}/commands/refactor-mart.md`.
Apply only approved staging candidates.
```

Include at least these eval scenarios:

- happy path where one `stg` candidate rewires multiple consumers
- partial failure where one checked `stg` candidate fails validation but others still continue

- [ ] **Step 4: Run markdownlint and the staging eval slice**

Run:

```bash
markdownlint \
  skills/applying-staging-candidate/SKILL.md \
  skills/applying-staging-candidate/references/staging-validation-contract.md
cd tests/evals && npm run eval:applying-staging-candidate
cd tests/evals && npm run eval:cmd-refactor-mart -- --filter-pattern "stg"
```

Expected:

- markdownlint passes
- staging-skill eval covers one candidate-scoped application path
- command eval exercises `/refactor-mart <plan-file> stg` without touching `int` mode

- [ ] **Step 5: Commit the staging-apply slice**

Run:

```bash
git add \
  skills/applying-staging-candidate/SKILL.md \
  skills/applying-staging-candidate/references/staging-validation-contract.md \
  tests/evals/prompts/skill-applying-staging-candidate.txt \
  tests/evals/packages/applying-staging-candidate/skill-applying-staging-candidate.yaml \
  tests/evals/prompts/cmd-refactor-mart-stg.txt \
  tests/evals/packages/cmd-refactor-mart/cmd-refactor-mart.yaml
git commit -m "feat: add staging candidate apply workflow"
```

Expected:

- commit succeeds with the staging-apply skill and `stg` command eval files staged

### Task 6: Add The Higher-Layer Apply Skill, Dependency Gating, And Full Command Evals

**Files:**

- Create: `skills/applying-refactor-mart-candidate/SKILL.md`
- Create: `skills/applying-refactor-mart-candidate/references/candidate-dependency-contract.md`
- Create: `tests/evals/prompts/skill-applying-refactor-mart-candidate.txt`
- Create: `tests/evals/packages/applying-refactor-mart-candidate/skill-applying-refactor-mart-candidate.yaml`
- Create: `tests/evals/prompts/cmd-refactor-mart-plan.txt`
- Create: `tests/evals/prompts/cmd-refactor-mart-int.txt`
- Create: `tests/evals/packages/cmd-refactor-mart/cmd-refactor-mart.yaml`
- Create: `tests/evals/fixtures/cmd-refactor-mart/plan-happy-path/manifest.json`
- Create: `tests/evals/fixtures/cmd-refactor-mart/stg-happy-path/manifest.json`
- Create: `tests/evals/fixtures/cmd-refactor-mart/int-happy-path/manifest.json`
- Create: `tests/evals/fixtures/cmd-refactor-mart/int-blocked-dependency/manifest.json`
- Modify: `tests/evals/package.json`
- Test: `tests/unit/repo_structure/test_python_package_layout.py`

- [ ] **Step 1: Write the dependency contract reference before the skill**

Create `skills/applying-refactor-mart-candidate/references/candidate-dependency-contract.md` with guidance like:

```md
# Candidate Dependency Contract

- `int` or `mart` candidates must declare `Depends on:`
- if any dependency is unchecked, failed, or unapplied, mark `Execution status: blocked`
- do not attempt execution for blocked candidates
- validate the changed shared model and every rewritten mart in `Validation:`
```

Include:

- blocked-versus-failed distinction
- requirement to treat mart rewrites as part of the same candidate scope when the `int` change depends on them

- [ ] **Step 2: Write the higher-layer apply skill**

Create `skills/applying-refactor-mart-candidate/SKILL.md` with frontmatter like:

```md
---
name: applying-refactor-mart-candidate
description: Use when one approved higher-layer candidate from a refactor-mart plan must be applied with dependency checks and candidate-scoped validation
user-invocable: false
argument-hint: "<plan-file> <candidate-id>"
---
```

Required behavior:

- read one candidate section from the plan
- reject `stg` candidates
- check `Depends on:` before any edits
- if blocked, update status and stop cleanly
- otherwise apply the `int` extraction and associated mart rewrites
- validate only the declared candidate scope
- update plan status in-place

- [ ] **Step 3: Add the full command prompts, package, fixtures, and package scripts**

Create or update:

```text
tests/evals/prompts/cmd-refactor-mart-plan.txt
tests/evals/prompts/cmd-refactor-mart-int.txt
tests/evals/packages/cmd-refactor-mart/cmd-refactor-mart.yaml
tests/evals/package.json
```

Use package-script additions like:

```json
{
  "scripts": {
    "eval:planning-refactor-mart": "./scripts/promptfoo.sh eval --no-cache -c packages/planning-refactor-mart/skill-planning-refactor-mart.yaml",
    "eval:applying-staging-candidate": "./scripts/promptfoo.sh eval --no-cache -c packages/applying-staging-candidate/skill-applying-staging-candidate.yaml",
    "eval:applying-refactor-mart-candidate": "./scripts/promptfoo.sh eval --no-cache -c packages/applying-refactor-mart-candidate/skill-applying-refactor-mart-candidate.yaml",
    "eval:cmd-refactor-mart": "./scripts/promptfoo.sh eval --no-cache -c packages/cmd-refactor-mart/cmd-refactor-mart.yaml"
  }
}
```

Put these scenarios in `cmd-refactor-mart.yaml`:

- `[smoke]` plan generation happy path
- `stg` wave happy path
- `int` wave happy path
- blocked dependency path where checked higher-layer candidate depends on unchecked or failed `stg`

- [ ] **Step 4: Run the full targeted verification suite**

Run:

```bash
markdownlint \
  commands/refactor-mart-plan.md \
  commands/refactor-mart.md \
  skills/planning-refactor-mart/SKILL.md \
  skills/planning-refactor-mart/references/plan-file-contract.md \
  skills/applying-staging-candidate/SKILL.md \
  skills/applying-staging-candidate/references/staging-validation-contract.md \
  skills/applying-refactor-mart-candidate/SKILL.md \
  skills/applying-refactor-mart-candidate/references/candidate-dependency-contract.md
cd lib && uv run pytest ../tests/unit/repo_structure/test_python_package_layout.py -q
cd tests/evals && npm run eval:planning-refactor-mart
cd tests/evals && npm run eval:applying-staging-candidate
cd tests/evals && npm run eval:applying-refactor-mart-candidate
cd tests/evals && npm run eval:cmd-refactor-mart
```

Expected:

- markdownlint passes on all new markdown files
- repo-structure regression passes
- all four new eval packages run and specifically cover planning, staging execution, higher-layer execution, and blocked dependency handling

- [ ] **Step 5: Commit the higher-layer slice and final eval entrypoints**

Run:

```bash
git add \
  skills/applying-refactor-mart-candidate/SKILL.md \
  skills/applying-refactor-mart-candidate/references/candidate-dependency-contract.md \
  tests/evals/prompts/skill-applying-refactor-mart-candidate.txt \
  tests/evals/packages/applying-refactor-mart-candidate/skill-applying-refactor-mart-candidate.yaml \
  tests/evals/prompts/cmd-refactor-mart-plan.txt \
  tests/evals/prompts/cmd-refactor-mart-int.txt \
  tests/evals/packages/cmd-refactor-mart/cmd-refactor-mart.yaml \
  tests/evals/fixtures/cmd-refactor-mart/plan-happy-path/manifest.json \
  tests/evals/fixtures/cmd-refactor-mart/stg-happy-path/manifest.json \
  tests/evals/fixtures/cmd-refactor-mart/int-happy-path/manifest.json \
  tests/evals/fixtures/cmd-refactor-mart/int-blocked-dependency/manifest.json \
  tests/evals/package.json
git commit -m "feat: add refactor-mart candidate execution workflows"
```

Expected:

- commit succeeds with the higher-layer apply workflow, fixtures, and eval entrypoints staged

## Self-Review

**Spec coverage:** This plan now covers both the repo-wide dbt project structure normalization and the new mart refactor workflow. It addresses the dbt standards for overview, staging, intermediate, marts, and the rest-of-project layout, then layers the two commands, three skills, markdown-plan contract, dependency-aware `stg`/`int` execution, and candidate-scoped testing on top of that normalized structure.

**Placeholder scan:** The plan names concrete files, eval packages, fixture directories, and commands. There are no `TODO`, `TBD`, or “implement later” placeholders.

**Type consistency:** The command and skill names are consistent with the approved design:

- commands: `refactor-mart-plan`, `refactor-mart`
- skills: `planning-refactor-mart`, `applying-staging-candidate`, `applying-refactor-mart-candidate`
- execution modes: `stg`, `int`
