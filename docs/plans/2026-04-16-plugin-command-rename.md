# Plugin Command Rename Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename the user-facing plugin slash commands from `/scope`, `/profile`, `/refactor` to `/scope-tables`, `/profile-tables`, `/refactor-query` without changing internal Python/CLI names or persisted pipeline contracts.

**Architecture:** This is a command-surface-only rename. Update the command spec identities plus every user-visible reference that emits or instructs those commands, then refresh eval prompts/assertions so the public contract and verification surface stay aligned. Keep internal module names, Typer entrypoints, catalog keys, and reset logic unchanged.

**Tech Stack:** Claude Code command specs, Markdown docs, Promptfoo eval prompts/configs, pytest-free text verification via `rg`, targeted eval runs via `npm`

---

## File Structure

**Modify command specs**

- `commands/scope.md` — rename public command identity to `scope-tables` and update self-references
- `commands/profile.md` — rename public command identity to `profile-tables` and update self-references
- `commands/refactor.md` — rename public command identity to `refactor-query` and update self-references

**Modify user-facing command guidance**

- `commands/status.md` — replace generated next-step recommendations and examples to emit the new slash commands
- `commands/init-ad-migration.md` — update user guidance that references old slash commands
- `lib/shared/init_templates.py` — update generated guidance strings that mention old slash commands, if they are user-facing

**Modify eval surface**

- `tests/evals/prompts/cmd-scope.txt` — update harness prompt to invoke `/scope-tables`
- `tests/evals/prompts/cmd-profile.txt` — update harness prompt to invoke `/profile-tables`
- `tests/evals/prompts/cmd-refactor.txt` — update harness prompt to invoke `/refactor-query`
- `tests/evals/prompts/cmd-live-pipeline.txt` — update the live pipeline prompt to use `/scope-tables` and `/profile-tables`
- `tests/evals/packages/cmd-status/cmd-status.yaml` — update descriptions and string expectations that mention old slash commands

**Do not modify**

- `packages/ad-migration-internal/pyproject.toml`
- `packages/ad-migration-internal/src/ad_migration_internal/entrypoints.py`
- `lib/shared/profile.py`, `lib/shared/refactor.py`, and related internal modules
- catalog JSON field names such as `profile` and `refactor`
- worktree/run slug prefixes unless a user-facing string explicitly exposes them

### Task 1: Rename The Public Command Identities

**Files:**

- Modify: `commands/scope.md`
- Modify: `commands/profile.md`
- Modify: `commands/refactor.md`

- [ ] **Step 1: Confirm the current command headers and self-references**

Run:

```bash
rg -n "^name: |/scope\\b|/profile\\b|/refactor\\b" commands/scope.md commands/profile.md commands/refactor.md
```

Expected:

- `commands/scope.md` shows `name: scope`
- `commands/profile.md` shows `name: profile`
- `commands/refactor.md` shows `name: refactor`
- each file contains self-references using the old slash command name

- [ ] **Step 2: Edit the command identities and in-file references**

Apply this rename set:

```text
commands/scope.md:
  name: scope                  -> name: scope-tables
  /scope                       -> /scope-tables

commands/profile.md:
  name: profile                -> name: profile-tables
  /profile                     -> /profile-tables

commands/refactor.md:
  name: refactor               -> name: refactor-query
  /refactor                    -> /refactor-query
```

Constraints:

- Keep file paths the same; do not rename the markdown files in this slice.
- Do not change internal CLI examples like `migrate-util ready`, `refactor context`, or `profile write`.
- Leave run slug examples alone unless the old command token appears in a user-facing slash-command instruction.

- [ ] **Step 3: Verify the command specs expose the new names**

Run:

```bash
rg -n "^name: |/scope-tables\\b|/profile-tables\\b|/refactor-query\\b" commands/scope.md commands/profile.md commands/refactor.md
```

Expected:

- `commands/scope.md` contains `name: scope-tables`
- `commands/profile.md` contains `name: profile-tables`
- `commands/refactor.md` contains `name: refactor-query`
- no remaining old slash-command self-references appear in those three files

- [ ] **Step 4: Commit the command-spec rename**

Run:

```bash
git add commands/scope.md commands/profile.md commands/refactor.md
git commit -m "rename public migration command specs"
```

Expected:

- commit succeeds with only the three command spec files staged

### Task 2: Update Status And User Guidance

**Files:**

- Modify: `commands/status.md`
- Modify: `commands/init-ad-migration.md`
- Modify: `lib/shared/init_templates.py`

- [ ] **Step 1: Find all remaining user-facing references to the old commands**

Run:

```bash
rg -n "(/scope\\b|/profile\\b|/refactor\\b)" commands/status.md commands/init-ad-migration.md lib/shared/init_templates.py
```

Expected:

- `commands/status.md` shows examples and recommendation rules using the old names
- `commands/init-ad-migration.md` may mention `/scope` and `/profile`
- `lib/shared/init_templates.py` may mention skill/command guidance that should be updated only if the text is shown to users

- [ ] **Step 2: Rewrite user-facing guidance to the new command names**

Update these mappings:

```text
/scope           -> /scope-tables
/profile         -> /profile-tables
/refactor        -> /refactor-query
```

Implementation notes:

- In `commands/status.md`, update both prose examples and the rule table that maps pipeline phases to emitted commands.
- In `commands/init-ad-migration.md`, update migration workflow instructions that mention the old slash commands.
- In `lib/shared/init_templates.py`, change only strings that are rendered into generated user-facing docs; do not rename internal concepts like “profile-derived materialization”.

- [ ] **Step 3: Verify that user guidance is internally consistent**

Run:

```bash
rg -n "(/scope\\b|/profile\\b|/refactor\\b|/scope-tables\\b|/profile-tables\\b|/refactor-query\\b)" commands/status.md commands/init-ad-migration.md lib/shared/init_templates.py
```

Expected:

- only the new slash-command names remain in user-facing command guidance
- any remaining `/scope`, `/profile`, or `/refactor` hits are either false positives inside filenames or intentionally untouched internal references; review each hit manually before proceeding

- [ ] **Step 4: Commit the guidance updates**

Run:

```bash
git add commands/status.md commands/init-ad-migration.md lib/shared/init_templates.py
git commit -m "update migration guidance to renamed commands"
```

Expected:

- commit succeeds with only the guidance files staged

### Task 3: Update Eval Prompts And Status Expectations

**Files:**

- Modify: `tests/evals/prompts/cmd-scope.txt`
- Modify: `tests/evals/prompts/cmd-profile.txt`
- Modify: `tests/evals/prompts/cmd-refactor.txt`
- Modify: `tests/evals/prompts/cmd-live-pipeline.txt`
- Modify: `tests/evals/packages/cmd-status/cmd-status.yaml`

- [ ] **Step 1: Capture the current eval references to the old public commands**

Run:

```bash
rg -n "(/scope\\b|/profile\\b|/refactor\\b)" \
  tests/evals/prompts/cmd-scope.txt \
  tests/evals/prompts/cmd-profile.txt \
  tests/evals/prompts/cmd-refactor.txt \
  tests/evals/prompts/cmd-live-pipeline.txt \
  tests/evals/packages/cmd-status/cmd-status.yaml
```

Expected:

- each prompt file contains old slash-command instructions
- `cmd-status.yaml` contains status descriptions or expectations mentioning `/scope` or `/profile`

- [ ] **Step 2: Edit the eval prompts and expectations**

Apply this rename set:

```text
tests/evals/prompts/cmd-scope.txt:
  /scope -> /scope-tables

tests/evals/prompts/cmd-profile.txt:
  /profile -> /profile-tables

tests/evals/prompts/cmd-refactor.txt:
  /refactor -> /refactor-query

tests/evals/prompts/cmd-live-pipeline.txt:
  /scope -> /scope-tables
  /profile -> /profile-tables

tests/evals/packages/cmd-status/cmd-status.yaml:
  update descriptions and unexpected/expected output terms so they match the new command names
```

Constraints:

- Do not rename eval package directories such as `cmd-scope` in this slice.
- Do not rename fixture folders in this slice.
- Keep prompt instructions pointing at the existing command markdown file paths unless the file paths themselves are intentionally being renamed.

- [ ] **Step 3: Verify the eval surface no longer expects the old slash commands**

Run:

```bash
rg -n "(/scope\\b|/profile\\b|/refactor\\b)" \
  tests/evals/prompts/cmd-scope.txt \
  tests/evals/prompts/cmd-profile.txt \
  tests/evals/prompts/cmd-refactor.txt \
  tests/evals/prompts/cmd-live-pipeline.txt \
  tests/evals/packages/cmd-status/cmd-status.yaml
```

Expected:

- no stale public-command references remain in the edited eval files

- [ ] **Step 4: Commit the eval text updates**

Run:

```bash
git add \
  tests/evals/prompts/cmd-scope.txt \
  tests/evals/prompts/cmd-profile.txt \
  tests/evals/prompts/cmd-refactor.txt \
  tests/evals/prompts/cmd-live-pipeline.txt \
  tests/evals/packages/cmd-status/cmd-status.yaml
git commit -m "align eval prompts with renamed migration commands"
```

Expected:

- commit succeeds with only eval prompt/config files staged

### Task 4: Verify The Command-Surface Rename End-To-End

**Files:**

- Verify: `commands/scope.md`
- Verify: `commands/profile.md`
- Verify: `commands/refactor.md`
- Verify: `commands/status.md`
- Verify: `commands/init-ad-migration.md`
- Verify: `lib/shared/init_templates.py`
- Verify: `tests/evals/prompts/cmd-scope.txt`
- Verify: `tests/evals/prompts/cmd-profile.txt`
- Verify: `tests/evals/prompts/cmd-refactor.txt`
- Verify: `tests/evals/prompts/cmd-live-pipeline.txt`
- Verify: `tests/evals/packages/cmd-status/cmd-status.yaml`

- [ ] **Step 1: Run a repo sweep for stale public command references**

Run:

```bash
rg -n "(/scope\\b|/profile\\b|/refactor\\b|name: scope\\b|name: profile\\b|name: refactor\\b)" \
  commands \
  lib/shared/init_templates.py \
  tests/evals
```

Expected:

- no stale command header names remain
- no stale user-facing slash-command references remain in the targeted surface
- if any hit remains, classify it as either missed user-facing text to update or intentionally internal/non-user-facing text to leave alone

- [ ] **Step 2: Run the targeted eval suites for the changed surface**

Run:

```bash
cd tests/evals && npm run eval:cmd-scope
cd tests/evals && npm run eval:cmd-profile
cd tests/evals && npm run eval:cmd-refactor
cd tests/evals && npm run eval:cmd-status
```

Expected:

- all four eval suites pass
- failures, if any, are limited to stale command-string expectations and should be fixed before completion

- [ ] **Step 3: Run the curated smoke eval pass**

Run:

```bash
cd tests/evals && npm run eval:smoke
```

Expected:

- smoke pass succeeds, confirming the rename did not regress adjacent command/skill surfaces

- [ ] **Step 4: Review the final diff and commit the verification-safe cleanup**

Run:

```bash
git status --short
git diff -- commands lib/shared/init_templates.py tests/evals
```

Expected:

- only the intended command-surface files are changed
- no internal CLI/module rename slipped into the diff

- [ ] **Step 5: Commit any final cleanup**

Run:

```bash
git add commands lib/shared/init_templates.py tests/evals
git commit -m "verify public migration command rename"
```

Expected:

- final verification/cleanup commit succeeds, or no-op if all prior commits already captured the finished state

## Self-Review

- Spec coverage: the plan covers command identities, status/help text, generated guidance, eval prompts, status expectations, and verification. It intentionally excludes internal CLI/module/catalog renames per the approved scope.
- Placeholder scan: no `TODO`, `TBD`, or “write tests later” placeholders remain.
- Type consistency: the public names are consistent throughout the plan: `/scope-tables`, `/profile-tables`, `/refactor-query`.
