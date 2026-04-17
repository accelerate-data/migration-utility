# Refactor Mart Higher-Layer Execution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `/refactor-mart` higher-layer execution for approved `int` and `mart` candidates with dependency gating, candidate-scoped validation, and eval coverage.

**Architecture:** Keep `/refactor-mart` as the only user-facing entrypoint. Preserve the existing internal `applying-staging-candidate` skill for `stg` mode, and add a separate internal `applying-mart-candidates` skill for the `int` mode wave that applies `Type: int` and `Type: mart` candidates only after dependencies are already `Execution status: applied`.

**Tech Stack:** Claude Code plugin markdown commands and skills, Promptfoo eval harness, JavaScript assertion helpers, markdownlint.

---

## Files And Responsibilities

- `commands/refactor-mart.md`: User-facing command contract for `stg|int` execution, dependency gating, internal skill dispatch, and final summary semantics.
- `skills/applying-staging-candidate/SKILL.md`: Existing internal staging-only skill; verify it remains `user-invocable: false` and explicitly staging-only.
- `skills/applying-mart-candidates/SKILL.md`: New internal higher-layer apply skill for one approved `int` or `mart` candidate.
- `skills/applying-mart-candidates/references/mart-validation-contract.md`: New status and validation contract for higher-layer candidate execution.
- `tests/evals/prompts/cmd-refactor-mart-int.txt`: Promptfoo harness prompt for `/refactor-mart <plan> int`.
- `tests/evals/packages/cmd-refactor-mart/cmd-refactor-mart.yaml`: Add higher-layer happy path, blocked dependency, and validation failure cases.
- `tests/evals/assertions/check-refactor-mart-mart-execution.js`: New assertion helper for higher-layer execution behavior.
- `tests/evals/assertions/check-refactor-mart-mart-execution.test.js`: Unit tests for the new assertion helper.
- `tests/evals/fixtures/cmd-refactor-mart/int-happy-path/**`: Fixture project with applied staging dependency and successful `int`/`mart` candidates.
- `tests/evals/fixtures/cmd-refactor-mart/int-blocked-dependency/**`: Fixture project where higher-layer candidates depend on unapplied or failed staging candidates.
- `tests/evals/fixtures/cmd-refactor-mart/int-validation-failure/**`: Fixture project where one higher-layer candidate fails scoped validation.

## Task 1: Command And Internal Skill Contracts

**Files:**

- Modify: `commands/refactor-mart.md`
- Verify: `skills/applying-staging-candidate/SKILL.md`
- Create: `skills/applying-mart-candidates/SKILL.md`
- Create: `skills/applying-mart-candidates/references/mart-validation-contract.md`

- [x] **Step 1: Update `/refactor-mart` command contract**

In `commands/refactor-mart.md`, replace the `int` application bullet that mentions a future workflow with the concrete internal skill:

```md
- `int` mode: run the internal `applying-mart-candidates` skill for
  `<plan-file> <candidate-id>`.
````

Keep the dependency rules before the apply step and clarify that dependency blocking happens before edits:

````md
For `int` mode, check dependencies before applying each selected candidate and before editing any dbt files:

- `Depends on: none` is satisfied only when present exactly as written.
- If `Depends on:` is missing, empty, malformed, ambiguous, or otherwise does
  not clearly declare upstream candidate IDs or `none`, the command blocks the
  candidate before skill invocation.
- A dependency is satisfied only when the referenced candidate section has
  `Execution status: applied`.
- If any referenced candidate section is missing or any dependency is not
  `Execution status: applied`, the command blocks the candidate before skill
  invocation, records the missing or unsatisfied dependency IDs, and owns the
  writeback.
````

- [x] **Step 2: Verify staging skill remains internal**

Confirm `skills/applying-staging-candidate/SKILL.md` keeps:

```yaml
user-invocable: false
```

No code change is needed if that line is already present.

- [x] **Step 3: Write higher-layer validation contract**

Create `skills/applying-mart-candidates/references/mart-validation-contract.md`:

```md
# Mart Candidate Validation Contract

Use this contract when applying one approved non-staging candidate from a refactor-mart plan.

## Scope

- One candidate is scoped to one `int` or `mart` output model.
- `Validation:` is the validation command or scope only.
- Consumer rewrites happen only when explicitly named in the human-readable candidate text or when an unambiguous local output rewrite is required.
- Do not infer broad rewrites from selector syntax such as `+int_sales_orders`.
- Ambiguous rewrite scope blocks before edits.
- Do not validate or invalidate unrelated candidates.

## Status Values

Write back exactly one of these values in the candidate section:

- `Execution status: applied`
- `Execution status: failed`
- `Execution status: blocked`

Use `applied` only when edits were attempted and candidate-scoped validation passed. Add one short `Validation result:` bullet in the same candidate section describing the command or scope used.

Use `failed` when edits were attempted but candidate-scoped validation did not pass. Add one short `Validation result:` bullet in the same candidate section describing the failure.

Use `blocked` when required inputs are missing before edits begin, such as a missing output path, ambiguous validation scope, or a missing required model. Add one short `Blocked reason:` bullet in the same candidate section.

## Writeback Rules

- Update only the selected candidate section.
- Preserve candidate IDs, approval state, candidate order, dependency declarations, and unrelated candidate statuses.
- Do not add alternate status fields such as `Applied:`.
- Do not edit staging candidates from this skill.
- Dependency-blocked writeback belongs to `/refactor-mart`, not this skill.
- If this skill somehow receives dependency metadata that failed command-level gating, stop with `DEPENDENCY_GATE_NOT_SATISFIED` and do not change candidate status.

## Validation Scope

Validation is candidate-scoped:

- validate the changed `int` or `mart` output model;
- validate every additional existing model required by the candidate section;
- prefer the command or scope listed in `Validation:` when it is executable;
- capture the command or scope in `Validation result:`; and
- do not broaden validation to unrelated dbt assets.
```

- [x] **Step 4: Create internal higher-layer apply skill**

Create `skills/applying-mart-candidates/SKILL.md`:

````md
---
name: applying-mart-candidates
description: Use internally when one approved intermediate or mart candidate from a refactor-mart plan must be applied and validated across its declared scope
user-invocable: false
argument-hint: "<plan-file> <candidate-id>"
---

# Applying Mart Candidates

Apply exactly one approved higher-layer candidate from a refactor-mart plan, validate its declared scope, and update only that candidate section.

## Inputs

Parse exactly two positional arguments:

```text
<plan-file> <candidate-id>
```

Read `${CLAUDE_PLUGIN_ROOT}/skills/applying-mart-candidates/references/mart-validation-contract.md` before editing files or plan status.

## Guards

- If the plan file is missing, stop with `PLAN_NOT_FOUND`.
- If the candidate section is missing, stop with `CANDIDATE_NOT_FOUND`.
- If the candidate is not approved with `- [x] Approve: yes`, stop with `CANDIDATE_NOT_APPROVED`.
- If `Type:` is `stg`, reject it with `STAGING_CANDIDATE_NOT_ALLOWED` and do not change its execution status.
- If `Type:` is anything other than `int` or `mart`, stop with `INVALID_CANDIDATE_TYPE`.
- Dependency-blocked writeback belongs to `/refactor-mart`, not this skill. If this skill somehow receives dependency metadata that failed command-level gating, stop with `DEPENDENCY_GATE_NOT_SATISFIED` and leave candidate status unchanged.
- If `Output:` is missing or does not identify an `int_*` or mart-level model path or model name, mark only this candidate `blocked`.
- If `Validation:` is missing, ambiguous, or names missing required models, mark only this candidate `blocked`.

## Workflow

1. Read the markdown plan and isolate exactly one `## Candidate: <candidate-id>` section.
2. Extract `Type:`, `Output:`, `Depends on:`, and `Validation:`.
3. Verify every declared dependency is already `Execution status: applied` before editing any dbt file. If dependency metadata is missing, empty, malformed, ambiguous, or references any missing or unapplied dependency, stop with `DEPENDENCY_GATE_NOT_SATISFIED` and do not change candidate status.
4. Create or update the declared `int` or `mart` output model.
5. Rewire only consumers that are explicitly named in the candidate section's human-readable text or are an unambiguous direct consequence of the output change. Do not infer broad rewrites from selector syntax such as `+int_sales_orders`.
6. Use `Validation:` as the validation command or scope only. Run the smallest validation command that covers the changed output and any explicitly required existing consumer models. Prefer the command listed in `Validation:`.
7. Update only this candidate section:
   - `Execution status: applied` when validation passes.
   - `Execution status: failed` when attempted validation fails.
   - `Execution status: blocked` when required candidate inputs are missing before edits.
   - Add exactly one `Validation result:` bullet for `applied` or `failed`.
   - Add exactly one `Blocked reason:` bullet for `blocked`.
   - Do not add alternate status fields such as `Applied:`.

## Rewire Rules

- Rewire only models explicitly named in the candidate section's human-readable text or required by an unambiguous local output rewrite.
- Do not infer broad consumer rewrites from `Validation:` selector syntax.
- Do not edit staging candidates or staging outputs.
- Do not silently continue past failed or unapplied dependencies.

## Validation

Validation is candidate-scoped:

- validate the changed `int` or `mart` output model;
- validate each additional existing model required by the candidate section;
- capture the command or scope used in a `Validation result:` bullet; and
- do not alter unrelated candidate statuses after success, failure, or block.
- dependency-gated blocking is owned by `/refactor-mart`, not this skill.

If validation fails, keep attempted file edits in place only when they help the user inspect the failure, and mark this candidate `failed`.

## Completion

Report:

```text
applying-mart-candidates complete -- <candidate-id>

status: applied|failed|blocked
validation: <command or reason>
plan: <plan-file>
```
````

- [x] **Step 5: Run markdownlint on changed markdown**

Run:

```bash
markdownlint commands/refactor-mart.md skills/applying-staging-candidate/SKILL.md skills/applying-mart-candidates/SKILL.md skills/applying-mart-candidates/references/mart-validation-contract.md docs/superpowers/plans/2026-04-17-refactor-mart-higher-layer-execution.md
```

Expected: no errors.

- [x] **Step 6: Commit contract slice**

Run:

```bash
git add commands/refactor-mart.md skills/applying-mart-candidates/SKILL.md skills/applying-mart-candidates/references/mart-validation-contract.md docs/superpowers/plans/2026-04-17-refactor-mart-higher-layer-execution.md
git commit -m "VU-1104: add higher-layer refactor-mart contracts"
```

## Task 2: Higher-Layer Eval Assertions And Fixtures

**Files:**

- Create: `tests/evals/assertions/check-refactor-mart-mart-execution.js`
- Create: `tests/evals/assertions/check-refactor-mart-mart-execution.test.js`
- Create: `tests/evals/prompts/cmd-refactor-mart-int.txt`
- Modify: `tests/evals/packages/cmd-refactor-mart/cmd-refactor-mart.yaml`
- Create: `tests/evals/fixtures/cmd-refactor-mart/int-happy-path/**`
- Create: `tests/evals/fixtures/cmd-refactor-mart/int-blocked-dependency/**`
- Create: `tests/evals/fixtures/cmd-refactor-mart/int-validation-failure/**`

- [ ] **Step 1: Write assertion helper tests first**

Create `tests/evals/assertions/check-refactor-mart-mart-execution.test.js` with tests for:

```js
test('passes when higher-layer statuses, validation details, and refs match', () => {
  // Plan has STG-001 applied, INT-001 applied, MART-001 applied.
  // int_sales_orders references stg_bronze__orders.
  // fct_sales references int_sales_orders.
  // Expected result passes.
});

test('passes when blocked higher-layer candidates record blocked reasons', () => {
  // Plan has INT-001 blocked with Blocked reason.
  // Expected result passes.
});

test('fails when a staging candidate status changes during int mode', () => {
  // Plan has STG-001 changed from planned to applied in an int-mode assertion.
  // Expected result fails.
});

test('fails when applied higher-layer candidate omits validation result', () => {
  // Plan has INT-001 applied without Validation result.
  // Expected result fails.
});

test('fails when declared consumer does not reference expected model', () => {
  // fct_sales does not ref int_sales_orders.
  // Expected result fails.
});
```

Run:

```bash
node --test tests/evals/assertions/check-refactor-mart-mart-execution.test.js
```

Expected: fail because `check-refactor-mart-mart-execution.js` does not exist yet.

- [ ] **Step 2: Implement assertion helper**

Create `tests/evals/assertions/check-refactor-mart-mart-execution.js` by following the structure of `check-refactor-mart-stg-execution.js`, with these higher-layer-specific rules:

```js
const fs = require('fs');
const path = require('path');
const { normalizeTerms, resolveProjectPath } = require('./schema-helpers');

// Parse candidate sections for STG, INT, and MART IDs.
// Validate expected_candidate_statuses.
// Require Validation result for expected_validation_results.
// Require Blocked reason for expected_blocked_reasons.
// Validate expected_model_refs pairs such as "fct_sales:int_sales_orders".
// Fail if any STG candidate has a status other than the expected status supplied by the test.
// Fail if an applied INT output is not an int_ model.
// Fail if an applied MART output is not present under dbt/models/marts or its model name does not start with a mart/fact/dim-style prefix already used by the fixture.
```

The helper must not require staging outputs for higher-layer status checks unless the test explicitly includes them in expected refs.

- [ ] **Step 3: Run assertion unit tests**

Run:

```bash
node --test tests/evals/assertions/check-refactor-mart-mart-execution.test.js
```

Expected: pass.

- [ ] **Step 4: Add int-mode eval prompt**

Create `tests/evals/prompts/cmd-refactor-mart-int.txt`:

```text
You are running an eval harness for `/refactor-mart` higher-layer mode in the fixture project at `{{fixture_path}}`.

Run `/refactor-mart {{plan_file}} int`.
Read `${CLAUDE_PLUGIN_ROOT}/commands/refactor-mart.md`.
Read `${CLAUDE_PLUGIN_ROOT}/skills/applying-mart-candidates/SKILL.md`.
Apply only approved `Type: int` and `Type: mart` candidates whose dependencies are satisfied by the command gate.

Harness overrides:

- Treat `{{fixture_path}}` as read-only input. Write all generated files and plan updates to `{{run_path}}`.
- You are already authorized to edit files under `{{run_path}}`; do not ask for permission before updating the plan.
- Final-answer summaries are not sufficient. The authoritative markdown plan file is `{{run_path}}/{{plan_file}}` and it must be updated in place.
- Do not create, copy, or update a duplicate plan file outside `{{run_path}}/{{plan_file}}`.
- Do not create or use a worktree.
- Do not run git commands, `/commit`, `/commit-push-pr`, or `git-checkpoints`.
- Do not spawn sub-agents for this eval.
- Do not run dbt. Simulate validation by inspecting whether every model required by each candidate's validation scope exists in the run project after rewiring.
- If a model named in `Validation:` exists but contains `EVAL_VALIDATION_FAIL`, treat validation as failed.
- For eval fixtures, dependency gating happens before edits and is owned by `/refactor-mart`. If dependency metadata is missing, empty, malformed, ambiguous, or any listed dependency is missing or its `Execution status:` is not `applied`, `/refactor-mart` blocks the candidate, records the reason, and does not invoke the apply skill.
- For eval fixtures, a successful `Type: int` candidate must write or preserve the declared int model and a successful `Type: mart` candidate must write or preserve the declared mart model.
- For eval fixtures, declared downstream model refs must match the expected refs in the assertion variables.
- Do not apply `Type: stg` candidates, and do not change their execution status in `int` mode.

The eval expects the final answer to summarize applied, failed, blocked, and skipped counts.
```

- [ ] **Step 5: Add happy-path fixture**

Create fixture files under `tests/evals/fixtures/cmd-refactor-mart/int-happy-path/`:

```text
manifest.json
dbt/dbt_project.yml
dbt/profiles.yml
dbt/models/staging/stg_bronze__orders.sql
dbt/models/intermediate/int_sales_orders.sql
dbt/models/marts/fct_sales.sql
docs/design/refactor-mart-int-happy-path.md
```

The plan must start with `STG-001` already applied, then approved `INT-001`, then approved `MART-001` depending on `INT-001`.

- [ ] **Step 6: Add blocked dependency fixture**

Create fixture files under `tests/evals/fixtures/cmd-refactor-mart/int-blocked-dependency/`:

```text
manifest.json
dbt/dbt_project.yml
dbt/profiles.yml
dbt/models/staging/stg_bronze__orders.sql
dbt/models/intermediate/int_sales_orders.sql
dbt/models/marts/fct_sales.sql
docs/design/refactor-mart-int-blocked-dependency.md
```

The plan must include approved higher-layer candidates whose dependencies point to staging candidates with `Execution status: planned` and `Execution status: failed`. Expected result: higher-layer candidates become `blocked`, get `Blocked reason:`, and their outputs are not rewritten.

- [ ] **Step 7: Add validation failure fixture**

Create fixture files under `tests/evals/fixtures/cmd-refactor-mart/int-validation-failure/`:

```text
manifest.json
dbt/dbt_project.yml
dbt/profiles.yml
dbt/models/staging/stg_bronze__orders.sql
dbt/models/intermediate/int_sales_orders.sql
dbt/models/marts/fct_sales.sql
docs/design/refactor-mart-int-validation-failure.md
```

Include `EVAL_VALIDATION_FAIL` in one model named by the failing candidate's `Validation:` field. Expected result: attempted candidate becomes `failed` with `Validation result:`, while unrelated approved candidates with satisfied dependencies can still be applied or remain unchanged according to the fixture.

- [ ] **Step 8: Register eval cases**

Modify `tests/evals/packages/cmd-refactor-mart/cmd-refactor-mart.yaml`:

```yaml
prompts:
  - id: cmd-refactor-mart-stg
    label: cmd-refactor-mart-stg
    raw: file://../../prompts/cmd-refactor-mart-stg.txt
  - id: cmd-refactor-mart-int
    label: cmd-refactor-mart-int
    raw: file://../../prompts/cmd-refactor-mart-int.txt
```

Add three tests using `check-refactor-mart-mart-execution.js` as the assertion for only the int prompt cases:

```yaml
- description: "[smoke] int happy path -- approved higher-layer candidates apply after satisfied dependencies"
  vars:
    fixture_path: "tests/evals/fixtures/cmd-refactor-mart/int-happy-path"
    plan_file: "docs/design/refactor-mart-int-happy-path.md"
    expected_candidate_statuses: "STG-001:applied,INT-001:applied,MART-001:applied"
    expected_output_terms: "applied,int_sales_orders,fct_sales"
    expected_model_refs: "int_sales_orders:stg_bronze__orders,fct_sales:int_sales_orders"
    expected_validation_results: "INT-001,MART-001"

- description: "int blocked dependency -- higher-layer candidates block before edits"
  vars:
    fixture_path: "tests/evals/fixtures/cmd-refactor-mart/int-blocked-dependency"
    plan_file: "docs/design/refactor-mart-int-blocked-dependency.md"
    expected_candidate_statuses: "STG-001:planned,STG-002:failed,INT-001:blocked,MART-001:blocked"
    expected_output_terms: "blocked,skipped"
    expected_blocked_reasons: "INT-001,MART-001"

- description: "int validation failure -- failed candidate remains scoped"
  vars:
    fixture_path: "tests/evals/fixtures/cmd-refactor-mart/int-validation-failure"
    plan_file: "docs/design/refactor-mart-int-validation-failure.md"
    expected_candidate_statuses: "STG-001:applied,INT-001:failed,MART-001:blocked"
    expected_output_terms: "failed,blocked"
    expected_validation_results: "INT-001"
    expected_blocked_reasons: "MART-001"
```

- [ ] **Step 9: Run eval assertion tests**

Run:

```bash
node --test tests/evals/assertions/check-refactor-mart-stg-execution.test.js tests/evals/assertions/check-refactor-mart-mart-execution.test.js
```

Expected: pass.

- [ ] **Step 10: Run command eval**

Run:

```bash
cd tests/evals && npm run eval:cmd-refactor-mart
```

Expected: pass. If model nondeterminism causes a case to fail while the contract and assertion are correct, capture the failure and revise the prompt or fixture to remove ambiguity.

- [ ] **Step 11: Commit eval slice**

Run:

```bash
git add tests/evals/assertions/check-refactor-mart-mart-execution.js tests/evals/assertions/check-refactor-mart-mart-execution.test.js tests/evals/prompts/cmd-refactor-mart-int.txt tests/evals/packages/cmd-refactor-mart/cmd-refactor-mart.yaml tests/evals/fixtures/cmd-refactor-mart/int-happy-path tests/evals/fixtures/cmd-refactor-mart/int-blocked-dependency tests/evals/fixtures/cmd-refactor-mart/int-validation-failure
git commit -m "VU-1104: add refactor-mart higher-layer eval coverage"
```

## Task 3: Verification, Reviews, Linear Update, And Final Commit

**Files:**

- Modify: Linear issue `VU-1104`
- Verify: local git branch and worktree

- [ ] **Step 1: Run changed-area validation**

Run:

```bash
markdownlint commands/refactor-mart.md skills/applying-staging-candidate/SKILL.md skills/applying-mart-candidates/SKILL.md skills/applying-mart-candidates/references/mart-validation-contract.md docs/superpowers/plans/2026-04-17-refactor-mart-higher-layer-execution.md
node --test tests/evals/assertions/check-refactor-mart-stg-execution.test.js tests/evals/assertions/check-refactor-mart-mart-execution.test.js
cd tests/evals && npm run eval:cmd-refactor-mart
```

Expected: all commands pass.

- [ ] **Step 2: Run independent quality gates**

Dispatch independent reviewers with only the issue text, approved plan, changed-file diff, and verification results:

- code review
- simplification review
- test coverage review
- acceptance-criteria review

Expected: all reviewers approve or findings are fixed and re-reviewed.

- [ ] **Step 3: Update Linear acceptance criteria**

Update the main issue description in `VU-1104` so completed acceptance criteria are checked in place. Do not add a duplicate acceptance criteria section.

- [ ] **Step 4: Post Linear implementation note**

Post a final implementation comment on `VU-1104` summarizing:

- what was implemented;
- tests and evals run;
- manual checks, or `No manual tests required.`;
- code review, simplification review, test coverage review, and AC review outcomes;
- remaining risks, if any.

- [ ] **Step 5: Create final implementation commit if needed**

If Linear note or small final fixes leave a local diff, stage only the relevant files and commit:

```bash
git status --short
git add <specific-files>
git commit -m "VU-1104: finalize higher-layer refactor-mart execution"
```

Expected: no uncommitted files remain.

- [ ] **Step 6: Confirm clean handoff state**

Run:

```bash
git status --short --branch
git log --oneline --decorate -n 5
```

Expected: branch is `feature/vu-1104-add-refactor-mart-higher-layer-execution-with-dependency` and worktree is clean.

## Manual Test Scope

No manual tests required.

## Skill Or Plugin Eval Coverage

Run `cd tests/evals && npm run eval:cmd-refactor-mart` because this change modifies the `/refactor-mart` command and its internal apply-skill workflow.

## Checkpoint Commits

1. `VU-1104: add higher-layer refactor-mart contracts`
2. `VU-1104: add refactor-mart higher-layer eval coverage`
3. Optional final commit only if final fixes remain after reviews and Linear updates.

## Self-Review

- Spec coverage: Each VU-1104 acceptance criterion maps to command mode support, internal staging rejection, dependency gating, blocked status writeback, higher-layer apply workflow, candidate-scoped validation, final summary semantics, and eval coverage tasks above.
- Placeholder scan: No unfinished placeholder language remains.
- Type consistency: Skill name is consistently `applying-mart-candidates`; command mode remains `int`; candidate types remain `stg`, `int`, and `mart`.
