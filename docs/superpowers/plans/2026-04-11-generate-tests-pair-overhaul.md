# Generate Tests Pair Overhaul Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `plugin/commands/generate-tests.md`, `plugin/skills/generating-tests`, and `plugin/skills/reviewing-tests` behave as a reliable package for existing-artifact review, targeted repair, and pair-level eval enforcement.

**Architecture:** Keep the existing generate -> review -> optional repair -> re-review loop, but tighten the behavioral contract where the generator consumes reviewer feedback and where the reviewer emits machine-consumable evidence. Fix eval harness failures separately from semantic skill failures so package pass rates remain meaningful.

**Tech Stack:** Markdown skill docs, Promptfoo eval packages, JavaScript assertion scripts, fixture-based eval inputs, SQLite Promptfoo DB.

---

## Task 1: Plan And Baseline

**Files:**

- Create: `docs/superpowers/plans/2026-04-11-generate-tests-pair-overhaul.md`
- Read: `plugin/commands/generate-tests.md`
- Read: `plugin/skills/generating-tests/SKILL.md`
- Read: `plugin/skills/reviewing-tests/SKILL.md`
- Read: `tests/evals/packages/generating-tests/skill-generating-tests.yaml`
- Read: `tests/evals/packages/reviewing-tests/skill-reviewing-tests.yaml`

- [x] **Step 1: Capture the package contract**

The package invariants are:

```text
1. Existing specs must still enter merge mode, review, and repair.
2. Reviewer evidence must be independent of generator branch manifests.
3. Feedback-driven reruns must repair named gaps narrowly.
4. Full-package eval failures must distinguish semantic failures from harness transport failures.
```

- [x] **Step 2: Record the concrete failing cases**

Use:

```bash
sqlite3 ~/.promptfoo/promptfoo.db "select id, description from evals order by created_at desc limit 20;"
```

Expected: identify the current `reviewing-tests`, `generating-tests`, and `cmd-generate-tests` runs plus their failure reasons.

## Task 2: Fix Reviewing-Tests Transport Failure

**Files:**

- Modify: `tests/evals/assertions/check-test-review.js`
- Modify: `tests/evals/packages/reviewing-tests/skill-reviewing-tests.yaml`
- Test: `tests/evals/prompts/skill-reviewing-tests.txt`

- [ ] **Step 1: Reproduce the iteration-2 failure**

Run:

```bash
cd tests/evals
./scripts/promptfoo.sh eval --no-cache -c packages/reviewing-tests/skill-reviewing-tests.yaml --filter-pattern 'review-approved-with-warnings' --max-concurrency 1 --no-table -o /tmp/review-approved-with-warnings.json
```

Expected: fail with either missing review JSON/artifact or a more specific runtime transport error.

- [ ] **Step 2: Inspect whether the output contains parseable JSON even when the artifact is missing**

Run:

```bash
jq '.results.results[0].response, .results.results[0].gradingResult' /tmp/review-approved-with-warnings.json
```

Expected: determine whether `check-test-review.js` should accept response JSON when `.staging/review.json` was not persisted.

- [ ] **Step 3: Implement the minimal harness fix**

If the raw response contains valid review JSON, update `check-test-review.js` to prefer extracted response JSON and only fail when neither response JSON nor artifact JSON is available.

- [ ] **Step 4: Re-run the targeted reviewing eval**

Run:

```bash
cd tests/evals
./scripts/promptfoo.sh eval --no-cache -c packages/reviewing-tests/skill-reviewing-tests.yaml --filter-pattern 'review-approved-with-warnings' --max-concurrency 1 --no-table -o /tmp/review-approved-with-warnings-rerun.json
```

Expected: PASS or a semantic failure reason instead of transport failure.

## Task 3: Tighten Generator Narrow-Repair Behavior

**Files:**

- Modify: `plugin/skills/generating-tests/SKILL.md`
- Modify: `tests/evals/prompts/skill-generating-tests.txt`
- Modify: `tests/evals/prompts/skill-generating-tests-view.txt`
- Test: `tests/evals/packages/generating-tests/skill-generating-tests.yaml`

- [ ] **Step 1: Reproduce the targeted repair failure**

Run:

```bash
cd tests/evals
./scripts/promptfoo.sh eval --no-cache -c packages/generating-tests/skill-generating-tests.yaml --filter-pattern 'feedback-targeted-repair' --max-concurrency 1 --no-table -o /tmp/generating-feedback-red.json
```

Expected: fail because unrelated scenarios such as `test_color_empty_when_null` are added.

- [ ] **Step 2: Tighten the generator instructions only where the model is slipping**

Add explicit rules that feedback-driven runs must:

```text
- preserve unrelated unit_tests verbatim
- preserve unrelated branch-to-scenario mappings
- avoid filling pre-existing uncovered branches unless the reviewer requested them
- prefer partial coverage over broad regeneration when the repair request is narrow
```

- [ ] **Step 3: Re-run the targeted generating eval**

Run:

```bash
cd tests/evals
./scripts/promptfoo.sh eval --no-cache -c packages/generating-tests/skill-generating-tests.yaml --filter-pattern 'feedback-targeted-repair' --max-concurrency 1 --no-table -o /tmp/generating-feedback-green.json
```

Expected: PASS with the requested branch repaired and unrelated rejected tests absent.

## Task 4: Verify Command-Level Existing-Artifact Repair Loop

**Files:**

- Read: `plugin/commands/generate-tests.md`
- Test: `tests/evals/packages/cmd-generate-tests/cmd-generate-tests.yaml`

- [ ] **Step 1: Run the command-level targeted loop case**

Run:

```bash
cd tests/evals
./scripts/promptfoo.sh eval --no-cache -c packages/cmd-generate-tests/cmd-generate-tests.yaml --filter-pattern 'review-loop-targeted-repair' --max-concurrency 1 --no-table -o /tmp/cmd-review-loop.json
```

Expected: either a clean two-iteration pass or a concrete failure showing whether the command is dropping review feedback, broadening repair, or misreporting summary status.

- [ ] **Step 2: Adjust only the proven weak point**

If the failure is:

```text
- command summary only: fix `check-command-summary.js` or YAML expectations
- dropped feedback: tighten `plugin/commands/generate-tests.md`
- broad repair: keep the fix in generating-tests, not in the command
```

## Task 5: Full-Package Verification

**Files:**

- Test: `tests/evals/packages/reviewing-tests/skill-reviewing-tests.yaml`
- Test: `tests/evals/packages/generating-tests/skill-generating-tests.yaml`

- [ ] **Step 1: Run full reviewing-tests**

Run:

```bash
cd tests/evals
./scripts/promptfoo.sh eval --no-cache -c packages/reviewing-tests/skill-reviewing-tests.yaml --no-table -o /tmp/reviewing-tests-full-final.json
```

Expected: zero transport failures and a stable semantic result set.

- [ ] **Step 2: Run full generating-tests**

Run:

```bash
cd tests/evals
./scripts/promptfoo.sh eval --no-cache -c packages/generating-tests/skill-generating-tests.yaml --no-table -o /tmp/generating-tests-full-final.json
```

Expected: targeted repair case included in package-level pass results.

- [ ] **Step 3: Pull final stats from Promptfoo DB**

Run:

```bash
sqlite3 ~/.promptfoo/promptfoo.db "
select eval_id, count(*) as total,
sum(case when success=1 then 1 else 0 end) as passed,
sum(case when success=0 then 1 else 0 end) as failed
from eval_results
where eval_id in ('<reviewing-eval-id>','<generating-eval-id>')
group by eval_id;"
```

Expected: final package pass/fail counts plus any remaining concrete failure reasons.
