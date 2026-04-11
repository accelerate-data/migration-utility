# Generating-Tests Skill TDD Notes

This note records how `plugin/skills/generating-tests` should be evaluated under the `writing-skills` standard.

## Current State

The repo already has strong GREEN-style application coverage:

- runnable package: `tests/evals/packages/generating-tests/skill-generating-tests.yaml`
- scenario fixtures: `tests/evals/fixtures/generating-tests/`
- command-level orchestration coverage: `tests/evals/packages/cmd-generate-tests/cmd-generate-tests.yaml`

What is still missing is explicit RED and REFACTOR evidence for the skill as process documentation.

## RED Scenarios To Capture

These scenarios should be run without loading `plugin/skills/generating-tests/SKILL.md`, then documented with the agent's verbatim mistakes and rationalizations.

| Failure mode | Expected baseline miss |
|---|---|
| Stage guard skipped | Agent writes a spec before `migrate-util ready <fqn> test-gen` passes |
| Merge-mode overwrite | Agent rewrites or drops existing `expect` blocks instead of merging |
| Schema invention | Agent emits unsupported fields or malformed `TestSpec` JSON |
| Stale manifest trust | Agent copies old `branch_manifest` instead of re-extracting from current SQL |
| MV collapse | Agent emits `object_type = view` for materialized views |
| Feedback ignored | Agent treats `feedback_for_generator` as advisory and leaves requested fixes undone |

## GREEN Checks

After the skill is rewritten, rerun:

```bash
cd tests/evals && npm run eval:generating-tests
```

Add targeted filtered runs for any newly added scenarios:

```bash
cd tests/evals
npx promptfoo eval -c packages/generating-tests/skill-generating-tests.yaml --filter-pattern "idempotent"
```

## REFACTOR Checks

After RED findings are known, the skill should explicitly close those loopholes. Re-test at least these rationalization classes:

- "existing spec means I can skip re-extraction"
- "reviewer owns coverage, so spec coverage is optional"
- "materialized views can be treated as normal views"
- "quality fixes mean regenerate everything"

## Success Standard

The skill is compliant with `writing-skills` when all of the following are true:

1. RED baseline failures are documented in repo-visible artifacts.
2. The skill text addresses the actual observed failures, not hypothetical ones.
3. GREEN evals pass with the rewritten skill.
4. REFACTOR checks cover previously observed loopholes and continue to pass.
