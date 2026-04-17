---
name: refactoring-sql
description: >
  Use when stored procedure, view, or materialized-view SQL must be restructured
  into import/logical/final CTEs before proof-backed migration persistence.
user-invocable: false
argument-hint: "<schema.object> — Table, View, or Materialized View FQN"
---

# Refactoring SQL

Refactor source SQL into import/logical/final CTEs, treat the extracted SQL as ground truth, and persist only proof-backed results.

## When to Use

Use this skill when:

- a stored procedure, view, or materialized view must be restructured before downstream model generation
- the `refactor` section must be backed by semantic review and, when available, executable compare
- sandbox execution may be unavailable and the run must fall back cleanly to semantic-review-only proof

Do not use this skill when:

- the `refactor` readiness guard fails
- the task is generating dbt models or test specs rather than restructuring source SQL
- the object already has a proof-backed refactor and the user did not ask for a rerun

## Quick Reference

| Item | Rule |
|---|---|
| Guard | `uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate-util ready refactor --object <table_fqn>` |
| Context | `uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" refactor context --table <table_fqn>` |
| Compare gate | Run `test-harness sandbox-status`; if unavailable or explicitly skipped, persist with `--no-compare-required` |
| Contracts | Use [references/context-fields.md](references/context-fields.md), [references/refactor-contracts.md](references/refactor-contracts.md), and [`../../lib/shared/refactor_error_codes.md`](../../lib/shared/refactor_error_codes.md) |
| Ground truth | Never modify the extracted SQL during self-correction |
| Style | Apply shared SQL and CTE style only; dbt model layer, YAML, and source-wrapper decisions belong to generating/reviewing model workflows |

## Implementation

1. Run the readiness guard. If `ready` is `false`, stop and report the returned `code` and `reason`.
2. Run `refactor context --table <table_fqn>`. Use [references/context-fields.md](references/context-fields.md) for which fields to pass to each sub-agent. If context assembly fails, map the failure using [references/refactor-contracts.md](references/refactor-contracts.md) and do not expose raw internal labels.
3. Create `.staging/`, then launch Sub-agent A and Sub-agent B in parallel using [references/sub-agent-prompts.md](references/sub-agent-prompts.md). They must not see each other’s output. Sub-agent B must follow [../_shared/references/sql-style.md](../_shared/references/sql-style.md) and [../_shared/references/cte-structure.md](../_shared/references/cte-structure.md), produce a literal `final AS (...)` CTE, and end with `SELECT * FROM final`.
4. Write `.staging/<table_fqn>-extracted.sql` and `.staging/<table_fqn>-refactored.sql`.
5. Run semantic review against extracted SQL vs refactored SQL only. The result must match [references/refactor-contracts.md](references/refactor-contracts.md). If semantic review fails, persist with `--no-compare-required`; this should normally remain `partial`.
6. If the caller did not skip compare and `test-harness sandbox-status` succeeds, run `compare-sql` with `.staging/<table_fqn>-extracted.sql`, `.staging/<table_fqn>-refactored.sql`, and `test-specs/<table_fqn>.json`, then write `.staging/<table_fqn>-compare.json`.
7. If any compare scenario fails, revise only the refactored SQL, rerun semantic review and compare, and stop after 3 iterations. The extracted SQL remains ground truth throughout.
8. Persist with `refactor write`: use `--compare-sql-file` when compare passed, otherwise use `--no-compare-required`. Do not invent fields or override the persisted status.
9. After successful write-back, remove `.staging/` and report the extracted SQL, refactored SQL, semantic review result, compare result or skip reason, and final persisted status.

## Common Mistakes

- Writing to `.staging` before creating it.
- Editing extracted SQL during self-correction.
- Comparing refactored SQL to dbt expectations instead of extracted SQL.
- Treating `final` as a conceptual last step instead of a literal CTE name.
- Using `SELECT *` inside import, logical, or final CTE definitions.
- Blocking on sandbox access when semantic-review-only fallback is allowed.
- Inventing surfaced codes or payload fields outside the canonical `/refactor` contract.
- Making dbt layer, folder, YAML, or `ref()`/`source()` placement decisions in this skill. This skill outputs refactored SQL only.

## References

- [references/context-fields.md](references/context-fields.md) — table/view context fields and how to pass them to sub-agents
- [references/routine-migration-ref.md](references/routine-migration-ref.md) — dialect-routed DML extraction and CTE restructuring rules
- [references/sub-agent-prompts.md](references/sub-agent-prompts.md) — Sub-agent A and B prompt templates
- [references/refactor-contracts.md](references/refactor-contracts.md) — persisted payload keys, semantic-review contract, and command error mapping
