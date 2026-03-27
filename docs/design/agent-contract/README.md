# Agent Contract

Contracts for the **batch GHA pipeline**: multi-agent ETL migration from SQL Server stored procedures to dbt models. These contracts govern the six LLM agents that run in GitHub Actions and whose outputs the desktop app displays and routes.

For the complementary interactive single-table path, see [SP → dbt Migration Plugin](../sp-to-dbt-plugin/README.md). The plugin's Python skills (`scope.py`, `assess.py`, `migrate.py`, `test_gen.py`) implement the deterministic parts of the pipeline below; the batch agents delegate to these skills where applicable and handle the judgment-heavy steps.

All contracts are batch-only. Single-table UI execution is a degenerate batch with one `items[]` element.

## Identifier Semantics

- `item_id` is the single canonical identifier for one table migration item across all stages.
- In this flow, one table maps to one migration item and one dbt model.

## Approval Ownership

- The application owns FDE review and approval workflows.
- Agents consume only approved inputs.
- Approved inputs may come from:
  - agent-generated candidates that FDE reviewed/approved
  - direct FDE-entered values approved in the app
- The application owns cross-agent routing/filtering between stages.
- Non-actionable items (for example `ambiguous_multi_writer`, `partial`, `no_writer_found`, `error`)
  are handled by the application, either by manual resolution or by removing the table from scope.

## Flow

1. Scoping: analysis agent maps target table to writer procedure candidate(s) and selects writer when resolvable.
2. Profiling: profiler agent proposes candidate migration decisions for FDE approval.
3. Decomposition: decomposer agent segments selected writer SQL into reusable logical blocks and split points.
4. Planning: planner agent consumes approved answers + approved decomposition, then produces materialization, tests, and documentation intent.
5. Test Generation: test generator agent produces branch-covering `unit_tests:` YAML fixtures from the planner output.
6. Migration: migrator agent converts planner output + test generator fixtures into dbt artifacts using tool-fetched facts.

## Workflow

- [Scoping Agent](scoping-agent.md) - input/output contract for table-to-writer procedure mapping.
- [Profiler Agent](profiler-agent.md) - required input and output schema for candidate generation. See [What to Profile and Why](what-to-profile-and-why.md) for rationale and detection options per field.
- [Decomposer Agent](decomposer-agent.md) - required input and output schema for SQL decomposition and model split-point proposals.
- [Planner Agent](planner-agent.md) - required input and output schema for design manifest generation.
- [Test Generator Agent](test-generator-agent.md) - required input and output schema for branch-covering fixture generation. See [Unit Test Strategy](../unit-test-strategy/) for the original design rationale and harness details (batch path only; for the interactive path `test_gen.py` uses AST-based inference without a live DB).
- [Migrator Agent](migrator-agent.md) - required input and output schema for dbt artifact generation.

## Contract Boundary

- Scoping output is minimal writer discovery/selection data.
- Profiler output is candidate proposals that require FDE judgment.
- Decomposer output is SQL decomposition proposals (logical blocks + split points).
- Planner captures final decisions, carries approved decomposition unchanged, and produces test plan/documentation metadata.
- Test generator fetches proc SQL via tools, generates synthetic fixtures, captures ground-truth proc output, and emits `unit_tests:` YAML blocks. It does not write dbt files.
- Migrator fetches direct facts via tools and materializes dbt files from planner output, incorporating test generator fixtures.
- Upstream agents should not emit fields that downstream stages can derive or fetch reliably.
- All agent outputs use `results[]` as the top-level per-item collection key.

## Workflow Semantics

- Scoping and profiling produce machine-readable outputs with per-item status and errors.
- FDE approval is the decision gate between profiler candidates and planner/migrator execution.
- Validation findings should be surfaced as structured warnings/errors.
- Scope stage submission contract:
  - submission payload includes `items[]` where each element carries an `item_id`.
  - submission payload includes `search_depth`; default is `2` when not explicitly provided.

## Diagnostics Schema

The following object schema is shared across `validation.issues[]`, `warnings[]`, and `errors[]`.

```json
{
  "code": "ANALYSIS_SELECTED_WRITER_NOT_IN_CANDIDATES",
  "message": "selected_writer must exist in candidate_writers when status is resolved.",
  "field": "selected_writer",
  "severity": "error|warning",
  "details": {}
}
```

Field requirements:

- `code`: stable machine-readable identifier.
- `message`: human-readable description.
- `field`: optional field path associated with the issue (empty or omitted for non-field errors).
- `severity`: `error` or `warning`.
- `details`: optional structured context object.

Usage rules:

- `validation.issues[]`: contract/internal consistency findings from validation checks.
- `warnings[]`: non-fatal execution or generation warnings.
- `errors[]`: fatal item-level failures.
