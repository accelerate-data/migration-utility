# Agent Contract

Contracts for the **batch pipeline**: multi-agent ETL migration from SQL Server stored procedures to dbt models. These contracts govern the six LLM agents that run in local batch mode (via `/batch-run` command) and GHA.

The shared Python CLIs (`discover.py`, `profile.py`, `migrate.py`, `test_harness.py`) implement the deterministic parts of the pipeline below; the batch agents delegate to these CLIs where applicable and handle the judgment-heavy steps. The interactive single-table path uses the same CLIs via skills (`/scoping-table`, `/analyzing-object`, `/profiling-table`, `/generating-model`).

All contracts are batch-only. Single-table interactive execution uses skills, not agents.

## Identifier Semantics

- `item_id` is the single canonical identifier for one table migration item across all stages.
- In this flow, one table maps to one migration item and one dbt model.

## Approval Ownership

- In batch mode, the `/batch-run` command owns FDE review at stage boundaries. Within a stage, agents run autonomously.
- In interactive mode, skills own per-step approval.
- Agents consume only approved inputs (catalog data from prior stages that the FDE has reviewed).
- Non-actionable items (for example `ambiguous_multi_writer`, `partial`, `no_writer_found`, `error`) are surfaced by the `/batch-run` command for FDE resolution (or via `migrate-util resolve` CLI).

## Invocation Paths

Agents and interactive users share the same Python CLI (`discover`) but consume its output differently:

- **Agent path:** `uv run discover <subcommand>` via Bash — returns structured JSON to stdout for programmatic parsing.
- **Interactive path:** `/listing-objects`, `/scoping-table`, `/analyzing-object` skills — the skill workflows format JSON output into human-readable text.

Agent definitions must not declare `skills:` for discover. Use `uv run` commands directly.

## Flow

1. **Scoping:** scoping agent delegates to `/scoping-table` skill per table for catalog-based writer identification (`is_updated=true`), procedure analysis (via `/analyzing-object`), and writer resolution. Writes scoping results to `catalog/tables/<table>.json` (scoping section) and resolved statements to `catalog/procedures/<writer>.json`.
2. **Profiling:** profiler agent delegates to `/profiling-table` skill per table for context assembly, LLM reasoning over the six profiling questions, and catalog persistence. FDE reviews at stage boundary before proceeding.
3. **Sandbox setup:** `test-harness sandbox-up` creates a throwaway database (`__test_<run_id>`) by cloning schema and procedures from a live SQL Server. This is a CLI command, not an agent. Requires live DB connection.
4. **Test generation:** test generator agent reads the same context as the model-generator (proc body, statements, profile, columns, source tables). It enumerates branches, synthesizes fixtures, executes the proc in the sandbox via MCP to capture ground truth, and writes `unit_tests:` JSON to `test-specs/<item_id>.json`. The test reviewer agent independently enumerates branches, scores coverage, and reviews fixture quality. It can kick back to the test generator for missing branches or quality issues. Maximum 2 review ↔ generator iterations. When procs need parameters, the agent infers defaults or asks the FDE inline.
5. **Migration:** model-generator agent delegates to `/generating-model` skill per item. Generates dbt model + schema YAML (with `unit_tests:` rendered). Runs `dbt test` and self-corrects until tests pass (max 3 iterations). May also create additional tests beyond the spec. The code reviewer agent then checks standards, correctness, and test integration. It can kick back to the model-generator for issues. Maximum 2 review ↔ model-generator iterations.
6. **Sandbox teardown:** `test-harness sandbox-down` drops the throwaway database.

## Workflow

- [Scoping Agent](scoping.md) — delegates to `/scoping-table` per table for writer discovery and resolution.
- [Profiler Agent](profiler.md) — delegates to `/profiling-table` per table for context assembly and LLM profiling. See [What to Profile and Why](what-to-profile-and-why.md) for the LLM reference tables.
- [Test Generator Agent](test-generator-agent.md) — synthesizes fixtures, executes proc in sandbox, captures ground truth, writes `unit_tests:` JSON to `test-specs/`. Lives in the `ground-truth-harness` plugin. Interactive path: `/generate-tests` skill.
- [Test Reviewer Agent](test-reviewer-agent.md) — LLM-based quality gate for test generation. Independently enumerates branches, scores coverage, reviews fixture quality. Can kick back to test generator. Lives in the `ground-truth-harness` plugin.
- [Model Generator Agent](model-generator.md) — delegates to `/generating-model` per item. Generates dbt artifacts, runs `dbt test`, self-corrects. Lives in the `migration` plugin.
- [Code Reviewer Agent](code-reviewer-agent.md) — LLM-based quality gate for migration output. Reviews standards, correctness, test integration. Can kick back to model-generator. Lives in the `migration` plugin.

## Contract Boundary

- Scoping writes results to `catalog/tables/<table>.json` (scoping section), not a separate output file. No summary JSON — the `migrate-util status` command derives status from catalog.
- Profiler writes to `catalog/tables/<table>.json` (profile section). FDE reviews at stage boundary.
- Test generator synthesizes fixtures, executes procs in a sandbox, captures ground truth, and writes `unit_tests:` JSON to `test-specs/`. It does not write dbt files.
- Test reviewer independently scores coverage and fixture quality. It does not generate fixtures or modify `test-specs/`.
- Model generator reads profile, statements, and approved test spec. Generates dbt artifacts, renders `unit_tests:` into schema YAML, runs `dbt test`, and self-corrects.
- Code reviewer checks standards, correctness, and test integration. It does not modify files.
- Upstream agents should not emit fields that downstream stages can derive or fetch reliably.
- All agent outputs use `results[]` as the top-level per-item collection key.

## Workflow Semantics

- Scoping and profiling produce machine-readable outputs with per-item status and errors.
- FDE approval is the decision gate between profiler candidates and model-generator execution.
- Validation findings should be surfaced as structured warnings/errors.
- Scope stage submission contract:
  - submission payload includes `items[]` where each element carries an `item_id`.
  - submission payload includes `search_depth`; default is `2` when not explicitly provided.

## Diagnostics Schema

The following object schema is shared across `validation.issues[]`, `warnings[]`, and `errors[]`.

```json
{
  "code": "SCOPING_NOT_COMPLETED",
  "message": "scoping section missing or no selected_writer in catalog for silver.dimcustomer.",
  "item_id": "silver.dimcustomer",
  "severity": "error",
  "details": {}
}
```

Field requirements:

- `code`: stable machine-readable identifier.
- `message`: human-readable description.
- `item_id`: fully qualified table name this entry relates to.
- `field`: optional field path associated with the issue (empty or omitted for non-field errors).
- `severity`: `error` or `warning`.
- `details`: optional structured context object.

Usage rules:

- `validation.issues[]`: contract/internal consistency findings from validation checks.
- `warnings[]`: non-fatal execution or generation warnings.
- `errors[]`: fatal item-level failures.
