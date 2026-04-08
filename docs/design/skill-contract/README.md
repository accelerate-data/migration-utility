# Skill Contract

Contracts for the migration pipeline: ETL migration from SQL Server stored procedures to dbt models. These contracts govern the skills that process one table at a time.

The shared Python CLIs (`discover.py`, `profile.py`, `migrate.py`, `test_harness.py`) implement the deterministic parts of the pipeline below; the skills delegate to these CLIs where applicable and handle the judgment-heavy steps.

## Identifier Semantics

- `item_id` is the single canonical identifier for one table migration item across all stages.
- In this flow, one table maps to one migration item and one dbt model.

## Flow

1. **Scoping:** scoping skill calls `discover refs` per table for catalog-based writer identification (`is_updated=true`), procedure analysis (via procedure-analysis reference), and writer resolution. Writes scoping results to `catalog/tables/<table>.json` (scoping section) and resolved statements to `catalog/procedures/<writer>.json`.
2. **Profiling:** profiler skill assembles context, applies LLM reasoning over the six profiling questions, and persists results to catalog. FDE reviews before proceeding.
3. **Sandbox setup:** `test-harness sandbox-up` creates a throwaway database by cloning schema and procedures from a live SQL Server. This is a CLI command, not a skill. Requires live DB connection.
4. **Test generation:** test generator skill reads the same context as the model-generator (proc body, statements, profile, columns, source tables). It enumerates branches, synthesizes fixtures, executes the proc in the sandbox via MCP to capture ground truth, and writes `unit_tests:` JSON to `test-specs/<item_id>.json`. The test reviewer skill independently enumerates branches, scores coverage, and reviews fixture quality. It can kick back to the test generator for missing branches or quality issues. Maximum 2 review / generator iterations. When procs need parameters, the skill infers defaults or asks the FDE inline.
5. **SQL refactoring:** refactoring-sql skill converts the stored procedure (or view) SQL into an import/logical/final CTE pattern using two isolated sub-agents. Proves equivalence via sandbox execution against the test spec. Writes `refactored_sql` to the catalog. Self-corrects up to 3 iterations if scenarios fail.
6. **Migration:** model-generator skill generates dbt model + schema YAML (with `unit_tests:` rendered). Runs `dbt test` and self-corrects until tests pass (max 3 iterations). May also create additional tests beyond the spec. The code reviewer skill then checks standards, correctness, and test integration. It can kick back to the model-generator for issues. Maximum 2 review / model-generator iterations.
7. **Sandbox teardown:** `test-harness sandbox-down` drops the throwaway database.

## Workflow

- [Scoping Skill](scoping.md) — writer discovery and resolution per table.
- [Profiler Skill](profiler.md) — context assembly and LLM profiling per table. See [What to Profile and Why](what-to-profile-and-why.md) for the LLM reference tables.
- [Test Generator Skill](test-generator.md) — synthesizes fixtures, executes proc in sandbox, captures ground truth, writes `unit_tests:` JSON to `test-specs/`.
- [Test Reviewer Skill](test-reviewer.md) — LLM-based quality gate for test generation. Independently enumerates branches, scores coverage, reviews fixture quality. Can kick back to test generator.
- [Refactoring SQL Skill](refactoring-sql.md) — converts stored procedure or view SQL into import/logical/final CTEs. Two isolated sub-agents (extract + restructure) with sandbox equivalence proof. Writes `refactored_sql` to catalog.
- [Model Generator Skill](model-generator.md) — generates dbt artifacts, runs `dbt test`, self-corrects.
- [Code Reviewer Skill](code-reviewer.md) — LLM-based quality gate for migration output. Reviews standards, correctness, test integration. Can kick back to model-generator.

## Contract Boundary

- Scoping writes results to `catalog/tables/<table>.json` (scoping section), not a separate output file.
- Profiler writes to `catalog/tables/<table>.json` (profile section). FDE reviews before proceeding.
- Test generator synthesizes fixtures, executes procs in a sandbox, captures ground truth, and writes `unit_tests:` JSON to `test-specs/`. It does not write dbt files.
- Test reviewer independently scores coverage and fixture quality. It does not generate fixtures or modify `test-specs/`.
- Refactoring-sql reads the test spec and sandbox to prove equivalence. Writes `refactored_sql` (CTE-structured T-SQL) to the catalog. Does not convert dialect or generate dbt files.
- Model generator reads profile, statements, and approved test spec. Generates dbt artifacts, renders `unit_tests:` into schema YAML, runs `dbt test`, and self-corrects.
- Code reviewer checks standards, correctness, and test integration. It does not modify files.
- Upstream skills should not emit fields that downstream stages can derive or fetch reliably.

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
