---
name: migrate-table
description: >
  End-to-end orchestrator for migrating a single stored procedure to a dbt model.
  Invoke when the user asks to "migrate a table", "convert a procedure to dbt",
  or "run the full migration pipeline". Coordinates discover, profile, and migrate
  skills in sequence with user gates.
user-invocable: true
argument-hint: "[ddl-path] [--table <fqn>] [--dbt-project-path <path>] [--non-interactive]"
---

# Migrate Table

Orchestrates the migration pipeline for a single table: discover the writer procedure, profile the table, generate a dbt model, and write artifacts. Each stage has user approval gates (unless `--non-interactive`).

## Arguments

| Argument | Required | Description |
|---|---|---|
| `ddl-path` | yes | Path to DDL artifacts directory |
| `--table` | no | Target table FQN — interactive picker if omitted |
| `--dbt-project-path` | no | Path to dbt project — auto-detected from `<ddl-path>/../dbt/` if omitted |
| `--non-interactive` | no | Skip all confirmation gates (for GHA/batch use) |

## Prerequisite check

Before starting the migration pipeline:

1. Confirm `<ddl-path>/manifest.json` exists — if not, stop: "Run `/setup-ddl` first."
2. Confirm a dbt project exists at `--dbt-project-path` or `<ddl-path>/../dbt/` — if not, stop: "Run `/init-dbt` first."
3. Confirm `catalog/` directory has table and procedure files — if not, stop: "Run `/setup-ddl` with `--catalog` first."

## Pipeline

### Stage 1: Discover — select table and writer

#### 1a. List tables

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover list \
  --ddl-path <ddl-path> --type tables
```

If `--table` was provided, skip the picker. Otherwise, present the table list and use `AskUserQuestion` to let the user pick.

#### 1b. Find writers

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover refs \
  --ddl-path <ddl-path> --name <selected_table>
```

Present the writers list. If exactly one writer, auto-select it and confirm with the user. If multiple writers, ask the user to pick one. If no writers found, stop with an error.

**Gate (interactive):** User confirms the selected writer procedure.

**Gate (non-interactive):** Auto-select if one writer; error if zero or multiple.

#### 1c. Show writer details and resolve statements

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover show \
  --ddl-path <ddl-path> --name <selected_writer>
```

Check `classification`:

- **`deterministic`**: Statements are already resolved. Persist to catalog automatically:

  ```bash
  uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover write-statements \
    --ddl-path <ddl-path> --name <selected_writer> --statements '<json>'
  ```

- **`claude_assisted`**: Read the `raw_ddl`, resolve each `claude`-action statement by following the call graph and classifying as `migrate` or `skip`. Present the resolved statements for user confirmation, then persist.

**Gate (interactive):** For `claude_assisted` procedures, user confirms resolved statements.

**Gate (non-interactive):** Auto-resolve and persist without confirmation.

### Stage 2: Profile

Run the `/profile` skill against the selected table and writer. This assembles catalog signals, runs LLM profiling (classification, keys, watermark, PII), and writes results to catalog.

**Gate (interactive):** User approves profile answers (classification, primary key, watermark, foreign keys, PII actions).

**Gate (non-interactive):** Auto-approve and write to catalog.

### Stage 3: Migrate

Run the `/migrate` skill against the selected table and writer. This assembles migration context, generates dbt SQL via LLM, runs logical equivalence check, and writes artifacts.

**Gate (interactive):** User approves generated dbt model and schema YAML before writing.

**Gate (non-interactive):** Auto-approve and write.

## Error handling

| Error | Behavior |
|---|---|
| Any skill exits non-zero | Surface the error message to the user. In interactive mode, ask if they want to retry or abort. In non-interactive mode, abort the pipeline. |
| Missing prerequisite (no profile, no statements) | Tell the user which prerequisite is missing and which command to run. |
| `dbt compile` fails after model write | Show the compile error. In interactive mode, offer to edit the model. In non-interactive mode, record as partial success. |

## Completion

After the pipeline completes, report:

```text
Migration complete for <table_fqn>:

  Writer:          <writer_fqn>
  Classification:  <profile_classification>
  Materialization: <materialization>
  Model:           <model_sql_path>
  Schema:          <schema_yml_path>
  dbt compile:     <passed|failed>

Next steps:
  - Review generated model at <model_sql_path>
  - Run: cd <dbt-project-path> && dbt run --select <model_name>
  - Run /migrate-table again for the next table
```
