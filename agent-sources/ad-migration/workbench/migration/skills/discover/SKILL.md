---
name: discover
description: >
  This skill should be used when the user asks to "list tables", "list procedures", "list views", "list functions", "show me the DDL for X", "inspect object X","what references Y", or wants to explore the structure of a DDL export directory. Use for any object inspection or reference tracing against a local DDL snapshot.
argument-hint: "[ddl-path] [subcommand] [options]"
---

# Discover

Instructions for using `discover` to explore a DDL artifact directory.

## Arguments

Parse `$ARGUMENTS` for `ddl-path` and optionally a subcommand with its options.

If `ddl-path` is missing from `$ARGUMENTS`, ask the user for it before proceeding. Do not assume any default path.

### Subcommands

**list** — enumerate objects by type:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover list \
  --ddl-path <path> --type <tables|procedures|views|functions>
```

| Option | Required | Values |
|---|---|---|
| `--ddl-path` | yes | path to DDL directory |
| `--type` | yes | `tables`, `procedures`, `views`, `functions` |
| `--dialect` | no | sqlglot dialect (default: `tsql`, ignored for current scope) |

**show** — inspect a single object (columns, refs, raw DDL):

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover show \
  --ddl-path <path> --name <schema.ObjectName>
```

| Option | Required | Values |
|---|---|---|
| `--ddl-path` | yes | path to DDL directory |
| `--name` | yes | fully-qualified object name (e.g. `dbo.FactSales`, `[silver].[DimProduct]`) |
| `--dialect` | no | sqlglot dialect (default: `tsql`, ignored for current scope) |

**refs** — find all procedures/views that reference an object:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover refs \
  --ddl-path <path> --name <schema.ObjectName>
```

| Option | Required | Values |
|---|---|---|
| `--ddl-path` | yes | path to DDL directory |
| `--name` | yes | fully-qualified object name |
| `--dialect` | no | sqlglot dialect (default: `tsql`, ignored for current scope) |

If no subcommand is specified in `$ARGUMENTS`, default to `list`.

## Workflow

Follow the step sequence in [`rules/workflow.md`](rules/workflow.md) for the `list → show → refs` flow, including how to present results and interpret output.

## Parse classification

When `show` returns results for a procedure, check `has_exec`, `parse_error`, and `refs` to determine the analysis path:

| `has_exec` | `parse_error` | Path | Action |
|---|---|---|---|
| `true` | any | **Claude-assisted** | Proc contains EXEC — read `raw_ddl` to follow call graph |
| `false` | `null` | **Deterministic** | sqlglot handled everything — report refs directly |
| `false` | set | **Claude-assisted** | Proc has unparseable syntax — read `raw_ddl` for manual analysis |

The `has_exec` flag is the primary signal. Any proc with EXEC/EXECUTE in its body is Claude-assisted regardless of `parse_error` or refs — even if the proc also has deterministic DML alongside the EXEC.

The following T-SQL patterns are fully deterministic: INSERT, UPDATE, DELETE, DELETE TOP, TRUNCATE, MERGE, SELECT INTO, CTE, multi-level CTE, CASE WHEN, LEFT/RIGHT JOIN, subqueries, correlated subqueries, window functions, IF/ELSE, BEGIN TRY/CATCH, and WHILE loops.

The following patterns require Claude: all EXEC variants (static proc calls, dynamic SQL, sp_executesql). See `docs/design/tsql-parse-classification/README.md` for the exhaustive list.

## Handling parse errors

Procedures with `parse_error` set are still loaded — they are not skipped. Their `raw_ddl` is preserved and can be read for manual inspection or passed to Claude. The `parse_error` field explains why sqlglot could not fully parse the procedure.

If `discover` exits with code 2, the directory itself could not be read (missing path, IO error). Individual proc parse failures do not cause exit code 2 — they are stored with `parse_error` and the remaining procs continue loading.
