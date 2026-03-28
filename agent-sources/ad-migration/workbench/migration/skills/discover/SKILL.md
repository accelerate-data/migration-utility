---
name: discover
description: >
  This skill should be used when the user asks to "list tables", "list procedures", "list views", "list functions", "show me the DDL for X", "inspect object X","what references Y", or wants to explore the structure of a DDL export directory. Use for any object inspection or reference tracing against a local DDL snapshot.
argument-hint: "[ddl-path] [subcommand] [options]"
---

# Discover

Instructions for using `discover` to explore a DDL artifact directory.

## Arguments

Parse `$ARGUMENTS`:

- `ddl-path` (required): path to the directory containing `.sql` files
- `subcommand` (optional): `list`, `show`, or `refs` — defaults to `list` if omitted
- remaining tokens: options for the subcommand (e.g. `--type tables`, `--name dbo.X`)

If `ddl-path` is missing from `$ARGUMENTS`, ask the user for it before proceeding. Do not assume `./artifacts/ddl` or any other default. The directory may contain any number of `.sql` files with any names; object types are auto-detected from `CREATE` statements inside.

## Workflow

Follow the step sequence in [`rules/workflow.md`](rules/workflow.md) for the `list → show → refs` flow, including how to present results and interpret output.

## Parse classification

When `show` returns results for a procedure, check the `parse_error` and `refs` fields to determine the analysis path:

| `parse_error` | `refs` | Path | Action |
|---|---|---|---|
| `null` | populated `writes_to`/`reads_from` | **Deterministic** | sqlglot handled everything — no Claude needed |
| `null` | empty `writes_to` and `reads_from` | **Deterministic** | Proc has no DML (e.g. only SET statements) |
| set (non-null) | empty or partial | **Claude-assisted** | Proc contains EXEC or dynamic SQL that sqlglot cannot parse |

For deterministic procs, report the refs directly. For Claude-assisted procs, tell the user the proc requires manual analysis and show the `parse_error` reason.

The following T-SQL patterns are fully deterministic: INSERT, UPDATE, DELETE, DELETE TOP, TRUNCATE, MERGE, SELECT INTO, CTE, multi-level CTE, CASE WHEN, LEFT/RIGHT JOIN, subqueries, correlated subqueries, window functions, IF/ELSE, BEGIN TRY/CATCH, and WHILE loops.

The following patterns require Claude: all EXEC variants (static proc calls, dynamic SQL, sp_executesql). See `docs/design/tsql-parse-classification/README.md` for the exhaustive list.

## Handling parse errors

Procedures with `parse_error` set are still loaded — they are not skipped. Their `raw_ddl` is preserved and can be read for manual inspection or passed to Claude. The `parse_error` field explains why sqlglot could not fully parse the procedure.

If `discover` exits with code 2, the directory itself could not be read (missing path, IO error). Individual proc parse failures do not cause exit code 2 — they are stored with `parse_error` and the remaining procs continue loading.
