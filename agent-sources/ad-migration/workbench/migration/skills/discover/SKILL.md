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

| Option | Required | Values |
|---|---|---|
| `--ddl-path` | yes | path to DDL directory |
| `--type` | yes | `tables`, `procedures`, `views`, `functions` |
| `--dialect` | no | sqlglot dialect (default: `tsql`) |

**show** — inspect a single object (columns, refs, raw DDL):

| Option | Required | Values |
|---|---|---|
| `--ddl-path` | yes | path to DDL directory |
| `--name` | yes | fully-qualified object name (e.g. `dbo.FactSales`, `[silver].[DimProduct]`) |
| `--dialect` | no | sqlglot dialect (default: `tsql`) |

**refs** — find all procedures/views that reference an object:

| Option | Required | Values |
|---|---|---|
| `--ddl-path` | yes | path to DDL directory |
| `--name` | yes | fully-qualified object name |
| `--dialect` | no | sqlglot dialect (default: `tsql`) |

If no subcommand is specified in `$ARGUMENTS`, default to `list`.

Invocation examples are in [`rules/workflow.md`](rules/workflow.md).

## Workflow

Follow the step sequence in [`rules/workflow.md`](rules/workflow.md) for the `list → show → refs` flow, including how to present results and interpret output.

## Parse classification

For procedures, `show` returns a `classification` field that tells the agent the analysis path:

| `classification` | Meaning | Action |
|---|---|---|
| `deterministic` | sqlglot parsed everything | Use `refs` and `write_operations` directly |
| `claude_assisted` | EXEC or unparseable syntax | Read `raw_ddl` to follow call graph |

The `classification` is derived from `has_exec` and `parse_error`. The `refs` object for procedures includes:

- `writes_to` — list of target table FQNs
- `reads_from` — list of source table FQNs
- `write_operations` — map of target FQN → operation names (e.g. `{"silver.dimcustomer": ["TRUNCATE", "INSERT"]}`)

See `docs/design/tsql-parse-classification/README.md` for the exhaustive list of deterministic vs Claude-assisted patterns.

## Handling parse errors

Procedures with `parse_error` set are still loaded — they are not skipped. Their `raw_ddl` is preserved and can be read for manual inspection or passed to Claude. The `parse_error` field explains why sqlglot could not fully parse the procedure.

If `discover` exits with code 2, the directory itself could not be read (missing path, IO error). Individual proc parse failures do not cause exit code 2 — they are stored with `parse_error` and the remaining procs continue loading.
