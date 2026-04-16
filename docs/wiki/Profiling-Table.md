# Profiling Table

## Purpose

Profiles a single table for migration by assembling deterministic context from catalog files, reasoning over six profiling questions (classification, primary key, foreign keys, natural vs surrogate key, watermark, PII), presenting results for user approval, and writing the approved profile to the table catalog file. The profile drives materialization, test generation, and model generation downstream.

## Invocation

```text
/profiling-table <schema.table>
```

Argument is the fully-qualified table name. The workflow asks if missing. The writer procedure is read automatically from the catalog's `scoping.selected_writer` field.

You can also use natural language in your Claude Code session, such as "profile a table", "classify this table", or "what kind of model is this table".

## Prerequisites

- `manifest.json` must exist in the project root. If missing, run `ad-migration setup-source` first.
- `catalog/tables/<table>.json` must exist. If missing, run `/listing-objects list tables` to see available tables.
- `scoping.selected_writer` must be set in the table catalog. If missing, run [[Analyzing Table]] first.
- The workflow checks profiling readiness and stops with an error code if the object is not ready.

## Pipeline

### 1. Assemble context (deterministic)

Reads catalog files for the table and its selected writer to assemble profiling context. The context includes:

- `catalog_signals` -- PKs, FKs, identity columns, unique indexes, change capture, sensitivity classifications
- `writer_references` -- outbound references from the writer procedure with column-level read/write flags
- `proc_body` -- full SQL body of the writer procedure
- `columns` -- target table column list with types and nullability
- `related_procedures` -- related procedure context when referenced by the writer

### 2. LLM profiling (reasoning)

The LLM reads the context JSON and answers six profiling questions using signal tables and pattern matching rules from the profiling-signals reference.

### 3. Present for approval (interactive)

Profile is presented as a structured summary. If a required question (Q1, Q2, Q4, Q5) cannot be answered with reasonable confidence, the ambiguity is presented to the user for guidance. The user must explicitly approve before the profile is persisted.

### 4. Write to catalog (deterministic)

Writes the approved profile to the table catalog file.

## Reads

| File | Description |
|---|---|
| `manifest.json` | Project root validation |
| `catalog/tables/<table>.json` | Column list, catalog signals, scoping section |
| `catalog/procedures/<writer>.json` | Writer procedure references and body |
| `ddl/procedures.sql` | Writer procedure raw DDL |

## Writes

### `profile` section in `catalog/tables/<table>.json`

The profile is written to the `profile` section of the table catalog file.

| Field | Type | Required | Description |
|---|---|---|---|
| `status` | string | yes | Enum: `ok`, `partial`, `error` |
| `writer` | string | yes | FQN of the writer procedure used for profiling |
| `classification` | object | no | Model classification results |
| `primary_key` | object | no | Primary key determination |
| `natural_key` | object | no | Natural key determination |
| `watermark` | object | no | Incremental watermark column |
| `foreign_keys` | array | no | Foreign key relationships with types |
| `pii_actions` | array | no | PII column handling recommendations |
| `warnings` | array | no | Diagnostics entries |
| `errors` | array | no | Diagnostics entries |

## The Six Profiling Questions

The workflow answers six questions about each table. Four are required (Q1, Q2, Q4, Q5) and two are nice-to-have (Q3, Q6):

| # | Question | Why it matters |
|---|---|---|
| Q1 | What kind of model is this? | Determines materialization strategy and whether SCD2 logic is needed |
| Q2 | Primary key candidate | Required for `unique_key` in incremental models |
| Q3 | Foreign key candidates | Needed for `relationships` tests and role-playing dimension detection |
| Q4 | Natural key vs surrogate key | Determines whether `dbt_utils.generate_surrogate_key` is needed |
| Q5 | Incremental watermark | Without it, the model can only do full refresh |
| Q6 | PII handling candidates | Compliance risk if missed |

For detailed signal tables, output field definitions, and a full JSON example, see [[Profiling Signals]].

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| Catalog file missing | Catalog file missing for table or writer | Run `ad-migration setup-source` and [[Analyzing Table]] first |
| IO/parse error | IO/parse error reading catalog files | Check file permissions and JSON validity in `catalog/` |
| Profile write validation failure | Invalid field values in the assembled profile | Re-run `/profiling-table`; if it persists, check the catalog for corruption |
| Profile write IO error | Catalog unreadable or write failure | Check file permissions on `catalog/tables/<table>.json` |
| Ambiguous classification | Write pattern signals conflict with column shape signals | The workflow stops and asks the user for guidance rather than auto-resolving |
| Missing watermark | No WHERE clause filter or datetime column found | Profile is written with `status: "partial"`. Model will fall back to full-refresh `table` materialization |
