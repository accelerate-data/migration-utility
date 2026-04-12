# Skill: Listing Objects

## Purpose

Read-only catalog viewer that displays whatever state exists in the catalog -- columns, references, scoping results, analyzed statements. Provides three subcommands for browsing extracted objects. Never writes to the catalog.

## Invocation

```text
/listing-objects <subcommand> [object]
```

If no subcommand is given, defaults to `list`.

| Subcommand | Argument | Description |
|---|---|---|
| `list` | `tables`, `procedures`, `views`, or `functions` | Enumerate all objects of a type |
| `show` | `<schema.object>` | Display full catalog state for one object |
| `refs` | `<schema.object>` | Show procedures/views that reference an object |

Trigger phrases: "list tables", "list procedures", "show me object X", "what references Y", "browse catalog contents".

## Prerequisites

- `manifest.json` must exist in the project root. If missing, run [[Command Setup DDL]] first.
- Catalog files must exist in `catalog/` (produced by [[Command Setup DDL]]).
- The `migrate-util ready` check must pass before listing. The skill runs `migrate-util ready <table_fqn> scope` and stops if the readiness check fails.

## Pipeline

### 1. Read manifest

Read `manifest.json` to confirm a valid project root.

### 2. Execute subcommand

Each subcommand runs a single CLI call and presents results.

**list:**

```bash
uv run --project <shared-path> discover list --type <type>
```

**show:**

```bash
uv run --project <shared-path> discover show --name <fqn>
```

**refs:**

```bash
uv run --project <shared-path> discover refs --name <fqn>
```

### 3. Present results

- `list` -- numbered list of all objects
- `show` -- full catalog state for the object (columns, refs, statements, scoping, profile)
- `refs` -- grouped list of writers and readers

If the user selects an object from `list`, the skill proceeds to `show`. If they ask what references it, proceeds to `refs`.

## Reads

| File | Subcommand | Description |
|---|---|---|
| `manifest.json` | all | Project root validation |
| `catalog/tables/<name>.json` | `show`, `refs` | Table catalog file |
| `catalog/procedures/<name>.json` | `show` | Procedure catalog file |
| `catalog/views/<name>.json` | `show` | View catalog file |
| `catalog/functions/<name>.json` | `show` | Function catalog file |
| `ddl/<type>.sql` | `show` | Raw DDL for the object |

## Writes

None. This skill is strictly read-only.

## JSON Format

### `discover list` output (`discover_list_output.json`)

```json
{
  "objects": [
    "dbo.dimcustomer",
    "silver.dimproduct",
    "silver.factsales"
  ]
}
```

| Field | Type | Description |
|---|---|---|
| `objects` | string[] | Sorted list of normalized fully-qualified names for all objects of the requested type |

### `discover show` output (`discover_show_output.json`)

```json
{
  "name": "silver.usp_load_dimcustomer",
  "type": "procedure",
  "raw_ddl": "CREATE PROCEDURE [silver].[usp_load_DimCustomer] AS ...",
  "columns": [],
  "params": [
    { "name": "@BatchId", "sql_type": "INT", "is_output": false, "has_default": true }
  ],
  "refs": {
    "reads_from": ["bronze.customer", "bronze.person"],
    "writes_to": ["silver.dimcustomer"],
    "write_operations": {
      "silver.dimcustomer": ["INSERT"]
    },
    "uses_functions": []
  },
  "routing_reasons": ["static_exec"],
  "statements": [
    { "type": "Command", "action": "skip", "sql": "TRUNCATE TABLE [silver].[DimCustomer]" },
    { "type": "Insert", "action": "migrate", "sql": "INSERT INTO [silver].[DimCustomer] SELECT ..." }
  ],
  "needs_llm": false,
  "parse_error": null
}
```

| Field | Type | Description |
|---|---|---|
| `name` | string | Normalized FQN (lowercase, no brackets) |
| `type` | string | Enum: `table`, `procedure`, `view`, `function` |
| `raw_ddl` | string | Raw DDL text from the `.sql` file |
| `columns` | array | Column definitions (populated for tables; empty for other types). Fields: `name`, `sql_type`, `is_nullable`, `is_identity` |
| `params` | array | Procedure parameters (populated for procedures; empty for other types). Fields: `name`, `sql_type`, `is_output`, `has_default` |
| `refs` | object or null | For procedures: `reads_from`, `writes_to`, `write_operations`, `uses_functions`. For views/functions: `reads_from`, `writes_to`. `null` if no references section |
| `routing_reasons` | string[] | Canonical routing reasons from procedure catalog. Empty for non-procedures |
| `statements` | array or null | Per-statement breakdown. Populated when `needs_llm` is false; `null` when `needs_llm` is true or for non-procedures |
| `needs_llm` | boolean or null | `true`: LLM must read `raw_ddl`. `false`: statements are complete. `null` for non-procedures |
| `parse_error` | string or null | Error message if sqlglot could not parse the body |

**Statement entry fields:**

| Field | Type | Description |
|---|---|---|
| `type` | string | sqlglot AST node class name (e.g., `Insert`, `Update`, `Command`) |
| `action` | string | Enum: `migrate`, `skip`, `needs_llm` |
| `sql` | string | SQL text of the statement (truncated to 200 chars) |

### `discover refs` output (`discover_refs_output.json`)

```json
{
  "name": "silver.dimcustomer",
  "source": "catalog",
  "readers": [
    "gold.usp_build_report_customer"
  ],
  "writers": [
    {
      "procedure": "silver.usp_load_dimcustomer",
      "write_type": "direct",
      "is_updated": true,
      "is_selected": true,
      "is_insert_all": false
    }
  ]
}
```

| Field | Type | Description |
|---|---|---|
| `name` | string | Normalized FQN of the target object |
| `source` | string | Always `"catalog"` -- data comes from setup-ddl catalog files |
| `readers` | string[] | Normalized FQNs of procedures/views that SELECT from the target |
| `writers` | array | Procedures/views that write to the target |

**Writer entry fields:**

| Field | Type | Description |
|---|---|---|
| `procedure` | string | Normalized FQN of the writing procedure/view |
| `write_type` | string | Always `"direct"` -- catalog reports direct references only |
| `is_updated` | boolean | Always `true` for writers |
| `is_selected` | boolean | `true` if the writer also reads from the target |
| `is_insert_all` | boolean | `true` if the writer uses INSERT with all columns |

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| Exit code 1 on any subcommand | Object not found or catalog file missing | Verify the object name matches catalog (use `list` to see available objects) |
| Exit code 2 on any subcommand | Catalog directory unreadable (IO error) | Check file permissions on `catalog/` directory |
| `parse_error` set on `discover show` | sqlglot could not parse the procedure body | `raw_ddl` is still preserved for manual inspection. The procedure can still be analyzed via [[Skill Analyzing Object]] using `raw_ddl` |
| Dynamic SQL writers missing from `refs` | `sys.dm_sql_referenced_entities` resolves at definition time | Known limitation -- procs that write only via dynamic SQL (`EXEC(@sql)`, `sp_executesql`) will not appear as writers |
