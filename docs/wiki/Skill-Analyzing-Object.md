# Skill: Analyzing Object

## Purpose

Deep-dive analysis of a single stored procedure. Resolves call graphs to base tables, classifies each statement as `migrate` or `skip`, produces a logic summary and migration guidance, and persists resolved statements to the procedure catalog file. This skill is called both directly by users and as a sub-agent by [[Skill Scoping Table]].

## Invocation

```text
/analyzing-object <schema.procedure>
```

Argument is the fully-qualified procedure name (e.g., `dbo.usp_load_DimCustomer`). The skill asks if missing. Only procedures are supported -- for views, functions, or tables, use [[Skill Listing Objects]] instead.

## Prerequisites

- `manifest.json` must exist in the project root. If missing, run [[Skill Setup DDL]] first.
- `catalog/procedures/<name>.json` must exist. If the object is a view, function, or table, the skill directs the user to `/listing-objects show <name>` instead.

## Pipeline

### 1. Fetch object data

```bash
uv run --project <shared-path> discover show --name <proc>
```

Returns `refs`, `statements`, `classification`, `raw_ddl`, `params`, and `routing_reasons`.

### 2. Classify statements

Checks the `classification` field from the catalog:

| Classification | Behavior |
|---|---|
| `deterministic` (with `statements` populated) | Refs and statements are pre-classified by the AST. Used as authoritative source of truth alongside the body. |
| `claude_assisted` (or `statements` is null) | Each statement must be classified from `raw_ddl` by the LLM. |

### 3. Resolve call graph

Read/write targets come from `refs`. Any ref that is a view, function, or procedure (not a base table) is resolved by running `discover show` on it to get its refs, following the chain until base tables are reached. The full lineage is presented as a tree:

```text
silver.usp_load_DimCustomer  (direct writer)
  +-- reads: silver.vw_ProductCatalog (view)
  |     +-- reads: bronze.Customer
  |     +-- reads: bronze.Product
  +-- reads: bronze.Person
  +-- writes: silver.DimCustomer
```

### 4. Logic summary

Plain-language step-by-step description of what the procedure does, derived from `raw_ddl`.

### 5. Migration guidance

Each statement is tagged as `migrate` or `skip`:

| Action | Meaning |
|---|---|
| `migrate` | Core transformation (INSERT, UPDATE, DELETE, MERGE, SELECT INTO) -- becomes the dbt model |
| `skip` | Operational overhead (SET, TRUNCATE, DROP/CREATE INDEX) -- dbt handles or ignores |

### 6. Persist resolved statements

**Deterministic procedures** (`classification: deterministic`, no `claude` actions): all statements are already AST-classified. Persisted immediately with `source: "ast"`. No additional user confirmation needed.

**Claude-assisted procedures** (`classification: claude_assisted` or statements containing `action: "claude"`):

1. Each `claude` statement is analyzed -- call graph followed, dynamic SQL resolved, classified as `migrate` or `skip`
2. Full resolved statement list presented for user confirmation
3. After confirmation, persisted with `source: "llm"`. Each statement includes a `rationale` field.

No `claude` actions are written to catalog -- all must be resolved before persisting.

```bash
mkdir -p .staging
# Write statements JSON to .staging/statements.json
uv run --project <shared-path> discover write-statements \
  --name <procedure_name> --statements-file .staging/statements.json; rm -rf .staging
```

## Reads

| File | Description |
|---|---|
| `manifest.json` | Project root validation |
| `catalog/procedures/<proc>.json` | Procedure metadata, refs, routing info |
| `ddl/procedures.sql` | Raw DDL (via `discover show`) |
| `catalog/views/<view>.json` | Resolved during call graph traversal |
| `catalog/functions/<func>.json` | Resolved during call graph traversal |

## Writes

### `statements[]` array in `catalog/procedures/<proc>.json`

| Field | Type | Required | Description |
|---|---|---|---|
| `type` | string | no | Statement type -- sqlglot AST node class name (e.g., `Insert`, `Update`, `Merge`, `Command`) |
| `action` | string | yes | Enum: `migrate`, `skip` |
| `source` | string | yes | Enum: `ast` (deterministic sqlglot parse), `llm` (Claude analysis) |
| `sql` | string | yes | SQL text of the statement |
| `rationale` | string | no | Why this statement is `migrate` or `skip`. Required for `source: "llm"`; optional for `source: "ast"` |

### Procedure catalog fields (read, not written by this skill)

These fields are set by [[Skill Setup DDL]] and read by analyzing-object:

| Field | Type | Description |
|---|---|---|
| `mode` | string | Canonical routing mode. Enum: `deterministic`, `control_flow_fallback`, `call_graph_enrich`, `dynamic_sql_literal`, `llm_required` |
| `routing_reasons` | string[] | Reasons for the routing mode |
| `needs_llm` | boolean | `true` when body contains EXEC(@var), TRY/CATCH, WHILE, or IF/ELSE |
| `needs_enrich` | boolean | `true` when body contains SELECT INTO, TRUNCATE, or static EXEC chains. Flipped to `false` by `catalog-enrich` |

### `mode` enum values

| Mode | Description |
|---|---|
| `deterministic` | AST can fully classify all statements. No LLM needed. |
| `control_flow_fallback` | IF/ELSE, WHILE, or TRY/CATCH present but statements classifiable after flattening. |
| `call_graph_enrich` | Static EXEC chains need call graph traversal to resolve targets. |
| `dynamic_sql_literal` | EXEC with string literal SQL (resolvable offline). |
| `llm_required` | Dynamic SQL with variable targets, external input, or unresolvable patterns. |

### `routing_reasons` enum values

| Reason | Description |
|---|---|
| `if_else` | IF/ELSE control flow detected |
| `while_loop` | WHILE loop detected |
| `try_catch` | TRY/CATCH block detected |
| `static_exec` | Static EXEC call to another procedure |
| `dynamic_sql_literal` | EXEC with string literal SQL |
| `dynamic_sql_variable` | EXEC(@var) or sp_executesql with variable SQL |
| `unsupported_syntax` | Syntax sqlglot cannot parse |
| `depth_limit_exceeded` | Call graph recursion hit depth limit |

## JSON Format

### Resolved statements example (deterministic)

```json
{
  "statements": [
    {
      "type": "Command",
      "action": "skip",
      "source": "ast",
      "sql": "TRUNCATE TABLE [silver].[DimCustomer]"
    },
    {
      "type": "Insert",
      "action": "migrate",
      "source": "ast",
      "sql": "INSERT INTO [silver].[DimCustomer] SELECT c.CustomerKey, c.FirstName, g.Region FROM [bronze].[Customer] c JOIN [bronze].[Geography] g ON c.GeographyKey = g.GeographyKey"
    }
  ]
}
```

### Resolved statements example (claude-assisted)

```json
{
  "statements": [
    {
      "type": "Command",
      "action": "skip",
      "source": "llm",
      "sql": "EXEC dbo.usp_truncate_staging @Table = 'DimCustomer'",
      "rationale": "Utility proc that truncates staging table. Operational overhead -- dbt handles table replacement via materialization."
    },
    {
      "type": "Insert",
      "action": "migrate",
      "source": "llm",
      "sql": "INSERT INTO [silver].[DimCustomer] SELECT ...",
      "rationale": "Core transformation that populates DimCustomer from bronze sources. This is the main SELECT that becomes the dbt model."
    }
  ]
}
```

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `discover show` exit code 1 | Object not found or catalog file missing | Verify the procedure name with `/listing-objects list procedures` |
| `discover show` exit code 2 | Catalog directory unreadable (IO error) | Check file permissions on `catalog/` |
| `parse_error` set in `discover show` output | sqlglot could not parse the procedure body | `raw_ddl` is preserved -- analysis proceeds using raw DDL text with LLM classification |
| `discover write-statements` exit code 1 | Procedure not found or invalid statements JSON | Verify statement JSON matches the schema above |
| `discover write-statements` exit code 2 | Invalid JSON input | Check `.staging/statements.json` format |
| Circular reference during call graph resolution | Procedure A calls Procedure B which calls Procedure A | Recursion stopped and the cycle is reported |
| Unresolvable dynamic SQL | EXEC(@variable) with runtime-determined table/column targets | Reported as unresolvable -- these require manual analysis or runtime tracing |
