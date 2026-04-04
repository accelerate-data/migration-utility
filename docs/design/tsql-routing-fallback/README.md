# T-SQL Routing Fallback Design

Replace the current binary "sqlglot parsed it" vs "Claude must read raw DDL" routing with a staged classifier that separates control flow, call-graph enrichment, dynamic SQL, and operational overhead.

## Decision

Keep `sqlglot` as the leaf-statement parser for DML and reference extraction, but stop using parser fallbacks (`Command` / `If`) as the top-level routing decision.

Add a lightweight T-SQL block segmenter ahead of `sqlglot` for nested control-flow constructs:

- `IF / ELSE`
- `WHILE`
- `BEGIN TRY / END TRY`
- `BEGIN CATCH / END CATCH`
- `BEGIN / END`

The segmenter builds a block tree. `sqlglot` then parses the leaf statements inside each block. Routing is derived from the block tree plus statement categories, not from a single coarse parse outcome.

## Why

The current routing already separates three paths via `scan_routing_flags()` in `catalog.py`:

- `needs_llm` → dynamic `EXEC(@sql)`, `IF`, `WHILE`, `BEGIN TRY` → `needs_llm: true`
- `needs_enrich` → static `EXEC dbo.proc`, `SELECT INTO`, `TRUNCATE` → deterministic after `catalog_enrich.py`
- neither → pure sqlglot deterministic

Static EXEC and enrichment-resolved patterns are already handled correctly. The problem is that **control-flow constructs** (`IF / ELSE`, `TRY / CATCH`, `WHILE`) are over-routed: `\bIF\b` in `_NEEDS_LLM_RE` matches all `IF` statements, including simple guards around ordinary DML. These are control-flow wrappers, not genuinely dynamic — their branch bodies often contain standard DML that sqlglot can parse if extracted.

The goal is to recover deterministic extraction from control-flow wrappers by segmenting the block structure before routing, rather than collapsing all control flow into `needs_llm`.

## Architecture

```text
Stored Procedure DDL
        │
        ▼
Segment procedure body into nested control-flow blocks
        │
        ▼
Build block tree
        │
        ├── Control-flow nodes
        │     recurse into child blocks
        │
        └── Leaf statements
              parse with sqlglot
                    │
                    ├── DML / skip statement → deterministic extraction
                    ├── static EXEC          → call-graph enrichment
                    ├── dynamic SQL literal  → parse embedded SQL if recoverable
                    └── dynamic SQL variable → LLM / unresolved
```

## Routing Model

Replace `needs_llm` / `needs_enrich` as the only internal routing primitives with a richer per-procedure summary:

```json
{
  "mode": "deterministic",
  "routing_reasons": ["if_else", "static_exec"],
  "needs_llm": false,
  "needs_enrich": true
}
```

`needs_llm` and `needs_enrich` remain in catalog files for backward compatibility. `mode` and `routing_reasons` become the canonical explanation. Both new fields must be optional in `procedure_catalog.json` so existing catalog files without them remain valid.

### Modes

| Mode | Meaning |
|---|---|
| `deterministic` | All required refs and statements were extracted without Claude |
| `control_flow_fallback` | Control flow was segmented recursively; leaf DML was still extracted deterministically |
| `call_graph_enrich` | Static `EXEC` was detected and must be completed by catalog enrichment |
| `dynamic_sql_literal` | Embedded SQL is visible as a string literal and may be parsed offline |
| `llm_required` | Dynamic SQL variables or unsupported nested syntax require Claude |

### Routing reasons

| Reason | Meaning |
|---|---|
| `if_else` | Conditional branch wrapper |
| `while_loop` | Loop wrapper |
| `try_catch` | Error-handling wrapper |
| `static_exec` | Static procedure call |
| `dynamic_sql_literal` | `sp_executesql N'...'` or equivalent literal SQL |
| `dynamic_sql_variable` | `EXEC (@sql)` or `sp_executesql @sql` |
| `unsupported_syntax` | Segmenter or parser could not classify the subtree |
| `depth_limit_exceeded` | Nested structure exceeded configured recursion limit |

## Block Tree

Model the procedure body as recursive nodes:

- `StatementNode`
- `IfNode`
- `WhileNode`
- `TryCatchNode`
- `ExecNode`
- `UnknownNode`

Each node stores:

- raw SQL slice
- child nodes when applicable
- extracted refs
- extracted statements
- provenance (`ast`, `control_flow_fallback`, `call_graph`, `llm`)

The segmenter is responsible only for finding block boundaries and building this tree. It is not responsible for full SQL semantics.

## Recursive Extraction

The extractor walks the tree bottom-up.

### Leaf statements

Pass leaf text to `sqlglot`:

- `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `SELECT INTO` → migrate
- `SET`, `DECLARE`, `RETURN`, `PRINT`, index management → skip
- `TRUNCATE` → skip statement, but preserve write target
- static `EXEC schema.proc` → mark call edge, no direct table refs

### Control-flow nodes

Recurse into child blocks:

- union `writes_to`
- union `reads_from`
- union `uses_functions`
- preserve statement provenance

Control-flow wrappers do not force the whole procedure onto the LLM path. Only unrecoverable child nodes do.

## Deeply Nested Procedures

Nested scenarios are handled by recursion over the block tree. Aggregation is bottom-up.

Example:

```text
TRY
  IF
    WHILE
      INSERT
  ELSE
    EXEC dbo.usp_Load
CATCH
  INSERT dbo.ErrorLog
```

Expected result:

- `INSERT` inside `WHILE` → deterministic via fallback
- static `EXEC dbo.usp_Load` → call-graph enrichment
- `INSERT dbo.ErrorLog` → deterministic via fallback
- overall proc → `control_flow_fallback` with `static_exec`, not `llm_required`

### Guardrails

To avoid pathological cases:

- maximum nesting depth
- maximum node count per proc
- maximum literal SQL reconstruction size

If a subtree exceeds limits, mark only that subtree with `depth_limit_exceeded` and escalate the procedure to `llm_required`.

## Dynamic SQL

Dynamic SQL splits into three categories:

| Pattern | Current routing | Proposed handling |
|---|---|---|
| `EXEC sp_executesql N'INSERT ...'` | Neither flag (DMF resolves it) | Keep DMF resolution. Only parse the embedded literal offline if DMF data is unavailable. |
| `EXEC sp_executesql @sql` | Neither flag (known routing gap) | `dynamic_sql_variable` → `llm_required`. Closes the existing gap where variable sp_executesql sets neither `needs_llm` nor `needs_enrich`. |
| `EXEC (@sql)` | `needs_llm` | `dynamic_sql_variable` → `llm_required` |

Note: `sp_executesql` with a static literal (pattern 57 in the parse classification doc) is already resolved by DMF in the current pipeline. The `dynamic_sql_literal` routing reason applies only when DMF data is absent and the literal is visible for offline parsing.

Variable reconstruction may be added later for simple concatenation, but it is not part of the initial design.

## Segmenter Requirements

The segmenter must be T-SQL-aware enough to avoid false boundaries in:

- string literals
- bracketed identifiers
- line comments
- block comments

It does not need to understand table names, expressions, or DML semantics. Its only job is to find control-flow and `BEGIN / END` boundaries safely.

`sqlglot` is still used after segmentation. It is not the segmenter.

## Integration Points

### `catalog.py`

- replace `scan_routing_flags()` with a richer routing summary builder that produces `mode` and `routing_reasons`
- continue writing `needs_llm` / `needs_enrich` compatibility flags to catalog JSON

### New module: `block_segmenter.py`

- new file in `lib/shared/` for the T-SQL block-tree segmenter
- separate from `loader_parse.py` to keep segmentation concerns distinct from sqlglot parsing
- `loader_parse.py` retains `classify_statement()` for leaf nodes and `extract_refs()` for DML extraction

### `loader_parse.py`

- add recursive extraction over the block tree (consumes segmenter output)
- keep `classify_statement()` for leaf nodes

### `catalog_enrich.py`

- continue to own static `EXEC` call-graph traversal via BFS
- the outer skip guard already uses `needs_llm`/`needs_enrich` flags from catalog; `_extract_calls()` internally uses its own `_EXEC_PROC_RE` regex for call-edge discovery
- use `routing_reasons` to gate whether call-graph enrichment runs (replaces the `needs_enrich` skip guard), but keep `_extract_calls()` and `_EXEC_PROC_RE` for concrete callee extraction — `routing_reasons` signals that static EXEC exists, not which procedures are called

### `discover.py`

- surface `routing_reasons` and `needs_llm: bool` in `discover show` output
- map `llm_required` mode → `needs_llm: true`; also propagate `needs_llm` from `extract_refs` when body parsing is incomplete
- preserve the existing `parse_error` → `needs_llm: true` branch. A proc with a parse error must remain `needs_llm: true` regardless of routing mode

### `procedure_catalog.json` schema

- add optional `mode` field (string enum)
- add optional `routing_reasons` field (array of strings)
- both fields are optional to preserve backward compatibility with existing catalog files

## Rollout

Implement in three steps:

1. Add `routing_reasons` and `mode` while preserving `needs_llm` / `needs_enrich`.
2. Add recursive control-flow segmentation for `IF / ELSE`, `WHILE`, and `TRY / CATCH`.
3. Narrow the Claude path to dynamic SQL variables and unsupported subtrees.

This keeps existing catalog consumers working while improving deterministic coverage.

## Non-goals

- full T-SQL parsing independent of `sqlglot`
- perfect reconstruction of arbitrary dynamic SQL
- semantic execution of branch predicates
- runtime evaluation of loops
