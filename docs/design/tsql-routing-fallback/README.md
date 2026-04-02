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

The current routing is too coarse:

- static `EXEC dbo.proc` is a call-graph problem, not an LLM problem
- `IF / ELSE` and `TRY / CATCH` are control-flow wrappers around ordinary DML
- `EXEC (@sql)` is dynamic SQL and may genuinely need Claude
- operational statements (`SET`, `DECLARE`, index management) should be skipped without affecting routing

These cases need different fallback behavior and different provenance in the catalog.

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

`needs_llm` and `needs_enrich` remain in catalog files for backward compatibility. `mode` and `routing_reasons` become the canonical explanation.

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

Dynamic SQL splits into two categories:

| Pattern | Handling |
|---|---|
| `EXEC sp_executesql N'INSERT ...'` | Parse the embedded literal SQL offline |
| `EXEC (@sql)` / `EXEC sp_executesql @sql` | Require Claude unless full reconstruction is deterministic |

Variable reconstruction may be added later for simple concatenation, but it is not part of the initial design. The initial design only parses embedded literal SQL that is directly visible.

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

- replace `scan_routing_flags()` with a richer routing summary builder
- continue writing compatibility flags to catalog JSON

### `loader_parse.py`

- add block-tree segmentation
- add recursive extraction over the block tree
- keep `classify_statement()` for leaf nodes

### `catalog_enrich.py`

- continue to own static `EXEC` call-graph traversal
- consume `routing_reasons` instead of re-inferring static `EXEC` from regex alone

### `discover.py`

- surface `routing_reasons`
- keep `classification` for current CLI compatibility
- only return `claude_assisted` when the stored routing mode is `llm_required`

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
