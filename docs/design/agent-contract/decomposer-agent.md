# Decomposer Agent Contract

The Decomposer agent takes a selected writer stored procedure and produces SQL decomposition proposals for dbt migration. It analyzes procedure SQL, breaks large/complex logic into reusable logical blocks, and identifies candidate split points that can be implemented as dbt-friendly units (CTEs, intermediate models, tables, or views).

## Philosophy and Boundary

- Decomposer is tool-first and deterministic (AST parse + rule engine).
- Decomposer proposes decomposition structure; it does not choose business keys/classification.
- Planner consumes decomposer proposals and finalizes design decisions.

## Required Input

```json
{
  "schema_version": "1.0",
  "run_id": "uuid",
  "items": [
    {
      "item_id": "dbo.fact_sales",
      "writer": "dbo.usp_load_fact_sales"
    }
  ]
}
```

## Decomposition Strategy

### 1. LoadWriterSql

- Load selected writer SQL from `sys.sql_modules`.
- Optionally load directly related procedures for context when unresolved references are detected.
- Unresolved-reference triggers:
  - unresolved `EXEC/EXECUTE` target in selected writer body.
  - object reference that cannot be resolved within selected writer scope (for example temp/intermediate
    object produced in a called procedure).
  - dependency edge from selected writer to another procedure that contributes write-side transforms
    for the same `item_id`.

### 2. NormalizeAndParse

- Split multi-step procedure SQL into statements.
- Parse statements into AST nodes (tool-based, no regex-only parsing).

### 3. BuildStatementGraph

- Build dependency graph across statements and temporary/intermediate artifacts.
- Preserve execution-order dependencies where required.
- Track artifact types explicitly:
  - CTEs
  - local temp tables (`#temp`)
  - global temp tables (`##temp`)
  - table variables (`@table_var`)
  - `SELECT INTO` created tables
  - intermediate persistent staging tables written/read inside the procedure
- Respect SQL Server scoping semantics for each artifact type when building edges.

### 4. SegmentLogicalBlocks

- Segment into logical blocks such as:
  - source extraction/filtering
  - dedupe/windowing
  - join/enrichment
  - aggregation/grain change
  - final projection/output
- Apply deterministic boundary rules in order:
  - start a new block at each statement boundary (`SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`,
    `TRUNCATE`, `SELECT INTO`).
  - start a new block at each temp/intermediate persistence boundary (`#temp`, table variable,
    staging table write/read transition).
  - within a statement, start a new block at each grain-change boundary (`GROUP BY`, `DISTINCT`,
    windowed dedupe filters such as `ROW_NUMBER() ... WHERE rn = 1`).
  - start a new block when moving from enrichment joins to final projection/output shaping.
  - keep contiguous operations in the same block when none of the above boundaries are crossed.
- Boundary precedence:
  - persistence boundary > statement boundary > grain-change boundary > enrichment/projection boundary.
- Block IDs must be stable and ordered by execution sequence.
- `TRUNCATE` handling:
  - treat `TRUNCATE TABLE` as a write-side boundary that starts a new block.
  - do not merge `TRUNCATE` with unrelated extraction/enrichment blocks.
  - preserve ordering constraints between `TRUNCATE` and subsequent insert/write blocks.

### 5. ProposeSplitPoints

- Propose candidate split points at stable boundaries (for example after grain changes,
  reusable enrichment blocks, or isolated heavy transforms).
- Generate candidate intermediate model names.
- Apply deterministic split rules:
  - propose a split after a block that ends with a persistence boundary or grain-change boundary.
  - propose a split after a block that is reused by 2 or more downstream blocks.
  - do not propose a split inside an atomic upsert/final-write block.
  - do not propose a split when total segmented blocks `< 3` (simple procedure threshold).
  - maximum proposed split points per item: `3` (highest-confidence first).
- Split suppression rules:
  - if a split would create a single-block intermediate model with no reuse, suppress it.
  - if a split would break required execution order semantics, suppress it.

### 6. ScoreAndValidate

- Score decomposition quality/confidence using deterministic rules.
- Validate acyclic block/split structure and references.
- Emit `ok|partial|error`.
- Scoring rules:
  - Block confidence base: `0.50`
    - `+0.20` if boundary is persistence- or statement-derived.
    - `+0.15` if boundary is grain-change-derived.
    - `+0.10` if block has clear single purpose classification.
    - `-0.20` if unresolved reference remains in block.
    - `-0.10` if block mixes multiple incompatible operation classes.
  - Split confidence base: `0.50`
    - `+0.20` if split occurs at persistence/grain-change boundary.
    - `+0.20` if upstream block is reused by 2+ downstream blocks.
    - `+0.10` if split isolates heavy transformation complexity.
    - `-0.25` if split creates trivial/non-reused intermediate model.
    - `-0.15` if ordering constraints around split are fragile.
  - Clamp all confidence scores to `[0,1]`.
- Status rules:
  - `ok`: at least one valid segmentation plan and no unresolved blocking references.
  - `partial`: segmentation produced, but one or more of the following hold:
    - at least one statement cannot be assigned to a block deterministically.
    - unresolved references remain after optional related-procedure loading.
    - no split points meet confidence threshold (`>= 0.75`) while decomposition has `>= 3` blocks.
    - all proposed split points are low confidence (`< 0.75`).
  - `error`: parsing/runtime failure prevents decomposition output.

## Naming and Reference Rules

- `source_sql_ref` must be a structured object:
  - `statement_indices`: zero-based statement indices from the normalized procedure statement list.
  - `line_span`: optional source line span object `{ "start": <int>, "end": <int> }`.
- `block_id` format must be stable and deterministic:
  - `<sequence>_<operation_class>_<target_hint>`
  - examples: `01_extract_sales_stage`, `02_enrich_customer`, `03_aggregate_daily`
  - `sequence` is 2-digit execution order.
  - `operation_class` is one of `extract|enrich|dedupe|aggregate|project|write`.
  - `target_hint` is derived from dominant relation/entity stem in the block.
- `proposed_model_name` format:
  - intermediate models: `int_<entity>_<purpose>`
  - final model names are planner-owned and not emitted by decomposer
  - names must be lowercase snake_case and unique per item.
- `purpose` describes what the block does.
- `rationale` describes why the block boundary exists.

## Output Schema (DecompositionProposal)

```json
{
  "schema_version": "",
  "run_id": "",
  "results": [
    {
      "item_id": "",
      "status": "",
      "segmented_logical_blocks": [],
      "candidate_model_split_points": [],
      "warnings": [],
      "validation": {…},
      "errors": []
    }
  ],
  "summary": {...}
}
```

**Example**

```json
{
  "schema_version": "1.0",
  "run_id": "uuid",
  "results": [
    {
      "item_id": "dbo.fact_sales",
      "status": "ok|partial|error",
      "segmented_logical_blocks": [
        {
          "block_id": "01_extract_sales_stage",
          "purpose": "Load source rows and apply base filters.",
          "rationale": ["Statement boundary and source extraction phase boundary."],
          "source_sql_ref": {
            "statement_indices": [0],
            "line_span": { "start": 12, "end": 37 }
          },
          "confidence": 0.91
        },
        {
          "block_id": "02_enrich_customer_product",
          "purpose": "Resolve dimension surrogate keys.",
          "rationale": ["Join/enrichment phase boundary after extract block."],
          "source_sql_ref": {
            "statement_indices": [1],
            "line_span": { "start": 38, "end": 79 }
          },
          "confidence": 0.88
        }
      ],
      "candidate_model_split_points": [
        {
          "split_after_block_id": "01_extract_sales_stage",
          "proposed_model_name": "int_fact_sales_source",
          "rationale": ["Reusable filtered source layer."],
          "confidence": 0.86
        }
      ],
      "warnings": [],
      "validation": {
        "passed": true,
        "issues": []
      },
      "errors": []
    }
  ],
  "summary": {
    "total": 1,
    "ok": 1,
    "partial": 0,
    "error": 0
  }
}
```

## Validation Checklist

- `item_id` is present.
- `status` is one of: `ok|partial|error`.
- every `split_after_block_id` exists in `segmented_logical_blocks[*].block_id`.
- block graph is acyclic.
- every confidence is within `[0,1]`.
- every `source_sql_ref` follows the structured format.
- every `block_id` follows naming rules and is unique within the item.
- every `proposed_model_name` follows naming rules and is unique within the item.
- every block has both `purpose` and `rationale`.
- if `status == "partial"`, `validation.issues` is non-empty.
- if `status == "error"`, `errors` is non-empty.

## Warning Conditions

Populate `warnings[]` (non-fatal) when any of the following are true:

- dynamic SQL detected in selected writer or related procedure context.
- temp/intermediate object references cannot be classified confidently.
- statement count exceeds decomposition complexity threshold (default: `>= 50` statements).
- low-confidence decomposition (all block or split confidences `< 0.75`).
- circular dependency detected and automatically broken with fallback ordering.

## Decomposer Boundary

Decomposer must not output:

- selected business answers (classification/keys/watermark/PII)
- materialization decisions
- generated dbt SQL or YAML

`validation.issues[]`, `warnings[]`, and `errors[]` use the shared diagnostics schema in
`docs/design/agent-contract/README.md`.
