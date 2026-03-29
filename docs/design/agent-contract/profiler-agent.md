# Profiler Agent Contract

The profiler agent proposes migration candidates for FDE review.
It should output only inferred candidates that require judgment, not direct facts planner can fetch via tools.

## Philosophy and Boundary

- Profiler is for candidate generation, not metadata transport.
- Planner retrieves direct facts (schema, constraints, object metadata) using tools.
- FDE approves profiler candidates before planner consumes them.
- Avoid duplicate derivation: if planner can fetch a fact reliably, profiler should not include it.

## Required Input

```json
{
  "schema_version": "1.0",
  "run_id": "uuid",
  "items": [
    {
      "item_id": "dbo.fact_sales",
      "selected_writer": "dbo.usp_load_fact_sales",
      "related_procedure_depth": 1
    }
  ]
}
```

## Input Semantics

- `related_procedure_depth` controls supporting procedure expansion around `selected_writer`.
- Units: procedure-call hops (`0` = selected writer only, `1` = direct related procedures).
- Valid range: integer `0..2`.
- Default: `1`.

## Profiling Pipeline

The profiler uses a 4-step pipeline: deterministic catalog collection, LLM inference, sampled tiebreaking, and output validation. See [What to Profile and Why](what-to-profile-and-why.md) for the full reference tables the LLM uses.

### 1. CollectCatalogSignals (Deterministic)

Collect declared facts from `sys.*` catalog views. These are ground truth — the LLM must not contradict them.

The batch agent queries `sys.*` directly via `mssql_mcp`. The interactive path uses `profile.py` (shared skill) which collects the same signals from `ddl_mcp` or `mssql_mcp` and returns them as structured JSON.

Catalog sources:

| Source | What it provides |
|---|---|
| `sys.indexes` + `sys.key_constraints` + `sys.index_columns` | Declared PKs and unique indexes |
| `sys.identity_columns` | Definitive surrogate key signal |
| `sys.foreign_keys` + `sys.foreign_key_columns` | Declared FK relationships |
| `sys.sensitivity_classifications` | PII labels (SQL Server 2019+, often unpopulated) |
| `sys.tables.is_tracked_by_cdc`, `sys.change_tracking_tables` | CDC/change tracking metadata |

Also load:

- Selected writer SQL from `sys.sql_modules`.
- Bounded related procedure set for supporting evidence (writer + related readers/writers based on `related_procedure_depth`).

### 2. LLMProfiling

Given catalog signals + proc body + column list + the reference tables from [What to Profile and Why](what-to-profile-and-why.md), infer all six candidate categories in one pass:

- `candidate_classifications` (required) — model type
- `candidate_primary_keys` (required) — PK columns
- `candidate_natural_keys` (conditional) — business keys
- `candidate_watermarks` (required) — incremental load columns
- `candidate_foreign_keys` (nice-to-have) — FK relationships with type
- `candidate_pii_actions` (nice-to-have) — PII masking/tokenization recommendations

Rules:

- Catalog facts override all inference. If `sys.key_constraints` declares a PK, the LLM must include it as the top candidate.
- Add provenance per candidate: `signal_sources` (`catalog|llm|sampled_profile`) and `evidence_refs` (stable references to source evidence).
- Return top `3` candidates per category, sorted by confidence descending. Include additional tied candidates when confidence delta is `<= 0.05`.
- Mark candidates as low-confidence when the LLM is uncertain — these are routed to step 3.

### 3. SampledProfiling (Deterministic — Tiebreaker Only)

Run SQL-based checks only for candidates the LLM flags as low-confidence. Do not run full table scans.

| Check | When to run |
|---|---|
| Uniqueness: `COUNT(*) = COUNT(DISTINCT col)` | Low-confidence PK candidates |
| Composite uniqueness: `COUNT(*) = COUNT(DISTINCT CONCAT(col_a, col_b))` | Low-confidence composite PK candidates |
| Orphan rate: LEFT JOIN orphan count | Low-confidence FK candidates |
| Monotonicity + null rate + MAX recency | Low-confidence watermark candidates |
| Value sampling with regex/NLP patterns (e.g. Presidio) | Low-confidence PII candidates |

Use `TABLESAMPLE` on large tables. Update candidate confidence scores based on results.

### 4. ValidateOutput

Run internal consistency checks on output items.

Thresholds:

- usable candidate: `confidence >= 0.75`
- low-confidence candidate: `confidence < 0.75`
- near-tie: confidence delta between top candidates `<= 0.05`

Set item `status`:

- `ok` when required categories have usable candidates (`candidate_classifications`, `candidate_primary_keys`, `candidate_watermarks`).
- `partial` when one or more required categories remain unresolved or only low-confidence candidates exist.
- `candidate_natural_keys` may be empty and still `ok` when `candidate_primary_keys[*].primary_key_type == "surrogate"` and all required categories are resolved.
- `error` on runtime failures.

Validation checklist:

- `item_id` is present.
- `status` is one of: `ok|partial|error`.
- candidate arrays are structurally valid.
- every candidate `confidence` is within `[0,1]`.
- every candidate has `rationale` as an array of strings.
- every candidate has `signal_sources` and `evidence_refs`.
- if `status == "ok"`: required candidate categories are present.
- if `status == "partial"`: `validation.issues` includes missing required categories.
- if `status == "error"`: `errors` is non-empty.
- `validation.passed` is `false` when any validation issue exists.
- summary counts match item-level statuses.

## Output Schema (TableProfile)

```json
{
  "schema_version": "",
  "run_id": "",
  "results": [
    {
      "item_id": "",
      "status": "",
      "candidate_classifications": [],
      "candidate_primary_keys": [],
      "candidate_natural_keys": [],
      "candidate_foreign_keys": [],
      "candidate_watermarks": [],
      "candidate_pii_actions": [],
      "warnings": [],
      "validation": {},
      "errors": []
    }
  ],
  "summary": {}
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
      "status": "ok",
      "candidate_classifications": [
        {
          "resolved_kind": "fact_transaction",
          "confidence": 0.88,
          "rationale": ["Pure INSERT with no UPDATE or DELETE in writer proc."],
          "signal_sources": ["llm"],
          "evidence_refs": ["proc:dbo.usp_load_fact_sales:insert_block"]
        }
      ],
      "candidate_primary_keys": [
        {
          "columns": ["sale_id"],
          "primary_key_type": "surrogate",
          "confidence": 0.97,
          "rationale": ["Declared PK constraint in catalog."],
          "signal_sources": ["catalog"],
          "evidence_refs": ["sys.key_constraints:pk_fact_sales"]
        }
      ],
      "candidate_natural_keys": [
        {
          "columns": ["order_id", "line_number"],
          "confidence": 0.78,
          "rationale": ["MERGE ON clause uses these columns as business key."],
          "signal_sources": ["llm"],
          "evidence_refs": ["proc:merge_on:order_id_line_number"]
        }
      ],
      "candidate_foreign_keys": [
        {
          "column": "customer_sk",
          "references_source_relation": "dbo.dim_customer",
          "references_column": "customer_sk",
          "fk_type": "standard",
          "confidence": 0.9,
          "rationale": ["Writer JOIN to dim_customer on customer_sk."],
          "signal_sources": ["llm"],
          "evidence_refs": ["proc:writer_join:customer_sk=dim_customer.customer_sk"]
        }
      ],
      "candidate_watermarks": [
        {
          "column": "load_date",
          "confidence": 0.94,
          "rationale": ["WHERE load_date > @last_run in writer proc."],
          "signal_sources": ["llm"],
          "evidence_refs": ["proc:where:load_date>@last_run"]
        }
      ],
      "candidate_pii_actions": [
        {
          "column": "customer_email",
          "entity": "email",
          "suggested_action": "mask",
          "confidence": 0.93,
          "rationale": ["Column name matches PII pattern."],
          "signal_sources": ["llm"],
          "evidence_refs": ["pii:name_pattern:email"]
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

## Classification Kinds

`candidate_classifications[*].resolved_kind` must be one of:

- `dim_non_scd`
- `dim_scd1`
- `dim_scd2`
- `dim_junk`
- `fact_transaction`
- `fact_periodic_snapshot`
- `fact_accumulating_snapshot`
- `fact_aggregate`

## Foreign Key Types

`candidate_foreign_keys[*].fk_type` must be one of:

- `standard`
- `role_playing`
- `degenerate`

## Suggested PII Actions

`candidate_pii_actions[*].suggested_action` must be one of:

- `mask`
- `drop`
- `tokenize`
- `keep`

## Namespace Rules

- `candidate_foreign_keys[*].references_source_relation` and `candidate_foreign_keys[*].references_column` are source-side SQL Server identifiers.
- Profiler must not emit dbt `ref()` names. Namespace translation is planner/migrator scope.

## What Profiler Must Not Output

- Direct metadata planner can fetch reliably (for example target schema definitions).
- Final dbt SQL or Jinja model content.
- Final materialization/test decisions.

`validation.issues[]`, `warnings[]`, and `errors[]` use the shared diagnostics schema in `docs/design/agent-contract/README.md`.

## Handoff

- Decomposer consumes `item_id` and `selected_writer` from application-routed inputs.
- Planner consumes selected profiler answers after FDE approval.
