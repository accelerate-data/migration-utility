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

## Profiling Strategy

### 1. LoadWriterContext

- Input: `item_id`, `selected_writer`.
- Load selected writer SQL from `sys.sql_modules`.
- Resolve a bounded related procedure set for supporting evidence (writer + related readers/writers).

### 2. CollectCatalogSignals

- Query `sys.*` catalog views first for declared facts:
  - key constraints and unique indexes (`sys.indexes`, `sys.key_constraints`, `sys.index_columns`)
  - identity columns (`sys.identity_columns`) — definitive surrogate key signal
  - foreign key constraints (`sys.foreign_keys`, `sys.foreign_key_columns`)
  - sensitivity classifications (if present, `sys.sensitivity_classifications`)
  - CDC/change tracking metadata (`sys.tables.is_tracked_by_cdc`, `sys.change_tracking_tables`)
- Treat declared catalog facts as highest-priority evidence.

### 3. ParseProcedureSignals

- Parse procedure bodies using SQL AST (not regex).
- Extract signals for:
  - model classification patterns (`INSERT`, `UPDATE`, `DELETE`, `MERGE`, `TRUNCATE`, `GROUP BY`)
  - key behavior (especially `MERGE ON`, update predicates, join predicates)
  - surrogate key generation: `NEWID()`, `NEWSEQUENTIALID()`, `NEXT VALUE FOR` in INSERT or
    DEFAULT expressions — definitive surrogate signals when `sys.identity_columns` is absent
  - watermark behavior (`WHERE > @last_run`, `BETWEEN @start/@end`)
  - PII hints (column usage/context)
- Parse selected writer first; use related procedures as supporting evidence.
- For foreign key candidates, apply signal sources in this order:
  - Reader proc JOIN analysis (highest confidence): use `sys.dm_sql_referencing_entities` on the
    target table to find all procs that read it; parse their JOIN conditions on the target table's
    columns. Multiple independent reader procs joining on the same column is very high confidence.
    Also surfaces role-playing dimensions (two columns both joining `dim_date`) and degenerate
    dimensions (columns used in WHERE/GROUP BY but never joined to any dimension).
  - Writer proc JOIN analysis: writer JOINs staging to dimension tables to resolve surrogate keys
    before inserting — confirms the relationship but in the less direct write direction.
  - Naming-convention heuristics and sampled referential integrity checks (steps 4 and 5).

### 4. ApplyHeuristicSignals

- Apply naming and structural heuristics when catalog/parse are incomplete:
  - key suffixes (`_sk`, `_id`, `_code`, `_number`)
  - watermark name patterns (`modified_at`, `load_date`, `_dt`, `_ts`)
  - fact/dim shape heuristics (measures + FKs, SCD columns, milestone dates)
  - PII column name patterns: `email`, `ssn`, `dob`, `phone`, `mobile`, `address`, `zip`,
    `postal_code`, `credit_card`, `card_number`, `passport`, `national_id`, `ip_address`,
    `birth_date`, `first_name`, `last_name`, `full_name` (case-insensitive, fuzzy match)

### 5. RunSampledProfiling

- Use sampled data checks only for unresolved/tied candidates:
  - uniqueness/null checks for key candidates
  - orphan-rate checks for foreign key candidates
  - monotonicity/null checks for watermark candidates
  - PII value sampling: for `varchar`/`nvarchar` columns that passed name-pattern heuristics,
    sample up to 100 distinct non-null values and apply regex/NLP patterns (e.g. Presidio) to
    confirm or refute the PII classification
- Do not run full table scans.

### 6. GenerateCandidates

- Produce candidate arrays with confidence + rationale:
  - `candidate_classifications` (required)
  - `candidate_primary_keys` (required)
  - `candidate_natural_keys` (conditional)
  - `candidate_watermarks` (required)
  - `candidate_foreign_keys` (nice-to-have)
  - `candidate_pii_actions` (nice-to-have)
- Add provenance per candidate:
  - `signal_sources`: `catalog|proc_parse|heuristic|sampled_profile|llm`
  - `evidence_refs`: stable references to source evidence
- Candidate set size rules:
  - return top `3` candidates per category, sorted by confidence descending.
  - include additional tied candidates when confidence delta is `<= 0.05`.

### 7. ResolveWithRuleEngine

- Apply deterministic scoring and thresholds.
- Catalog facts take precedence over inferred patterns.
- Keep multiple candidates when confidence is close.
- Apply FK type resolution rules from `what-to-profile-and-why.md` (Q4) for
  `candidate_foreign_keys[*].fk_type`.
- Thresholds:
  - usable candidate: `confidence >= 0.75`
  - low-confidence candidate: `confidence < 0.75`
  - near-tie: confidence delta between top candidates `<= 0.05`
- Set item `status`:
  - `ok` when required categories have usable candidates
    (`candidate_classifications`, `candidate_primary_keys`, `candidate_watermarks`).
  - `partial` when one or more required categories remain unresolved or only low-confidence
    candidates exist.
  - `candidate_natural_keys` may be empty and still `ok` when
    `candidate_primary_keys[*].primary_key_type == "surrogate"` and all required categories are
    resolved.
  - `error` on runtime failures

### 8. LLMFallbackForAmbiguity

- Use only when deterministic steps cannot disambiguate.
- LLM output is advisory ranking/rationale.
- LLM must not override hard catalog facts.

### 9. ValidateOutput

- Run internal consistency checks on output items.
- This is not FDE approval and not runtime error handling.
- Runtime failures must be reported in `errors`.
- Validation checklist:
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
      "candidate_classifications": [
        {
          "resolved_kind": "fact_transaction",
          "confidence": 0.88,
          "rationale": ["Transaction-level insert behavior detected."],
          "signal_sources": ["proc_parse"],
          "evidence_refs": ["proc:dbo.usp_load_fact_sales:insert_block_1"]
        }
      ],
      "candidate_primary_keys": [
        {
          "columns": ["sale_id"],
          "primary_key_type": "surrogate|natural|composite|unknown",
          "confidence": 0.97,
          "rationale": ["High uniqueness and stable writer behavior."],
          "signal_sources": ["catalog", "proc_parse"],
          "evidence_refs": ["sys.key_constraints:pk_fact_sales", "proc:merge_on:sale_id"]
        }
      ],
      "candidate_natural_keys": [
        {
          "columns": ["order_id", "line_number"],
          "confidence": 0.78,
          "rationale": ["Business-level row identity pattern."],
          "signal_sources": ["proc_parse", "heuristic"],
          "evidence_refs": ["proc:merge_on:order_id_line_number"]
        }
      ],
      "candidate_foreign_keys": [
        {
          "column": "customer_sk",
          "references_source_relation": "dbo.dim_customer",
          "references_column": "customer_sk",
          "fk_type": "standard|role_playing|degenerate",
          "confidence": 0.9,
          "rationale": ["Reader JOIN evidence and naming pattern evidence."],
          "signal_sources": ["proc_parse", "heuristic"],
          "evidence_refs": ["proc:reader_join:customer_sk=dim_customer.customer_sk"]
        }
      ],
      "candidate_watermarks": [
        {
          "column": "load_date",
          "confidence": 0.94,
          "rationale": ["Monotonic load timestamp in writer logic."],
          "signal_sources": ["proc_parse", "heuristic"],
          "evidence_refs": ["proc:where:load_date>@last_run"]
        }
      ],
      "candidate_pii_actions": [
        {
          "column": "customer_email",
          "entity": "email",
          "suggested_action": "mask",
          "confidence": 0.93,
          "rationale": ["Column and value pattern evidence."],
          "signal_sources": ["heuristic", "sampled_profile"],
          "evidence_refs": ["pii:name_pattern:email", "pii:sample:email_match"]
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

- `candidate_foreign_keys[*].references_source_relation` and
  `candidate_foreign_keys[*].references_column` are source-side SQL Server identifiers.
- Profiler must not emit dbt `ref()` names. Namespace translation is planner/migrator scope.

## What Profiler Must Not Output

- Direct metadata planner can fetch reliably (for example target schema definitions).
- Final dbt SQL or Jinja model content.
- Final materialization/test decisions.

`validation.issues[]`, `warnings[]`, and `errors[]` use the shared diagnostics schema in
`docs/design/agent-contract/README.md`.

## Handoff

- Decomposer consumes `item_id` and `selected_writer` from application-routed inputs.
- Planner consumes selected profiler answers after FDE approval.
