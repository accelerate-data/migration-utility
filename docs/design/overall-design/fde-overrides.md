# FDE Overrides Design

FDE edits are stored in a local SQLite `fde_overrides` table. This document defines the schema and the editable field set per agent stage.

## Table Schema

```sql
CREATE TABLE fde_overrides (
  id                   INTEGER PRIMARY KEY,
  project_id           TEXT NOT NULL,  -- UUID; references projects.id
  table_id             TEXT NOT NULL,
  stage                TEXT NOT NULL,
  field                TEXT NOT NULL,
  fde_value            TEXT NOT NULL,  -- JSON-encoded; scalars, arrays, and objects all stored as JSON
  source_run_id        TEXT NOT NULL,
  source_submitted_ts  TEXT NOT NULL,  -- ISO 8601 UTC; for tooltip display only
  UNIQUE (project_id, table_id, stage, field)
);
```

- `field` uses dot-notation for nested scalars (`plan.materialization`) and bracket-notation for
  array elements keyed by stable ID (`split_points[01_extract_sales_stage].proposed_model_name`).
- `fde_value` is always JSON-encoded regardless of type (`"table"`, `["sale_id"]`, `false`, etc.).
- Effective value consumed by the next agent: `COALESCE(fde_value, agent_default)` where
  `agent_default` is the top-ranked candidate or agent-selected value.

---

## Visual Indicators in the Modal

When the stage N modal shows stage N-1's output, each editable field is in one of two states:

| State | Condition | Visual treatment |
|---|---|---|
| **Plain** | No `fde_overrides` row for this field | Field shows raw agent output; no marker |
| **Overridden** | Override exists | Field shows FDE value with a pencil icon; tooltip shows original agent value |

If a table is dirty, it will only appear in the tab where `dirty_from` points — the FDE naturally
works forward from the earliest dirty stage. See [README.md](README.md) Stage Gate.

---

## Editable Fields per Stage

### `scoping-agent`

The FDE override is required when status is `ambiguous_multi_writer`; optional otherwise.

| `field` | Type | Agent default | FDE action |
|---|---|---|---|
| `selected_writer` | string | Top-ranked candidate or agent-resolved writer | Select a different writer from `candidate_writers` |

**Example rows for `dbo.fact_sales`:**

| `table_id` | `stage` | `field` | `fde_value` | `source_run_id` | `source_submitted_ts` |
|---|---|---|---|---|---|
| `dbo.fact_sales` | `scoping-agent` | `selected_writer` | `"dbo.usp_load_fact_sales_v2"` | `uuid-abc` | `2024-01-15T10:30:00Z` |

**Read-only:** `candidate_writers`, `write_type`, `call_path`, `confidence`, `rationale`.

---

### `profiler-agent`

The profiler outputs ranked candidates; the system auto-selects the top candidate as the default
answer. FDE overrides record deviations from the auto-selection.

| `field` | Type | Agent default | FDE action |
|---|---|---|---|
| `answers.classification` | string | Highest-confidence `resolved_kind` | Select a different classification from candidates |
| `answers.primary_key` | JSON array | Highest-confidence `columns` | Select different columns from candidates |
| `answers.primary_key_type` | string | Highest-confidence `primary_key_type` | Override type (`surrogate`, `natural`, `composite`) |
| `answers.natural_key` | JSON array | Highest-confidence natural key columns, or `null` | Select different columns or set to `null` |
| `answers.watermark` | string or null | Highest-confidence watermark column | Select a different column or set to `null` |
| `answers.foreign_keys` | JSON array | All candidates with `confidence >= 0.75` | Add, remove, or edit FK entries |
| `answers.pii_actions` | JSON array | All candidates with `confidence >= 0.75` | Add, remove, or change `action` per column |
| `plan.materialization` | string | Derived from classification rules | Override materialization strategy for the migrator |
| `plan.documentation.model_name` | string | Derived from table name | Override dbt model name |
| `plan.documentation.model_description` | string | Generated | Override model description |
| `plan.documentation.owner` | string | None | Set team/owner |
| `plan.documentation.tags` | JSON array | None | Set tags |

These plan fields are consumed by the migrator agent when building dbt artifacts.

**Example rows for `dbo.fact_sales`:**

| `table_id` | `stage` | `field` | `fde_value` | `source_run_id` | `source_submitted_ts` |
|---|---|---|---|---|---|
| `dbo.fact_sales` | `profiler-agent` | `answers.classification` | `"fact_transaction"` | `uuid-def` | `2024-01-15T11:00:00Z` |
| `dbo.fact_sales` | `profiler-agent` | `answers.primary_key` | `["sale_id"]` | `uuid-def` | `2024-01-15T11:00:00Z` |
| `dbo.fact_sales` | `profiler-agent` | `answers.primary_key_type` | `"surrogate"` | `uuid-def` | `2024-01-15T11:00:00Z` |
| `dbo.fact_sales` | `profiler-agent` | `answers.natural_key` | `["order_id","line_number"]` | `uuid-def` | `2024-01-15T11:00:00Z` |
| `dbo.fact_sales` | `profiler-agent` | `answers.watermark` | `"load_date"` | `uuid-def` | `2024-01-15T11:00:00Z` |
| `dbo.fact_sales` | `profiler-agent` | `answers.foreign_keys` | `[{"column":"customer_sk","references_source_relation":"dbo.dim_customer","references_column":"customer_sk","fk_type":"standard"}]` | `uuid-def` | `2024-01-15T11:00:00Z` |
| `dbo.fact_sales` | `profiler-agent` | `answers.pii_actions` | `[{"column":"customer_email","action":"mask"}]` | `uuid-def` | `2024-01-15T11:00:00Z` |

**Read-only:** `confidence`, `rationale`, `signal_sources`, `evidence_refs`, `warnings`, `validation`, `errors`.

---

### `test-generator-agent` and `migrator-agent`

These are final output stages. Their modals are read-only — no FDE overrides are permitted.
No rows are ever written to `fde_overrides` for these stages.
