# View Classification Signals

Rules for classifying a view as `stg` (staging) or `mart` (reporting/mart).

## Classification: stg vs mart

A view is `stg` when it is a thin layer that cleans, renames, or lightly transforms source data for consumption by procedures or other views. It is `mart` when it computes business-level aggregations, joins multiple entities, or is a reporting-ready dataset.

### Signal table

| Signal | Classification |
|---|---|
| Any referenced view has `profile.classification: mart` | mart (inherit) |
| `sql_elements` contains `aggregation` | mart |
| `sql_elements` contains `group_by` | mart |
| `sql_elements` contains `window_function` | mart |
| `sql_elements` contains `join` to more than one table | mart |
| `is_materialized_view: true` with aggregation or group_by | mart |
| `is_materialized_view: true` with no aggregation (lookup MV) | stg |
| Single source table, no aggregation, light CASE/column rename | stg |
| No `sql_elements` and `logic_summary` describes pass-through/rename | stg |
| No `sql_elements` and `logic_summary` describes aggregation/reporting | mart |

### Tie-breaking rules

- When signals conflict, choose `mart`. A false positive mart is safer than a false positive stg.
- If `sql_elements` is null (parse error) and `logic_summary` is also absent, classify as `mart` and note the uncertainty in the rationale.
- View name starting with `vw_stg`, `stg_`, or similar naming convention is weak evidence for `stg` — do not use as sole signal.

## SCD2 base-table signals

Views can provide evidence that an underlying table is SCD2. These signals do not affect the view's own `stg`/`mart` classification — they are cross-object signals to note in the rationale when profiling the base table.

| View pattern | Base-table signal |
|---|---|
| `WHERE is_current = 1` or `WHERE current_flag = 1` filtering a base table | Strong SCD2 signal on the base table — view exposes "current state" of a versioned dimension |
| `WHERE end_date IS NULL` or `WHERE valid_to = '9999-12-31'` filtering a base table | Strong SCD2 signal on the base table — sentinel date or NULL marks the active row |
| `ROW_NUMBER() OVER (PARTITION BY business_key ORDER BY effective_date DESC) = 1` | Medium SCD2 signal — view derives latest version via windowing instead of a flag column |

When these patterns are observed, record them in the view's profiling rationale so they can inform table classification during table profiling.

---

## Dependency inspection

The context JSON provides `references` and `referenced_by` with `object_type` on each `in_scope` entry.

- `references.tables` — tables this view reads from; `object_type: "table"`
- `references.views` — views this view reads from; `object_type: "view"` — run `uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover show --name <fqn>` to inspect their profile if needed
- `references.functions` — functions used; `object_type: "function"`
- `referenced_by.procedures` — procedures that read this view; `object_type: "procedure"`
- `referenced_by.views` — views that depend on this view; `object_type: "view"`

When any `references.views.in_scope` entry is present, run `uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover show --name <fqn>` to check if it already has a `profile.classification`. Inherit `mart` if any dependency is classified `mart`. This is rare — most views read directly from tables.

## Output format

```json
{
  "classification": "stg",
  "rationale": "Single source table with no aggregation or join — pass-through column selection.",
  "source": "llm"
}
```

| Field | Valid values |
|---|---|
| `classification` | `stg`, `mart` |
| `source` | `llm` |
