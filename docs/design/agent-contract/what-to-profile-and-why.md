# What to Profile and Why

Reference for the profiler agent. This document serves two purposes:

1. Defines the six profiling questions, why each matters, and what signals to look for.
2. Acts as **prompt context** for the LLM profiling step — the pattern tables below are the knowledge the LLM applies, not rules we implement in code.

## Profiling Pipeline

The `/profile` skill runs the full pipeline for one table:

| Step | Nature | What it does |
|---|---|---|
| 1. Context assembly | Deterministic | `profile.py` reads catalog files + DDL, cherry-picks relevant signals, outputs context JSON. |
| 2. LLM profiling | LLM | Claude reasons over context JSON + this document, produces classification, keys, watermarks, FKs, PII. |
| 3. Catalog update | Deterministic | Skill writes `profile` section back into `catalog/tables/<table>.json`. |

All catalog signals (PKs, FKs, identity, CDC, sensitivity, read/write references) are pre-captured by setup-ddl (VU-766). No live database access at profiling time. No sampled data profiling.

**Key principle:** Catalog signals are facts, not candidates. If the catalog declares a PK, that's the PK — the LLM does not re-infer it. The LLM fills in what the catalog doesn't answer: classification, natural keys, watermarks, FK types, PII actions.

## Necessity Summary

| # | Question | Verdict | Impact if missing |
|---|---|---|---|
| Q1 | What kind of model is this? | **Required** | Wrong materialization, wrong test suite |
| Q2 | Primary key candidate | **Required** | Cannot write `unique_key` or uniqueness tests |
| Q3 | Foreign key candidates | Nice-to-have | Missing `relationships` tests only; SQL still works |
| Q4 | Natural key vs surrogate key | **Required** | Wrong `generate_surrogate_key` decision, wrong merge key |
| Q5 | Incremental watermark | **Required** | Must fall back to full-refresh `table` materialization |
| Q6 | PII handling candidates | Nice-to-have | SQL correct; compliance risk if PII flows unmasked |

---

## Catalog Signals (Pre-captured by setup-ddl)

These are already in `catalog/tables/<table>.json` from setup-ddl (VU-766). `profile.py` reads and includes them in the context JSON.

| Catalog field | What it provides |
|---|---|
| `primary_keys` | Declared PKs and unique constraints — **fact, not candidate** |
| `auto_increment_columns` | Definitive surrogate key signal — **fact** |
| `foreign_keys` | Declared FK relationships — **fact** (LLM classifies `fk_type`) |
| `sensitivity_classifications` | PII labels (often empty) — **fact when present** |
| `change_capture` | Informs incremental strategy |
| `referenced_by` | Which procs/views/functions reference this table, with `is_selected`/`is_updated` per column |

Additionally, `catalog/procedures/<writer>.json` provides `references` — all tables the writer proc touches, with column-level `is_selected`/`is_updated` flags.

The LLM must treat catalog facts as ground truth and not contradict them.

---

## LLM Reference: Q1 — What Kind of Model Is This?

**Why required:** Model type drives materialization strategy (`incremental`, `table`, `snapshot`), which dbt tests to generate, and whether SCD2 history logic is needed. A wrong classification produces wrong code, not just suboptimal code.

### Write-Pattern Signals

| Proc pattern | Classification |
|---|---|
| Pure `INSERT`, no `UPDATE` or `DELETE` | `fact_transaction` |
| `INSERT … SELECT … GROUP BY` (aggregation before write) | `fact_aggregate` |
| `TRUNCATE` + `INSERT` with descriptive VARCHAR columns | `dim_non_scd` |
| `TRUNCATE` + `INSERT` with measure + FK columns | `fact_periodic_snapshot` |
| `MERGE` — simple `WHEN MATCHED THEN UPDATE` only | `dim_scd1` |
| `MERGE` — expire matched row + insert history row (`valid_to`, `is_current`) | `dim_scd2` |
| `INSERT` then `UPDATE` targeting milestone date columns on existing rows | `fact_accumulating_snapshot` |
| Cross-join `INSERT` of low-cardinality flag combinations | `dim_junk` |

### Column Shape Signals

| Column pattern | Signal |
|---|---|
| `valid_from` / `valid_to` / `is_current` / `current_flag` | `dim_scd2` |
| Multiple milestone date columns (`order_date`, `ship_date`, `close_date`) | `fact_accumulating_snapshot` |
| `snapshot_date` / `as_of_date` / `period_date` | `fact_periodic_snapshot` |
| Multiple `BIT`/`TINYINT` flag columns, all low-cardinality | `dim_junk` candidate |
| Surrogate PK (`_sk`) + separate natural key column | Dimension (SCD1 or SCD2) |
| FK columns (`_sk`) + numeric measure columns | Fact table |

---

## LLM Reference: Q2 — Primary Key Candidate

**Why required:** dbt `unique_key` in incremental models requires an explicit PK. Missing it forces a full-refresh model or produces duplicates.

### Signals (after catalog)

| Source | Notes |
|---|---|
| MERGE ON clause in proc code | Business key / table grain — the strongest code-level signal. Fabric Warehouse schemas often omit declared constraints, making this the primary fallback. |
| UPDATE / DELETE `WHERE col = @param` in proc | Single-row lookup key |

---

## LLM Reference: Q3 — Foreign Key Candidates

**Why nice-to-have:** Not needed for correct dbt SQL. Needed for `relationships` tests in schema YAML. Also the only way to detect role-playing dimensions and degenerate dimensions.

### Signals (after catalog)

| Source | Notes |
|---|---|
| Reader proc JOIN analysis | Use `sys.dm_sql_referencing_entities` on the target table to find procs that read it; parse their JOIN conditions. Multiple independent reader procs joining on the same column is very high confidence. Surfaces role-playing dimensions (two columns both joining `dim_date`) and degenerate dimensions (columns used in WHERE/GROUP BY but never joined to a dimension). |
| Writer proc JOIN analysis | Writer JOINs staging to dimension tables to resolve surrogate keys before inserting — confirms the relationship but in the less direct write direction. |
| Naming-convention patterns | Strip `_sk`/`_id` suffix; check if stem matches a known dimension table name. `date_key`/`date_sk` → `dim_date` is high confidence; `<name>_id` in a fact table is medium. |

### FK Type Resolution

| `fk_type` | Rule |
|---|---|
| `standard` | One fact column joins one dimension key with no multi-role pattern |
| `role_playing` | Two or more distinct fact columns join the same dimension relation+key (e.g. `order_date_sk` and `ship_date_sk` both to `dim_date.date_sk`) |
| `degenerate` | Column behaves as a business key in fact usage (SELECT/GROUP BY/WHERE) but no dimension join target is found |

---

## LLM Reference: Q4 — Natural Key vs Surrogate Key

**Why required:** Determines whether the model calls `dbt_utils.generate_surrogate_key` and whether the incremental `unique_key` is a raw column or a generated hash.

### Signals (after catalog identity columns)

| Signal | Notes |
|---|---|
| `NEWID()` / `NEWSEQUENTIALID()` / `NEXT VALUE FOR` in proc body | Definitive proc-assigned surrogate |
| Column name suffix: `_sk` / `_guid` → surrogate; `_code` / `_number` / `_num` → natural | Reliable in well-named Kimball schemas |
| MERGE ON uses a different column from the INSERT's PK column | Classic pattern: MERGE ON `customer_id` (natural); INSERT populates `customer_sk` (surrogate) |

**Note:** The MERGE ON clause simultaneously answers Q2, Q4, and partially Q3. When a MERGE is present, analyze it first.

---

## LLM Reference: Q5 — Incremental Watermark

**Why required:** Without a watermark column the model can only be materialized as `table` (full refresh). For large fact tables this is operationally unacceptable.

### Signals

| Source | Notes |
|---|---|
| WHERE clause in proc body | `WHERE load_date > @last_run` / `BETWEEN @start AND @end` — nearly definitive |
| Column name patterns | `modified_at`, `updated_at`, `load_date`, `etl_date`, `batch_date`, abbreviations (`_dt`, `_ts`, `_dttm`). Must be datetime type, not varchar. |
| CDC / CT metadata | `sys.tables.is_tracked_by_cdc`, `sys.change_tracking_tables` — informs strategy, does not identify the watermark column |

### dbt Strategy Mapping

| Watermark type | Recommended dbt strategy |
|---|---|
| `datetime` / `datetime2` column | `microbatch` (dbt 1.6+, preferred) or `merge` |
| Integer identity column | `merge` or `delete+insert` |
| Append-only, no updates | `append` |
| Date-partitioned table | `insert_overwrite` |

---

## LLM Reference: Q6 — PII Handling Candidates

**Why nice-to-have:** Does not affect SQL correctness. A missed PII column flowing unmasked into a gold model is a compliance incident.

### Signals (after catalog sensitivity classifications)

| Source | Notes |
|---|---|
| Column name patterns | `email`, `ssn`, `dob`, `phone`, `mobile`, `address`, `zip`, `postal_code`, `credit_card`, `card_number`, `passport`, `national_id`, `ip_address`, `birth_date`, `first_name`, `last_name`, `full_name`. Case-insensitive, fuzzy match. |
| Column type + context | VARCHAR/NVARCHAR columns with PII-suggestive names deserve higher scrutiny |

### Suggested Actions

| Action | When |
|---|---|
| `mask` | Default for confirmed PII |
| `drop` | Column not needed downstream |
| `tokenize` | Joinability must be preserved |
| `keep` | Explicit business justification |

**dbt output:** Write PII metadata as column-level `meta` tags in the generated schema YAML:

```yaml
columns:
  - name: customer_email
    meta:
      contains_pii: true
      pii_type: email_address
```

---

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

## Namespace Rules

- `candidate_foreign_keys[*].references_source_relation` and `references_column` are source-side SQL Server identifiers.
- Profiler must not emit dbt `ref()` names. Namespace translation is migrator scope.
