# Profiling Signals

Operational reference for answering the six profiling questions. Used by both the `/profiling-table` skill and the profiler agent.

**Key principle:** Catalog signals are facts, not candidates. If the catalog declares a PK, that is the PK. The LLM fills in what the catalog does not answer.

## Catalog Signals (Pre-captured)

These are already in `catalog/tables/<table>.json` from setup-ddl. `profile.py` reads and includes them in the context JSON.

| Catalog field | What it provides |
|---|---|
| `primary_keys` | Declared PKs and unique constraints — **fact, not candidate** |
| `auto_increment_columns` | Definitive surrogate key signal — **fact** |
| `foreign_keys` | Declared FK relationships — **fact** (LLM classifies `fk_type`) |
| `sensitivity_classifications` | PII labels (often empty) — **fact when present** |
| `change_capture` | Informs incremental strategy |
| `referenced_by` | Which procs/views/functions reference this table, with `is_selected`/`is_updated` per column |

Additionally, `catalog/procedures/<writer>.json` provides `references` — all tables the writer proc touches, with column-level `is_selected`/`is_updated` flags.

---

## Q1 — Classification

Determines materialization (`incremental`, `table`, `snapshot`) and test strategy.

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

## Q2 — Primary Key

Check catalog `primary_keys` first. If declared, that is the answer (`source: "catalog"`).

If no declared PK:

| Signal | Notes |
|---|---|
| MERGE ON clause in proc body | Business key / table grain — strongest code-level signal. Fabric Warehouse schemas often omit declared constraints. |
| UPDATE / DELETE `WHERE col = @param` in proc | Single-row lookup key |

---

## Q3 — Foreign Keys

Check catalog `foreign_keys` first. If declared, those are confirmed. Classify `fk_type` using proc JOIN patterns.

If no declared FKs:

| Signal | Notes |
|---|---|
| Reader proc JOIN analysis | Find procs that read this table; parse their JOIN conditions. Multiple independent readers joining on the same column is high confidence. Surfaces role-playing and degenerate dimensions. |
| Writer proc JOIN analysis | Writer JOINs staging to dimension tables to resolve surrogate keys — confirms relationship. |
| Naming-convention patterns | Strip `_sk`/`_id` suffix; check if stem matches a known dimension table name. |

### FK Type Resolution

| `fk_type` | Rule |
|---|---|
| `standard` | One fact column joins one dimension key with no multi-role pattern |
| `role_playing` | Two or more distinct fact columns join the same dimension relation+key |
| `degenerate` | Column behaves as a business key (SELECT/GROUP BY/WHERE) but no dimension join target found |

---

## Q4 — Natural Key vs Surrogate Key

Check catalog `auto_increment_columns` first. If present, the PK is surrogate (`source: "catalog"`).

If no identity column:

| Signal | Notes |
|---|---|
| `NEWID()` / `NEWSEQUENTIALID()` / `NEXT VALUE FOR` in proc body | Definitive proc-assigned surrogate |
| Column name suffix: `_sk` / `_guid` → surrogate; `_code` / `_number` → natural | Reliable in well-named Kimball schemas |
| MERGE ON uses different column from INSERT's PK column | Classic pattern: MERGE ON `customer_id` (natural); INSERT populates `customer_sk` (surrogate) |

**Note:** The MERGE ON clause simultaneously answers Q2, Q4, and partially Q3. When a MERGE is present, analyze it first.

---

## Q5 — Watermark

| Signal | Notes |
|---|---|
| WHERE clause in proc body | `WHERE load_date > @last_run` / `BETWEEN @start AND @end` — nearly definitive |
| Column name patterns | `modified_at`, `updated_at`, `load_date`, `etl_date`, `batch_date`, `_dt`, `_ts`, `_dttm`. Must be datetime type, not varchar. |
| CDC / CT metadata | `change_capture` from catalog — informs strategy, does not identify the watermark column |

### dbt Strategy Mapping

| Watermark type | Recommended dbt strategy |
|---|---|
| `datetime` / `datetime2` column | `microbatch` (dbt 1.6+, preferred) or `merge` |
| Integer identity column | `merge` or `delete+insert` |
| Append-only, no updates | `append` |
| Date-partitioned table | `insert_overwrite` |

---

## Q6 — PII Actions

Check catalog `sensitivity_classifications` first. If populated, those are confirmed (`source: "catalog"`).

For remaining columns:

| Signal | Notes |
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

---

## Allowed Enum Values

`resolved_kind`: `dim_non_scd`, `dim_scd1`, `dim_scd2`, `dim_junk`, `fact_transaction`, `fact_periodic_snapshot`, `fact_accumulating_snapshot`, `fact_aggregate`

`fk_type`: `standard`, `role_playing`, `degenerate`

`source`: `catalog`, `llm`, `catalog+llm`
