# What to Profile and Why

Reference for the profiler agent's seven candidate fields: why each is needed for a
stored-procedure-to-dbt migration, and the ranked options for deriving it.

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

## Detection Signal Priority (All Questions)

Regardless of which question is being answered, apply signal sources in this order:

1. **SQL Server catalog (`sys.*`)** — declared facts; definitive when present.
2. **Stored procedure body parse** — the richest source for everything catalog misses.
   `sqlglot` (Python, MIT, T-SQL dialect) is the recommended parser; it produces an AST
   from `sys.sql_modules` text.
3. **Naming-convention heuristics** — fast, high-signal in well-named Kimball schemas.
4. **Data profiling (sampled)** — expensive; use only as tiebreaker for unresolved candidates.
5. **LLM chain-of-thought** — for genuinely ambiguous cases and PII edge cases.

---

## Input - Who Writes the Table?

The profiler receives `selected_writer` as input and assumes Q1 is already resolved. See the [Scoping Agent](scoping-agent.md) for how writer discovery and selection work.

The options below are retained for reference — they describe how the scoping agent answers Q1.

### Options

| Approach | Coverage | What it misses |
|---|---|---|
| `sys.dm_sql_referencing_entities` with `is_updated`/`is_insert_all` | ~70–80 % | Dynamic SQL, `TRUNCATE`, CLR procs |
| sqlglot static parse of `sys.sql_modules` text (INSERT/UPDATE/MERGE/SELECT INTO targets) | ~85–90 % combined with catalog | Obfuscated dynamic SQL |
| Query Store + Extended Events (runtime) | 100 % — only approach catching dynamic SQL | Requires live workload; does not work on idle systems |
| Commercial lineage (Manta, Dataedo) | Most comprehensive | Cost; out of scope for bounded migration |

**Recommended:** catalog → sqlglot static parse. Flag any proc body containing `EXEC(@sql)` or
`sp_executesql` as "dynamic SQL detected — manual verification required."

---

## Q2 — What Kind of Model Is This?

**Why required:** Model type drives materialization strategy (`incremental`, `table`, `snapshot`),
which dbt tests to generate, and whether SCD2 history logic is needed in the SQL. A wrong
classification produces wrong code, not just suboptimal code.

### Write-Pattern Signals (Strongest)

Parse the stored procedure body for DML patterns. These are nearly deterministic:

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

### Column Heuristic Signals (Secondary)

| Column pattern | Signal |
|---|---|
| `valid_from` / `valid_to` / `is_current` / `current_flag` | `dim_scd2` |
| Multiple milestone date columns (`order_date`, `ship_date`, `close_date`) | `fact_accumulating_snapshot` |
| `snapshot_date` / `as_of_date` / `period_date` | `fact_periodic_snapshot` |
| Multiple `BIT`/`TINYINT` flag columns, all low-cardinality | `dim_junk` candidate |
| Surrogate PK (`_sk`) + separate natural key column | Dimension (SCD1 or SCD2) |
| FK columns (`_sk`) + numeric measure columns | Fact table |

### Data Profiling Signals (Tiebreaker)

| Metric | Query | Signal |
|---|---|---|
| Row growth rate | Compare `COUNT(*)` across daily snapshots | Steady growth → `fact_transaction`; batch step → `fact_periodic_snapshot` |
| Versions per business key | `AVG(COUNT(*)) GROUP BY business_key` | Average > 1.2 → `dim_scd2` |
| NULL milestone dates | `SUM(CASE WHEN step2_date IS NULL ...)` | Partial NULLs → `fact_accumulating_snapshot` |

### LLM Fallback

For unresolved cases, use a structured chain-of-thought prompt:

1. What is the target table?
2. What DML operations appear (INSERT, UPDATE, DELETE, MERGE, TRUNCATE)?
3. For MERGE: describe WHEN MATCHED and WHEN NOT MATCHED clauses.
4. Is there history-preservation logic (expiry dates, `is_current`, OUTPUT clause)?
5. Are there multiple milestone date columns being SET on UPDATE?
6. Is there a GROUP BY before INSERT?
7. Is there TRUNCATE before INSERT?
8. List target columns and apparent types.
9. Classify with confidence (high / medium / low).

**Recommended:** Rule-based parse (sqlglot) → column heuristics → profiling tiebreaker → LLM
CoT for anything still unresolved. Require human classification when all three disagree.

**Existing tooling gap:** No open-source tool automates stored-procedure → dimensional model
classification. `sqlglot` is the right building block for the parse layer.

---

## Q3 — Primary Key Candidate

**Why required:** dbt `unique_key` in incremental models requires an explicit PK. Missing it
forces a full-refresh model or produces duplicates.

### Options (Priority Order)

| Priority | Source | Notes |
|---|---|---|
| 1 | `sys.indexes` + `sys.key_constraints` + `sys.index_columns` | Declared PK; definitive |
| 2 | MERGE ON clause in proc code | Business key / table grain — the strongest code-level signal. Fabric Warehouse schemas often omit declared constraints, making this the primary fallback |
| 3 | UPDATE / DELETE `WHERE col = @param` in proc | Single-row lookup key |
| 4 | Uniqueness profiling: `COUNT(*) = COUNT(DISTINCT col)` | Undeclared single-column PK candidates |
| 5 | Composite uniqueness: `COUNT(*) = COUNT(DISTINCT CONCAT(col_a, col_b))` (pipe-delimited concat) | Undeclared composite PK candidates |

---

## Q4 — Foreign Key Candidates

**Why nice-to-have:** Not needed for correct dbt SQL. Needed for `relationships` tests in the
generated schema YAML. Also the only way to detect role-playing dimensions (two columns both
referencing the same dimension, e.g. `order_date_sk` and `ship_date_sk` both joining `dim_date`)
and degenerate dimensions (columns used in WHERE/GROUP BY but never joined to any dimension table).

### Options (Priority Order)

| Priority | Approach | Notes |
|---|---|---|
| 1 | `sys.foreign_keys` + `sys.foreign_key_columns` | Declared FKs; definitive but usually absent in warehouse schemas |
| 2 | Reader proc JOIN analysis | Use `sys.dm_sql_referencing_entities` on the target table to find all procs that read it; parse their JOIN conditions. FK relationship is expressed most clearly in the read direction — multiple independent reader procs joining on the same column is very high confidence. Also surfaces role-playing dimensions and degenerate dimensions |
| 3 | Writer proc JOIN analysis | Writer JOINs staging to dimension tables to resolve surrogate keys before inserting — confirms the relationship but in the less direct write direction |
| 4 | Naming-convention heuristics | Strip `_sk`/`_id` suffix; check if stem matches a known dimension table name. `date_key`/`date_sk` → `dim_date` is high confidence; `<name>_id` in a fact table is medium |
| 5 | Referential integrity profiling (LEFT JOIN orphan count) | `orphan_count = 0` → strong FK candidate; `> 5 %` → probably not FK. Use `TABLESAMPLE` on large tables; only run on candidates that passed an earlier filter |

**Note:** Reader proc analysis (priority 2) is the practical primary source in Fabric Warehouse
schemas where declared FK constraints are absent. Weight confidence by the number of independent
reader procs that confirm the same JOIN — one proc = medium; three or more = high.

### FK Type Resolution Rules

Classify each FK candidate into `standard`, `role_playing`, or `degenerate` using deterministic
rules:

| `fk_type` | Rule |
|---|---|
| `standard` | Declared FK exists to one dimension relation, or parse evidence shows one fact column joining one dimension key with no multi-role pattern |
| `role_playing` | Two or more distinct fact columns join/reference the same dimension relation+key (for example `order_date_sk` and `ship_date_sk` both to `dim_date.date_sk`) |
| `degenerate` | Column behaves as a business key in fact usage (`SELECT/GROUP BY/WHERE` and grain signals), but no declared FK and no stable dimension join target is found |

Conflict handling:

- When signals disagree, keep multiple candidates with per-candidate confidence.
- Do not upgrade `degenerate` to `standard|role_playing` without join or catalog evidence.

---

## Q5 — Natural Key vs Surrogate Key

**Why required:** Determines whether the model calls `dbt_utils.generate_surrogate_key` and
whether the incremental `unique_key` is a raw column or a generated hash.

### Options

| Signal | Source | Notes |
|---|---|---|
| `sys.identity_columns` | Catalog | Definitive for IDENTITY-generated surrogates |
| `NEWID()` / `NEWSEQUENTIALID()` / `NEXT VALUE FOR` in proc body | Static parse | Definitive for proc-assigned GUIDs / sequences |
| Column name suffix: `_sk` / `_guid` → surrogate; `_code` / `_number` / `_num` → natural | Heuristic | Reliable in well-named Kimball schemas |
| Value pattern: sequential integers / GUIDs → surrogate; business codes → natural | Data sample | Run on 100-row sample |
| 1:1 cardinality with row count + INT/BIGINT type | Data profile | Strongly suggests surrogate |
| MERGE ON clause uses a different column from the INSERT's PK column | Code analysis | Classic pattern: MERGE ON `customer_id` (natural); INSERT populates `customer_sk` (surrogate) |

**Note:** The MERGE ON clause simultaneously answers Q3, Q5, and partially Q4. When a MERGE is
present, parse it first before running any other signals.

---

## Q6 — Incremental Watermark

**Why required:** Without a watermark column the model can only be materialized as `table` (full
refresh). For large fact tables this is operationally unacceptable.

### Options (Priority Order)

| Priority | Approach | Notes |
|---|---|---|
| 1 | WHERE clause parse in proc body | `WHERE load_date > @last_run` / `BETWEEN @start AND @end` — nearly definitive |
| 2 | Column name heuristics | `modified_at`, `updated_at`, `load_date`, `etl_date`, `batch_date`. Use case-insensitive fuzzy match including abbreviations (`_dt`, `_ts`, `_dttm`). Validate that the matched column is a datetime type, not varchar |
| 3 | CDC / CT metadata | `sys.tables.is_tracked_by_cdc`, `sys.change_tracking_tables` — informs strategy choice, does not identify the user watermark column |
| 4 | Data profiling (sampled) | Monotonicity check, null rate, recency of MAX value. Use `TABLESAMPLE` only when options 1–2 produce multiple candidates |

### dbt Strategy Mapping

| Watermark type | Recommended dbt strategy |
|---|---|
| `datetime` / `datetime2` column | `microbatch` (dbt 1.6+, preferred) or `merge` |
| Integer identity column | `merge` or `delete+insert` |
| Append-only, no updates | `append` |
| Date-partitioned table | `insert_overwrite` |

---

## Q7 — PII Handling Candidates

**Why nice-to-have:** Does not affect SQL correctness. A missed PII column flowing unmasked into
a gold model is a compliance incident. The cost of missing it justifies including it in the
profiler.

### Options (Layered Approach)

| Layer | Approach | Notes |
|---|---|---|
| 1 | `sys.sensitivity_classifications` (SQL Server 2019+) | Free and instant. Many shops will not have this populated — treat as opportunistic bonus |
| 2 | Column name pattern matching | Match against known PII patterns (`email`, `ssn`, `dob`, `phone`, `credit_card`, `passport`, `ip_address`, etc.). Case-insensitive, fuzzy. Fails on obfuscated or abbreviated names |
| 3 | Value sampling with Microsoft Presidio (`presidio-structured`) | Open source (Python). Sample 100 rows per `varchar`/`nvarchar` column; `PandasAnalysisBuilder` returns a column → entity-type map. Detects email, SSN, credit card, phone, IP, names, and national IDs for 15+ regions |
| 4 | LLM classification (Claude) | Best for edge cases where name and value pattern disagree. Input: `{table_name, column_name, data_type, sample_values[10], heuristic_result}`. Output: `{is_pii, pii_type, confidence}` |

**dbt output:** Write PII metadata as column-level `meta` tags in the generated schema YAML:

```yaml
columns:
  - name: customer_email
    meta:
      contains_pii: true
      pii_type: email_address
```

This is the dbt-native way to carry PII metadata into downstream governance tooling.

---

## Source Notes

Research conducted March 2026. Key references:

- sqlglot T-SQL parser: <https://github.com/tobymao/sqlglot>
- dbt surrogate key guide: <https://www.getdbt.com/blog/guide-to-surrogate-key>
- dbt-utils `generate_surrogate_key`: <https://github.com/dbt-labs/dbt-utils>
- Kimball dimensional modeling techniques: <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/>
- Microsoft Presidio structured data: <https://microsoft.github.io/presidio/structured/>
- `sys.sensitivity_classifications`: <https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-sensitivity-classifications-transact-sql>
- dbt microbatch strategy: <https://docs.getdbt.com/docs/build/incremental-microbatch>
