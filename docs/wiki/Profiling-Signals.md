# Profiling Signals Reference

Detailed signal tables and output field definitions for the six profiling questions answered by `/profiling-table`. See [[Profiling Table]] for the main skill documentation.

## Q1 -- What kind of model is this? (Required)

Determines materialization strategy, dbt tests, and whether SCD2 history logic is needed.

**Write-pattern signals:**

| Proc pattern | Classification |
|---|---|
| Pure INSERT, no UPDATE or DELETE | `fact_transaction` |
| INSERT with GROUP BY (aggregation before write) | `fact_aggregate` |
| TRUNCATE + INSERT with descriptive VARCHAR columns | `dim_non_scd` |
| TRUNCATE + INSERT with measure + FK columns | `fact_periodic_snapshot` |
| MERGE with simple WHEN MATCHED THEN UPDATE only | `dim_scd1` |
| MERGE with expire matched row + insert history row (`valid_to`, `is_current`) | `dim_scd2` |
| INSERT then UPDATE targeting milestone date columns | `fact_accumulating_snapshot` |
| Cross-join INSERT of low-cardinality flag combinations | `dim_junk` |

**Column shape signals:**

| Column pattern | Signal |
|---|---|
| `valid_from` / `valid_to` / `is_current` / `current_flag` | `dim_scd2` |
| Multiple milestone date columns (`order_date`, `ship_date`, `close_date`) | `fact_accumulating_snapshot` |
| `snapshot_date` / `as_of_date` / `period_date` | `fact_periodic_snapshot` |
| Multiple BIT/TINYINT flag columns, all low-cardinality | `dim_junk` candidate |
| Surrogate PK (`_sk`) + separate natural key column | Dimension (SCD1 or SCD2) |
| FK columns (`_sk`) + numeric measure columns | Fact table |

**`classification` output fields:**

| Field | Type | Description |
|---|---|---|
| `resolved_kind` | string | Enum: `dim_non_scd`, `dim_scd1`, `dim_scd2`, `dim_junk`, `fact_transaction`, `fact_periodic_snapshot`, `fact_accumulating_snapshot`, `fact_aggregate` |
| `rationale` | string | Why this classification was chosen |
| `source` | string | Enum: `catalog`, `llm`, `catalog+llm` |

## Q2 -- Primary key candidate (Required)

Required for `unique_key` in incremental models. Missing it forces full-refresh or produces duplicates.

**Signals beyond catalog PKs:**

| Source | Notes |
|---|---|
| MERGE ON clause in proc code | Business key / table grain -- strongest code-level signal |
| UPDATE / DELETE WHERE col = @param | Single-row lookup key |

**`primary_key` output fields:**

| Field | Type | Description |
|---|---|---|
| `columns` | string[] | PK column names |
| `primary_key_type` | string | Enum: `surrogate`, `natural`, `composite`, `unknown` |
| `rationale` | string | Why these columns were identified as PK and why this type |
| `source` | string | Enum: `catalog`, `llm`, `catalog+llm` |

## Q3 -- Foreign key candidates (Nice-to-have)

Needed for `relationships` tests and detecting role-playing / degenerate dimensions.

**FK type resolution:**

| `fk_type` | Rule |
|---|---|
| `standard` | One fact column joins one dimension key with no multi-role pattern |
| `role_playing` | Two or more distinct fact columns join the same dimension relation+key |
| `degenerate` | Column behaves as business key in fact usage but no dimension join target found |

**`foreign_keys[]` output fields:**

| Field | Type | Description |
|---|---|---|
| `column` | string | FK column name on this table |
| `references_source_relation` | string | Referenced table (source-side identifier) |
| `references_column` | string | Referenced column |
| `fk_type` | string | Enum: `standard`, `role_playing`, `degenerate` |
| `rationale` | string | Why this FK type was chosen |
| `source` | string | Enum: `catalog`, `llm`, `catalog+llm` |

## Q4 -- Natural key vs surrogate key (Required)

Determines whether the model calls `dbt_utils.generate_surrogate_key` and whether the incremental `unique_key` is a raw column or generated hash.

**Signals:**

| Signal | Notes |
|---|---|
| `NEWID()` / `NEWSEQUENTIALID()` / `NEXT VALUE FOR` in proc body | Definitive proc-assigned surrogate |
| Column name suffix `_sk` / `_guid` | Surrogate |
| Column name suffix `_code` / `_number` / `_num` | Natural |
| MERGE ON uses different column from INSERT's PK column | MERGE ON = natural key; INSERT PK = surrogate key |

**`natural_key` output fields:**

| Field | Type | Description |
|---|---|---|
| `columns` | string[] | Natural key column names |
| `rationale` | string | Why these columns are the natural key |
| `source` | string | Enum: `catalog`, `llm`, `catalog+llm` |

## Q5 -- Incremental watermark (Required)

Without a watermark column, the model can only be `table` (full refresh). Operationally unacceptable for large fact tables.

**Signals:**

| Source | Notes |
|---|---|
| WHERE clause in proc body | `WHERE load_date > @last_run` -- nearly definitive |
| Column name patterns | `modified_at`, `updated_at`, `load_date`, `etl_date`, `batch_date`, `_dt`, `_ts`, `_dttm` |
| CDC / CT metadata | Informs strategy but does not identify the watermark column |

**`watermark` output fields:**

| Field | Type | Description |
|---|---|---|
| `column` | string | Watermark column name |
| `rationale` | string | Why this column was chosen |
| `source` | string | Enum: `catalog`, `llm`, `catalog+llm` |

## Q6 -- PII handling candidates (Nice-to-have)

Does not affect SQL correctness. Missing PII detection is a compliance risk.

**Signals beyond catalog sensitivity classifications:**

| Source | Notes |
|---|---|
| Column name patterns | `email`, `ssn`, `dob`, `phone`, `address`, `zip`, `credit_card`, `passport`, `national_id`, `ip_address`, `birth_date`, `first_name`, `last_name`, `full_name` |
| Column type + context | VARCHAR/NVARCHAR with PII-suggestive names |

**`pii_actions[]` output fields:**

| Field | Type | Description |
|---|---|---|
| `column` | string | Column containing PII |
| `entity` | string | PII entity type (e.g., `email_address`, `phone_number`) |
| `suggested_action` | string | Enum: `mask`, `drop`, `tokenize`, `keep` |
| `rationale` | string | Why this action was suggested |
| `source` | string | Enum: `catalog`, `llm`, `catalog+llm` |

**Suggested action meanings:**

| Action | When |
|---|---|
| `mask` | Default for confirmed PII |
| `drop` | Column not needed downstream |
| `tokenize` | Joinability must be preserved |
| `keep` | Explicit business justification |

## The `source` attribution field

Every decision point in the profile includes a `source` field indicating how the answer was derived:

| Value | Meaning |
|---|---|
| `catalog` | Entirely from catalog signals (PKs, FKs, identity columns, sensitivity classifications) |
| `llm` | Entirely from LLM reasoning over proc body, column names, or patterns |
| `catalog+llm` | Catalog provided partial signal, LLM completed the answer |

Catalog signals are treated as facts. The LLM fills in what the catalog does not answer.

## Full profile example

```json
{
  "profile": {
    "status": "ok",
    "writer": "silver.usp_load_dimcustomer",
    "classification": {
      "resolved_kind": "dim_scd1",
      "rationale": "MERGE with WHEN MATCHED THEN UPDATE on all non-key columns. No valid_from/valid_to columns present, ruling out SCD2.",
      "source": "catalog+llm"
    },
    "primary_key": {
      "columns": ["CustomerKey"],
      "primary_key_type": "surrogate",
      "rationale": "CustomerKey is an identity column (catalog PK constraint PK_DimCustomer). Auto-increment seed=1, increment=1.",
      "source": "catalog"
    },
    "natural_key": {
      "columns": ["CustomerAlternateKey"],
      "rationale": "MERGE ON clause uses CustomerAlternateKey to match source to target, confirming it as the business key.",
      "source": "llm"
    },
    "watermark": {
      "column": "ModifiedDate",
      "rationale": "Proc WHERE clause filters on ModifiedDate > @LastLoadDate. DATETIME2 type confirms suitability as watermark.",
      "source": "llm"
    },
    "foreign_keys": [
      {
        "column": "GeographyKey",
        "references_source_relation": "silver.DimGeography",
        "references_column": "GeographyKey",
        "fk_type": "standard",
        "rationale": "Declared FK constraint FK_DimCustomer_Geography. Single column, single dimension -- standard FK.",
        "source": "catalog"
      }
    ],
    "pii_actions": [
      {
        "column": "EmailAddress",
        "entity": "email_address",
        "suggested_action": "mask",
        "rationale": "Column name matches PII pattern 'email'. NVARCHAR type confirms string content.",
        "source": "llm"
      },
      {
        "column": "Phone",
        "entity": "phone_number",
        "suggested_action": "mask",
        "rationale": "Column name matches PII pattern 'phone'.",
        "source": "llm"
      }
    ],
    "warnings": [],
    "errors": []
  }
}
```
