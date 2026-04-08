# Skill: Profiling Table

## Purpose

Profiles a single table for migration by assembling deterministic context from catalog files, reasoning over six profiling questions (classification, primary key, foreign keys, natural vs surrogate key, watermark, PII), presenting results for user approval, and writing the approved profile to the table catalog file. The profile drives materialization, test generation, and model generation downstream.

## Invocation

```text
/profiling-table <schema.table>
```

Argument is the fully-qualified table name. The skill asks if missing. The writer procedure is read automatically from the catalog's `scoping.selected_writer` field.

Trigger phrases: "profile a table", "classify a table", "what kind of model is this table", "determine PK, FK, watermark, or PII for a migration target".

## Prerequisites

- `manifest.json` must exist in the project root. If missing, run [[Command Setup DDL]] first.
- `catalog/tables/<table>.json` must exist. If missing, run `/listing-objects list tables` to see available tables.
- `scoping.selected_writer` must be set in the table catalog. If missing, run [[Skill Scoping Table]] first.
- The `migrate-util guard` check must pass before profiling. The skill runs `migrate-util guard <table_fqn> profile` and stops if any guard fails.

## Pipeline

### 1. Assemble context (deterministic)

```bash
uv run --project <shared-path> profile context --table <table>
```

The CLI reads the selected writer from the table's catalog scoping section. Output is a JSON matching `profile_context.json` schema containing:

- `catalog_signals` -- PKs, FKs, identity columns, unique indexes, change capture, sensitivity classifications
- `writer_references` -- outbound references from the writer procedure with column-level read/write flags
- `proc_body` -- full SQL body of the writer procedure
- `columns` -- target table column list with types and nullability
- `related_procedures` -- related procedure context when referenced by the writer

### 2. LLM profiling (reasoning)

The LLM reads the context JSON and answers six profiling questions using signal tables and pattern matching rules from the profiling-signals reference.

### 3. Present for approval (interactive)

Profile is presented as a structured summary. If a required question (Q1, Q2, Q4, Q5) cannot be answered with reasonable confidence, the ambiguity is presented to the user for guidance. The user must explicitly approve before the profile is persisted.

### 4. Write to catalog (deterministic)

```bash
mkdir -p .staging
# Write profile JSON to .staging/profile.json
uv run --project <shared-path> profile write \
  --table <table> \
  --profile-file .staging/profile.json; rm -rf .staging
```

## Reads

| File | Description |
|---|---|
| `manifest.json` | Project root validation |
| `catalog/tables/<table>.json` | Column list, catalog signals, scoping section |
| `catalog/procedures/<writer>.json` | Writer procedure references and body |
| `ddl/procedures.sql` | Writer procedure raw DDL |

## Writes

### `profile` section in `catalog/tables/<table>.json`

The profile section follows `table_catalog.json#/$defs/profile_section`.

| Field | Type | Required | Description |
|---|---|---|---|
| `status` | string | yes | Enum: `ok`, `partial`, `error` |
| `writer` | string | yes | FQN of the writer procedure used for profiling |
| `classification` | object | no | Model classification results |
| `primary_key` | object | no | Primary key determination |
| `natural_key` | object | no | Natural key determination |
| `watermark` | object | no | Incremental watermark column |
| `foreign_keys` | array | no | Foreign key relationships with types |
| `pii_actions` | array | no | PII column handling recommendations |
| `warnings` | array | no | Diagnostics entries |
| `errors` | array | no | Diagnostics entries |

## The Six Profiling Questions

### Q1 -- What kind of model is this? (Required)

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

**`classification` object fields:**

| Field | Type | Description |
|---|---|---|
| `resolved_kind` | string | Enum: `dim_non_scd`, `dim_scd1`, `dim_scd2`, `dim_junk`, `fact_transaction`, `fact_periodic_snapshot`, `fact_accumulating_snapshot`, `fact_aggregate` |
| `rationale` | string | Why this classification was chosen |
| `source` | string | Enum: `catalog`, `llm`, `catalog+llm` |

### Q2 -- Primary key candidate (Required)

Required for `unique_key` in incremental models. Missing it forces full-refresh or produces duplicates.

**Signals beyond catalog PKs:**

| Source | Notes |
|---|---|
| MERGE ON clause in proc code | Business key / table grain -- strongest code-level signal |
| UPDATE / DELETE WHERE col = @param | Single-row lookup key |

**`primary_key` object fields:**

| Field | Type | Description |
|---|---|---|
| `columns` | string[] | PK column names |
| `primary_key_type` | string | Enum: `surrogate`, `natural`, `composite`, `unknown` |
| `rationale` | string | Why these columns were identified as PK and why this type |
| `source` | string | Enum: `catalog`, `llm`, `catalog+llm` |

### Q3 -- Foreign key candidates (Nice-to-have)

Needed for `relationships` tests and detecting role-playing / degenerate dimensions.

**FK type resolution:**

| `fk_type` | Rule |
|---|---|
| `standard` | One fact column joins one dimension key with no multi-role pattern |
| `role_playing` | Two or more distinct fact columns join the same dimension relation+key |
| `degenerate` | Column behaves as business key in fact usage but no dimension join target found |

**`foreign_keys[]` entry fields:**

| Field | Type | Description |
|---|---|---|
| `column` | string | FK column name on this table |
| `references_source_relation` | string | Referenced table (source-side SQL Server identifier) |
| `references_column` | string | Referenced column |
| `fk_type` | string | Enum: `standard`, `role_playing`, `degenerate` |
| `rationale` | string | Why this FK type was chosen |
| `source` | string | Enum: `catalog`, `llm`, `catalog+llm` |

### Q4 -- Natural key vs surrogate key (Required)

Determines whether the model calls `dbt_utils.generate_surrogate_key` and whether the incremental `unique_key` is a raw column or generated hash.

**Signals:**

| Signal | Notes |
|---|---|
| `NEWID()` / `NEWSEQUENTIALID()` / `NEXT VALUE FOR` in proc body | Definitive proc-assigned surrogate |
| Column name suffix `_sk` / `_guid` | Surrogate |
| Column name suffix `_code` / `_number` / `_num` | Natural |
| MERGE ON uses different column from INSERT's PK column | MERGE ON = natural key; INSERT PK = surrogate key |

**`natural_key` object fields:**

| Field | Type | Description |
|---|---|---|
| `columns` | string[] | Natural key column names |
| `rationale` | string | Why these columns are the natural key |
| `source` | string | Enum: `catalog`, `llm`, `catalog+llm` |

### Q5 -- Incremental watermark (Required)

Without a watermark column, the model can only be `table` (full refresh). Operationally unacceptable for large fact tables.

**Signals:**

| Source | Notes |
|---|---|
| WHERE clause in proc body | `WHERE load_date > @last_run` -- nearly definitive |
| Column name patterns | `modified_at`, `updated_at`, `load_date`, `etl_date`, `batch_date`, `_dt`, `_ts`, `_dttm` |
| CDC / CT metadata | Informs strategy but does not identify the watermark column |

**`watermark` object fields:**

| Field | Type | Description |
|---|---|---|
| `column` | string | Watermark column name |
| `rationale` | string | Why this column was chosen |
| `source` | string | Enum: `catalog`, `llm`, `catalog+llm` |

### Q6 -- PII handling candidates (Nice-to-have)

Does not affect SQL correctness. Missing PII detection is a compliance risk.

**Signals beyond catalog sensitivity classifications:**

| Source | Notes |
|---|---|
| Column name patterns | `email`, `ssn`, `dob`, `phone`, `address`, `zip`, `credit_card`, `passport`, `national_id`, `ip_address`, `birth_date`, `first_name`, `last_name`, `full_name` |
| Column type + context | VARCHAR/NVARCHAR with PII-suggestive names |

**`pii_actions[]` entry fields:**

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

## The `source` Attribution Field

Every decision point in the profile includes a `source` field indicating how the answer was derived:

| Value | Meaning |
|---|---|
| `catalog` | Entirely from catalog signals (PKs, FKs, identity columns, sensitivity classifications) |
| `llm` | Entirely from LLM reasoning over proc body, column names, or patterns |
| `catalog+llm` | Catalog provided partial signal, LLM completed the answer |

Catalog signals are treated as facts. The LLM fills in what the catalog does not answer.

## JSON Format

### Profile section example

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

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `profile context` exit code 1 | Catalog file missing for table or writer | Run [[Command Setup DDL]] and [[Skill Scoping Table]] first |
| `profile context` exit code 2 | IO/parse error reading catalog files | Check file permissions and JSON validity in `catalog/` |
| `profile write` exit code 1 | Validation failure -- invalid JSON, missing required fields, or invalid enum values | Check profile JSON against the field definitions and enum values above |
| `profile write` exit code 2 | IO error -- catalog unreadable or write failure | Check file permissions on `catalog/tables/<table>.json` |
| Ambiguous classification | Write pattern signals conflict with column shape signals | The skill stops and asks the user for guidance rather than auto-resolving |
| Missing watermark | No WHERE clause filter or datetime column found | Profile is written with `status: "partial"`. Model will fall back to full-refresh `table` materialization |
