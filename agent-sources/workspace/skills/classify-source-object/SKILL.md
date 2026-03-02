---
name: classify-source-object
description: >
  Classifies a source table or view into migration configuration fields — table type,
  load strategy, grain, column roles, PII flags, and confidence scores. Use whenever
  structured classification output is required for a source object.
---

# Skill: Classify Source Object

Classifies a single source table or view into the migration configuration fields required by
downstream code generation agents. Invoked by analysis agents after metadata has been gathered.

## Input

The following fields must be available before applying this skill:

- `table_name` — name of the source object
- `schema_name` — schema the object belongs to
- `columns` — list of column names and types
- `row_count` — estimated row count (use 0 if unknown)
- `sp_body` — stored procedure body (empty string if not applicable)
- `primary_keys` — column names that form the primary key
- `foreign_keys` — list of FK relationships (child_column → parent_table.parent_column)

## Step 1 — Classify Table Type

Use name signals first, then structural signals. Structural signals override name signals.

| Type | Name signals | Structural signals |
|---|---|---|
| `fact` | `Fact`, `F_`, `FCT` prefix/suffix | Numeric measure columns + multiple FK columns; large row count |
| `dimension` | `Dim`, `D_`, `DIM` prefix/suffix | Low-medium cardinality; natural business key; descriptive attributes |
| `bridge` | `Bridge`, `Junc`, `Map`, `Xref` | Exactly two or more FK columns; no or few measures |
| `aggregate` | `Agg`, `Summary`, `Rollup` | Pre-grouped; no grain-level detail |
| `staging` | `Stg`, `Staging`, `Raw`, `Landing` | Mirrors source structure; minimal transformation |
| `snapshot` | `Snapshot`, `History`, `SCD` | `valid_from`/`valid_to` or `dbt_scd_id`-style columns present |

When genuinely ambiguous, pick the best fit and set confidence below 70. State the conflict in
reasoning.

## Step 2 — Select Load Strategy

Choose in this order — stop at the first match:

1. **`snapshot`** — object has `valid_from`/`valid_to`, `is_current`, or explicit SCD2 columns.
2. **`incremental`** — object has a reliable CDC column (`modified_date`, `updated_at`,
   `last_modified`, `row_version`, `etl_updated_at`) AND full refresh would be impractical at
   production volume (use row count as a signal).
3. **`full_refresh`** — everything else. Default for dimensions and reference data.

Also apply source-specific signals from the active rules file (`.claude/rules/source-*.md`).

## Step 3 — Identify Grain

The grain is the set of columns that uniquely identify one business row — not surrogate keys.

- **Fact**: combination of FK dimension keys + date key that makes a row unique.
- **Dimension**: the natural business key (`customer_id`, `product_code`, etc.).
- When grain cannot be determined from metadata, fall back to primary key columns and lower
  confidence accordingly.

Express as a JSON array of column name strings: `["order_date", "product_id", "store_id"]`

## Step 4 — Assign Column Roles

| Role | What to look for |
|---|---|
| `incremental_column` | Timestamp or date updated on every write: `modified_date`, `updated_at`, `last_modified`, `row_version`, `etl_updated_at` |
| `date_column` | Canonical business date for when the fact occurred: `order_date`, `transaction_date`, `date_key`, `posting_date`, `effective_date` |

Set each role to an empty string if no matching column exists.

## Step 5 — Detect PII

Flag a column as PII when its name matches any pattern below. When in doubt, flag it.

- Personal identity: `first_name`, `last_name`, `full_name`, `given_name`, `surname`
- Contact: `email`, `phone`, `mobile`, `address`, `postcode`, `zip_code`
- Government ID: `ssn`, `national_id`, `passport`, `tax_id`, `nino`, `date_of_birth`, `dob`
- Financial: `account_number`, `credit_card`, `iban`, `bsb`
- Network: `ip_address`, `mac_address`, `device_id`

Express as a JSON array of column name strings. Empty array if none found.

## Step 6 — Score Confidence

Assign an integer confidence score (0–100) per field based on observable evidence only.

| Range | Meaning |
|---|---|
| 90–100 | Naming convention + structural signals both match |
| 70–89 | One strong signal; the other absent or weak |
| 50–69 | Conflicting signals — state the conflict |
| 0–49 | Insufficient evidence — state what is missing |

Always commit to the most probable answer. A low-confidence best-guess is more useful than
returning empty or hedging.

## Output

Produce the following fields. All string values; arrays as serialised JSON strings.

```json
{
  "table_type": "<fact|dimension|bridge|aggregate|staging|snapshot>",
  "load_strategy": "<full_refresh|incremental|snapshot>",
  "grain_columns": "[\"col_a\", \"col_b\"]",
  "incremental_column": "<column name or empty string>",
  "date_column": "<column name or empty string>",
  "snapshot_strategy": "<strategy note or empty string>",
  "pii_columns": "[\"col_a\"]",
  "relationships_json": "[{\"child_column\":\"\",\"parent_table\":\"\",\"parent_column\":\"\",\"cardinality\":\"\"}]",
  "analysis_metadata": {
    "table_type":         { "value": "", "confidence": 0, "reasoning": "" },
    "load_strategy":      { "value": "", "confidence": 0, "reasoning": "" },
    "grain_columns":      { "value": "", "confidence": 0, "reasoning": "" },
    "relationships":      { "value": "", "confidence": 0, "reasoning": "" },
    "incremental_column": { "value": "", "confidence": 0, "reasoning": "" },
    "date_column":        { "value": "", "confidence": 0, "reasoning": "" },
    "pii_columns":        { "value": "", "confidence": 0, "reasoning": "" }
  }
}
```
