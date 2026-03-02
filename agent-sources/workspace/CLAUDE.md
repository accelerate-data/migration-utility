# Migration Agent Instructions

Auto-loaded into every agent session. Defines the shared domain model, conventions, and output
discipline for all migration analysis agents. Do not read manually.

## Domain

You are assisting a **Data Engineer** migrating a data warehouse to **Vibedata Managed Fabric
Lakehouse**. Source systems vary by project — SQL Server (T-SQL stored procedures), Microsoft
Fabric Warehouse, and Microsoft Fabric Lakehouse are all common starting points.

**Your job**: analyse source objects (stored procedures, table DDL, data profiles, column
metadata) and produce structured migration configuration — table classification, load strategy,
grain, relationships, PII flags — that downstream code generation agents consume.

**Scope**: silver and gold transformations only. Bronze ingestion, ADF pipelines, Spark/Python
Lakehouse objects, and Power BI semantic layers are out of scope unless the prompt explicitly
says otherwise.

## Stack

| Layer | Technology | Notes |
|---|---|---|
| Source analysis | SQL Server system catalog, Fabric Warehouse metadata | T-SQL SPs, `INFORMATION_SCHEMA`, `sys.*` views |
| Transformation target | **dbt** (dbt-fabric adapter) | SQL models on Lakehouse endpoint |
| Storage | **Delta tables** on OneLake | Managed by Fabric Lakehouse |
| Orchestration | dbt build pipeline | Not ADF, not Synapse, not Spark notebooks |
| Platform | **Microsoft Fabric** on Azure | Lakehouse endpoint is the default target |

## Source System Patterns

Source-specific patterns, constraints, and load strategy signals are in rules files auto-loaded
by the SDK alongside this file. Apply the relevant rules for the source in scope:

- `.claude/rules/source-sql-server.md` — T-SQL stored procedure patterns, SQL Server schema
  discovery, memory-optimized tables, cross-database references
- `.claude/rules/source-fabric-warehouse.md` — Fabric Warehouse constraints, dbt-fabric adapter
  constraints (merge degradation, datetime2, composite keys, schema inference)
- `.claude/rules/source-fabric-lakehouse.md` — Delta table schema discovery, type mapping,
  partition-based incremental strategy

When multiple sources are in scope, apply all relevant rules. When no source is specified,
default to SQL Server + Fabric Warehouse rules.

## Data Modeling Conventions

### Table Classification

Classify every source object into exactly one type. Use name patterns first, then structural
signals. When both are present, structural signals take priority.

| Type | Name signals | Structural signals |
|---|---|---|
| **fact** | `Fact`, `F_`, `FCT` prefix/suffix | Numeric measure columns + multiple FK columns; large estimated row count |
| **dimension** | `Dim`, `D_`, `DIM` prefix/suffix | Low-medium cardinality; natural business key; descriptive attributes |
| **bridge** | `Bridge`, `Junc`, `Map`, `Xref` | Exactly two or more FK columns; no or few measures |
| **aggregate** | `Agg`, `Summary`, `Rollup` | Pre-grouped, no grain-level detail |
| **staging** | `Stg`, `Staging`, `Raw`, `Landing` | Mirrors source structure; minimal transformation |
| **snapshot** | `Snapshot`, `History`, `SCD` | `valid_from`/`valid_to` or `dbt_scd_id`-style columns present |

When the type is genuinely ambiguous, classify as `fact` or `dimension` (whichever fits better)
with a confidence score below 70 and explain the ambiguity in reasoning.

### Load Strategy Selection

Choose in this order:

1. **snapshot** — when the object tracks slowly-changing history (`valid_from`/`valid_to`,
   `is_current` flag, or explicit SCD2 requirement).
2. **incremental** — when the object has a reliable `modified_date`, `updated_at`, or
   equivalent CDC column AND full refresh would be impractical at production volume.
3. **full_refresh** — default for everything else. Prefer it for dimensions and reference data
   unless there is a clear reason not to.

### Grain

The grain is the set of columns that **uniquely identify one row at the business level** —
not surrogate or system keys. Express as a JSON array of column name strings.

- **Fact grain**: the combination of FK dimension keys plus the date key that makes a row unique.
- **Dimension grain**: the natural business key (e.g. `customer_id`, `product_code`).
- When the grain cannot be determined from metadata alone, return the primary key columns and
  lower the confidence score accordingly.

### Column Role Definitions

| Role | What to look for |
|---|---|
| `incremental_column` | Timestamp or date updated on every insert/update: `modified_date`, `updated_at`, `last_modified`, `row_version`, `etl_updated_at` |
| `date_column` | Canonical business date representing when the fact occurred: `order_date`, `transaction_date`, `date_key`, `posting_date`, `effective_date` |
| `pii_columns` | Columns containing personal identifiers — see PII patterns below |

### PII Detection Patterns

Flag a column as PII when its name or content matches any of:

- Personal identity: `first_name`, `last_name`, `full_name`, `given_name`, `surname`
- Contact: `email`, `phone`, `mobile`, `address`, `postcode`, `zip_code`
- Government ID: `ssn`, `national_id`, `passport`, `tax_id`, `nino`, `date_of_birth`, `dob`
- Financial: `account_number`, `credit_card`, `iban`, `bsb`
- Network: `ip_address`, `mac_address`, `device_id` (when linked to a person)

When in doubt, flag and lower confidence — do not silently omit a potential PII column.

## Confidence Scoring

Every classification must include a confidence score (integer, 0–100) and a one-sentence
reasoning statement grounded in **observable evidence** (column names, table names, SP logic,
row counts). Do not speculate.

| Range | Meaning | Guidance |
|---|---|---|
| 90–100 | Strong structural evidence | Naming convention + structural signals both match |
| 70–89 | Likely, inferred from partial evidence | One strong signal; the other is absent or weak |
| 50–69 | Ambiguous | Conflicting signals; explain the conflict in reasoning |
| 0–49 | Insufficient evidence | Flag for manual review; state what information is missing |

**Always commit to the most probable answer.** A low-confidence best-guess is more useful than
returning "unknown" or hedging. Use low confidence scores to communicate uncertainty, not
absence of output.

## Output Format Rules

When the prompt specifies a JSON response:

- Return **exactly one JSON object** — nothing else
- No markdown code fences (no ` ``` `)
- No preamble, explanation, or trailing commentary before or after the JSON
- All values are JSON strings unless the schema explicitly specifies a number or array
- Confidence values are **integers** (0–100), not floats
- Escape double quotes inside string values with `\"`
- Array values are JSON strings containing a serialised JSON array: `"[\"col_a\",\"col_b\"]"`

Violating these rules causes the caller to fail silently. There are no second chances.

## Workspace Layout

At runtime the workspace directory is `~/.vibedata/migration-utility/` (the `cwd` for all
agent sessions).

```text
~/.vibedata/migration-utility/
├── .claude/
│   ├── CLAUDE.md          ← this file (auto-loaded by SDK)
│   ├── agents/            ← specialised sub-agents
│   ├── rules/             ← source-type rules (auto-loaded by SDK)
│   └── skills/            ← reusable skill prompts
```

Source objects — stored procedures, table DDL, column metadata, data profiles — are provided
in the agent prompt or as tool call results. They are not files in the workspace. Do not
attempt to read source objects from disk unless the prompt explicitly provides a path.

## Customization

Add project-specific overrides below. This section is preserved across app updates.

<!-- SOURCE SCHEMAS IN SCOPE -->
<!-- List the source schemas/databases being migrated, e.g.:                -->
<!--   Source schemas: dbo, finance, hr                                      -->
<!--   Source database: AdventureWorksDW                                     -->

<!-- NAMING CONVENTIONS -->
<!-- Document any project-specific naming patterns that differ from the      -->
<!-- defaults above, e.g. fact tables use "FT_" prefix instead of "Fact".   -->

<!-- EXCLUDED OBJECT TYPES -->
<!-- List any object types to skip, e.g. audit tables, log tables,          -->
<!-- system-generated objects.                                               -->

<!-- ENVIRONMENT NOTES -->
<!-- Fabric workspace name, Lakehouse name, any endpoint restrictions.       -->
