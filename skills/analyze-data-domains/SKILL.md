---
name: table-domain-classifier
description: >
  Use this skill whenever the user wants to analyse a database schema, list of tables, DDL scripts,
  ERD, or data dictionary and needs to: identify what each table represents, group tables into
  business data domains, classify tables by their dimensional modeling role (fact, dimension,
  staging, reference, bridge, aggregate), or map out dependencies and lineage between tables.
  Trigger for requests like "organise these tables", "what domain does this table belong to",
  "find all dependencies for X", "classify my schema", "which tables are fact tables", or
  any time the user pastes or uploads a list of table names, DDL, or an ER diagram and asks
  what to do with them. Also trigger when the user is designing a new data warehouse and
  asks how to structure or group tables they have identified.
compatibility: python3, pandas (optional — for CSV input parsing)
---

# Table Domain Classifier

Identifies every table in a schema, assigns it to a **business data domain**, classifies its
**dimensional modeling role**, and maps **upstream/downstream dependencies**. Output is a
structured, human-readable report plus an optional machine-readable JSON manifest.

---

## Scope

This skill covers:

- **Input formats**: table name lists (plain text, CSV), DDL (`CREATE TABLE` statements),
  ERD descriptions, data dictionary documents, or a verbal description of tables.
- **Domain grouping**: assigning each table to one canonical business domain
  (Customer, Product, Finance, Sales, Operations, HR, Reference/Lookup, Staging, etc.).
- **DW role classification**: labelling each table as one of the eight standard dimensional
  modeling roles (Fact, Dimension, Bridge, Aggregate, Staging, Reference/Lookup, ODS, Unknown).
- **Dependency mapping**: identifying foreign-key relationships, logical upstream/downstream
  chains, and cross-domain dependencies.
- **Dependency type labelling**: distinguishing hard dependencies (FK constraints),
  soft dependencies (logical join patterns), and ETL dependencies (load-order requirements).

This skill does **not**:

- Execute live database queries or connect to a running database engine.
- Recommend physical partitioning, indexing, or storage layouts.
- Perform data profiling or data quality assessment.

---

## When to Use

| User intent | Example trigger phrase |
|---|---|
| Classify an existing schema | "Organise these tables into domains" |
| Identify fact vs. dimension tables | "Which of these are fact tables?" |
| Map table dependencies | "What does FACT_SALES depend on?" |
| Plan a new data warehouse | "I have these source tables — how should I group them?" |
| Audit a data warehouse | "Show me all tables in the Customer domain" |
| Onboard to an unfamiliar schema | "Explain what each table in this schema does" |
| Prepare for a Bus Matrix | "Identify the conformed dimensions across these tables" |

Do **not** use this skill if the user only wants to query table contents, run SQL, or
perform data transformation — those are separate concerns.

---

## Arguments

The skill accepts input in any of the following forms. Collect whichever the user provides;
ask for clarification only if the minimum required information (a list of table names) is absent.

| Argument | Required | Type | Description |
|---|---|---|---|
| `tables` | ✅ Yes | List / DDL / text | Table names, CREATE TABLE DDL, or a CSV with a `table_name` column. At minimum, a plain list of table names is sufficient. |
| `foreign_keys` | ❌ Optional | DDL / list | FK constraint definitions (`FOREIGN KEY … REFERENCES …`). Enables hard dependency detection. If absent, dependencies are inferred from naming conventions. |
| `existing_domains` | ❌ Optional | List / text | Any domain groupings the user has already decided. The skill will respect these and only classify unassigned tables. |
| `industry_context` | ❌ Optional | Text | Industry hint (Retail, Banking, Insurance, Health Care, Telecom, etc.). Improves domain and role inference when table names are ambiguous. |
| `naming_convention` | ❌ Optional | Text | Table naming prefix/suffix conventions in use (e.g., `FACT_`, `DIM_`, `STG_`, `LKP_`). Accelerates role detection. |
| `output_format` | ❌ Optional | `report` \| `json` \| `both` | Desired output. Defaults to `report` (human-readable markdown). |

---

## Guardrails

1. **Never invent table definitions.** If a table's purpose cannot be determined from its name,
   columns, or context, classify it as `role: Unknown` and `domain: Unclassified`. Do not guess.

2. **Respect user-supplied domain assignments.** If `existing_domains` is provided, do not
   override those assignments. Only classify the remaining unassigned tables.

3. **Distinguish hard from inferred dependencies.** Hard dependencies (from FK constraints)
   are facts. Inferred dependencies (from name pattern matching) are assumptions — label them
   explicitly as `inferred` and note the basis for inference.

4. **Flag ambiguous classifications.** When a table could belong to two domains or two roles,
   list both candidates with a confidence score (`high` / `medium` / `low`) and explain the
   ambiguity rather than silently picking one.

5. **Do not cross role boundaries without evidence.** A table classified as a Staging table
   must not be promoted to a Dimension or Fact without explicit column evidence (surrogate key,
   SCD columns, numeric measures) or user confirmation.

6. **Preserve original table names exactly.** Never rename, normalise, or abbreviate table
   names in the output. Use the exact casing and spelling from the input.

7. **Scope dependencies to the provided schema.** Do not assume external dependencies exist
   unless the user mentions them. If a FK references a table not in the input list, flag it
   as an `external reference` rather than silently ignoring it.

8. **One primary domain per table.** Every table must be assigned to exactly one primary domain.
   Secondary domain tags are allowed but must be marked as `secondary`.

---

## Step-by-Step Execution

### Step 1 — Parse and Inventory the Input

Collect and normalise all provided tables into a working list:

```
For each table in input:
  - Extract: table_name, columns (if DDL provided), FK constraints (if provided)
  - Normalise table_name to UPPER_SNAKE_CASE for pattern matching (preserve original for output)
  - Note: source format (name-only | DDL | CSV)
```

If the input is DDL, extract:
- Table name
- Column names and data types
- Primary key columns
- Foreign key columns and their referenced tables
- Any CHECK or DEFAULT constraints that hint at domain (e.g., `status IN ('ACTIVE','INACTIVE')`)

If the input is a plain name list, proceed with name-pattern analysis only.

> If `tables` is absent entirely: ask the user to provide at minimum a list of table names
> before proceeding. Do not attempt classification on zero input.

---

### Step 2 — Classify the Dimensional Modeling Role

Assign each table one of the eight DW roles. Apply rules in priority order — stop at the
first rule that matches.

Read `references/dw_table_patterns.md` for the full pattern library and decision tree.

**Priority order:**

| Priority | Role | Primary Identification Signal |
|---|---|---|
| 1 | **Staging** | Prefix `STG_`, `STAGE_`, `RAW_`, `LAND_`, `SRC_`; or no surrogate key + source system columns |
| 2 | **Fact** | Prefix `FACT_`, `FCT_`, `F_`; or ≥3 FK columns + numeric additive measures; or suffix `_FACT`, `_SALES`, `_EVENTS`, `_TRANSACTIONS` |
| 3 | **Aggregate** | Prefix `AGG_`, `SUMM_`, `RPT_`, `ROLLUP_`; or references a Fact table with fewer dimension FKs |
| 4 | **Dimension** | Prefix `DIM_`, `D_`; or surrogate key + many descriptive text columns + SCD columns (`EFFECTIVE_DATE`, `IS_CURRENT`) |
| 5 | **Bridge** | Prefix `BRG_`, `BRIDGE_`, `XREF_`; or exactly 2 FK columns and a weighting/group key column |
| 6 | **Reference / Lookup** | Prefix `LKP_`, `REF_`, `LOOKUP_`, `CODE_`; or ≤5 columns, one code + one description |
| 7 | **ODS** | Prefix `ODS_`, `CURR_`, `CURRENT_`; or near-identical structure to a source system table with a load timestamp |
| 8 | **Unknown** | Does not match any pattern above |

For each table, record:
- `dw_role`: the assigned role
- `dw_role_confidence`: `high` (prefix match) | `medium` (structural match) | `low` (inferred)
- `dw_role_evidence`: the specific signal(s) that determined the role

---

### Step 3 — Assign Business Data Domains

Assign each table to one primary business domain. Apply domain rules in order.

Read `references/domain_taxonomy.md` for the full domain definitions, keyword lists,
and industry-specific variants.

**Domain detection order (apply first match):**

1. **Explicit prefix/suffix** — table name contains a domain keyword segment
   (e.g., `CUST_`, `_CUSTOMER`, `PROD_`, `_PRODUCT`, `FIN_`, `_FINANCE`).
2. **Column-level signals** — key column names strongly suggest a domain
   (e.g., `customer_id`, `policy_number`, `claim_amount`, `employee_id`).
3. **FK graph position** — a table that FKs into a known-domain table inherits a candidate domain.
4. **Industry context** — if `industry_context` is provided, apply the industry-specific
   keyword list from `references/domain_taxonomy.md`.
5. **Unclassified** — assign if no evidence found; flag for manual review.

For each table, record:
- `domain`: primary domain name
- `domain_confidence`: `high` | `medium` | `low`
- `domain_evidence`: the signal(s) used
- `secondary_domains` (optional): list of other applicable domains

---

### Step 4 — Map Dependencies

Build the dependency graph using all available evidence.

#### 4a — Hard Dependencies (FK constraints)

For each FK constraint found in the DDL:
```
source_table.column → target_table.column
dependency_type: hard
direction: source_table depends on target_table
```

#### 4b — Inferred Dependencies (name pattern matching)

When FK constraints are not provided, infer from column naming:
- A column named `<table>_KEY`, `<table>_ID`, or `<table>_SK` in Table A, where `<table>`
  matches a table name in the schema → inferred dependency: A → that table.
- Label all inferred dependencies as `dependency_type: inferred`.

#### 4c — ETL Load-Order Dependencies

Derive load order from the dependency graph:
- Tables with no incoming dependencies → **Tier 1** (load first — reference tables, date dim)
- Tables that depend only on Tier 1 → **Tier 2** (dimension tables)
- Tables that depend on Tier 2 → **Tier 3** (fact tables)
- Tables that depend on Tier 3 → **Tier 4** (aggregates, reporting tables)

#### 4d — Cross-Domain Dependencies

For every dependency where source and target belong to different domains, flag it as a
**cross-domain dependency** — these require special attention in ETL orchestration and
data ownership governance.

---

### Step 5 — Identify Conformed Dimensions and Shared Tables

Scan for tables that are referenced by multiple fact tables or multiple domains:

```
For each table T:
  count_fact_references = number of fact tables that FK into T
  count_domain_references = number of distinct domains that contain tables FKing into T

  if count_fact_references >= 2 → flag as "conformed dimension candidate"
  if count_domain_references >= 2 → flag as "cross-domain shared table"
```

These flags are inputs to a Bus Matrix and integration planning.

---

### Step 6 — Produce the Output Report

#### 6a — Summary Section

```markdown
## Schema Classification Summary

| Metric | Count |
|---|---|
| Total tables analysed | N |
| Tables classified (role known) | N |
| Tables unclassified | N |
| Domains identified | N |
| Cross-domain dependencies | N |
| Conformed dimension candidates | N |
| External references (FK to unlisted table) | N |
```

#### 6b — Domain Groups Section

For each domain, list its tables grouped by DW role:

```markdown
## Domain: Customer

**Tables: N | Fact: X | Dimension: Y | Staging: Z**

| Table | DW Role | Confidence | Key Dependencies |
|---|---|---|---|
| DIM_CUSTOMER | Dimension | High | — |
| DIM_CUSTOMER_BEHAVIOR_MINI | Dimension | High | DIM_CUSTOMER |
| FACT_CUSTOMER_ORDERS | Fact | High | DIM_CUSTOMER, DIM_DATE, DIM_PRODUCT |
| STG_CUSTOMER | Staging | High | — |
| BRIDGE_ACCOUNT_HOLDER | Bridge | High | DIM_CUSTOMER |
```

#### 6c — Dependency Map Section

```markdown
## Dependency Map

### Tier 1 — No Upstream Dependencies (load first)
- DIM_DATE
- LKP_CURRENCY
- LKP_STATUS_CODE

### Tier 2 — Depends on Tier 1 Only
- DIM_CUSTOMER → DIM_DATE
- DIM_PRODUCT → DIM_DATE

### Tier 3 — Depends on Tier 2
- FACT_POS_SALES → DIM_DATE, DIM_PRODUCT, DIM_STORE, DIM_CUSTOMER

### Tier 4 — Depends on Tier 3
- AGG_MONTHLY_SALES → FACT_POS_SALES

### Cross-Domain Dependencies
| Source Table | Source Domain | Target Table | Target Domain | Type |
|---|---|---|---|---|
| FACT_ORDER_LINES | Sales | DIM_CUSTOMER | Customer | hard |
```

#### 6d — Ambiguity & Flags Section

List all tables with low-confidence classifications, unclassified tables, external references,
and conformed dimension candidates:

```markdown
## Flags & Ambiguities

### Unclassified Tables (manual review required)
- `TRANSACTION_LOG` — name matches both Staging and Fact patterns; no columns provided.
  Recommendation: provide DDL to resolve.

### External References
- `FACT_SALES.TERRITORY_KEY` references `DIM_TERRITORY` — not found in input schema.

### Conformed Dimension Candidates
- `DIM_DATE` — referenced by 4 fact tables across 3 domains.
- `DIM_PRODUCT` — referenced by 3 fact tables across 2 domains.
```

#### 6e — JSON Manifest (if `output_format` is `json` or `both`)

```json
{
  "schema_name": "<user-provided or 'Unknown'>",
  "analysed_at": "<ISO timestamp>",
  "tables": [
    {
      "table_name": "FACT_POS_SALES",
      "dw_role": "Fact",
      "dw_role_confidence": "high",
      "dw_role_evidence": "FACT_ prefix; 5 FK columns; additive numeric measures",
      "domain": "Sales",
      "domain_confidence": "high",
      "domain_evidence": "FACT_ + SALES suffix; columns: sales_amount, quantity_sold",
      "secondary_domains": [],
      "dependencies": [
        {"table": "DIM_DATE", "type": "hard", "column": "date_key"},
        {"table": "DIM_PRODUCT", "type": "hard", "column": "product_key"},
        {"table": "DIM_STORE", "type": "hard", "column": "store_key"}
      ],
      "tier": 3,
      "flags": []
    }
  ],
  "domains": ["Sales", "Customer", "Product", "Reference"],
  "conformed_dimension_candidates": ["DIM_DATE", "DIM_PRODUCT"],
  "cross_domain_dependencies": [],
  "external_references": [],
  "unclassified_tables": []
}
```

---

## Reference Files

Load these when the relevant step requires deeper pattern matching:

| File | When to Load |
|---|---|
| `references/dw_table_patterns.md` | Step 2 — DW role classification; full pattern library and decision tree |
| `references/domain_taxonomy.md` | Step 3 — Business domain assignment; keyword lists by domain and industry |
