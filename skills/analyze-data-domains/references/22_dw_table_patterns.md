# DW Table Patterns Reference

Used by Step 2 of the table-domain-classifier skill.
Contains the full pattern library and decision tree for classifying a table's dimensional modeling role.

---

## Table of Contents
1. [Role Definitions](#role-definitions)
2. [Prefix & Suffix Pattern Library](#prefix--suffix-pattern-library)
3. [Structural Signals (Column-Level)](#structural-signals-column-level)
4. [Decision Tree](#decision-tree)
5. [Edge Cases & Tie-Breaking Rules](#edge-cases--tie-breaking-rules)
6. [Confidence Scoring](#confidence-scoring)

---

## Role Definitions

| Role | Definition | Primary Purpose |
|---|---|---|
| **Fact** | Central measurement table at a declared grain. Contains numeric measures and FK references to all dimension tables. | Quantitative analysis — "how much, how many" |
| **Dimension** | Descriptive context table. Wide, denormalized, relatively few rows. Joined to fact tables. | Filtering, grouping, labelling — "who, what, where, when" |
| **Bridge** | Resolves many-to-many between fact and dimension. Contains group keys and optional weighting factors. | Multivalued dimension handling |
| **Aggregate** | Pre-summarised version of a fact table at a coarser grain. Improves query performance. | Performance optimisation |
| **Staging** | Intermediate, ETL-only table. Not exposed to end users. Contains raw or partially transformed source data. | ETL landing and transformation |
| **Reference / Lookup** | Small, stable code-description tables. No FK dependencies. Shared across many tables. | Decode codes to descriptions |
| **ODS** | Operational Data Store. Near-real-time integration layer. Near-identical structure to source systems. | Near-real-time reporting |
| **Unknown** | Cannot be classified from available evidence. | Manual review required |

---

## Prefix & Suffix Pattern Library

### FACT tables — Prefix Patterns
| Pattern | Confidence | Notes |
|---|---|---|
| `FACT_` | High | Industry standard |
| `FCT_` | High | Common abbreviation |
| `F_` | Medium | Ambiguous — validate with structural signals |
| `TXN_`, `TRX_` | Medium | Transaction tables — often fact grain |
| `SALES_`, `ORDERS_`, `EVENTS_` | Medium | Subject-matter prefixes for likely fact tables |
| `MEASURE_` | Medium | Explicit measurement prefix |

### FACT tables — Suffix Patterns
| Pattern | Confidence | Notes |
|---|---|---|
| `_FACT` | High | |
| `_SALES`, `_ORDERS`, `_TRANSACTIONS` | Medium | |
| `_EVENTS`, `_ACTIVITY`, `_LOGS` | Medium | Verify: may be Staging or ODS |
| `_SNAPSHOT` | Medium | Periodic or accumulating snapshot — still a Fact |
| `_HISTORY` | Low | Could be SCD Type 2 Dimension history or a Fact; check structure |

### DIMENSION tables — Prefix Patterns
| Pattern | Confidence | Notes |
|---|---|---|
| `DIM_` | High | Industry standard |
| `D_` | Medium | Validate with structural signals |
| `MASTER_` | Medium | e.g., `MASTER_PRODUCT`, `MASTER_CUSTOMER` |

### DIMENSION tables — Suffix Patterns
| Pattern | Confidence | Notes |
|---|---|---|
| `_DIM` | High | |
| `_MASTER` | Medium | |
| `_PROFILE` | Low | Could be dimension or ODS |

### BRIDGE tables — Prefix Patterns
| Pattern | Confidence | Notes |
|---|---|---|
| `BRG_` | High | |
| `BRIDGE_` | High | |
| `XREF_` | High | Cross-reference — commonly bridge pattern |
| `MAP_` | Medium | May be lookup or bridge |
| `ASSOC_` | Medium | Association table |

### AGGREGATE tables — Prefix Patterns
| Pattern | Confidence | Notes |
|---|---|---|
| `AGG_` | High | |
| `SUMM_` | High | |
| `RPT_` | High | Reporting table |
| `ROLLUP_` | High | |
| `CUBE_` | High | Pre-computed cube |
| `WEEKLY_`, `MONTHLY_`, `ANNUAL_` | Medium | Time-grain prefix on an otherwise fact-like name |

### STAGING tables — Prefix Patterns
| Pattern | Confidence | Notes |
|---|---|---|
| `STG_` | High | |
| `STAGE_` | High | |
| `RAW_` | High | |
| `LAND_` | High | Landing zone |
| `SRC_` | High | Source replica |
| `EXT_` | Medium | External data |
| `TEMP_`, `TMP_` | Medium | Temporary staging |
| `INBOUND_` | Medium | |

### REFERENCE / LOOKUP tables — Prefix Patterns
| Pattern | Confidence | Notes |
|---|---|---|
| `LKP_` | High | |
| `REF_` | High | |
| `LOOKUP_` | High | |
| `CODE_` | High | |
| `TYPE_` | Medium | e.g., `TYPE_PAYMENT`, `TYPE_STATUS` |
| `CAT_` | Medium | Category tables |

### ODS tables — Prefix Patterns
| Pattern | Confidence | Notes |
|---|---|---|
| `ODS_` | High | |
| `CURR_` | High | Current state snapshot |
| `CURRENT_` | High | |
| `LIVE_` | Medium | |
| `NEAR_RT_` | Medium | Near-real-time |

---

## Structural Signals (Column-Level)

Apply these when prefix/suffix matching is inconclusive or when DDL is available.

### Fact Table Signals
| Signal | Strength |
|---|---|
| ≥ 3 FK columns pointing to other tables | Strong |
| Columns with numeric additive names: `amount`, `quantity`, `revenue`, `cost`, `count`, `units`, `price` | Strong |
| Composite PK made entirely of FK columns | Strong |
| No surrogate key (natural composite PK) | Supporting |
| Column named `grain` or grain-describing comment | Supporting |
| Degenerate dimension column (transaction_number, order_number — VARCHAR, not FK) | Supporting |

### Dimension Table Signals
| Signal | Strength |
|---|---|
| Surrogate key column: `*_KEY`, `*_SK`, `*_ID` as INT/BIGINT PK | Strong |
| Natural key column: `*_NATURAL_KEY`, `*_SOURCE_ID`, `*_CODE` alongside surrogate key | Strong |
| SCD columns: `effective_date`, `expiration_date`, `is_current`, `valid_from`, `valid_to` | Strong |
| Many VARCHAR columns (> 10 text columns) | Supporting |
| Hierarchy columns at multiple levels (e.g., `category`, `subcategory`, `department`) | Supporting |

### Bridge Table Signals
| Signal | Strength |
|---|---|
| Exactly 2 FK columns forming composite PK | Strong |
| Column named `group_key`, `*_group_key`, `weighting_factor`, `weight` | Strong |
| No numeric measure columns | Supporting |
| Table name contains two domain nouns: `BRIDGE_ACCOUNT_CUSTOMER`, `XREF_PATIENT_DIAGNOSIS` | Supporting |

### Aggregate Table Signals
| Signal | Strength |
|---|---|
| FK to a known Fact table in the schema | Strong |
| Fewer dimension FKs than the base Fact table it summarises | Strong |
| Date FK pointing to a coarser date dimension (monthly, quarterly) | Strong |
| Columns named `total_*`, `sum_*`, `avg_*`, `count_*` | Supporting |
| `period_type`, `period_key`, `snapshot_date` columns | Supporting |

### Staging Table Signals
| Signal | Strength |
|---|---|
| `load_timestamp`, `load_date`, `etl_batch_id`, `source_system` columns | Strong |
| `record_status`, `error_code`, `reject_flag` columns | Strong |
| No surrogate key | Supporting |
| Column structure mirrors a known source system | Supporting |
| No FK constraints | Supporting |

### Reference / Lookup Table Signals
| Signal | Strength |
|---|---|
| ≤ 5 columns | Strong |
| One code column + one description column | Strong |
| No FK columns | Strong |
| Very low row count (< 100 expected) | Supporting |
| Column names: `code`, `description`, `label`, `display_name` | Supporting |

### ODS Table Signals
| Signal | Strength |
|---|---|
| Near-identical column structure to a named source system table | Strong |
| `last_updated`, `as_of_datetime`, `snapshot_timestamp` columns | Strong |
| Frequent updates expected (not append-only) | Supporting |
| `source_system_id`, `legacy_id` present alongside surrogate key | Supporting |

---

## Decision Tree

```
Given a table T with name N and optional columns C:

1. Does N match a Staging prefix/suffix?
   YES → role = Staging (High if prefix match; Medium if suffix)
   NO → continue

2. Does N match a Reference/Lookup prefix?
   YES → role = Reference (High)
   NO → continue

3. Does N match a Bridge prefix/suffix?
   YES → role = Bridge (High)
         Validate: check for 2 FK columns and group_key (if DDL available)
   NO → continue

4. Does N match a Fact prefix/suffix?
   YES → role = Fact (High)
         Validate: check for ≥3 FK columns and numeric measures (if DDL available)
   NO → continue

5. Does N match an Aggregate prefix/suffix?
   YES → role = Aggregate (High)
   NO → continue

6. Does N match an ODS prefix/suffix?
   YES → role = ODS (High)
   NO → continue

7. Does N match a Dimension prefix/suffix?
   YES → role = Dimension (High)
         Validate: check for surrogate key and text columns (if DDL available)
   NO → continue

8. DDL available? Apply structural signal matching (see above).
   Strong signals present → assign role with confidence = Medium
   Only supporting signals → assign role with confidence = Low
   No signals → role = Unknown
```

---

## Edge Cases & Tie-Breaking Rules

### Snapshot tables
- `*_SNAPSHOT` → classify as **Fact (Periodic Snapshot)** if it has a date FK and numeric measures.
- If it has no numeric measures and only status/flag columns → classify as **Dimension** variant.

### History tables
- `*_HISTORY`, `*_HIST` → classify as **Dimension** if it has SCD columns (`effective_date`, `is_current`).
- Classify as **Fact** if it has multiple date FKs and numeric measures (accumulating snapshot pattern).

### Audit / Log tables
- `AUDIT_*`, `*_LOG`, `*_AUDIT` → classify as **Staging** by default.
- Reclassify as **Fact** only if they have numeric measures and explicit dimension FK columns.

### Consolidated / Combined tables
- Tables that combine data from multiple domains (e.g., `FACT_CONSOLIDATED_PROFIT_LOSS`) →
  assign to the domain of the primary subject matter; add secondary domain tags for all others.

### ETL Control tables
- `ETL_CONTROL_*`, `BATCH_*`, `WATERMARK_*` → classify as **Staging** (ETL metadata).
  These are never exposed to end users.

---

## Confidence Scoring

| Score | Meaning | Criteria |
|---|---|---|
| `high` | Certain | Explicit prefix/suffix match OR 2+ strong structural signals |
| `medium` | Probable | Ambiguous name but 1 strong structural signal OR multiple supporting signals |
| `low` | Possible | Name-only inference OR only supporting signals; no structural evidence |

When confidence is `low`, always add the table to the **Flags & Ambiguities** section of the report.
