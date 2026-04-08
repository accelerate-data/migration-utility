# Proposed Showcase Migration Flow

## Purpose

This plugin does not perform full warehouse migrations. Its purpose is to minimize the migration effort required to bring a subset of tables (1-2 facts and all related dimensions) to Fabric Lakehouse, so vibeData can be showcased on top of real, correctly-migrated data. The showcase output is treated as **foundation quality** — these models become the starting point for the customer's actual migration, not throwaway demos.

## Design Principles

1. **Start from the end**: User specifies 2-3 target fact tables. The agent works backwards through the dependency graph.
2. **The dependency tree IS the scope**: No triage, no domain classification, no foundation layer. The tree of objects needed to produce the target facts is the complete scope.
3. **Autonomous with self-checks**: Agent runs the full pipeline on the dependency tree without human bridging. Self-checks between steps catch errors. Human validates at the end.
4. **Foundation quality**: Full test coverage with ground truth. Models are correct and complete — they become the starting point for real migration.
5. **Agent detects, human gates**: For ambiguous patterns (SCD2, multi-table writers), the agent detects and summarizes; the human makes the final call.

## The Flow

### Step 1: Init
Ask target platform upfront (Fabric Lakehouse). Scaffold project + dbt in one step. Merge current init + init-dbt.

### Step 2: Extract
Extract DDL + catalog for the whole database (or specified schemas). Extraction is cheap I/O. The full catalog is needed because dependency resolution traces cross-schema references (a procedure in `dbo` may write to a table in `dim` and read from `staging`).

### Step 3: User Specifies Target Facts
The user provides 2-3 fact table names. That is the only required input beyond init.

Example: "I want `fact.fct_sales` and `fact.fct_returns`"

### Step 4: Dependency Resolution (automatic, backwards from targets)
The agent traces the full dependency tree from each target fact:

```
fact.fct_sales
├─ writer: dbo.usp_load_fct_sales
│   reads from:
│   ├─ staging.stg_sales → SOURCE (no writer, leaf node)
│   ├─ dim.dim_customer
│   │   ├─ writer: dbo.usp_load_dim_customer
│   │   │   reads from:
│   │   │   ├─ staging.stg_customer → SOURCE
│   │   │   └─ staging.stg_person → SOURCE
│   │   └─ (resolved)
│   ├─ dim.dim_product
│   │   ├─ writer: dbo.usp_load_dim_product
│   │   │   reads from:
│   │   │   ├─ staging.stg_product → SOURCE
│   │   │   └─ staging.stg_product_subcategory → SOURCE
│   │   └─ (resolved)
│   ├─ dim.dim_date → no writer, static reference → DBT SEED
│   └─ dim.dim_employee
│       ├─ writer: dbo.usp_load_dim_employee
│       │   reads from:
│       │   └─ staging.stg_employee → SOURCE
│       └─ (resolved)
```

**Resolution rules:**
- **Trace everything needed** for the target facts to produce exactly the same output as the source system
- **Leaf nodes** (tables with no writer, or staging tables that are just raw source data) → dbt source
- **Static reference data** (dim_date, dim_currency — no writer, seeded from flat data) → dbt seed
- **ETL plumbing** (etl_batch_control, audit_log, temp tables) → excluded (not needed for data correctness)
- **Shared dependencies** (dim_customer referenced by both fct_sales and fct_returns) → resolved once, deduplicated

**Agent presents the resolved tree:**
"To showcase these 2 facts, I need to migrate 7 objects and create 6 dbt sources + 1 dbt seed. Here's the tree. Proceed?"

Human confirms or adjusts (e.g., "skip dim_employee, we don't need it for the showcase").

### Step 5: Autonomous Pipeline (single pass, dependency order)
Process the tree bottom-up: seeds → sources → dims → facts.

For each object that needs full migration:
```
Scope → self-check → Profile → self-check → Refactor → self-check → Generate → dbt compile + test
```

**Self-checks between steps (all deterministic, naming-agnostic):**
- After scope: Writer INSERT/UPDATE/MERGE target matches table (SQL parse). Reference graph matches catalog.
- After profile: Classification matches structural patterns (type-based, not name-based). E.g., SCD2 must have 2+ date/datetime cols + boolean col.
- After refactor: Refactored SQL compiles (sqlglot). Source table set preserved (set comparison).
- After generate: `dbt compile` + `dbt test` pass.

**Escalation policy:** Self-check fails → self-correct up to N times (N scales with downstream cost: scope=1, profile=2, refactor=2, generate=3) → escalate to human with specific context.

**SCD2 / multi-table writer handling:** Agent detects using structural signals (shared business key, column type superset, UPDATE+INSERT behavioral pattern). Agent summarizes what it found and why it classified the pattern. Human makes the gate decision on whether to generate a snapshot vs. separate models.

**2+ soft warnings across the pipeline accumulate to an escalation** even if no hard gate failed.

### Step 6: Test Generation (foundation quality)
Full branch-covering test generation with ground truth capture against sandbox. This is not a shortcut showcase — these tests validate that the migrated models produce exactly the same output as the source procedures.

### Step 7: Validate and Review
- `dbt build` on the full set
- Human reviews generated models, test results, and any escalated items
- Output: working dbt project that vibeData can be showcased on top of

### Step 8: Generate Ingestion and Parallel Run Manifests

After the pipeline completes and models are validated, the agent gathers information via a questionnaire and generates two separate manifest packages — one for ingestion (getting data into the lakehouse) and one for the parallel run (proving source and target match over 3-5 days). Each manifest has an agent-ready version (structured YAML for programmatic consumption) and a human-readable version (markdown for manual execution).

---

#### Ingestion Manifest

**Questionnaire (ingestion):**
- For each source table group: Where does this data originate? (database / app / file / API)
- Access method? (direct connection / file path / API endpoint / already in Azure)
- Refresh cadence: How often does source data update? When is it considered complete for a given day?
- Naming conventions: Schema/layer names (bronze, staging, marts), table prefixes (stg_, dim_, fct_), case convention

**Agent-ready: `ingestion-manifest.yaml`**

```yaml
naming:
  bronze_schema: bronze
  staging_schema: staging
  marts_schema: marts
  table_prefix: {stg_, dim_, fct_}
  case: snake_case

groups:
  - id: crm_tables
    origin: app
    origin_detail: Salesforce CRM
    recommended_method: data_factory
    refresh_cadence: daily
    refresh_complete_by: "04:00 UTC"
    tables:
      - source_name: Customer
        target_name: stg_customer
        target_schema: bronze
        row_estimate: 45000
        columns: 12
      - source_name: Person
        target_name: stg_person
        target_schema: bronze
        row_estimate: 52000
        columns: 8

  - id: erp_tables
    origin: database
    origin_detail: SQL Server (prod-sql.company.com:1433/AdventureWorks)
    recommended_method: mirroring
    refresh_cadence: hourly
    tables:
      - source_name: staging.stg_sales
        target_name: stg_sales
        target_schema: bronze
        row_estimate: 1200000
        columns: 15
      - source_name: staging.stg_returns
        target_name: stg_returns
        target_schema: bronze
        row_estimate: 38000
        columns: 9

  - id: reference_data
    origin: file
    origin_detail: Azure Data Lake (adls://reference-data/)
    recommended_method: shortcut
    refresh_cadence: static
    tables:
      - source_name: dim_date.csv
        target_name: dim_date
        target_schema: bronze
        delivery: dbt_seed
        row_estimate: 3650
        columns: 20
```

**Human-readable: `ingestion-manifest.md`**

Markdown document containing:
- Summary: X tables from Y source groups, recommended methods per group
- Per group: origin description, recommended Fabric ingestion method with rationale, table list with row estimates
- Decision guide: why Mirroring vs Data Factory vs Shortcut was recommended for each group (based on origin type, accessibility, refresh needs)
- Setup checklist: step-by-step instructions for configuring each ingestion method
- Prerequisites: required permissions, network access, credentials

---

#### Parallel Run Manifest

**Questionnaire (parallel run):**
- Can Fabric DirectQuery reach the source DW's output tables?
- Authentication method for read-only comparison access (SQL auth / Entra ID / service principal)
- Report audience (technical team only / customer stakeholders)
- Comparison window (default: 5 days)
- Source ETL schedule (when does the source DW complete its daily/hourly load?)

**Agent-ready: `parallel-run-manifest.yaml`**

```yaml
source_access:
  type: direct_query
  auth_method: service_principal
  endpoint: prod-sql.company.com:1433/AdventureWorks

comparison_window_days: 5
source_etl_complete_by: "06:00 UTC"
audience: stakeholder

table_pairs:
  - name: fct_sales
    source_fqn: fact.fct_sales
    target_fqn: "{marts_schema}.fct_sales"
    primary_key: sales_key
    natural_key: [order_id, line_number]
    measures:
      - column: quantity
        type: integer
        aggregation: sum
      - column: unit_price
        type: decimal
        aggregation: sum
      - column: line_total
        type: decimal
        aggregation: sum
      - column: tax_amt
        type: decimal
        aggregation: sum
    date_column: order_date
    expected_daily_delta: ~600 rows

  - name: dim_customer
    source_fqn: dim.dim_customer
    target_fqn: "{marts_schema}.dim_customer"
    primary_key: customer_key
    natural_key: customer_id
    scd_type: scd2
    scd_columns:
      valid_from: datetime
      valid_to: datetime
      is_current: bit
    comparison_note: "Compare current rows (is_current=1) for row-level match. Compare history row count for completeness."

  - name: dim_product
    source_fqn: dim.dim_product
    target_fqn: "{marts_schema}.dim_product"
    primary_key: product_key
    natural_key: product_id
    measures:
      - column: list_price
        type: decimal
        aggregation: avg
      - column: standard_cost
        type: decimal
        aggregation: avg
```

**Human-readable: `parallel-run-manifest.md`**

Markdown document containing:
- Summary: X table pairs to compare over Y days, comparison approach
- Per table pair: source and target locations, primary key, what measures to compare, special handling (SCD2 tables, date partitioning)
- Daily comparison protocol: when to run, what to check, what constitutes a pass vs. investigation
- Semantic model setup guide: step-by-step instructions for building the Fabric Semantic Model with DirectQuery to both source and target
- Report specification: what pages/visuals to create, what measures to define, drill-down behavior for mismatches
- Escalation criteria: what mismatch percentage triggers investigation, who to notify

**Agent-ready prompt: `parallel-run-setup-prompt.md`**

Ready-to-use prompt for another agent to build the Fabric Semantic Model + Report. References `parallel-run-manifest.yaml` for all table pairs, keys, and measures. Usable by another Claude Code session, a human analyst, or any agent that can create Fabric artifacts.

## What This Flow Eliminates (vs. full migration design)

| Eliminated concept | Why |
|---|---|
| Whole-DB triage | User already knows the 2-3 target facts |
| Domain classification | Dependency tree is the only scope |
| Foundation layer detection | dim_date gets pulled in if a fact needs it, classified as seed |
| Inter-domain ordering | One dependency tree, one execution pass |
| Phased wave execution | The wave IS the 2-3 facts + deps |
| Domain-level review | Review the whole set at once (10-15 objects) |
| Manual sourcing discovery | Gone | Manifest auto-generated from dependency tree + questionnaire |

## What This Flow Preserves

| Preserved concept | Why |
|---|---|
| Self-checks between steps | Small scope doesn't mean errors don't cascade |
| Foundation-quality tests | These models become the starting point for real migration |
| Naming-agnostic detection | Still needed for SCD2 and structural analysis |
| Confidence flags on multi-table writers | Agent detects, human gates |
| Structural/behavioral signals only | Names are opinions, structure is physics |
| Foundation quality output | Models + tests + manifests are starting point for production migration |

## Ideal UX

A single command: `/showcase fact.fct_sales fact.fct_returns`

The agent does everything from dependency resolution through manifest generation, stopping only to:
1. Confirm the dependency tree (step 4)
2. Escalate on self-check failures
3. Present SCD2/multi-table writer decisions for human gating
4. Ask the sourcing/comparison questionnaire (step 8)
5. Present final review (step 7)

## Output Artifacts

| Artifact | Format | Purpose |
|---|---|---|
| dbt models (staging + marts) | .sql + .yml | The migration itself |
| dbt seeds | .csv + .yml | Static reference data (dim_date, etc.) |
| dbt sources.yml | .yml | Bronze layer source declarations |
| `ingestion-manifest.yaml` | Agent-ready | Structured ingestion spec — source groups, methods, tables, refresh cadence |
| `ingestion-manifest.md` | Human-readable | Setup guide with rationale, checklists, prerequisites |
| `parallel-run-manifest.yaml` | Agent-ready | Structured comparison spec — table pairs, keys, measures, SCD handling |
| `parallel-run-manifest.md` | Human-readable | Daily protocol, semantic model setup guide, escalation criteria |
| `parallel-run-setup-prompt.md` | Agent-ready | Prompt for building the Fabric Semantic Model + Report |
