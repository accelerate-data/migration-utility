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

Source-specific patterns are in `.claude/rules/` and auto-loaded alongside this file.

## Custom Skills

### classify-source-object

When analyzing a source table or view and structured migration configuration is needed
(table type, load strategy, grain, PII flags, column roles, confidence scores), read and
follow the skill at `.claude/skills/classify-source-object/SKILL.md`.


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
