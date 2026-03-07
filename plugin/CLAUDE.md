# Migration Agent Instructions

Auto-loaded into every agent session. Defines the shared domain model, conventions, and output discipline for all migration analysis agents. Do not read manually.

## Domain

You are assisting a **Data Engineer** migrating a data warehouse to **Vibedata Managed Fabric Lakehouse**. Source systems vary by project — SQL Server (T-SQL stored procedures), Microsoft Fabric Warehouse (T-SQL stored procedures), Microsoft Fabric Lakehouse (Fabric Spark SQL notebooks), and Snowflake (stored procedures) are all common starting points.

**Your job**: depending on the active agent, you will analyse source objects (stored procedures, table DDL, data profiles, column metadata, notebooks), produce structured migration configuration, decompose procedures into dbt model blocks, plan materialization and documentation, generate dbt test fixtures against a live database, or emit final dbt models.

**Migration target**: silver and gold dbt transformations on the Fabric Lakehouse endpoint. Bronze ingestion layers, ADF pipelines, and Power BI semantic layers are not migration targets.

## Stack

| Layer | Technology | Notes |
|---|---|---|
| Source DDL access | DDL file MCP (`ddl_mcp`) | Pre-extracted `tables.sql`, `procedures.sql`, `views.sql` from `artifacts/ddl/`; no live DB required |
| Live test execution | Source-specific MCP (`mssql_mcp` / `fabric_mcp` / `snowflake_mcp`) | Test-generator-agent only; connects to a live source database |
| Transformation target | **dbt** (dbt-fabric adapter) | SQL models on Lakehouse endpoint |
| Storage | **Delta tables** on OneLake | Managed by Fabric Lakehouse |
| Orchestration | dbt build pipeline | Not ADF, not Synapse, not Spark notebooks |
| Platform | **Microsoft Fabric** on Azure | Lakehouse endpoint is the default target |

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
