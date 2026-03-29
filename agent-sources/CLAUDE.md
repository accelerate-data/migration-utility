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
| Live source DB access | `mssql` MCP via genai-toolbox (`mssql_mcp/tools.yaml`) | `setup-ddl` skill and test-generator agent; connects to live SQL Server; requires `toolbox` binary on PATH |
| Transformation target | **dbt** (dbt-fabric adapter) | SQL models on Lakehouse endpoint |
| Storage | **Delta tables** on OneLake | Managed by Fabric Lakehouse |
| Orchestration | dbt build pipeline | Not ADF, not Synapse, not Spark notebooks |
| Platform | **Microsoft Fabric** on Azure | Lakehouse endpoint is the default target |

## Commit Discipline

When operating in a git-enabled project directory, commit at logical checkpoints so work is never lost mid-session. Each commit should represent a coherent, self-contained unit of progress.

| Checkpoint | When to commit |
|---|---|
| After DDL extraction | `setup-ddl` completes writing DDL files and catalog |
| After discovery | `discover` produces new analysis or annotations |
| After scoping | Scoping agent finalises scope configuration |
| After model generation | A dbt model is written or updated |
| After config changes | Manifest, project config, or schema changes |

Commit messages should follow `type: short description` format (e.g. `feat: extract DDL from AdventureWorks`, `chore: update scope config for silver layer`). Do not batch unrelated changes into a single commit.

If the working directory is not a git repository, skip all commit steps silently — do not warn repeatedly.