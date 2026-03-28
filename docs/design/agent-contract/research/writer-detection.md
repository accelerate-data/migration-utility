# Research: Writer Detection — "Who writes this SQL Server table?"

## Executive Summary

No single approach reliably answers "who writes this table?" for T-SQL stored procedures. The recommended strategy combines **catalog views** (fast, cheap, catches ~70-80% of static writes) with **static T-SQL parsing via sqlglot** (catches MERGE, SELECT INTO, and multi-statement patterns the catalog misses) and **ADF pipeline metadata** (maps orchestration → proc → table). Runtime DMVs and commercial lineage tools are useful supplements but not primary.

**Dynamic SQL remains the universal blind spot.** Every static approach misses `EXEC(@sql)` / `sp_executesql` writes. Only runtime tracing or code-text search can catch these.

---

## 1. SQL Server Catalog Views

### Available Views

| View | Scope | Key Columns | Since |
|---|---|---|---|
| `sys.sql_dependencies` | Object-level deps | `class`, `object_id`, `column_id` | SQL 2005 (deprecated) |
| `sys.sql_expression_dependencies` | Persistent catalog | `referencing_id`, `referenced_id`, `is_caller_dependent` | SQL 2008 |
| `sys.dm_sql_referencing_entities` | "Who references X?" | `referencing_schema_name`, `referencing_entity_name` | SQL 2008 |
| `sys.dm_sql_referenced_entities` | "X references what?" | `is_selected`, `is_updated`, `is_select_all`, `is_insert_all`, `is_delete_all` | SQL 2008 (write cols: 2012+, `is_insert_all`: 2016+) |
| `sys.sql_modules` | Full proc text | `definition` (nvarchar(max)) | SQL 2005 |
| `sys.objects` | Object metadata | `type`, `create_date`, `modify_date` | SQL 2005 |

### Key Query: Find All Procs That Write to a Table

```sql
-- Approach A: dm_sql_referencing_entities → dm_sql_referenced_entities
-- Find all stored procs, then check if they write to target table
SELECT
    re.referencing_schema_name,
    re.referencing_entity_name,
    ref.referenced_entity_name,
    ref.is_updated,
    ref.is_insert_all,
    ref.is_select_all
FROM sys.dm_sql_referencing_entities('dbo.TargetTable', 'OBJECT') re
CROSS APPLY sys.dm_sql_referenced_entities(
    QUOTENAME(re.referencing_schema_name) + '.' +
    QUOTENAME(re.referencing_entity_name), 'OBJECT') ref
WHERE ref.referenced_entity_name = 'TargetTable'
  AND (ref.is_updated = 1 OR ref.is_insert_all = 1);
```

```sql
-- Approach B: Direct referencing lookup (simpler, no write filter)
SELECT
    referencing_schema_name,
    referencing_entity_name,
    referencing_class_desc
FROM sys.dm_sql_referencing_entities('dbo.TargetTable', 'OBJECT');
```

```sql
-- Approach C: Text search fallback for dynamic SQL
SELECT
    OBJECT_SCHEMA_NAME(object_id) AS schema_name,
    OBJECT_NAME(object_id) AS proc_name
FROM sys.sql_modules
WHERE definition LIKE '%INSERT%INTO%TargetTable%'
   OR definition LIKE '%UPDATE%TargetTable%'
   OR definition LIKE '%MERGE%INTO%TargetTable%'
   OR definition LIKE '%SELECT%INTO%TargetTable%';
```

### What the Catalog Views Provide

- `is_updated = 1`: Proc UPDATEs or INSERTs into the table (column-level granularity available for UPDATE targets)
- `is_insert_all = 1`: Proc uses INSERT without explicit column list (SQL Server 2016+)
- `is_selected = 1`: Proc reads from the table
- Cross-database references: `referenced_database_name` populated for 3/4-part names
- Cross-server references: `referenced_server_name` populated for 4-part names

### What the Catalog Views Miss

| Gap | Impact | Workaround |
|---|---|---|
| **Dynamic SQL** (`EXEC(@sql)`, `sp_executesql`) | High — common in ETL procs | Text search in `sys.sql_modules` with LIKE |
| **Synonyms** | Resolves to synonym object, not base table | Join `sys.synonyms` → `base_object_name` |
| **TRUNCATE TABLE** | Not flagged in `is_updated` | Text search in `sys.sql_modules` |
| **Temp tables** with deferred name resolution | Dependencies abandoned at creation | No reliable static workaround |
| **CLR stored procedures** | Not tracked as referencing entities | Manual documentation |
| **Orchestrator-only procs** (EXEC other_proc) | Nested proc calls visible but write target attribution requires recursive traversal | Recursive CTE on `sys.sql_expression_dependencies` |

### Verdict

Best **first pass** — cheap, fast, built-in, catches the majority of static writes. Must be supplemented for dynamic SQL and synonyms.

---

## 2. Static T-SQL Code Analysis (Parsing)

### Approach

Extract proc definitions from `sys.sql_modules.definition`, then parse T-SQL to extract write-target tables from INSERT INTO, UPDATE, MERGE INTO, SELECT INTO, DELETE FROM, and TRUNCATE TABLE statements.

### Available Parsers

| Parser | Language | T-SQL Support | Writes Detection | License | Notes |
|---|---|---|---|---|---|
| **[sqlglot](https://github.com/tobymao/sqlglot)** | Python | Yes (dedicated `tsql` dialect) | INSERT, UPDATE, MERGE, CTAS, DELETE | MIT | **Recommended.** 31 dialects, AST-level extraction, active maintenance. Has transpiled 500+ T-SQL dashboards at scale. |
| **[sqllineage](https://github.com/reata/sqllineage)** | Python | Limited (ansi/Hive/Spark focus) | INSERT target only | MIT | Graph-based lineage, but T-SQL not a first-class dialect |
| **[sqlfluff](https://github.com/sqlfluff/sqlfluff)** | Python | Partial (T-SQL dialect exists) | Linting focus, not lineage | MIT | Best for linting/formatting, not write-target extraction |
| **[moz-sql-parser](https://github.com/klahnakoski/mo-sql-parsing)** | Python | Partial | JSON AST, manual extraction needed | MPL 2.0 | Simpler API but less T-SQL coverage |
| **tsql-parser** (ANTLR) | Go | Native T-SQL grammar | Full AST | Apache 2.0 | Based on official ANTLR T-SQL grammar; Go only |
| **[Visual Expert](https://www.visual-expert.com)** | Commercial | Full | Impact analysis, CRUD matrix | Commercial | GUI tool, not embeddable |

### sqlglot Example: Extract Write Targets

```python
import sqlglot
from sqlglot import exp

sql = """
CREATE PROCEDURE dbo.usp_LoadFact
AS
BEGIN
    MERGE INTO dbo.FactSales AS tgt
    USING staging.Sales AS src ON tgt.SalesKey = src.SalesKey
    WHEN MATCHED THEN UPDATE SET tgt.Amount = src.Amount
    WHEN NOT MATCHED THEN INSERT (SalesKey, Amount)
        VALUES (src.SalesKey, src.Amount);

    INSERT INTO dbo.AuditLog (ProcName, RunTime)
    VALUES ('usp_LoadFact', GETDATE());
END
"""

for statement in sqlglot.parse(sql, dialect="tsql"):
    if isinstance(statement, exp.Merge):
        target = statement.this  # the target table
        print(f"MERGE target: {target}")
    elif isinstance(statement, exp.Insert):
        target = statement.this
        print(f"INSERT target: {target}")
    elif isinstance(statement, exp.Update):
        target = statement.this
        print(f"UPDATE target: {target}")
```

### Regex Approach (Simpler, Less Reliable)

```python
import re

WRITE_PATTERN = re.compile(
    r"""
    (?:INSERT\s+INTO|UPDATE|MERGE\s+INTO|SELECT\s+INTO|TRUNCATE\s+TABLE)
    \s+
    (?:\[?(\w+)\]?\.)?   # optional schema
    \[?(\w+)\]?          # table name
    """,
    re.IGNORECASE | re.VERBOSE
)

def extract_write_targets(proc_text: str) -> list[tuple[str, str]]:
    return WRITE_PATTERN.findall(proc_text)
```

### Gap Analysis

| Gap | Impact | Workaround |
|---|---|---|
| **Dynamic SQL** (`EXEC('INSERT INTO ' + @tbl ...)`) | High — string contents not parseable as SQL without eval | Regex on string literals inside EXEC/sp_executesql |
| **Synonyms** | Parser sees synonym name, not base table | Post-resolve via `sys.synonyms` |
| **Views masking base tables** | Parser sees view name as target | Recursive resolution via `sys.sql_expression_dependencies` |
| **Cross-database writes** (3-part names) | sqlglot handles 3-part names | No extra work needed |
| **Linked server writes** (4-part names) | sqlglot may partially parse | Manual extraction for `[Server].[DB].[Schema].[Table]` |
| **Orchestrator-only procs** | Parsing sees EXEC other_proc, not the write inside it | Recursive proc resolution needed |
| **Conditional logic** (`IF @flag = 1 INSERT...`) | Parser extracts target but can't evaluate runtime branching | Accept false positives — list all potential write targets |

### Verdict

**Best complement to catalog views.** sqlglot with the `tsql` dialect provides AST-level accuracy for static SQL. Combined with catalog views, covers ~85-90% of write targets. Dynamic SQL remains the gap.

---

## 3. ADF / Orchestration Metadata

### What ADF Provides

ADF pipeline definitions are JSON files (ARM templates or Git-exported JSON) that describe:

- **Pipeline** → contains **Activities**
- **Stored Procedure Activity** → references `storedProcedureName` + linked service (i.e., which SQL Server instance)
- **Copy Activity** → references `source` dataset + `sink` dataset (which may be tables)
- **Lookup Activity** → references a query or stored procedure
- **Execute Pipeline Activity** → invokes another pipeline (orchestration nesting)

### Parsing ADF JSON for Proc→Table Mapping

```python
import json
from pathlib import Path

def extract_proc_calls(pipeline_json: dict) -> list[dict]:
    """Extract stored procedure activity details from ADF pipeline JSON."""
    results = []
    for activity in pipeline_json.get("properties", {}).get("activities", []):
        if activity["type"] == "SqlServerStoredProcedure":
            tp = activity.get("typeProperties", {})
            results.append({
                "pipeline": pipeline_json["name"],
                "activity": activity["name"],
                "proc_name": tp.get("storedProcedureName"),
                "linked_service": activity.get("linkedServiceName", {}).get("referenceName"),
            })
        elif activity["type"] == "ExecutePipeline":
            tp = activity.get("typeProperties", {})
            results.append({
                "pipeline": pipeline_json["name"],
                "activity": activity["name"],
                "invoked_pipeline": tp.get("pipeline", {}).get("referenceName"),
                "type": "pipeline_call",
            })
    return results
```

### Microsoft Purview Integration

- Purview can ingest ADF pipeline runs and build lineage graphs
- **Limitation**: Purview lineage is limited to **table and view sources/sinks in Copy Activities** — it does NOT trace inside stored procedure logic
- Stored procedure lineage requires the "Lineage extraction" toggle in Purview
  scan config (Azure SQL DB only, not on-prem SQL Server)

### Gap Analysis

| Gap | Impact | Workaround |
|---|---|---|
| **Dynamic SQL inside procs** | ADF only knows it called the proc, not what the proc did | Combine with approach 1 or 2 |
| **Procs not called by ADF** | Ad-hoc procs, SQL Agent jobs, app-tier calls invisible | Must also scan SQL Agent, app configs |
| **Parameterized pipeline runs** | Proc name may come from a parameter/variable | Parse expression language in ADF JSON |
| **Synapse/Fabric pipelines** | Similar JSON structure but different ARM schema | Adapt parser per service |
| **Orchestrator-only procs** | ADF calls proc A which calls proc B — only proc A visible | Combine with recursive proc analysis |

### Verdict

Essential for **mapping the execution graph** (pipeline → proc → table) but does NOT answer what a proc writes to. Must be combined with catalog views or parsing to complete the picture.

---

## 4. Runtime DMV Tracing

### Available DMVs

| DMV | What It Shows | Persistence | Write Detection |
|---|---|---|---|
| `sys.dm_exec_procedure_stats` | Aggregate stats per cached proc plan | Until plan eviction or restart | **No** — execution counts, CPU, reads, writes (I/O pages), but no target table info |
| `sys.dm_exec_query_stats` | Per-query stats within procs | Until plan eviction or restart | **No** — same limitation |
| `sys.dm_exec_query_plan` | XML showplan for cached plan | Until plan eviction | **Partial** — plan XML contains table references but extracting write targets from XML is complex |
| `sys.dm_db_index_usage_stats` | Per-index usage counters | Until restart | **Yes** — `last_user_update`, `user_updates` count per index; proves table was written to recently |
| Query Store (`sys.query_store_*`) | Persisted query text + plans + runtime stats | Survives restarts | **Partial** — query text available, but no direct "this proc wrote to this table" column |

### Key Query: Which Procs Recently Executed (and Might Write to Table)?

```sql
-- Procs that recently ran (from plan cache)
SELECT
    DB_NAME(database_id) AS db_name,
    OBJECT_SCHEMA_NAME(object_id, database_id) AS schema_name,
    OBJECT_NAME(object_id, database_id) AS proc_name,
    execution_count,
    last_execution_time,
    total_logical_writes,
    total_physical_reads
FROM sys.dm_exec_procedure_stats
WHERE database_id = DB_ID()
ORDER BY last_execution_time DESC;
```

```sql
-- Tables that were recently written to (index usage stats)
SELECT
    OBJECT_SCHEMA_NAME(object_id) AS schema_name,
    OBJECT_NAME(object_id) AS table_name,
    last_user_update,
    user_updates
FROM sys.dm_db_index_usage_stats
WHERE database_id = DB_ID()
  AND user_updates > 0
ORDER BY last_user_update DESC;
```

### Extended Events Approach

```sql
-- Create XE session to capture stored proc executions with write info
CREATE EVENT SESSION [TraceWrites] ON SERVER
ADD EVENT sqlserver.module_end (
    SET collect_statement = (1)
    ACTION (
        sqlserver.database_name,
        sqlserver.sql_text,
        sqlserver.session_id
    )
    WHERE sqlserver.database_name = N'YourDB'
)
ADD TARGET package0.event_file (
    SET filename = N'TraceWrites.xel',
        max_file_size = (100)  -- MB
)
WITH (STARTUP_STATE = OFF);
```

### Query Store: Extract Write Queries

```sql
-- SQL Server 2016+ / Azure SQL
SELECT
    qsqt.query_sql_text,
    qsp.query_id,
    qsrs.last_execution_time,
    qsrs.count_executions
FROM sys.query_store_query_text qsqt
JOIN sys.query_store_query qsq ON qsqt.query_text_id = qsq.query_text_id
JOIN sys.query_store_plan qsp ON qsq.query_id = qsp.query_id
JOIN sys.query_store_runtime_stats qsrs ON qsp.plan_id = qsrs.plan_id
WHERE qsqt.query_sql_text LIKE '%INSERT%INTO%TargetTable%'
   OR qsqt.query_sql_text LIKE '%UPDATE%TargetTable%'
   OR qsqt.query_sql_text LIKE '%MERGE%INTO%TargetTable%'
ORDER BY qsrs.last_execution_time DESC;
```

### Gap Analysis

| Gap | Impact | Workaround |
|---|---|---|
| **Dynamic SQL** | **Caught!** Query Store / XE capture the actual executed SQL, including dynamic SQL | This is the main advantage over static analysis |
| **Plan cache volatility** | `dm_exec_procedure_stats` reset on restart/eviction | Use Query Store for persistence |
| **No direct proc→table→write mapping** | DMVs give execution stats, not "proc P wrote to table T" | Parse query text from Query Store or plan XML |
| **Synonyms** | Resolved at execution time — runtime captures actual target | **Caught** by runtime approaches |
| **Cross-database writes** | Captured if executed in-context | Generally visible in query text |
| **Orchestrator-only procs** | Execution stats show they ran, nested proc calls visible in XE | Combine XE with causality tracking |
| **Performance overhead** | XE and Query Store add I/O overhead | Use judiciously in production |
| **Historical depth** | Only captures what ran during the monitoring window | Must enable before analysis period |

### Verdict

**Best approach for catching dynamic SQL** and validating static analysis results. Query Store is the most practical (persists across restarts, low overhead). Use as **validation layer** rather than primary discovery — you need the procs to actually execute during your monitoring window.

---

## 5. Lineage Tools

### Commercial Tools

| Tool | SQL Server Support | Proc Analysis | Dynamic SQL | Pricing | Notes |
|---|---|---|---|---|---|
| **[IBM Manta Data Lineage](https://www.ibm.com/products/manta-data-lineage)** | Full (scanner connects directly) | Column-level lineage through proc logic | Partial — parses string literals in EXEC | ~$30K+/year | Most comprehensive. Parses SQL code, ETL, BI. Now part of IBM watsonx. |
| **[Dataedo](https://docs.dataedo.com/docs/documenting-technology/supported-databases/sql-server-data-lineage/)** | Full | Column-level lineage, resolves synonyms including cross-DB linked server synonyms | Limited | $175+/user/month | Good synonym resolution. |
| **[Microsoft Purview](https://learn.microsoft.com/en-us/purview/concept-data-lineage)** | Azure SQL DB/MI only (not on-prem) | Copy Activity lineage only, no proc internals | No | Included in Azure | Free with Azure, but shallow — ADF activity-level only, not proc-level |
| **[SqlDBM](https://sqldbm.com/)** | Full | Procs in diagrams, column-level visualization | No | $25+/user/month | Primarily a modeling tool; lineage is secondary |
| **[Visual Expert](https://www.visual-expert.com)** | Full | CRUD matrix, impact analysis | Partial | Commercial (quote) | Desktop GUI, not API-embeddable |
| **[Ataccama ONE](https://docs.ataccama.com/one/latest/lineage/azure-data-factory-lineage-scanner.html)** | Via ADF scanner | ADF pipeline lineage | No | Enterprise pricing | ADF-focused, not proc-level |

### Open-Source Tools

| Tool | SQL Server Support | Proc Analysis | Dynamic SQL | Notes |
|---|---|---|---|---|
| **[OpenLineage](https://openlineage.io/)** | No native SQL Server integration | No — focused on Spark/Airflow/dbt job lineage | No | Standard API spec. Would need custom SQL Server adapter. |
| **[Apache Atlas](https://atlas.apache.org/)** | No native SQL Server integration | No — Hadoop/Hive/Spark focus | No | Java-heavy, not practical for SQL Server migration |
| **[sqllineage](https://github.com/reata/sqllineage)** | Partial (ansi parser) | INSERT target extraction | No | Python, lightweight, but T-SQL not first-class |
| **[Marquez](https://marquezproject.ai/)** | Via OpenLineage | No | No | OpenLineage reference implementation |

### Gap Analysis (All Lineage Tools)

| Gap | Commercial (Manta/Dataedo) | Open-Source |
|---|---|---|
| **Dynamic SQL** | Manta: partial (string literal parsing). Others: no. | No coverage |
| **Synonyms** | Dataedo resolves, including cross-DB. Manta resolves. | No coverage |
| **Cross-database writes** | Generally supported | No coverage |
| **Orchestrator-only procs** | Manta traces nested calls. Others vary. | No coverage |
| **Cost** | $30K+/year (Manta) or $175+/user/month (Dataedo) | Free |
| **Embeddability** | API access varies; Manta has REST API | Full control |

### Verdict

Commercial tools (especially Manta) provide the most complete out-of-box lineage but are **expensive and overkill for a migration utility** that only needs to answer "who writes this table?" for a bounded set of procs. The open-source ecosystem has **no production-ready SQL Server stored procedure lineage tool**. Better to build a targeted solution using approaches 1+2+3.

---

## Recommended Strategy for the Migration Utility

### Tiered Approach

```text
Layer 1: Catalog views (sys.dm_sql_referenced_entities with is_updated/is_insert_all)
    ↓ fast, built-in, ~70-80% coverage
Layer 2: sqlglot T-SQL parsing of sys.sql_modules.definition
    ↓ catches MERGE, SELECT INTO, multi-statement; ~85-90% combined
Layer 3: ADF pipeline JSON parsing → proc mapping
    ↓ maps execution graph; combined with 1+2 gives proc→table→pipeline
Layer 4: Query Store text search (validation)
    ↓ catches dynamic SQL; confirms active write paths
```

### Confidence Scoring

| Evidence | Confidence |
|---|---|
| Catalog `is_updated=1` + sqlglot confirms INSERT/UPDATE/MERGE | **High** |
| Catalog `is_updated=1` only (no sqlglot match — possible synonym/view) | **Medium** |
| sqlglot finds write but catalog doesn't track (e.g., dynamic SQL detected in string) | **Medium** |
| Only LIKE match in `sys.sql_modules` (table name in string literal) | **Low** — likely dynamic SQL, needs validation |
| Query Store confirms runtime write | **High** (but only for procs that ran during window) |

### Key Implementation Decisions

1. **Use sqlglot (Python, MIT)** as the parser — it has a dedicated T-SQL dialect, handles MERGE, and is actively maintained.
2. **Resolve synonyms** by joining `sys.synonyms` on `base_object_name` after initial discovery.
3. **Resolve views** by recursively querying `sys.sql_expression_dependencies` for view → base table mapping.
4. **Flag dynamic SQL** procs by detecting `EXEC(` and `sp_executesql` patterns in proc text — mark these as "needs manual review" or "needs runtime validation."
5. **ADF parsing** provides the orchestration context (which pipeline triggers which proc) — essential for migration ordering.

---

## Sources

- [sys.dm_sql_referenced_entities — Microsoft Learn](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-sql-referenced-entities-transact-sql?view=sql-server-ver17)
- [sys.dm_sql_referencing_entities — Microsoft Learn](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-sql-referencing-entities-transact-sql?view=sql-server-ver17)
- [sys.sql_expression_dependencies — Microsoft Learn](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-sql-expression-dependencies-transact-sql?view=sql-server-ver17)
- [sys.dm_exec_procedure_stats — Microsoft Learn](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-exec-procedure-stats-transact-sql?view=sql-server-ver17)
- [Sommarskog: Where Is That Table Used?](https://www.sommarskog.se/sqlutil/SearchCode.html)
- [SQLGlot GitHub](https://github.com/tobymao/sqlglot)
- [SQLGlot T-SQL Dialect](https://sqlglot.com/sqlglot/dialects/tsql.html)
- [sqllineage GitHub](https://github.com/reata/sqllineage)
- [DataHub: Column-Level Lineage from SQL](https://blog.datahubproject.io/extracting-column-level-lineage-from-sql-779b8ce17567)
- [IBM Manta Data Lineage](https://www.ibm.com/products/manta-data-lineage)
- [Dataedo SQL Server Lineage](https://docs.dataedo.com/docs/documenting-technology/supported-databases/sql-server-data-lineage/)
- [Microsoft Purview Data Lineage](https://learn.microsoft.com/en-us/purview/concept-data-lineage)
- [OpenLineage](https://openlineage.io/)
- [SqlDBM](https://sqldbm.com/)
- [ADF Pipeline Lineage Extraction](https://mainri.ca/2024/08/26/extract-sql-adf-synapse-pipeline-lineage/)
- [Push ADF Lineage to Purview — Microsoft Learn](https://learn.microsoft.com/en-us/azure/data-factory/tutorial-push-lineage-to-purview)
- [Capturing Proc Executions with Extended Events](https://blog.sqlauthority.com/2024/07/11/capturing-stored-procedure-executions-with-extended-events-in-sql-server/)
- [SQL Server Object Dependencies with DMVs](https://www.mssqltips.com/sqlservertip/4868/finding-sql-server-object-dependencies-with-dmvs/)
