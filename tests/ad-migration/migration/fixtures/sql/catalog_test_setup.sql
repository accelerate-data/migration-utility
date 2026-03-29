-- catalog_test_setup.sql
-- Idempotent setup for VU-766 catalog integration tests.
-- Target: MigrationTest database on local Docker SQL Server.
-- Usage: sqlcmd -S localhost -U sa -P 'P@ssw0rd123' -d MigrationTest -i catalog_test_setup.sql -C

-- ── Schema ────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'test_catalog')
    EXEC('CREATE SCHEMA test_catalog');
GO

-- ── Drop existing objects (dependency order: procs, functions, views, tables) ──

-- Procedures
DROP PROCEDURE IF EXISTS test_catalog.usp_indirect_hop2;
DROP PROCEDURE IF EXISTS test_catalog.usp_indirect_hop1;
DROP PROCEDURE IF EXISTS test_catalog.usp_calls_other;
DROP PROCEDURE IF EXISTS test_catalog.usp_insert_writer;
DROP PROCEDURE IF EXISTS test_catalog.usp_update_writer;
DROP PROCEDURE IF EXISTS test_catalog.usp_merge_writer;
DROP PROCEDURE IF EXISTS test_catalog.usp_delete_writer;
DROP PROCEDURE IF EXISTS test_catalog.usp_reader_only;
DROP PROCEDURE IF EXISTS test_catalog.usp_multi_table;
DROP PROCEDURE IF EXISTS test_catalog.usp_column_detail;
DROP PROCEDURE IF EXISTS test_catalog.usp_select_into;
DROP PROCEDURE IF EXISTS test_catalog.usp_truncate_insert;
DROP PROCEDURE IF EXISTS test_catalog.usp_try_catch;
DROP PROCEDURE IF EXISTS test_catalog.usp_while_loop;
DROP PROCEDURE IF EXISTS test_catalog.usp_if_else;
DROP PROCEDURE IF EXISTS test_catalog.usp_dynamic_sql;
DROP PROCEDURE IF EXISTS test_catalog.usp_sp_executesql;
DROP PROCEDURE IF EXISTS test_catalog.usp_cross_db;
DROP PROCEDURE IF EXISTS test_catalog.usp_empty;
GO

-- Functions
DROP FUNCTION IF EXISTS test_catalog.fn_get_val;
GO

-- Views (schema-bound must be dropped before its base table)
DROP VIEW IF EXISTS test_catalog.vw_schema_bound;
DROP VIEW IF EXISTS test_catalog.vw_readonly;
GO

-- Tables (FK dependency order: children first)
DROP TABLE IF EXISTS test_catalog.target_multi_fk;
DROP TABLE IF EXISTS test_catalog.target_fk;
DROP TABLE IF EXISTS test_catalog.target_select_into_dest;
DROP TABLE IF EXISTS test_catalog.target_insert;
DROP TABLE IF EXISTS test_catalog.target_update;
DROP TABLE IF EXISTS test_catalog.target_merge;
DROP TABLE IF EXISTS test_catalog.target_delete;
DROP TABLE IF EXISTS test_catalog.target_readonly;
DROP TABLE IF EXISTS test_catalog.target_multi;
DROP TABLE IF EXISTS test_catalog.staging_multi;
DROP TABLE IF EXISTS test_catalog.target_composite_pk;
DROP TABLE IF EXISTS test_catalog.target_unique_idx;
DROP TABLE IF EXISTS test_catalog.target_identity;
DROP TABLE IF EXISTS test_catalog.target_no_constraints;
DROP TABLE IF EXISTS test_catalog.target_truncate;
DROP TABLE IF EXISTS test_catalog.target_indirect;
DROP TABLE IF EXISTS test_catalog.target_no_refs;
DROP TABLE IF EXISTS test_catalog.target_schema_bound;
GO

-- ── Tables ────────────────────────────────────────────────────────────────

-- DMF detection targets
CREATE TABLE test_catalog.target_insert (id INT IDENTITY(1,1) PRIMARY KEY, val NVARCHAR(50), modified_dt DATETIME2);
CREATE TABLE test_catalog.target_update (id INT PRIMARY KEY, val NVARCHAR(50));
CREATE TABLE test_catalog.target_merge (id INT PRIMARY KEY, val NVARCHAR(50), valid_from DATETIME2);
CREATE TABLE test_catalog.target_delete (id INT PRIMARY KEY, val NVARCHAR(50));
CREATE TABLE test_catalog.target_readonly (id INT PRIMARY KEY, val NVARCHAR(50));
CREATE TABLE test_catalog.target_multi (id INT PRIMARY KEY, val NVARCHAR(50));
CREATE TABLE test_catalog.staging_multi (id INT, val NVARCHAR(50));
GO

-- Catalog signal targets
CREATE TABLE test_catalog.target_composite_pk (col_a INT, col_b INT, val NVARCHAR(50), PRIMARY KEY (col_a, col_b));
CREATE TABLE test_catalog.target_unique_idx (id INT PRIMARY KEY, code NVARCHAR(10));
CREATE UNIQUE INDEX uq_code ON test_catalog.target_unique_idx(code);
GO

CREATE TABLE test_catalog.target_fk (id INT PRIMARY KEY, parent_id INT, CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES test_catalog.target_insert(id));
CREATE TABLE test_catalog.target_multi_fk (id INT PRIMARY KEY, a_id INT, b_id INT, CONSTRAINT fk_a FOREIGN KEY (a_id) REFERENCES test_catalog.target_insert(id), CONSTRAINT fk_b FOREIGN KEY (b_id) REFERENCES test_catalog.target_update(id));
CREATE TABLE test_catalog.target_identity (id INT IDENTITY(100,5) PRIMARY KEY, val NVARCHAR(50));
CREATE TABLE test_catalog.target_no_constraints (col1 NVARCHAR(50), col2 INT);
GO

-- AST enrichment targets
CREATE TABLE test_catalog.target_truncate (id INT PRIMARY KEY, val NVARCHAR(50));
CREATE TABLE test_catalog.target_indirect (id INT PRIMARY KEY, val NVARCHAR(50));
GO

-- Edge case targets
CREATE TABLE test_catalog.target_no_refs (id INT PRIMARY KEY, val NVARCHAR(50));
CREATE TABLE test_catalog.target_schema_bound (id INT PRIMARY KEY, val NVARCHAR(50));
GO

-- ── Views ─────────────────────────────────────────────────────────────────

CREATE VIEW test_catalog.vw_readonly AS
SELECT id, val FROM test_catalog.target_readonly;
GO

CREATE VIEW test_catalog.vw_schema_bound WITH SCHEMABINDING AS
SELECT id, val FROM test_catalog.target_schema_bound;
GO

-- ── Functions ─────────────────────────────────────────────────────────────

CREATE FUNCTION test_catalog.fn_get_val(@id INT)
RETURNS NVARCHAR(50)
AS
BEGIN
    RETURN (SELECT val FROM test_catalog.target_readonly WHERE id = @id);
END;
GO

-- ── Procedures ────────────────────────────────────────────────────────────

-- DMF: Direct INSERT writer
CREATE PROCEDURE test_catalog.usp_insert_writer AS
BEGIN
    INSERT INTO test_catalog.target_insert(val, modified_dt) VALUES('x', GETDATE());
END;
GO

-- DMF: Direct UPDATE writer
CREATE PROCEDURE test_catalog.usp_update_writer AS
BEGIN
    UPDATE test_catalog.target_update SET val = 'y' WHERE id = 1;
END;
GO

-- DMF: MERGE writer
CREATE PROCEDURE test_catalog.usp_merge_writer AS
BEGIN
    MERGE test_catalog.target_merge AS t
    USING (SELECT 1 AS id, 'v' AS val) AS s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET val = s.val
    WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val);
END;
GO

-- DMF: DELETE writer
CREATE PROCEDURE test_catalog.usp_delete_writer AS
BEGIN
    DELETE FROM test_catalog.target_delete WHERE id = 1;
END;
GO

-- DMF: Read-only proc
CREATE PROCEDURE test_catalog.usp_reader_only AS
BEGIN
    SELECT id, val FROM test_catalog.target_readonly;
END;
GO

-- DMF: Multi-table proc (read from staging, write to target)
CREATE PROCEDURE test_catalog.usp_multi_table AS
BEGIN
    INSERT INTO test_catalog.target_multi(id, val)
    SELECT id, val FROM test_catalog.staging_multi;
END;
GO

-- DMF: Column-level detail (UPDATE specific columns)
CREATE PROCEDURE test_catalog.usp_column_detail AS
BEGIN
    UPDATE test_catalog.target_update SET val = 'z' WHERE id = 1;
END;
GO

-- DMF: Proc calling another proc via static EXEC
CREATE PROCEDURE test_catalog.usp_calls_other AS
BEGIN
    EXEC test_catalog.usp_insert_writer;
END;
GO

-- AST: SELECT INTO (DMF misses the target table)
CREATE PROCEDURE test_catalog.usp_select_into AS
BEGIN
    SELECT id, val
    INTO test_catalog.target_select_into_dest
    FROM test_catalog.target_readonly;
END;
GO

-- AST: TRUNCATE + INSERT
CREATE PROCEDURE test_catalog.usp_truncate_insert AS
BEGIN
    TRUNCATE TABLE test_catalog.target_truncate;
    INSERT INTO test_catalog.target_truncate(id, val)
    SELECT id, val FROM test_catalog.staging_multi;
END;
GO

-- AST: Indirect writer hop 1 (calls the insert writer)
CREATE PROCEDURE test_catalog.usp_indirect_hop1 AS
BEGIN
    EXEC test_catalog.usp_insert_writer;
END;
GO

-- AST: Indirect writer hop 2 (calls hop1 -> calls insert writer)
CREATE PROCEDURE test_catalog.usp_indirect_hop2 AS
BEGIN
    EXEC test_catalog.usp_indirect_hop1;
END;
GO

-- AST: Dynamic SQL with EXEC(@sql)
CREATE PROCEDURE test_catalog.usp_dynamic_sql AS
BEGIN
    DECLARE @sql NVARCHAR(MAX) = N'INSERT INTO test_catalog.target_indirect(id, val) VALUES(1, ''x'')';
    EXEC(@sql);
END;
GO

-- AST: sp_executesql
CREATE PROCEDURE test_catalog.usp_sp_executesql AS
BEGIN
    DECLARE @sql NVARCHAR(MAX) = N'INSERT INTO test_catalog.target_indirect(id, val) VALUES(1, ''x'')';
    EXEC sp_executesql @sql;
END;
GO

-- needs_llm: TRY/CATCH with DML inside
CREATE PROCEDURE test_catalog.usp_try_catch AS
BEGIN
    BEGIN TRY
        INSERT INTO test_catalog.target_insert(val, modified_dt) VALUES('x', GETDATE());
    END TRY
    BEGIN CATCH
        PRINT ERROR_MESSAGE();
    END CATCH;
END;
GO

-- needs_llm: WHILE loop with DML inside
CREATE PROCEDURE test_catalog.usp_while_loop AS
BEGIN
    DECLARE @i INT = 0;
    WHILE @i < 3
    BEGIN
        INSERT INTO test_catalog.target_insert(val, modified_dt) VALUES('x', GETDATE());
        SET @i = @i + 1;
    END;
END;
GO

-- needs_llm: IF/ELSE with DML in both branches
CREATE PROCEDURE test_catalog.usp_if_else AS
BEGIN
    IF EXISTS (SELECT 1 FROM test_catalog.target_readonly)
        INSERT INTO test_catalog.target_insert(val, modified_dt) VALUES('y', GETDATE());
    ELSE
        INSERT INTO test_catalog.target_insert(val, modified_dt) VALUES('n', GETDATE());
END;
GO

-- Edge: Cross-database reference
CREATE PROCEDURE test_catalog.usp_cross_db AS
BEGIN
    SELECT * FROM tempdb.dbo.sysobjects;
END;
GO

-- Edge: Empty proc (no references)
CREATE PROCEDURE test_catalog.usp_empty AS
BEGIN
    SET NOCOUNT ON;
END;
GO

PRINT 'catalog_test_setup.sql completed successfully';
GO
