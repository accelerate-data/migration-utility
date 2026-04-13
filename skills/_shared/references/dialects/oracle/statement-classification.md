# Statement Classification — Oracle PL/SQL

This reference is for LLM fallback cases. Use it when `discover show` returns `needs_llm: true` or when statements contain `action: "needs_llm"` entries. Control-flow wrappers like `IF/ELSIF/ELSE` and `BEGIN...EXCEPTION` blocks are often handled deterministically before this skill is needed.

## Migrate — transformation logic to preserve in dbt

| Statement type | Example | Notes |
|---|---|---|
| INSERT...SELECT | `INSERT INTO silver.t SELECT ... FROM bronze.s` | Target table = write target, source tables = reads. The SELECT may contain UNION ALL, analytic functions, CONNECT BY, PIVOT/UNPIVOT, or any join/subquery variant — classify the outer INSERT as migrate regardless of SELECT complexity. Often preceded by `EXECUTE IMMEDIATE 'TRUNCATE TABLE silver.t'` (which is skip) as a truncate-and-reload pattern. |
| UPDATE | `UPDATE silver.t SET col = (SELECT val FROM bronze.s WHERE ...)` | Oracle UPDATE has no FROM clause — uses correlated subqueries or inline views instead. Target table = write target. |
| DELETE | `DELETE FROM silver.t WHERE ...` | Target table = write target |
| MERGE | `MERGE INTO silver.t USING bronze.s ON (t.id = s.id)` | Target = write, USING source = read. Oracle MERGE uses parens around the ON condition. |
| INSERT ALL / INSERT FIRST | `INSERT ALL INTO t1 VALUES (...) INTO t2 VALUES (...) SELECT ...` | Oracle multi-table insert — no T-SQL equivalent. Each INTO clause is a separate write target; the trailing SELECT is the read source. |
| CTAS | `CREATE TABLE silver.t AS SELECT ... FROM bronze.s` | Oracle CREATE TABLE AS SELECT. Write target = new table, read = source tables. |
| CTE + INSERT | `WITH cte AS (...) INSERT INTO silver.t SELECT * FROM cte` | The DML at the end is the migrate statement |
| CTE + MERGE | `WITH src AS (...) MERGE INTO silver.t USING src ON (...)` | CTE defines the USING source; MERGE is the migrate statement |
| EXECUTE IMMEDIATE (literal) | `EXECUTE IMMEDIATE 'INSERT INTO silver.t SELECT ...'` | Classify the embedded DML directly |
| EXECUTE IMMEDIATE (variable) | `EXECUTE IMMEDIATE v_sql` | Trace variable assignments to find the SQL string and classify the embedded DML |
| EXECUTE IMMEDIATE (USING) | `EXECUTE IMMEDIATE 'INSERT INTO silver.t VALUES (:1, :2)' USING v1, v2` | Bind variables don't change classification — classify the embedded DML |
| DBMS_SQL.PARSE | `DBMS_SQL.PARSE(v_cur, v_sql, DBMS_SQL.NATIVE)` | Second argument contains the SQL string — trace it and classify |
| Procedure call (static) | `schema.pkg.proc_name(...)` or `schema.proc_name(...)` | Follow the called routine via catalog — run `discover show` on it to get its refs |
| Procedure call (remote) | `proc_name@dblink(...)` | **Flag as error** — DB link is out of scope for this migration |
| FORALL...INSERT | `FORALL i IN 1..v_arr.COUNT INSERT INTO t VALUES (v_arr(i))` | Bulk DML — classify the inner DML statement. Target table = write target. |
| FORALL...UPDATE | `FORALL i IN 1..v_arr.COUNT UPDATE t SET col = v_arr(i) WHERE ...` | Bulk DML — classify the inner DML statement |
| FORALL...DELETE | `FORALL i IN 1..v_arr.COUNT DELETE FROM t WHERE id = v_arr(i)` | Bulk DML — classify the inner DML statement |
| BULK COLLECT + FORALL | `SELECT ... BULK COLLECT INTO v_arr; FORALL i ... INSERT ...` | The SELECT is the read, the FORALL DML is the write |

## Skip — operational overhead, dbt handles or ignores

| Statement type | Example | Notes |
|---|---|---|
| DBMS_OUTPUT.PUT_LINE | `DBMS_OUTPUT.PUT_LINE('Loading...')` | Logging |
| RAISE_APPLICATION_ERROR | `RAISE_APPLICATION_ERROR(-20001, 'Error')` | Error handling |
| COMMIT / ROLLBACK / SAVEPOINT | `COMMIT; ROLLBACK TO sp1; SAVEPOINT sp1;` | Transaction control — dbt manages transactions |
| ALTER SESSION | `ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'` | Session config |
| TRUNCATE (via EXECUTE IMMEDIATE) | `EXECUTE IMMEDIATE 'TRUNCATE TABLE silver.t'` | Load pattern — Oracle requires dynamic SQL for TRUNCATE in PL/SQL |
| DROP/CREATE INDEX (via EXECUTE IMMEDIATE) | `EXECUTE IMMEDIATE 'DROP INDEX idx1'` | Index management — dbt post-hooks or manual |
| Variable declaration | `v_count NUMBER := 0;` | PL/SQL variable declaration |
| EXCEPTION WHEN | `EXCEPTION WHEN NO_DATA_FOUND THEN ...` | Error handling block |
| NULL; | `NULL;` | No-op placeholder |
| PRAGMA | `PRAGMA AUTONOMOUS_TRANSACTION;` | Compiler directive |
| RETURN | `RETURN;` or `RETURN 0;` | Early exit — no data operation |

## Reading control flow

If a proc reaches this skill with unresolved control flow:

1. **Trace all branches** — DML may appear in IF/ELSIF/ELSE paths or inside BEGIN...EXCEPTION blocks
2. **Classify each DML statement** in every branch using the tables above
3. **Union the write targets** across all branches — the proc may write to different tables depending on the path
4. **FOR loops** — cursor FOR loops (`FOR rec IN (SELECT ...) LOOP`) and numeric FOR loops (`FOR i IN 1..10 LOOP`) — the DML inside is the same as outside a loop, just repeated. Classify normally.

## Reading dynamic SQL

When the proc builds SQL in a variable and executes it:

1. Find the `v_sql VARCHAR2(...)` declaration and trace all assignments (`v_sql := ...`, `v_sql := v_sql || ...`)
2. Reconstruct the SQL string from the concatenation
3. Classify the reconstructed SQL using the migrate/skip tables above
4. If the target table is in a variable, note it as unresolvable — report what you can determine
5. Also check for `DBMS_SQL` usage — the second argument to `DBMS_SQL.PARSE()` contains the SQL string; trace and classify it the same way
