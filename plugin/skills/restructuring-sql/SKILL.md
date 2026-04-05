---
name: restructuring-sql
description: >
  Internal sub-skill. Restructures a T-SQL stored procedure into an
  import/logical/final CTE pattern. Invoked by /refactoring-sql in parallel
  with /extracting-sql. Not for direct use.
context: fork
user-invocable: false
argument-hint: "<schema.table>"
---

# Restructuring SQL

Restructure a T-SQL stored procedure into an import/logical/final CTE pattern. The result is written to `.staging/refactored.sql`.

## Step 1: Load context

```bash
mkdir -p .staging
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" refactor context --table $ARGUMENTS
```

Read the output JSON. Use `proc_body`, `statements` (action=migrate only), `columns`, `source_tables`, and `profile`.

## Step 2: Restructure into CTEs

Follow [references/sp-migration-ref.md](../refactoring-sql/references/sp-migration-ref.md) for CTE restructuring patterns.

1. Analyse the procedure's data flow: source tables read, transformations applied, target table written
2. Restructure into import CTE → logical CTE → final CTE pattern:

   **Import CTEs:** One per source table. `SELECT *` (or needed columns) from the bracket-quoted table reference. Name descriptively after the source.

   **Logical CTEs:** One transformation step per CTE. Each does one thing: join, filter, aggregate, or transform. Names describe the transformation.

   **Final CTE:** Assembles the final column list matching the target table.

3. End with: `SELECT * FROM final`
4. Keep T-SQL syntax (ISNULL, CONVERT, etc.) — no dialect conversion
5. Replace procedure parameters with literal defaults where possible
6. Flatten nested subqueries into sequential CTEs
7. Temp tables become logical CTEs
8. Cursor loops become set-based operations (window functions, JOINs)

## Step 3: Write output

Write the refactored CTE SQL to `.staging/refactored.sql`. Output only the SQL — no explanation.

## References

- [references/sp-migration-ref.md](../refactoring-sql/references/sp-migration-ref.md) — CTE restructuring patterns and DML type rules
