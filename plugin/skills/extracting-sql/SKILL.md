---
name: extracting-sql
description: >
  Internal sub-skill. Extracts the core SELECT from a T-SQL stored procedure
  as a pure T-SQL SELECT statement. Invoked by /refactoring-sql in parallel
  with /restructuring-sql. Not for direct use.
context: fork
user-invocable: false
argument-hint: "<schema.table>"
---

# Extracting SQL

Extract the core transformation logic from a T-SQL stored procedure as a pure SELECT statement. The result is written to `.staging/extracted.sql`.

## Step 1: Load context

```bash
mkdir -p .staging
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" refactor context --table $ARGUMENTS
```

Read the output JSON. Use `proc_body`, `statements` (action=migrate only), and `columns`.

## Step 2: Extract core SELECT

Follow [references/sp-migration-ref.md](../refactoring-sql/references/sp-migration-ref.md) for extraction rules per DML type.

1. Identify the DML pattern(s) in the migrate statements (INSERT...SELECT, MERGE, UPDATE, DELETE, temp table chains, cursor loops, dynamic SQL)
2. Apply the extraction rules from the reference for each pattern
3. Produce a single pure T-SQL SELECT statement that returns exactly the rows and columns the procedure would write to the target table
4. Keep T-SQL syntax (ISNULL, CONVERT, etc.) — no dialect conversion
5. Replace procedure parameters with literal defaults where possible

## Step 3: Write output

Write the extracted SELECT SQL to `.staging/extracted.sql`. Output only the SQL — no explanation.

## References

- [references/sp-migration-ref.md](../refactoring-sql/references/sp-migration-ref.md) — DML extraction rules per statement type (INSERT, MERGE, UPDATE, DELETE, dynamic SQL)
