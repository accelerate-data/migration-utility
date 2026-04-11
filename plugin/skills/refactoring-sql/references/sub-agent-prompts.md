# Sub-agent Prompt Templates

Prompt templates for the two parallel sub-agents launched in Step 2 of the refactoring skill. Angle-bracket placeholders (`<proc_body>`, `<statements>`, `<columns>`, `<source_tables>`, `<profile>`) must be substituted with actual values from the Step 1 context output before passing to the sub-agent.

## Sub-agent A

Extract core SELECT prompt template:

```text
You are extracting the core transformation logic from a source routine
as a pure SELECT statement.

Read references/routine-migration-ref.md for extraction rules per DML type (routes to dialect-specific routine-migration-ref.md).

Procedure body:
<proc_body>

Resolved statements (action=migrate only):
<statements>

Target table columns:
<columns>

Instructions:
1. Identify the DML pattern(s) in the migrate statements (INSERT...SELECT, MERGE,
   UPDATE, DELETE, temp table chains, cursor loops, dynamic SQL)
2. Apply the extraction rules from references/routine-migration-ref.md for each pattern
3. Produce a single pure SELECT statement that returns exactly the rows
   and columns the procedure would write to the target table
4. Keep source dialect syntax (e.g. ISNULL/NVL, CONVERT/TO_CHAR) — no dialect conversion at this stage
5. Replace procedure parameters with literal defaults where possible

Return ONLY the extracted SELECT SQL, nothing else.
```

For views, substitute `<view_sql>` for `<proc_body>` and omit `<statements>`.

## Sub-agent B

Refactor into CTEs prompt template:

```text
You are restructuring a source routine into a clean CTE-based SELECT
following the import/logical/final CTE pattern.

Read references/routine-migration-ref.md for dialect-specific restructuring rules.

Procedure body:
<proc_body>

Resolved statements (action=migrate only):
<statements>

Target table columns:
<columns>

Source tables:
<source_tables>

Profile:
<profile>

Instructions:
1. Analyse the procedure's data flow: source tables read, transformations applied,
   target table written
2. Restructure into import CTE -> logical CTE -> final CTE pattern:

   Import CTEs: One per source table. SELECT * (or needed columns) from the
   dialect-quoted table reference. Name descriptively after the source.

   Logical CTEs: One transformation step per CTE. Each does one thing: join,
   filter, aggregate, or transform. Names describe the transformation.

   Final CTE: Must be literally named `final` and must assemble the final column list matching the target table.

3. You MUST create a CTE literally named `final AS (...)`
4. End with: SELECT * FROM final
5. Do not end with `SELECT * FROM updated`, `SELECT * FROM surviving`, or any other non-`final` CTE
6. Keep source dialect syntax (e.g. ISNULL/NVL, CONVERT/TO_CHAR) — no dialect conversion at this stage
7. Replace procedure parameters with literal defaults where possible
8. Flatten nested subqueries into sequential CTEs
9. Temp tables become logical CTEs
10. Cursor loops become set-based operations (window functions, JOINs)

Return ONLY the refactored CTE SELECT SQL, nothing else.
```

For views, substitute `<view_sql>` for `<proc_body>` and omit `<statements>`. Keep `<profile>` when it is present in the refactor context.
