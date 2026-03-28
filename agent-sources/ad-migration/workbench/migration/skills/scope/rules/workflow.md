# scope Workflow

## Invoke

Run `scope` against the target table using the `ddl-path` from `$ARGUMENTS`:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" scope \
  --ddl-path <ddl-path> \
  --table <fqn>
```

## Evaluate results

Check the `writers[]`, `errors[]`, and `llm_required[]` arrays in the output.

### Confirmed writers (deterministic)

Writers with `"status": "confirmed"` (confidence ≥ 0.70) are definite write targets. These come from deterministic AST analysis. Report them directly with procedure name, write operations, and confidence.

### Suspected writers (deterministic)

Writers with `"status": "suspected"` (confidence < 0.70) need verification. For each:

1. Run `discover show --ddl-path <ddl-path> --name <procedure-fqn>` to get the `raw_ddl`.
2. Read the procedure body to verify whether it writes to the target table.
3. Confirm or reject — report the decision and reasoning to the user.

Do not proceed to migration steps until every suspected entry has a decision.

### LLM-required procs

Procs in `llm_required` contain unparseable control flow (IF/ELSE, TRY/CATCH) or EXEC/dynamic SQL that sqlglot cannot analyse. For each:

1. Run `discover show --ddl-path <ddl-path> --name <procedure-fqn>` to get `raw_ddl` and `statements`.
2. Read the procedure body. Identify:
   - Which tables the proc writes to (INSERT/UPDATE/DELETE/MERGE/TRUNCATE/SELECT INTO)
   - Which tables it reads from (FROM/JOIN)
   - What other procs it calls (EXEC)
   - For dynamic SQL (`EXEC (@sql)`, `sp_executesql`): decode the SQL string if possible, otherwise flag for manual review
3. Produce a writer entry with `analysis: "claude_assisted"`, your confidence score (0.0-1.0), and a rationale explaining your reasoning.
4. Merge with deterministic writers before applying resolution rules.

### PARSE_FAILED errors

Procs with `"code": "PARSE_FAILED"` failed to parse at the block level (entire CREATE PROCEDURE was unparseable). Handle like `llm_required` — read `raw_ddl` and analyse manually.

### Cross-DB errors

Procs with `"code": "ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE"` reference tables in other databases. Exclude from the migration plan and note that cross-database writes need a separate pipeline.

## Report

Present the final results:

```text
Writers for silver.FactInternetSales:

  Confirmed — deterministic (1):
    - silver.usp_stage_factinternetsales
      Operations: TRUNCATE, INSERT
      Confidence: 0.90
      Analysis: deterministic

  Confirmed — claude_assisted (1):
    - silver.usp_conditional_loader
      Operations: MERGE
      Confidence: 0.85
      Analysis: claude_assisted
      Rationale: IF/ELSE branches both MERGE into target table.

  LLM required (1):
    - silver.usp_load_factinternetsales
      Status: needs LLM analysis (contains EXEC)
```

Include: procedure name, write type, operations, confidence, status, analysis tier. For LLM-required: procedure name and reason.
