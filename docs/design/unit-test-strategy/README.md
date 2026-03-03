# Unit Test Strategy: Test Data Generation for Proc-to-dbt Migration

## Decision

Build a **four-stage LLM-assisted pipeline** that goes from T-SQL stored procedure source to branch-covering dbt unit test fixtures. No off-the-shelf tool does this end-to-end — the individual pieces exist and compose well. The expected output for each fixture is captured by running the actual proc against the synthetic inputs (not LLM-generated), eliminating hallucination on the hardest part.

---

## Context

The testing goal is behavioral equivalence: for the same input, the dbt model must produce identical output to the stored procedure it replaces. Synthetic fixtures can cover all branching behavior if generated systematically from the proc's AST. Every branch, join cardinality pattern, and edge case is constructible once you have extracted them from the proc source.

The pipeline below makes fixture construction systematic rather than ad-hoc, driven by the decomposer agent's AST analysis which already runs earlier in the migration flow.

---

## What Exists Today

| Component | Tool | Status |
|---|---|---|
| dbt unit test YAML format (`unit_tests:`) | dbt-core 1.8+ | Production-ready |
| Agent skill for writing dbt unit tests | [dbt-labs/dbt-agent-skills](https://github.com/dbt-labs/dbt-agent-skills) | Prompting guide only |
| LLM schema-level test data generation | [Google arXiv 2504.17203](https://arxiv.org/abs/2504.17203) | Research, Gemini-based |
| Program analysis + LLM branch coverage | [TELPA arXiv 2404.04966](https://arxiv.org/abs/2404.04966) | Research, Python only |
| SQL-aware branch coverage via SMT | [ParSEval (VLDB 2025)](https://github.com/sfu-db/ParSEval) | SELECT queries only, no T-SQL |
| T-SQL proc body parsing (IF/ELSE/WHILE) | [ANTLR T-SQL grammar](https://github.com/antlr/grammars-v4/blob/master/sql/tsql/TSqlParser.g4) | Available |
| T-SQL individual statement parsing | [sqlglot](https://github.com/tobymao/sqlglot) | Good for statements, not proc control flow |
| SQL Server coverage harness | [dotnet-sqltest](https://github.com/cagrin/dotnet-sqltest) + SQLCoverLib | Cobertura XML, Testcontainers built-in |
| SQL Server unit test harness | [tSQLt](https://tsqlt.org/) | Does not run on Fabric (CLR required) |
| Cross-db row diff | [data-diff](https://github.com/datafold/data-diff) | **Archived May 2024 — do not use** |
| dbt model comparison (within warehouse) | [dbt-audit-helper](https://github.com/dbt-labs/dbt-audit-helper) | Production-ready |
| dbt test runner via MCP | dbt-core-mcp | Available for CI wiring |

---

## The Four-Stage Pipeline

```text
┌──────────────────────────────────────────────────┐
│ Stage 1 — Branch Extraction                      │
│                                                  │
│ Input:  T-SQL proc body + source table schemas   │
│ Tool:   LLM (Claude) + sqlglot for statements    │
│                                                  │
│ Extract all coverage-relevant paths:             │
│  · WHERE clause predicates                       │
│  · CASE WHEN / ELSE branches                     │
│  · MERGE WHEN MATCHED / NOT MATCHED / BY SOURCE  │
│  · IF / ELSE / ELSE IF control flow              │
│  · JOIN types (INNER vs LEFT → match/no-match)   │
│  · NULL handling (IS NULL / COALESCE paths)      │
│  · ROW_NUMBER() tie-breaking partitions          │
│  · SCD2 insert / update-expire / no-change paths │
│  · Aggregation grain (GROUP BY edge cases)       │
│                                                  │
│ Output: branch manifest JSON (see schema below)  │
└──────────────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────┐
│ Stage 2 — Fixture Generation                     │
│                                                  │
│ Input:  branch manifest + table schemas + FK map │
│ Tool:   LLM (Claude), CoT prompting              │
│                                                  │
│ For each branch, generate minimum input rows:    │
│  · Positive case: rows that satisfy the branch   │
│  · Negative case: rows that bypass the branch    │
│  · FK consistency: topological sort by FK graph, │
│    generate parent rows before child rows        │
│  · Correlated columns generated together         │
│    (start/end dates, amount/currency, etc.)      │
│                                                  │
│ Output: {table_name: [rows]} per test scenario   │
└──────────────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────┐
│ Stages 3+4 — Ground-Truth Harness                │
│                                                  │
│ See ground-truth-harness.md for full design:     │
│  · ephemeral SQL Server container (Testcontainers│
│  · fixture execution + output capture            │
│  · dotnet-sqltest coverage (Cobertura XML)       │
│  · LLM gap-fill loop for uncovered branches      │
│  · dbt unit_tests: YAML emission                 │
└──────────────────────────────────────────────────┘
```

---

## Stage 1: Branch Extraction Prompt Strategy

Use Claude with a structured chain-of-thought prompt. sqlglot handles individual SQL statement parsing (WHERE, CASE, MERGE clauses); the LLM handles IF/ELSE/WHILE procedural control flow which sqlglot does not reliably parse in T-SQL proc bodies.

```text
Given this T-SQL stored procedure body and table schemas:

<proc_body>...</proc_body>
<schemas>...</schemas>

Step 1 — List every branch, condition, and code path:
  - All IF/ELSE/ELSE IF conditions and what each path does
  - All WHERE clause predicates (which rows are filtered in/out)
  - All CASE WHEN branches including ELSE fallback
  - All MERGE arms: WHEN MATCHED, WHEN NOT MATCHED, WHEN NOT MATCHED BY SOURCE
  - All JOIN types: which JOIN produces no-match rows that matter?
  - NULL handling: IS NULL / IS NOT NULL / COALESCE / ISNULL patterns
  - Window function edge cases: ROW_NUMBER() ties within a partition
  - Temporal/SCD patterns: rows that already exist vs new rows

Step 2 — For each branch, define:
  - Condition: what data triggers this path?
  - Positive input: minimum rows to satisfy the condition
  - Negative input: minimum rows to bypass the condition
  - FK dependencies: which parent table rows must exist first?
  - Line span: which lines in the proc body does this branch occupy?

Step 3 — Output as JSON per the branch manifest schema.
```

See [ground-truth-harness.md](ground-truth-harness.md) for the branch manifest JSON schema and field reference.

---

## Stage 2: FK-Consistent Fixture Generation

Group correlated columns in a single prompt call so semantic consistency is preserved (contract start/end dates, amount/currency pairs). Post-process FK constraints by generating rows in topological FK order — this is deterministic and does not require an extra LLM call.

1. Build FK graph from `sys.foreign_keys` + proc JOIN analysis (already done by profiler agent).
2. Topological-sort tables: dimension tables before fact tables, staging before target.
3. Generate fixture rows per table in that order, referencing only already-generated keys.
4. For many-to-many test cases, explicitly generate duplicate parent-key rows to verify the proc's deduplication/aggregation logic handles them correctly.

---

## Stage 4: dbt YAML Format

```yaml
unit_tests:
  - name: test_scd2_existing_row_expired_on_match
    model: dim_customer
    given:
      - input: source('fabric_wh', 'staging_customer')
        rows:
          - {customer_id: 1, name: "Alice Updated", effective_from: "2024-01-15"}
      - input: ref('dim_customer')
        rows:
          - {customer_sk: 101, customer_id: 1, name: "Alice",
             effective_from: "2023-01-01", effective_to: null, is_current: true}
    expect:
      rows:
        # expired row
        - {customer_sk: 101, customer_id: 1, name: "Alice",
           effective_from: "2023-01-01", effective_to: "2024-01-14", is_current: false}
        # new row
        - {customer_sk: 102, customer_id: 1, name: "Alice Updated",
           effective_from: "2024-01-15", effective_to: null, is_current: true}

  - name: test_scd2_no_change_row_preserved
    model: dim_customer
    given:
      - input: source('fabric_wh', 'staging_customer')
        rows:
          - {customer_id: 1, name: "Alice", effective_from: "2024-01-15"}
      - input: ref('dim_customer')
        rows:
          - {customer_sk: 101, customer_id: 1, name: "Alice",
             effective_from: "2023-01-01", effective_to: null, is_current: true}
    expect:
      rows:
        - {customer_sk: 101, customer_id: 1, name: "Alice",
           effective_from: "2023-01-01", effective_to: null, is_current: true}
```

Naming convention: `test_<load_pattern>_<scenario_description>`.

---

## End-to-End Comparison Workflow

After unit tests validate the dbt model logic, a secondary comparison runs against the WideWorldImporters dataset:

```text
Fixture CSVs (versioned in git)
  → Load to SQL Server Docker → EXEC proc → output table A
  → Load as dbt seeds → dbt run → output table B (in Fabric)
  → Land table A in Fabric (ADF pipeline)
  → dbt-audit-helper: compare_and_classify_query_results(A, B)
  → Investigate mismatches
```

Both output tables are in Fabric when comparison runs, so dbt-audit-helper works within a single connection. Fixtures are versioned in git — stable and reusable as a golden dataset.

**Do not use data-diff (archived May 2024).**

---

## Integration with Agent Pipeline

| Agent | Contribution |
|---|---|
| Decomposer | Block-level proc segmentation + statement indices used in Stage 1 extraction |
| Profiler | FK map (reader proc JOIN analysis) used in Stage 2 topological ordering |
| Test Generator | Runs Stage 1–4 pipeline and emits `unit_tests:` YAML blocks; outputs FixtureManifest |
| Migrator | Consumes FixtureManifest; incorporates `unit_tests:` blocks into model schema YAML |
| Migrator | Validates via `dbt test --select "model,test_type:unit"` before marking item complete |

---

## Tooling Summary

| Stage | Tool | Notes |
|---|---|---|
| Proc control flow parsing | LLM (Claude) | Handles IF/ELSE/WHILE — sqlglot cannot reliably |
| SQL statement parsing | sqlglot | WHERE, CASE, MERGE clauses within statements |
| Fixture generation | LLM (Claude) | CoT prompt, grouped correlated columns |
| FK ordering | Custom (topological sort) | Deterministic, no extra LLM call |
| Ground-truth capture + coverage | dotnet-sqltest + SQLCoverLib | Testcontainers built-in, Cobertura XML output |
| dbt YAML emission | Migrator agent (template) | Renders `unit_tests:` blocks |
| dbt test execution | dbt-core-mcp | `dbt test --select model,test_type:unit` |
| End-to-end comparison | dbt-audit-helper | Both sides materialized in Fabric |

---

## What We Do Not Need

- tSQLt — does not run on Fabric (CLR); transaction/rollback pattern replaces its isolation mechanism
- data-diff — archived May 2024
- SQLServerCoverage — dotnet-sqltest/SQLCoverLib covers this with better packaging
- Redgate SQL Data Generator — commercial; schema-aware but not proc-aware
- Full bronze extraction for unit tests
- Production Fabric Warehouse access in CI
