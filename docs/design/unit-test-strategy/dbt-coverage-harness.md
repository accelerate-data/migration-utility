# dbt Model Coverage Harness

CI hook that runs branch coverage against deployed dbt models using existing `unit_tests:` fixtures. Same pipeline as [ground-truth-harness.md](ground-truth-harness.md) with two substitutions.

## Substitutions from Ground-Truth Harness

| | Ground-Truth Harness | dbt Coverage Harness |
|---|---|---|
| SQL source | `sys.sql_modules` via MCP tool call | `target/compiled/<model>.sql` from `dbt compile` |
| Execution engine | SQL Server (testcontainers) | DuckDB (in-process) |
| Coverage mechanism | dotnet-sqltest + Cobertura XML | sqlglot probe columns + DuckDB `EXPLAIN ANALYZE` |
| Fixture source | LLM-generated synthetic rows | Existing `unit_tests:` YAML `given` rows |
| Gap-fill output | Fully captured expected rows (auto, from proc execution) | `unit_tests[]` stubs for FDE to complete (no proc to run) |

Branch manifest schema, coverage resolver, and termination rules are identical.

## Input

```text
target/compiled/<project>/<model>.sql   — compiled dbt SQL
models/<model>.yml                      — unit_tests: given rows (fixture source)
```

## Coverage Mechanism

No OSS tool instruments SQL branch execution in DuckDB. Two complementary approaches cover different branch types without modifying the main query output.

| Branch type | Method |
|---|---|
| CASE WHEN arms | sqlglot probe column injection |
| WHERE predicates | sqlglot probe column injection |
| LEFT JOIN no-match | DuckDB `EXPLAIN ANALYZE` cardinality |

### CASE WHEN arms — sqlglot probe columns

Inject a probe column per CASE expression alongside the original:

```sql
-- original
CASE WHEN status = 'active' THEN 1 ELSE 0 END AS is_active

-- probe column (added to SELECT)
CASE WHEN status = 'active' THEN 'active_arm' ELSE 'else_arm' END AS _probe_is_active
```

Run each fixture's `given` rows through the rewritten SQL. Collect distinct `_probe_*` values across all output rows. Arms that never appear → uncovered.

### WHERE predicates — sqlglot probe columns

Remove the WHERE clause; rewrite each leaf predicate as a boolean column:

```sql
-- original
WHERE email IS NOT NULL AND amount > 0

-- rewritten (no WHERE)
SELECT *, (email IS NOT NULL) AS _probe_email, (amount > 0) AS _probe_amount
FROM ...
```

Both `true` and `false` must appear per probe column across all fixtures. Missing `false` → filter-out path never exercised.

> **Limitation:** probe columns detect per-leaf predicate coverage only. Compound predicate interactions (e.g. `A IS NULL AND B > 0` where each leaf is individually exercised but never together) require additional fixture authoring.

### LEFT JOIN no-match — DuckDB EXPLAIN ANALYZE

Run the original (unmodified) query and parse the `EXPLAIN ANALYZE` operator tree:

```sql
EXPLAIN ANALYZE <compiled_sql>
```

If a join operator shows `Rows: 0` on the probe side → the no-match path was never exercised. No SQL rewrite needed — cardinality is free from the plan.

## Pipeline

```text
dbt compile
  │
  ├─ Stage 1: sqlglot branch extraction
  │    · parse target/compiled/<model>.sql
  │    · extract CASE arms, WHERE predicates, JOIN types
  │    · output: branch manifest (same schema as ground-truth-harness.md)
  │
  ├─ Stage 2: sqlglot AST rewrite
  │    · CASE WHEN + WHERE branches: inject probe columns, remove WHERE
  │    · JOIN branches: no rewrite needed
  │    · output: rewritten SQL (probe version) + original SQL (EXPLAIN version)
  │
  ├─ Stage 3: DuckDB execution (per fixture)
  │    · load unit_tests: given rows into DuckDB tables
  │    · run rewritten SQL → collect distinct _probe_* values from output rows
  │    · run EXPLAIN ANALYZE on original SQL → parse join operator cardinality
  │
  ├─ Stage 4: Coverage Resolver
  │    · CASE/WHERE: arms/predicates with no probe hit → uncovered
  │    · JOIN: operators with Rows: 0 on probe side → no-match path uncovered
  │    · uncovered_branches[] = union across all fixture runs
  │
  ├─ if uncovered_branches not empty (max 3 iterations):
  │    · LLM gap-fill prompt (same as ground-truth-harness.md)
  │    · output: new unit_tests: YAML stubs for FDE to fill in
  │    · loop back
  │
  └─ Stage 5: Report
       · coverage: complete | partial
       · uncovered_branches[] flagged for FDE fixture authoring
       · CI exit code: 0 (complete) | 1 (partial)
```

## CI Integration

```bash
dbt compile --select <model>
python -m harness.dbt_coverage --model <model> --threshold 100
```

Runs without warehouse access. Fails the PR if coverage drops below threshold.

## Termination

Same rules as ground-truth-harness.md:

| Condition | `coverage` | `status` | CI |
|---|---|---|---|
| All branches hit | `complete` | `ok` | exit 0 |
| Max 3 iterations reached | `partial` | `partial` | exit 1 |
| LLM returns no fixture for a branch | `partial` | `ok` | exit 1 |
