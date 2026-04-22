# Eval Assertion Helper Tests

Colocated `*.test.js` files exercise assertion helpers from the same caller-facing boundary as Promptfoo: helper output, context vars, temporary run artifacts, and returned pass/fail reasons.

The following helpers remain intentionally uncovered by colocated deterministic tests:

- `check-dbt-aware-refactored-sql.js`: scenario-specific refinement of refactored SQL artifact checks; stable coverage belongs in the dbt-aware refactoring eval fixture.
- `check-ifelsetarget-branch-semantics.js`: fixture-specific branch semantics oracle for the `IfElseTarget` scenario.
- `check-refactor-mart-plan.js`: large markdown-contract oracle whose stable behavior is already covered through refactor-mart planning eval fixtures; colocated unit tests would mostly snapshot the plan contract internals.

Do not add tests for one-line compatibility barrels or scenario-only oracles unless a regression shows reusable behavior worth protecting outside Promptfoo fixtures.
