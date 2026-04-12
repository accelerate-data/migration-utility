# Stage 3 -- Test Generation

`/generate-tests` creates migration test scenarios, reviews them independently, and captures ground truth in the sandbox.

## Prerequisites

- `manifest.json` with `sandbox.database`
- a running sandbox
- completed scoping
- completed profiling

## Invocation

```text
/generate-tests silver.DimCustomer silver.FactInternetSales
```

## Pipeline

1. generate the test scenarios with `/generating-tests`
2. review them with `/reviewing-tests`
3. execute approved scenarios in the sandbox
4. write dbt-ready YAML test artifacts

The batch command handles the orchestration, commits successful outputs, and can raise a PR at the end.

## What gets written

| File | Purpose |
|---|---|
| `test-specs/<item_id>.json` | intermediate spec with fixtures, branch manifest, and captured expectations |
| `test-specs/<item_id>.yml` | dbt-ready committed artifact |

The JSON spec is the working artifact for the pipeline. The YAML file is the user-facing dbt artifact that gets committed for downstream model generation.

## Sandbox teardown

Use `/teardown-sandbox` after you are done with test generation and SQL proof workflows that depend on the sandbox.

## Next step

Proceed to [[Stage 5 SQL Refactoring]].
