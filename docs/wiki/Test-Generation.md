# Test Generation

`/generate-tests` creates migration test scenarios, reviews them independently, and captures ground truth in the sandbox.

## Prerequisites

- `manifest.json` with `runtime.sandbox`
- a running sandbox
- completed scoping
- completed profiling

## Invocation

```text
/generate-tests silver.DimCustomer silver.FactInternetSales
```

## Pipeline

1. generate the test scenarios
2. run the independent review loop
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

Use `ad-migration teardown-sandbox` after you are done with test generation and SQL proof workflows that depend on the sandbox. See [[Sandbox Operations]] for the sandbox lifecycle.

## Next step

Proceed to [[SQL Refactoring]].
