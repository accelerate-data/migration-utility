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
4. write the reviewed JSON test spec with captured expectations

The batch command handles the orchestration, commits successful outputs, and can raise a PR at the end.

## What gets written

| File | Purpose |
|---|---|
| `test-specs/<item_id>.json` | reviewed spec with fixtures, branch manifest, unit test definitions, and captured expectations |

The JSON spec is the committed test-generation artifact. `/generate-model` consumes it and renders dbt `unit_tests:` into the generated schema YAML.

## Sandbox teardown

Use `ad-migration teardown-sandbox` after you are done with test generation and SQL proof workflows that depend on the sandbox. See [[Sandbox Operations]] for the sandbox lifecycle.

## Next step

Proceed to [[SQL Refactoring]].
