# Plan File Contract

The refactor-mart plan is markdown optimized for human review and LLM
interpretation. Do not create a Python parser, JSON schema, or parallel machine
contract for this file.

Each candidate must occupy exactly one level-2 markdown section. Do not add a
separate `## Candidates` wrapper and do not nest candidates under `###`
headings:

```md
## Candidate: <ID>

- [ ] Approve: yes|no
- Type: stg|int|mart
- Output: <path>
- Depends on: <ids>|none
- Validation: <models or command scope>
- Execution status: planned|applied|failed|blocked
```

## Candidate IDs

- Use `STG-` for staging candidates.
- Use `INT-` for intermediate candidates.
- Use `MART-` for mart candidates.
- Keep IDs stable within the plan file. Prefer short numbers such as `STG-001`
  over descriptive IDs that may change during editing.

## Approval

- Use unchecked approval syntax by default: `- [ ] Approve: no`.
- Use checked approval syntax only when the change is narrow, evidence-backed,
  and mechanically safe: `- [x] Approve: yes`.
- Do not auto-approve broad grain changes, ambiguous business logic, or changes
  that require domain review.

## Dependencies

- Staging candidates normally use `Depends on: none`.
- Non-staging candidates must list every required upstream candidate ID.
- A dependency is satisfied only after the referenced candidate has
  `Execution status: applied`.
- Use comma-separated IDs for multiple dependencies.

## Validation

Validation must name the smallest useful dbt scope for proving the candidate:

- a single model, such as `dbt build --select stg_bronze__customer`;
- a downstream model group, such as `dbt build --select +int_sales_orders`; or
- a mart target, such as `dbt build --select fct_sales`.

## Status Updates

Planning writes new candidates with `Execution status: planned`. Apply commands
are responsible for changing status to `applied`, `failed`, or `blocked` and for
recording the reason in the same candidate section.
