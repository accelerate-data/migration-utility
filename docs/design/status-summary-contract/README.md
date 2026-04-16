# Status Summary Contract

`/status` has two display modes with different responsibilities.

- `/status` is the pipeline dashboard.
- `/status <object>` is the detailed evidence view.

Summary mode must keep table-level evidence out of the dashboard so the command does not infer
workflow decisions from stale artifacts, hidden diagnostic details, or source/seed catalog state.

## Decision

Summary `/status` shows a pipeline stage table for active migration objects only.

Source tables and seed tables are excluded from the summary table because they are intentionally
outside the migration workflow. They may contribute to counts or setup readiness, but they are not
rows in the active pipeline table.

The summary table shows object identity, object type, and stage state only:

```text
migration status - 8 pipeline objects

  Object                    type    scope      profile    test-gen       refactor    migrate
  ------------------------------------------------------------------------------------------
  silver.dim_customer       table   ok         ok         setup-blocked  blocked     blocked
  silver.dim_product        table   ok         pending    blocked        blocked     blocked
  silver.fact_sales         table   ok         ok         ok             warning     blocked
  silver.fact_returns       table   error      blocked    blocked        blocked     blocked
  silver.vw_sales           view    ok         ok         pending        blocked     blocked
```

Stage cells use a small vocabulary: `ok`, `pending`, `blocked`, `setup-blocked`, `warning`,
`error`, and `N/A`.

Summary mode must not show selected writers, diagnostic messages, source dependencies, test branch
counts, test-spec details, refactored SQL details, resolved diagnostic rationale, or review-file
contents.

## Diagnostics

Summary `/status` shows diagnostics in a separate table. Each row is an object with at least one
diagnostic.

```text
diagnostics

  Object                    errors        warnings                    details
  -------------------------------------------------------------------------------
  silver.fact_returns       1 unresolved  0                           /status silver.fact_returns
  silver.fact_sales         0             1 unresolved                /status silver.fact_sales
  dim.dim_address           0             1 resolved                  /status dim.dim_address
```

Use resolved/unresolved language in user-facing output:

- `unresolved` diagnostics are active and can affect the next action.
- `resolved` diagnostics were reviewed or accepted and do not block the active workflow.

Summary mode may show resolved diagnostic counts, but it must not show resolved diagnostic codes,
messages, rationales, or review metadata. Users can inspect object-level status or diagnostic
review workflows when they need detail.

## Next Action

Summary `/status` presents one "What to do next" section after the pipeline and diagnostics tables.

The next action is selected from deterministic pipeline state, not from table artifacts. Setup
blocks, unresolved errors, and current pipeline phase take precedence over later-stage artifacts
that may already exist on disk.

```text
What to do next

  !ad-migration setup-target
```

## Detailed Status

`/status <object>` owns object-level evidence. It may show selected writer, diagnostic messages,
test generation details, refactor status, and setup/readiness evidence for that object.

The summary dashboard should route users to detailed status through the diagnostics table or a
short detail hint, for example:

```text
details: /status silver.fact_sales
```

## Rationale

The LLM status command became unreliable when summary mode mixed pipeline routing with detailed
artifact interpretation. Stale test specs, refactor files, and catalog diagnostics can conflict
with deterministic readiness output.

Keeping summary mode as a dashboard and moving evidence into `/status <object>` makes the contract
clear: the CLI owns deterministic state, and the LLM renders concise pipeline state plus one next
action.
