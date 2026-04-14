# VU-1063 Eval Run Cleanup Design

## Decision

Prune stale eval run directories from `tests/evals/output/runs/` at the start of the next eval run. A run directory is stale when its modified time is older than 24 hours.

## Scope

This change applies only to the promptfoo eval harness workspace-copy flow in `tests/evals/scripts/run-workspace-extension.js` and the related ignore rule in `.gitignore`.

## Behavior

On the first `beforeEach` hook invocation in a promptfoo process, the extension scans `tests/evals/output/runs/` and removes run directories older than 24 hours.

The cleanup runs before the current test's fixture copy is created. This guarantees the active run directory cannot be deleted during its own setup.

If the runs root does not exist, cleanup is a no-op.

Fresh run directories newer than 24 hours are preserved for short-term debugging.

## Rejected Alternatives

### Delete runs immediately after each eval

Rejected because it removes the most useful debugging artifact too early and makes post-failure inspection harder.

### Add a dedicated `preeval` npm script

Rejected because the existing extension already owns eval workspace lifecycle setup, so adding a second entrypoint would split closely related behavior without a clear benefit.

### Keep a 7-day TTL

Rejected because the approved retention window for this issue is 24 hours.

## Implementation Boundaries

Keep the change inside the existing extension flow and avoid changing eval package wiring.

Add an explicit `.gitignore` entry for `tests/evals/output/runs/` even though `tests/evals/output/` is already ignored, so the run-directory rule is visible and unambiguous.

Guard cleanup so it runs once per promptfoo process rather than once per test case.

## Error Handling

Missing output directories are not errors.

Filesystem failures during pruning should surface as normal extension errors rather than being silently ignored.

## Testing

Add focused coverage for the pruning behavior:

- stale run directories older than 24 hours are removed
- fresh run directories newer than 24 hours are preserved
- missing `tests/evals/output/runs/` is handled as a no-op
- the current eval still receives a fresh `run_path` after pruning

Manual testing is not required if automated coverage proves the retention behavior and active-run creation flow.
