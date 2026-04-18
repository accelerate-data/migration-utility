# Dependency Gate

Run before any dbt edit.

Satisfied declarations:

- `Depends on: none`
- `Depends on:` followed by upstream candidate IDs whose sections all have `Execution status: applied`

Gate failure cases:

- `Depends on:` is missing, empty, malformed, ambiguous, or mixes prose with IDs.
- A referenced dependency section is missing.
- A dependency exists but is not `Execution status: applied`.

On failure, stop with `DEPENDENCY_GATE_NOT_SATISFIED` and leave the candidate section unchanged. `/refactor-mart` owns dependency-blocked status writeback and records missing/unsatisfied dependency IDs plus their actual statuses.
