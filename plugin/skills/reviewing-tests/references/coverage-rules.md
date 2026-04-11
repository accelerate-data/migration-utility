# Coverage Rules

## Untestable Branches

Mark a branch as `untestable` only when static fixtures cannot represent the branch behavior.

Common examples:

- non-deterministic time or random functions
- dynamic SQL where the runtime chooses table or column targets
- external service calls
- other runtime-only side effects

Not enough on its own:

- the branch is inconvenient to model
- the generator omitted a scenario
- the source logic is complex but still data-driven

Every untestable branch needs:

- `id`
- `description`
- `rationale`

## Coverage Boundaries

Use `complete` only when every testable branch is covered.

Use `partial` when:

- one or more testable branches are still uncovered
- the review depends on `untestable` classifications to explain the remainder

Do not treat a branch as covered just because a scenario name implies it. Coverage is based on fixture logic plus expected behavior.
