# Fixture Quality Rules

Review each scenario for:

- fixture realism
- scenario isolation
- FK consistency
- edge cases
- required source columns

## Required Source Columns

Flag a missing source column only when the reviewed logic needs it.

Required columns usually include columns used in:

- joins
- filters
- CASE predicates
- computed expressions
- inserted or updated target values
- grouping or ordering that changes branch behavior

Do not require a source column just because it is `NOT NULL` in the source catalog.

## Good vs Bad NOT NULL Review

Good:

- The logic joins on `ProductID`, but the fixture omits `ProductID`.
- The logic computes `Status` from `SellEndDate`, but the fixture omits `SellEndDate`.
- The logic inserts `ProductName`, but the fixture omits `ProductName`.

Bad:

- The source catalog marks `MakeFlag` as `NOT NULL`, but the reviewed statement never reads `MakeFlag`.
- The source catalog marks `ProductNumber` as `NOT NULL`, but the staged model only selects `ProductID` and `ProductName`.

## Severity

Use `error` when the fixture cannot validly exercise the reviewed branch.

Use `warning` when:

- the fixture is unrealistic but still valid
- the scenario is noisy or combines too many ideas
- the test misses a useful edge case without making the current case invalid
