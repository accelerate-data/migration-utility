# VU-1069 Assertion Helper Design

## Decision

Add a broader reusable artifact-checking helper to `tests/evals/assertions/schema-helpers.js` that centralizes fixture-relative artifact loading, parse failures, haystack extraction, and expected/forbidden term checks.

Refactor the first implementation slice to use that helper in these assertion files:

- `tests/evals/assertions/check-output-terms.js`
- `tests/evals/assertions/check-listing-output.js`
- `tests/evals/assertions/check-command-review-result.js`

## Scope

This issue only removes repeated assertion boilerplate. It does not change what the affected assertions validate, add new assertion types, or redesign the eval harness.

Before editing assertions, do a quick sweep across the remaining files in `tests/evals/assertions/` to identify other near-match candidates. Treat that sweep as a fit check for the helper contract, not as a requirement to refactor every similar file in this issue.

## Helper Boundary

The shared helper should accept enough information to:

- resolve an artifact path from eval context
- read either raw text or JSON artifacts
- fail consistently when an artifact is missing or unparseable
- select the artifact value to inspect
- check expected and forbidden terms against a lowercase haystack

The helper must stay assertion-friendly rather than becoming a general assertion framework. Assertion-specific structural checks remain in each wrapper file.

## Assertion Boundaries

Keep assertion-specific logic in the assertion file when it is tied to that artifact's schema or scenario gating. Examples include:

- deciding whether a scenario should be skipped
- checking required vars such as `target_table` or `target_view`
- validating domain fields such as review verdicts, branch counts, or catalog sections
- returning domain-specific success reasons

Move only the repeated artifact and term-checking pattern into `schema-helpers.js`.

## Documentation

Document the new assertion pattern with a short example close to the helper, so assertion authors can copy the intended usage from the same file that exports it.

## Rejected Alternatives

### Term-only helper

Rejected because it leaves most of the duplicated path resolution, file loading, parsing, and failure handling in the assertion wrappers.

### Mini assertion DSL

Rejected because it adds indirection and abstraction cost beyond the needs of this refactor.

## Error Handling

Missing artifact and parse errors should still fail the assertion, but they should do so through one shared message path in `schema-helpers.js`.

Assertion-specific validation failures should continue to originate from the assertion file so debugging still points to the relevant rule.

## Testing

Verify behavior by running two representative scenarios per affected eval package instead of the full package:

- `tests/evals/packages/listing-objects/skill-listing-objects.yaml`
- `tests/evals/packages/analyzing-table/skill-analyzing-table-readiness.yaml`
- `tests/evals/packages/cmd-generate-tests/cmd-generate-tests.yaml`

Success means the targeted scenarios produce the same pass/fail outcomes as before the refactor.

No new pytest coverage is required for this issue. The proof point is unchanged eval behavior for the refactored assertions plus the new helper example.
