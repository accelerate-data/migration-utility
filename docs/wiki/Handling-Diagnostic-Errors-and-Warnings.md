# Handling Diagnostic Errors and Warnings

Use this page when `/status` shows table or object diagnostics.

Diagnostics are reviewed one object at a time. Start with the object that blocks the next migration step, then rerun `/status` after each review.

## Start with status

Run:

```text
/status
/status <schema.table>
```

Use batch status to find objects with errors or warnings. Use single-object status to see the specific stage, diagnostic code, and next recommended action.

Errors normally block progress. Warnings may need a catalog correction, a human decision, or an accepted-review record.

## Review one object

Ask Claude Code to review diagnostics for the affected object:

```text
Review diagnostics for silver.DimCustomer
```

The precise command form is:

```text
/reviewing-diagnostics <schema.table>
```

For example:

```text
/reviewing-diagnostics silver.DimCustomer
```

## What the review does

The review inspects the active diagnostics, catalog state, writer context, references, and existing diagnostic review records for that object.

It can:

- fix stale or incorrect catalog facts
- ask you to choose when multiple catalog fixes are plausible
- record an accepted warning when the warning is real but does not block migration
- leave the diagnostic active when there is not enough evidence to fix or accept it

It does not suppress errors by default.

## Common catalog fixes

Diagnostics often come from stale or incorrect catalog state, such as:

- wrong selected writer
- profile attached to the wrong writer
- stale profile after scoping changed
- warning text that points to a better writer candidate

When there is one clear correction, the review updates the catalog and reports the changed paths.

## Accepting warnings

Accept a warning only when the table-specific evidence was inspected and the warning does not block a safe migration path.

Accepted warnings are recorded in `catalog/diagnostic-reviews.json` and matched to the active warning by object, diagnostic code, and message identity.

Do not accept vague warnings with reasons like "reviewed" or "acceptable." The rationale should explain why this specific warning is safe for this specific object.

## After review

Run:

```text
/status <schema.table>
```

Then:

- continue with the next recommended command if the object is unblocked
- repeat `/reviewing-diagnostics <schema.table>` if a new warning appears
- commit reviewed catalog changes when they are ready for review

## Related pages

- [[Status Dashboard]]
- [[Troubleshooting and Error Codes]]
- [[Browsing the Catalog]]
- [[Scoping]]
- [[Profiling]]
