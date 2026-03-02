# Frontend QA Checklist

Use this checklist before finalizing UI work.

## Layout and Responsiveness

- Verify desktop and mobile layouts for clipping, overflow, and wrapping.
- Verify primary CTA remains visible and reachable on small screens.
- Verify spacing rhythm stays consistent across sections.

## Interaction Quality

- Verify hover, focus, active, disabled, and loading states for interactive controls.
- Verify keyboard navigation order and escape paths for dialogs/sheets.
- Verify async actions provide clear progress and completion feedback.

## Accessibility and Readability

- Verify text contrast for body copy, labels, and secondary metadata.
- Verify icon-only controls include accessible labels.
- Verify heading structure reflects information hierarchy.

## State Coverage

- Verify empty state, success state, and error state are all implemented.
- Verify long-content behavior in tables, cards, and side panels.
- Verify destructive actions include confirmation and clear copy.

## Code Quality

- Verify style choices use project tokens and semantic classes.
- Verify duplicated UI patterns are extracted into reusable components.
- Verify TypeScript and relevant tests pass for changed areas.
