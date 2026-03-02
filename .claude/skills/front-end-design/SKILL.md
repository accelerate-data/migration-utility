---
name: front-end-design
description: Design and implement intentional, high-quality frontend UI for React and Tailwind surfaces with a clear visual direction, token-driven styles, and responsive behavior. Use when asked to create or restyle screens, design components, improve layout/UX polish, or turn rough UI requirements into production-ready frontend code.
---

# Front End Design

## Overview

Produce frontend implementations that are visually distinct, structurally clear, and shippable.
Drive from design intent first, then execute with tokenized styles, accessible interactions, and responsive checks.

## Workflow

1. Define intent and constraints from the request and codebase context.
2. Select one visual direction before writing UI code.
3. Implement layout and components with reusable patterns.
4. Apply motion and interaction states intentionally.
5. Verify responsive and accessibility behavior before finishing.

## Step 1: Define Intent and Constraints

- Read existing design system files and app patterns first.
- Preserve established product style unless explicitly asked to introduce a new direction.
- Extract concrete goals: target user action, page purpose, content hierarchy, and device constraints.

For this repo, align with:
- `app/src/styles/globals.css`
- `.claude/rules/frontend-design.md`

## Step 2: Pick a Visual Direction

- Choose one coherent direction and apply it consistently across typography, spacing, surfaces, and motion.
- Avoid generic defaults. Prefer deliberate contrast, rhythm, and hierarchy.
- Use `references/visual-directions.md` to pick a direction quickly.

## Step 3: Implement with Reusable UI Structure

- Build page structure first: information hierarchy, sections, and navigation flow.
- Build component internals second: states, affordances, copy tone, and icon usage.
- Favor design tokens and semantic utility classes over one-off values.
- Keep code maintainable: extract repeated UI patterns into components.

## Step 4: Add Interaction and Motion

- Use animation only where it communicates state or sequence.
- Prefer quick durations and subtle transitions over decorative motion.
- Ensure focus states, keyboard paths, and disabled/loading states are explicit.

## Step 5: Validate Before Handoff

- Check desktop and mobile breakpoints.
- Check empty, loading, success, and error states.
- Check contrast and readability across all key text/surface combinations.
- Run project-appropriate checks (for example TypeScript and tests if UI logic changed).

Use `references/qa-checklist.md` for a fast pre-ship pass.

## Reference Files

- `references/visual-directions.md`: Select a stylistic direction with concrete color/typography/motion choices.
- `references/qa-checklist.md`: Run a release-focused UI quality checklist.
