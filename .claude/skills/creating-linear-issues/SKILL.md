---
name: creating-linear-issues
description: Use when creating, decomposing, or refining Linear issues from product requests, bug reports, or issue breakdowns in this repository
---

# Creating Linear Issues

## Overview

Turn a request into a clear Linear issue only after codebase review, clarification, and plan approval. Product scope belongs in the issue; implementation detail belongs in the plan, not the ticket body.

## When to Use

- User asks to create an issue, file a bug, track a feature, or break down an existing issue.
- The request is still ambiguous enough that the wrong milestone, cycle, or decomposition would create churn.
- Do not use for implementation, PR raising, or closing work.

## Quick Reference

| Step | Requirement |
|---|---|
| 0 | Classify first: `feature`, `bug`, or `spike` |
| 1 | Search the codebase and existing Linear issues first |
| 2 | Answer clarification questions yourself when the code gives one clear answer |
| 3 | Ask at most one user question at a time, only for unresolved forks |
| 4 | Enter plan mode and show the full issue draft plan before creating anything |
| 5 | Create or update the issue only after plan approval |

## Implementation

**Tool contract:** use `mcp__codex_apps__linear_mcp_server_get_issue`, `list_issues`, `list_projects`, `get_project`, `list_milestones`, `list_cycles`, `list_issue_labels`, `save_issue`, and `save_comment`. Retry once on tool failure, then stop and report the exact failing step.

**Classification is required before planning:**

- `feature` for net-new functionality or capability changes
- `bug` for regressions, defects, broken behavior, or incorrect output
- `spike` for research, design, investigation, or documentation-driven discovery work

Each kind has its own path:

- `feature` path: user outcome, scope, acceptance criteria, and rollout constraints
- `bug` path: symptom, impact, repro, severity, and fix acceptance criteria
- `spike` path: question to answer, research boundary, deliverable, and exit criteria

**Clarification protocol:**

1. Search the codebase first.
2. For each open question:
   - If the code answers it confidently, state the decision and move on.
   - If exactly one viable path exists, state it and move on.
   - If two or more viable paths remain, ask one question and wait.
3. Never batch questions.
4. Do not enter plan mode while any gap remains unresolved.

**Plan mode is required.** Present the full plan before creating the issue. The plan must include:

- selected issue kind and why
- issue type: feature, bug, or decomposition
- resolved project, milestone, and cycle strategy
- dedupe result
- issue draft outline
- decomposition approach when scope is too large for one issue

**Issue body contract:**

- Product-level only. No file paths, modules, or architecture.
- Use this structure:

```md
## Problem
...

## Goal
...

## Non-goals
- ...

## Acceptance Criteria
- [ ] ...

## Risks
- ...

## Test Notes
- ...
```

**Resolution rules:**

- Dedupe before creating.
- Never create without a project, milestone, and cycle unless the user explicitly approves the exception.
- Decompose by end-to-end slice, not by frontend/backend split.
- Use literal Markdown in `description`; never send escaped `\n` or escaped checkboxes.

## Common Mistakes

- Asking the user for scope details the codebase already answers.
- Entering plan mode before milestone, cycle, or decomposition gaps are resolved.
- Creating the issue immediately after drafting instead of waiting for plan approval.
- Writing implementation details into the issue description.

- [`references/linear-operations.md`](references/linear-operations.md) — required MCP tools and fallback policy
