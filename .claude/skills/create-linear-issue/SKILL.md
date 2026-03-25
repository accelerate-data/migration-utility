---
name: create-linear-issue
description: |
  Creates Linear issues from product thoughts, feature requests, or bug reports. Decomposes large issues into smaller ones.
  Triggers on "create issue", "log a bug", "file a ticket", "new feature", "break down <issue-id>", or "/create-issue".
---

# Create Linear Issue

Turn a short product thought into a clear, product-level Linear issue.

See `../../rules/codex-execution-policy.md` for execution mode.

## Flow Overview

```text
User request
  │
  ├─ "break down <issue-id>" ──► Decompose Path
  │
  └─ new issue
       │
       ├─ classify: feature or bug
       │
       ├─ [bug only] Investigate ──► Bug Path
       │
       └─ [feature] ──► Feature Path
              │
              ├─ estimate ≤ L ──► Create single issue
              └─ estimate > L or multi-platform ──► Decompose Path
```

Every path ends with: **Dedupe → Draft → Confirm → Create → Return ID + URL**.

---

## 1) Tool Contract

Use these exact tools:

| Tool | Purpose |
|---|---|
| `mcp__linear__list_issues` | dedupe search, child discovery |
| `mcp__linear__get_issue` | fetch parent for decomposition |
| `mcp__linear__list_projects` | project resolution |
| `mcp__linear__get_project` | full project details |
| `mcp__linear__list_milestones` | milestone discovery for resolved project |
| `mcp__linear__list_issue_labels` | label selection |
| `mcp__linear__save_issue` | create/update issue(s) |
| `mcp__linear__save_comment` | optional rationale notes on parent |

Required fields for `save_issue`: `team`, `title`. Include `description`, `project`, `labels`, `estimate`, `assignee: "me"` when available.

**Fallback:** if a required tool fails after one retry, stop and report. Do not fabricate IDs, labels, or project names.

---

## 2) Core Rules

1. **Product-level only.** No file names, component names, or architecture in issue body.
2. **Confirm before creating.** Always show final draft before `save_issue`.
3. **Clarifications:** ask at most 2 targeted questions. If confidence >= 80%, default assumptions and proceed.
4. **Idempotency:** re-runs must not duplicate issues. Reuse an open near-duplicate when appropriate.
5. **Acceptance criteria** use Markdown checkboxes (`- [ ] ...`).
6. **Project resolution:** derive from user input or issue context. Never hardcode a project name.
7. **Milestone resolution:** from the resolved project only. If no clear match, ask the user.
8. **Decomposition by feature slice only.** No frontend/backend/API splits. Each child must be an integrated, end-to-end testable outcome.
9. **Multi-platform rule:** distinct source or target platforms always decompose into separate children (one per platform).

---

## 3) Estimate Table

| Label | Points | Agent effort |
|---|---|---|
| XS | 1 | < 10 min |
| S | 2 | ~30 min |
| M | 3 | 1-2 hours |
| L | 5 | Half day |

- `L` is the maximum single-issue size.
- If scope exceeds `L`, switch to Decompose Path.

---

## 4) Issue Description Schema

Every issue description uses this template:

```md
## Problem
...

## Goal
...

## Non-goals
- ...

## Acceptance Criteria
- [ ] ...
- [ ] ...

## Risks
- ...

## Test Notes
- ...
```

---

## 5) Shared Procedures

These procedures are used by multiple paths. Each path references them by name.

### Dedupe Check

1. Search open issues with `list_issues` using title/keyword query.
2. If a near-duplicate exists, present it and ask whether to reuse/update instead.

### Project and Milestone Resolution

1. Resolve the target project from explicit user input, parent issue context, or team defaults via `list_projects`/`get_project`.
2. Fetch milestones for that project with `list_milestones`.
3. If exactly one milestone maps to the feature intent, include it in the draft.
4. If ambiguous, ask the user before `save_issue`.
5. Never pick a milestone from a different project.

---

## 6) Feature Path

Use when the request is a new feature (not a bug, not a decomposition).

### Step 1 — Scope decision

Ask the user: **proceed directly**, or **explore alternatives first**?

- In both cases, internally review the codebase for feasibility and scope.
- The difference is whether the user sees options before requirements are drafted.

### Step 2a — Direct path

Review the codebase to assess feasibility and scope.

- **Internal (not in the issue):** feasibility signal, scope estimate (XS–L), constraints.
- **For the issue:** numbered requirements + checkbox ACs. Product-level only.

### Step 2b — Exploration path (when complexity is high)

Run parallel research:

1. **Codebase analyst** — feasibility, constraints, scope (internal only).
2. **External researcher** — how similar products handle this, UX/backend patterns.

Synthesize into 2-3 product-level options. Always include the user's original approach. No implementation details.

### Step 3 — User picks (exploration path only)

Present options and let the user choose.

### Step 4 — Requirements and estimate

Write requirements for the chosen approach. Apply estimate table. If estimate > L or the request spans multiple platforms, switch to **Decompose Path**.

### Step 5 — Dedupe, draft, confirm, create

1. Run **Dedupe Check**.
2. Run **Project and Milestone Resolution**.
3. Draft title, estimate, project, milestone, labels, description (Issue Description Schema).
4. Confirm draft with user. Max 2 refinement rounds, then proceed with best assumptions.
5. Create with `save_issue` (`assignee: "me"` when allowed).
6. Return issue ID + URL.

---

## 7) Bug Path

Use when the request describes a bug or regression.

### Step 1 — Investigation

Spawn an `Explore` sub-agent to review code and recent git history. It returns:

- **Internal (not in the issue):** likely root cause, affected scope, recent relevant commits, estimated fix complexity (XS–L).
- **For the issue:** user-visible symptom, reproduction steps, severity, frequency.

### Step 2 — Present findings

Show the user the product-level findings. Ask to confirm or correct.

### Step 3 — Estimate

Use the sub-agent's internal scope signal. Apply estimate table.

### Step 4 — Dedupe, draft, confirm, create

1. Run **Dedupe Check**.
2. Run **Project and Milestone Resolution**.
3. Draft title, estimate, project, milestone, labels (`bug`), description (Issue Description Schema).
4. Confirm draft with user.
5. Create with `save_issue` (`assignee: "me"` when allowed).
6. Return issue ID + URL.

---

## 8) Decompose Path

Use when scope exceeds `L`, the request spans multiple platforms, or the user explicitly asks to break down an existing issue.

### Step 1 — Resolve parent

- If decomposing an existing issue: fetch it with `get_issue`.
- If creating a new parent: draft the parent issue first using **Feature Path** steps 1-4, but skip creation until children are planned.

### Step 2 — Plan children

1. Split into 2-4 child issues, each <= `L`.
2. **Traceability:** each child maps to exactly one AC group from the parent.
3. **Multi-platform:** one child per distinct platform.
4. Resolve milestone candidates from the project; if unclear, ask user.

### Step 3 — Confirm child plan

Present the parent + children plan to the user. Show title, estimate, milestone, and AC mapping for each child.

### Step 4 — Create

1. Run **Dedupe Check** for each child.
2. Create the parent issue (if new) with `save_issue`.
3. Create children with `save_issue`, setting the `parent` field to the parent issue ID. Include AC-group mapping in each child's description.
4. Update the parent description to list child issue IDs and their AC-group mapping.
5. Optionally add a rationale comment on the parent with `save_comment`.
6. Return all issue IDs + URLs.

---

## 9) Output Hygiene

- Never inline long command/test output into Linear issue fields.
- Keep Linear descriptions concise and product-facing.

---

## References

- [`references/linear-operations.md`](references/linear-operations.md) — required MCP tools and fallback policy
