---
name: refactor-mart-plan
description: >
  Mart refactor planning command. Analyzes one or more selected targets as a
  bounded mart-domain unit, writes a markdown candidate plan, and does not apply
  dbt model changes.
user-invocable: true
argument-hint: "<schema.table> [schema.table ...]"
---

# Refactor Mart Plan

Analyze selected mart-domain targets, identify staging and higher-layer refactor candidates, and write a reviewable markdown plan artifact. This command is analysis-only: it must not create, edit, delete, or rewire dbt models.

## Guards

- `manifest.json` must exist. If missing, fail with `MANIFEST_NOT_FOUND`.
- `dbt/dbt_project.yml` must exist. If missing, fail with `DBT_PROJECT_MISSING` and tell the user to run `ad-migration setup-target`.
- At least one target FQN must be supplied in `$ARGUMENTS`. If none are supplied, fail with `TARGET_REQUIRED`.
- Every target must resolve to a catalog table, view, or materialized view entry. If a target is unknown, report `CATALOG_OBJECT_NOT_FOUND` for that target and continue planning the remaining valid targets.

## Progress Tracking

Use `TaskCreate` and `TaskUpdate` to show live progress. Create one task per selected target with status `pending`. Update each target to `in_progress` before analysis starts and to `completed` or `cancelled` after its planning result is known.

## Pipeline

### Step 1 -- Setup

1. Parse one or more FQN targets from `$ARGUMENTS`.
2. Generate a run slug:
   - Single target: `refactor-mart-plan-<schema>-<name>` in lowercase with dots replaced by hyphens.
   - Multiple targets: choose a concise domain slug, such as `refactor-mart-plan-sales`, in lowercase hyphen-separated form.
3. Run the `git-checkpoints` skill with the run slug as the argument.
   - If it returns the default branch name, use the current repository root as the working directory.
   - Otherwise, use the returned worktree path for all reads and plan writes.
4. Generate a plan filename under `docs/design/`:
   - Single target:
     `docs/design/refactor-mart-<schema>-<name>-plan.md`
   - Multiple targets:
     `docs/design/refactor-mart-<domain-slug>-plan.md`

### Step 2 -- Analyze Candidates

Run `/planning-refactor-mart <schema.table> [schema.table ...]` in the selected working directory.

The planning skill must:

- read the relevant catalog and existing dbt model context;
- identify candidate `stg`, `int`, and `mart` changes;
- distinguish pure source-normalization staging candidates from higher-layer dependency or business-logic candidates;
- declare dependencies for every non-staging candidate;
- preselect approval only when the evidence is strong and the change is mechanically safe;
- leave uncertain or broad candidates unapproved;
- write exactly one markdown plan artifact; and
- avoid mutating dbt model files.

### Step 3 -- Write Plan Artifact

The plan must be optimized for agent and human review, not Python parsing. Each candidate must occupy one level-2 markdown section using this shape. Do not wrap candidates in a separate `## Candidates` section or use `### Candidate` headings:

```md
## Candidate: <ID>

- [ ] Approve: yes|no
- Type: stg|int|mart
- Output: <path>
- Depends on: <ids>|none
- Validation: <models or command scope>
- Execution status: planned
```

Candidate IDs use stable prefixes:

- `STG-` for staging candidates
- `INT-` for intermediate candidates
- `MART-` for mart candidates

The plan must also include:

- source target list;
- separate `## Targets`, `## Assumptions`, and `## Non-Goals` sections;
- rejected or deferred candidates when relevant;
- execution instructions: run `/refactor-mart <plan-file> stg` before `/refactor-mart <plan-file> int`; and
- a statement that this planning run did not mutate dbt models.

### Step 4 -- Summarize

Print the plan path and candidate counts:

```text
refactor-mart-plan complete -- plan written to <plan-file>

stg: <n> | int: <n> | mart: <n>
approved: <n> | needs review: <n>

Next: review approvals, then run /refactor-mart <plan-file> stg.
```

If no valid targets were planned, do not write an empty plan. Report the per-item errors and stop.
