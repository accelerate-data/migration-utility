---
name: planning-refactor-mart
description: Use when a selected mart unit must be analyzed into staging and higher-layer refactor candidates before any code changes
user-invocable: false
argument-hint: "<schema.table> [schema.table ...]"
---

# Planning Refactor Mart

Analyze one or more selected targets that form one bounded mart-domain unit and
write a markdown plan of refactor candidates. This skill is planning-only: do
not create, edit, delete, or rewire dbt models.

## Inputs

- One or more selected mart-domain targets from the command arguments that
  together form one bounded unit of work.
- The active project root.
- Existing `catalog/`, `manifest.json`, and `dbt/` context.
- The required plan output path from `/refactor-mart-plan`.

If the selected targets do not form a coherent unit of work, stop and ask the
user for a narrower target set. Do not silently widen the plan to the whole
project.

## Required References

Read these before drafting candidates:

- `references/plan-file-contract.md`
- `../_shared/references/dbt-project-standards.md`
- `../_shared/references/model-naming.md`
- `../_shared/references/sql-style.md`
- `../_shared/references/yaml-style.md`

## Analysis Flow

1. Identify the selected mart-domain unit:
   - normalize each target FQN;
   - resolve the catalog entry for each target;
   - inspect existing dbt models in `dbt/models/staging/`,
     `dbt/models/intermediate/`, and `dbt/models/marts/`; and
   - note assumptions when catalog or dbt context is incomplete.
2. Propose staging candidates when a source-facing transformation can be
   isolated as a pure staging wrapper:
   - one source relation in;
   - no joins;
   - no aggregations;
   - no business categorization;
   - no grain change; and
   - no target-specific filtering unless it is source hygiene already present
     in the existing staging contract.
3. Propose intermediate candidates when logic is reusable across the selected
   mart unit but is not source-facing:
   - joins across staging models;
   - reusable deduplication or conformance logic;
   - repeatable dimensional shaping; or
   - reusable fact preparation.
4. Propose mart candidates only for final presentation-layer cleanup:
   - replacing duplicated final select logic;
   - rewiring a mart to new staging or intermediate candidates; or
   - separating final metric naming from reusable upstream logic.
5. Reject or defer candidates that are too broad, mix grains, need domain
   confirmation, or would require execution behavior from `/refactor-mart`.

## Dependency Rules

- `stg` candidates usually depend on `none`.
- `int` candidates must depend on all upstream `stg` or `int` candidates they
  require.
- `mart` candidates must depend on all upstream `stg`, `int`, or `mart`
  candidates they require.
- If a higher-layer candidate can still run without a proposed upstream change,
  explain that in the candidate notes and keep dependencies minimal.

## Approval Rules

Preselect approval with `- [x] Approve: yes` only when all are true:

- the candidate is narrow and mechanically safe;
- the source and downstream usage evidence is clear;
- validation scope is explicit; and
- applying it does not require business interpretation.

Use `- [ ] Approve: no` when confidence is lower, when the candidate is broad,
or when user review is required before model changes.

Never auto-approve candidates when the evidence comes from dynamic SQL recovery,
string-built SQL, partial parsing, or inferred source names. Leave those
candidates unapproved even when the recovered transformation looks simple.
Never auto-approve column renames that carry business meaning, such as source
codes mapped into alternate-key fields, unless an existing reviewed catalog or
dbt model already proves the mapping.

## Plan Writing

Write exactly one markdown plan to the path supplied by the command. The plan
must include:

- title and target list;
- assumptions and non-goals;
- candidate summary counts by type;
- one level-2 section per candidate using `references/plan-file-contract.md`;
- rejected or deferred candidates when useful;
- execution order that runs staging first, then higher-layer candidates; and
- a statement that no dbt models were mutated.

Candidate sections must use `## Candidate: <ID>` exactly. Do not put candidates
under a `## Candidates` wrapper or use `### Candidate` headings. Candidate
sections must include approval, type, output, dependencies, validation, and
execution status. Use `Execution status: planned` for every new candidate.

## Output

Return a concise summary with:

- the plan path;
- candidate counts by `stg`, `int`, and `mart`;
- approved versus review-required counts; and
- the next command to run after review:
  `/refactor-mart <plan-file> stg`.
