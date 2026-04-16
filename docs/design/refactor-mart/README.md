# Refactor Mart

`/refactor-mart` is a mart-driven refactor workflow with an analysis step and two execution waves. The workflow starts from selected marts, derives upstream `stg` and `int` opportunities, and uses a markdown plan file as the review and execution contract.

## Commands

- `/refactor-mart-plan <tables...>` analyzes one selected mart unit and writes a markdown plan file. It does not apply code changes.
- `/refactor-mart <plan-file> stg` applies approved staging candidates from the plan and validates the affected scope.
- `/refactor-mart <plan-file> int` applies approved higher-layer candidates from the plan and validates the affected scope.

The command surface is mart-oriented because the user selects final business outputs, even when the workflow creates or rewrites upstream `stg` and `int` models.

## Skills

- `planning-refactor-mart` analyzes the selected mart unit, derives candidates, and writes the markdown plan.
- `applying-staging-candidate` applies one approved staging candidate for a source table and rewires all affected downstream consumers.
- `applying-refactor-mart-candidate` applies one approved higher-layer candidate, including `int` extraction and the associated mart rewrites.

Validation is a required phase inside the apply skills rather than a separate top-level skill.

## Layer Rules

The workflow follows dbt layer boundaries.

- `stg` is row-preserving and source-conformed. Allowed work is renaming, type casting, basic row-level computations, and row-level categorization. No grain changes belong in `stg`.
- `int` owns non-staging reusable logic, including joins, aggregations, pivots, business-step transformations, and any logic that is not common row-preserving source normalization.
- `mart` remains the business-facing final model at the target grain.

If a transformation is valid and shared at the source-table level, it belongs in `stg`. If it is not valid staging, it becomes an `int` or mart candidate instead.

## Plan File

`/refactor-mart-plan` writes a markdown plan file under `docs/design/` and the workflow treats that file as the source of truth for approval, dependencies, and execution status.

The plan file is optimized for LLM interpretation rather than Python parsing:

- one candidate per section
- explicit labeled fields on their own lines
- checkbox-based approval
- stable candidate IDs
- dependency fields written as plain text
- execution status written in regular markdown, not a machine-only schema

Use a regular shape like this:

```md
# Refactor Mart Plan: <slug>

## Candidate: STG-001
- [x] Approve: yes
- Type: stg
- Output: models/staging/<source_system>/stg_<source_system>__<entity>.sql
- Scope: source table <name>
- Consumers: <consumer>, <consumer>
- Depends on: none
- Why: shared row-preserving renames, casts, computations, and categorizations
- Validation: <model>, <consumer>, <consumer>
- Execution status: planned
```

The model is the primary interpreter of this file. Python should be limited to file I/O and simple orchestration, not semantic parsing.

## Candidate Types

The plan may contain these candidate types:

- `stg` candidates, scoped to one source table, for shared or source-conformed row-preserving transformations
- `int` candidates, scoped to one reusable higher-layer transformation
- mart rewrite candidates when a final model needs an explicit rewrite separate from the `int` candidate that introduced the change

`stg` candidates are source-table scoped because one staging model may fan out to many downstream consumers.

## Approval Model

`/refactor-mart-plan` suggests candidate approvals by preselecting `[x]` where confidence is high, but the user remains the approver. The user edits the markdown plan directly.

- checked candidates are eligible for execution
- unchecked candidates are ignored
- the plan file is committed so approval and execution history are visible in git

## Dependency Model

Higher-layer candidates may depend on prior staging work or other higher-layer work. Each non-staging candidate must declare explicit dependencies using stable candidate IDs.

Example:

```md
## Candidate: INT-003
- [x] Approve: yes
- Type: int
- Depends on: STG-001, STG-002
```

`/refactor-mart <plan-file> int` must verify that every dependency for a checked candidate has already been applied successfully. If a required dependency is unchecked, failed, or not yet applied, the candidate is marked blocked and is not executed.

This prevents a user from selecting downstream work while deselecting required upstream `stg`.

## Execution Waves

### `stg` wave

`/refactor-mart <plan-file> stg`:

- reads approved `stg` candidates from the plan
- applies them one candidate at a time, potentially in parallel when candidates are independent
- updates downstream consumers to use the new staging models
- validates the changed staging model and every declared downstream consumer
- records applied, failed, or blocked status in the plan

### `int` wave

`/refactor-mart <plan-file> int`:

- reads approved non-staging candidates from the plan
- enforces dependency satisfaction before execution
- applies one candidate at a time, including the linked mart rewrites in that candidate scope
- validates the changed shared model and every rewritten mart in that candidate scope
- records applied, failed, or blocked status in the plan

`int` mode includes the mart rewrites needed to complete the higher-layer refactor. It is not useful to extract an `int` model without rewiring the dependent marts in the same scoped change.

## Validation

Validation is scoped to the candidate being applied.

- `stg` validation covers the changed `stg_*` model and every downstream consumer named in the candidate
- `int` validation covers the changed `int_*` model and every rewritten mart named in the candidate
- failures are isolated to the candidate scope and must not invalidate unrelated approved work

The workflow should report candidate-level success and failure in both command output and plan-file status updates.

Run dbt Project Evaluator after the changed candidate scope builds successfully. Treat evaluator findings as refactor review evidence: fix findings that align with the generated dbt standards, and document exceptions when a rule conflicts with migration fidelity or an explicit project standard.

## Reason For Markdown Plans

The plan is intentionally human-editable and model-readable. A Python-first plan format would add parser complexity around a document whose real consumer is the LLM.

Markdown is the durable contract because it supports:

- human review and direct checkbox approval
- clear candidate explanations and dependency notes
- model-friendly structured interpretation
- git-native history for plan changes and execution results
