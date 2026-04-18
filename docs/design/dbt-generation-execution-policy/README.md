# dbt Generation Execution Policy

## Decision

Model generation and mart-refactor application should validate generated dbt artifacts without running full dbt dependency builds.

Data-backed execution belongs to the operator after source tables have been replicated into the target.

## Generation Boundary

`/generating-model` owns artifact generation, schema YAML updates, and rendering canonical dbt `unit_tests:` from approved test specs.

It must not run `dbt build` or materialize the generated model as part of validation.

Generated artifacts are committed so users and agents can run normal dbt commands later.

## Generate-Model Orchestration

`/generate-model` should be a coordinator, not the place where long sub-agent
prompts live inline.

The command should launch one generation sub-agent per model. Each generation
sub-agent calls `/generating-model` for its assigned model and performs
compile validation only.

After generation completes, the command should launch one review sub-agent per
model. Each review sub-agent calls `/reviewing-model`, runs the bounded
review/fix loop for that model, and delegates any requested revision back
through `/generating-model`.

After reviewed artifacts are ready, the command should launch one unit-test
setup sub-agent for the whole model list. This agent resolves the direct source
and `ref()` parents needed by the selected dbt unit tests and materializes those
parents with empty relations.

After parent setup, the command should launch one unit-test repair sub-agent per
model. Each repair agent runs only the scoped dbt unit tests for that model and
uses the source procedure SQL or `selected_writer_ddl_slice` to fix generated
model errors.

Each sub-agent prompt should live in a referenced prompt file rather than being
embedded directly in `commands/generate-model.md`. The slash command should
read as orchestration: stage, prompt reference, inputs, expected artifact, and
failure semantics.

## Unit Tests

dbt unit tests do not need production-like replicated rows, but they still need
every referenced source or `ref()` input relation to exist in the target
warehouse.

Before running unit tests, the agent should create empty direct-parent relations
with:

```bash
dbt run --select "<direct_parents>" --empty
```

This prepares only the relations dbt needs for unit-test compilation and does
not build the generated model or the full upstream graph.

After direct parents exist, generation may run only dbt unit tests scoped to the
generated model.

The policy must not run broad schema/data tests as part of generation, because those tests depend on target data state and can pass vacuously or fail for environment reasons.

## Compile Validation

Generation should still run `dbt compile` scoped to the generated model.

Compile validates project structure, refs, macros, YAML shape, and model syntax without materializing the model.

## Mart Candidate Application

`applying-mart-candidates` follows the same execution boundary as generation.

It may compile the changed candidate scope, materialize direct parents with
`dbt run --empty`, and run dbt unit tests scoped to the changed candidate models
when unit tests exist.

It must not run `dbt build`, materialize the changed candidate model, or run
broad data/schema tests.

## Staging Candidate Application

Staging candidate application should not broaden into full target execution.

It should compile the changed staging model plus resolved consumers. Unit-test
execution is appropriate only when a scoped dbt unit test exists for the changed
scope; required direct parents should be prepared with `dbt run --empty`.

## Operator Execution

After `ad-migration replicate-source-tables`, users can run dbt directly:

```bash
cd dbt
dbt build --select +<model>
```

Claude Code may run the same dbt commands when asked, but those commands are explicit operator execution, not generation validation.
