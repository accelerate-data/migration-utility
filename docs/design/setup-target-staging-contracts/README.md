# Setup-Target Staging Contracts

## Decision

`setup-target` generates contracted source-facing staging wrappers, full-shape passthrough unit tests, and validates the generated staging layer with dbt compile and dbt build.

## Reason

Generated silver and mart models depend on source-facing staging wrappers. The staging layer must fail early when the physical source shape drifts, before downstream model generation or refactor work reports success.

## Generated Staging Boundary

Each generated `stg_bronze__*` wrapper is a one-to-one pass-through over one dbt source. Its contract covers the full emitted source table shape using target-normalized catalog data types. The source-native type remains catalog metadata; the staging contract uses the target-adapter-compatible type.

Contracts are enforced on staging models. Source-level data tests remain on source YAML where catalog metadata supports them.

## Unit Tests

Each generated staging model gets a baseline dbt unit test that feeds one full-shape source row and expects the same full-shape row from the staging model. The test covers every emitted column so accidental projection, rename, filter, or transformation changes fail.

The unit test proves passthrough behavior. Data tests prove live source quality. The contract proves the build-time shape/type boundary.

## Validation

After generating staging artifacts, `setup-target` runs dbt compile for the generated staging scope and then dbt build for that scope. Compile catches malformed SQL/YAML/Jinja. Build enforces contracts, runs staging unit tests, and runs selected data tests according to dbt selection.

## Reruns

Before downstream mart models exist, rerunning `setup-target` deletes setup-target-owned source/staging dbt artifacts and regenerates them from current catalog and target settings.

The mart-exists check reuses the existing generated-model/status signal. `setup-target` does not diff generated files or infer ownership from the filesystem.

After downstream mart models exist, rerunning `setup-target` fails fast. The failure points users to the preserve-catalog reset flow so generated target/dbt state can be cleared without losing extracted catalog scope/profile work.

## Ownership Boundary

`setup-target` owns generated source YAML, generated staging model YAML, and generated `stg_bronze__*` wrappers. It does not merge regenerated staging/source artifacts with user-authored downstream models.
