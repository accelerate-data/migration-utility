# Setup-Target Staging Contracts

## Decision

`setup-target` generates contracted source-facing staging wrappers, full-shape passthrough unit tests, and validates the generated staging layer with dbt compile and dbt build.

## Reason

Generated silver and mart models depend on source-facing staging wrappers. The staging layer must fail early when the physical source shape drifts, before downstream model generation or refactor work reports success.

## Generated Staging Boundary

Each generated `stg_bronze__*` wrapper is a one-to-one pass-through over one dbt source. Its contract covers the full emitted source table shape using target-normalized catalog data types. The source-native type remains catalog metadata; the staging contract uses the target-adapter-compatible type.

Staging contracts read only catalog column `sql_type`. Missing `sql_type` is a setup-target error because generated contracts must not fall back to source-native or legacy type fields.

Contracts are enforced on staging models. Source-level data tests remain on source YAML where catalog metadata supports them.

## Unit Tests

Each generated staging model gets a baseline dbt unit test that feeds one full-shape source row and expects the same full-shape row from the staging model. The test covers every emitted column so accidental projection, rename, filter, or transformation changes fail.

Unit tests are rendered through the same shared dbt YAML writer used by migrated-model test rendering. The canonical output shape is top-level `unit_tests:` in a model-path YAML file, not `unit_tests` nested under a model entry.

The unit test proves passthrough behavior. Data tests prove source quality. The contract proves the build-time shape/type boundary.

## Data Tests

`setup-target` continues to emit catalog-derived source data tests in `_staging__sources.yml`.

Target source tables are created before dbt validation. They are empty at setup time, so generated source data tests such as `not_null`, `unique`, and `relationships` can run without requiring source data to be loaded.

Source freshness is not part of setup-target validation. Freshness metadata may be generated, but running freshness checks remains a separate dbt operation.

## Validation

After generating target source tables and staging artifacts, `setup-target` runs dbt compile for the generated setup-target scope and then dbt build for that scope. Compile catches malformed SQL/YAML/Jinja. Build enforces staging contracts, runs staging unit tests, and runs generated source data tests.

The dbt selector is derived from the artifacts generated during the current setup-target run. It does not select every file under `models/staging`.

## Reruns

Before downstream mart models exist, rerunning `setup-target` deletes setup-target-owned source/staging dbt artifacts and regenerates them from current catalog and target settings.

The mart-exists check runs before setup-target mutates `manifest.json` or dbt artifacts. It reuses the existing generated-model/status signal. `setup-target` does not diff generated files or infer ownership from the filesystem.

After downstream mart models exist, rerunning `setup-target` fails fast. The failure points users to the preserve-catalog reset flow so generated target/dbt state can be cleared without losing extracted catalog scope/profile work.

## Ownership Boundary

`setup-target` owns generated source YAML, generated staging model YAML, and generated `stg_bronze__*` wrappers. It does not merge regenerated staging/source artifacts with user-authored downstream models.
