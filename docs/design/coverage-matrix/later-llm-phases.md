# Later LLM-heavy Phase Coverage

Current statement-family coverage for later LLM-heavy phases:

- profiling
- ground-truth generation
- test generation and review
- model generation and review
- command workflows that orchestrate those phases

| Statement family / downstream concern | Unit | Integration | Promptfoo | Notes |
|---|---|---|---|---|
| Direct insert/select and table-load reasoning | [`tests/unit/test_profile.py`](../../../tests/unit/test_profile.py), [`tests/unit/test_migrate.py`](../../../tests/unit/test_migrate.py) |  | [`tests/evals/packages/profiler/skill-profiling-table.yaml`](../../../tests/evals/packages/profiler/skill-profiling-table.yaml), [`tests/evals/packages/model-generator/skill-generating-model.yaml`](../../../tests/evals/packages/model-generator/skill-generating-model.yaml) | Current Promptfoo evidence is selected-scenario coverage, not full downstream statement-family coverage. |
| MERGE-driven dimension reasoning | [`tests/unit/test_migrate.py`](../../../tests/unit/test_migrate.py) |  | [`tests/evals/packages/profiler/skill-profiling-table.yaml`](../../../tests/evals/packages/profiler/skill-profiling-table.yaml), [`tests/evals/packages/model-generator/skill-generating-model.yaml`](../../../tests/evals/packages/model-generator/skill-generating-model.yaml) | Present in selected profiler/model scenarios. |
| Full reload / `TRUNCATE + INSERT` reasoning | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py), [`tests/unit/test_migrate.py`](../../../tests/unit/test_migrate.py) |  | [`tests/evals/packages/profiler/skill-profiling-table.yaml`](../../../tests/evals/packages/model-generator/skill-generating-model.yaml) | Existing evidence is partial and scenario-specific. |
| `SELECT INTO` and table-creation side effects | [`tests/unit/test_catalog_enrich.py`](../../../tests/unit/test_catalog_enrich.py) |  |  | No current downstream Promptfoo evidence found. |
| Static EXEC / call-graph downstream reasoning | [`tests/unit/test_profile.py`](../../../tests/unit/test_profile.py), [`tests/unit/test_catalog_enrich.py`](../../../tests/unit/test_catalog_enrich.py) |  |  | Related-procedure context is covered in unit tests; no direct downstream Promptfoo evidence found. |
| Dynamic SQL and control-flow downstream reasoning |  |  |  | No current downstream phase evidence found beyond parser/routing-layer tests. |
| Advanced joins, subqueries, windowing, set ops | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py) |  |  | Current evidence is parser-layer only, not later-phase coverage. |
| SCD2 / snapshot downstream reasoning | [`tests/unit/test_migrate.py`](../../../tests/unit/test_migrate.py) |  |  | Current unit evidence is contract-level; no current Promptfoo scenario evidence found. |
| Ground-truth capture and sandbox execution | [`tests/unit/test_test_harness.py`](../../../tests/unit/test_test_harness.py) | [`tests/unit/test_test_harness_integration.py`](../../../tests/unit/test_test_harness_integration.py) |  | Covers harness behavior, not the full downstream statement-family set. |
| Test generation and review behavior |  |  |  | No current automated evidence found. |
| Model review behavior |  |  |  | No current automated evidence found. |
| Command workflows (`/profile`, `/generate-model`) |  |  |  | No current direct evidence found. |
