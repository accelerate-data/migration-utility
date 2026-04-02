# Scoping Coverage

Current statement-family coverage for the scoping phase.

Scoping includes writer discovery, candidate analysis, routing outcomes, and persisted scoping results.

| Statement family / outcome | Unit | Integration | Promptfoo | Notes |
|---|---|---|---|---|
| Direct deterministic writer (`INSERT...SELECT`, `MERGE`, `TRUNCATE + INSERT`) | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py), [`tests/unit/test_discover.py`](../../../tests/unit/test_discover.py) |  | [`tests/evals/packages/scoping/skill-analyzing-object.yaml`](../../../tests/evals/packages/scoping/skill-analyzing-object.yaml) | Promptfoo coverage exists for selected direct-writer outcomes, not the full deterministic family. |
| CTE-driven deterministic writer | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py) |  |  | No current scoping-specific Promptfoo evidence found. |
| Join and subquery deterministic writer | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py) |  |  | No current scoping-specific Promptfoo evidence found. |
| Static EXEC call graph / transitive writer | [`tests/unit/test_catalog_enrich.py`](../../../tests/unit/test_catalog_enrich.py), [`tests/unit/test_discover.py`](../../../tests/unit/test_discover.py) |  | [`tests/evals/packages/scoping/skill-analyzing-object.yaml`](../../../tests/evals/packages/scoping/skill-analyzing-object.yaml) | Promptfoo coverage exists for selected call-graph scenarios. |
| Dynamic SQL only / no static writer | [`tests/unit/test_catalog.py`](../../../tests/unit/test_catalog.py), [`tests/unit/test_discover.py`](../../../tests/unit/test_discover.py), [`tests/unit/test_catalog_enrich.py`](../../../tests/unit/test_catalog_enrich.py) |  | [`tests/evals/packages/scoping/skill-analyzing-object.yaml`](../../../tests/evals/packages/scoping/skill-analyzing-object.yaml) | Includes dynamic `EXEC` and current `sp_executesql @sql` limitation path. |
| Control flow (`IF`, `TRY/CATCH`, `WHILE`, nested`) | [`tests/unit/test_catalog.py`](../../../tests/unit/test_catalog.py), [`tests/unit/test_discover.py`](../../../tests/unit/test_discover.py), [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py) |  |  | Unit coverage exists for routing and extraction limits. No current scoping Promptfoo evidence found for these outcome shapes. |
| Ambiguous multi-writer |  |  | [`tests/evals/packages/scoping/skill-analyzing-object.yaml`](../../../tests/evals/packages/scoping/skill-analyzing-object.yaml) | Promptfoo-only evidence today. |
| No-writer-found |  |  | [`tests/evals/packages/scoping/skill-analyzing-object.yaml`](../../../tests/evals/packages/scoping/skill-analyzing-object.yaml) | Promptfoo-only evidence today. |
| Partial / blocked scoping outcome |  |  | [`tests/evals/packages/scoping/skill-analyzing-object.yaml`](../../../tests/evals/packages/scoping/skill-analyzing-object.yaml) | Promptfoo-only evidence today. |
| View-backed writer outcome |  |  | [`tests/evals/packages/scoping/skill-analyzing-object.yaml`](../../../tests/evals/packages/scoping/skill-analyzing-object.yaml) | Promptfoo-only evidence today. |
| Cross-database or linked-server outcome | [`tests/unit/test_catalog.py`](../../../tests/unit/test_catalog.py) |  | [`tests/evals/packages/scoping/skill-analyzing-object.yaml`](../../../tests/evals/packages/scoping/skill-analyzing-object.yaml) | Unit evidence is DMF/out-of-scope handling; Promptfoo includes a cross-db scoping scenario. |
| Command workflow (`/scope`) guards and aggregation |  |  |  | No current direct evidence found. |
