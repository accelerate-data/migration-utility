# Coverage Matrix

Source of truth for current automated coverage across:

1. statement families
2. workflow phases
3. test harnesses

Behavior remains defined by the design docs and the tests themselves. This topic records where coverage currently exists.

## Coverage Types

| Type | Meaning |
|---|---|
| Unit | Python tests over fixture DDL, direct library calls, or isolated helpers. |
| Integration | Docker SQL Server-backed tests against live SQL Server behavior. |
| Promptfoo | Non-interactive LLM eval coverage via Promptfoo. |

## Reading the Tables

- Each phase gets its own coverage table so statement-family coverage stays readable.
- Each cell should contain direct evidence links when coverage exists.
- Empty cells mean no current evidence was found for that harness in that phase.
- Notes call out intentional limitations or important scope boundaries.

## Phase Docs

- [Scoping Coverage](scoping.md) — statement-family coverage for writer discovery, routing, and scoping outcomes
- [Later LLM-heavy Phase Coverage](later-llm-phases.md) — statement-family coverage for profiling, test generation/review, model generation/review, and related downstream reasoning

## Shared Boundaries

Some coverage is phase-agnostic and should still be tracked as shared boundaries:

| Area | Unit | Integration | Promptfoo | Notes |
|---|---|---|---|---|
| Parser and classification layer | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py), [`tests/unit/test_discover.py`](../../../tests/unit/test_discover.py), [`tests/unit/test_catalog.py`](../../../tests/unit/test_catalog.py), [`tests/unit/test_catalog_enrich.py`](../../../tests/unit/test_catalog_enrich.py) |  |  | Primary source of deterministic statement coverage and routing behavior. |
| Live SQL Server statement boundaries |  | [`tests/unit/test_test_harness_integration.py`](../../../tests/unit/test_test_harness_integration.py) |  | Current integration coverage is mostly harness-oriented; direct `setup-ddl` / `catalog_enrich` / `discover show` boundary coverage is still sparse. |

## Intentional Limitations

| Area | Evidence |
|---|---|
| Dynamic `sp_executesql @sql` routing gap | [T-SQL Parse Classification](../tsql-parse-classification/README.md), [`tests/unit/test_catalog.py`](../../../tests/unit/test_catalog.py) |
| sqlglot extraction inside `TRY/CATCH`, `WHILE`, and nested control flow | [T-SQL Parse Classification](../tsql-parse-classification/README.md), [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py) |

## Maintenance

- Update the phase tables when new tests land.
- Prefer direct file links over prose summaries.
- If a test covers only part of a statement family, note the scope in the phase doc rather than overstating coverage.
