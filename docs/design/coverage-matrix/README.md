# Coverage Matrix

Recommended automated coverage across two dimensions:

1. statement coverage against the T-SQL parse-classification design
2. phase coverage across the migration pipeline

This document defines the recommended coverage shape, not the behavior itself. Behavior remains defined by the design docs and the tests themselves.

## Status Legend

| Status | Meaning |
|---|---|
| Covered | The recommended automated coverage exists. |
| Gap | The recommended automated coverage does not yet exist. |
| Intentional limitation | Current behavior is documented as a limitation and should be treated explicitly in tests. |

## Coverage Types

| Type | Meaning |
|---|---|
| Unit | Python tests over fixture DDL, direct library calls, or isolated helpers. |
| Integration | Docker SQL Server-backed tests against live SQL Server behavior. |
| Eval | Promptfoo-based non-interactive skill coverage. |

## Coverage Ownership

Recommended ownership by test type:

- Unit tests own full statement-family coverage at the parser and classification layer.
- Integration tests own live-system boundaries only: DMF behavior, enrichment seams, sandbox execution, and cross-process wiring.
- Promptfoo evals own LLM behavior coverage.

Recommended ownership by workflow phase:

- Scoping eval coverage is scenario-first: one Promptfoo scenario per distinct outcome shape is sufficient.
- Profiling eval coverage is statement-family-first: Promptfoo should cover the full set of statement families that materially change profiling reasoning.
- Test generation and test review eval coverage are statement-family-first.
- Model generation and model review eval coverage are statement-family-first.
- Command eval coverage is phase-first: guards, aggregation, summaries, and result handling.

## Statement Coverage

The statement matrix below maps the scenarios documented in [T-SQL Parse Classification](../tsql-parse-classification/README.md) to current automated coverage.

Recommended ownership:

- Statement breadth belongs in unit tests.
- Integration tests should cover live SQL Server boundaries, DMF-dependent behavior, enrichment seams, and cross-process wiring.
- Promptfoo evals are only needed when a statement family changes LLM routing, reasoning, or output shape.

### Deterministic Patterns

| Pattern(s) | Fixture(s) | Status | Recommended coverage type | Evidence |
|---|---|---|---|---|
| 1, 6 | `usp_LoadDimProduct` | Covered | Unit | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py) |
| 2 | `usp_SimpleUpdate` | Covered | Unit | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py) |
| 3 | `usp_SimpleDelete` | Covered | Unit | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py) |
| 4 | `usp_DeleteTop` | Covered | Unit | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py) |
| 5 | `usp_TruncateOnly` | Covered | Unit | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py), [`tests/unit/test_discover.py`](../../../tests/unit/test_discover.py) |
| 7 | `usp_MergeDimProduct` | Covered | Unit | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py) |
| 8 | `usp_SelectInto` | Covered | Unit | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py), [`tests/unit/test_catalog_enrich.py`](../../../tests/unit/test_catalog_enrich.py) |
| 9 | `usp_LoadWithCTE` | Covered | Unit | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py) |
| 10 | `usp_LoadWithMultiCTE` | Covered | Unit | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py) |
| 11 | `usp_SequentialWith` | Covered | Unit | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py) |
| 12 | `usp_LoadWithCase` | Covered | Unit | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py) |
| 13 | `usp_LoadWithLeftJoin` | Gap | Unit | Listed in [`tests/unit/test_discover.py`](../../../tests/unit/test_discover.py), but no direct ref/assertion coverage was found. |
| 14 | `usp_RightOuterJoin` | Covered | Unit | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py) |
| 15 | `usp_SubqueryInWhere` | Covered | Unit | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py) |
| 16 | `usp_CorrelatedSubquery` | Covered | Unit | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py) |
| 17 | `usp_WindowFunction` | Covered | Unit | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py) |
| 19 | `usp_UnionAll` | Gap | - | Fixture exists in [`tests/unit/fixtures/discover/flat/ddl/procedures.sql`](../../../tests/unit/fixtures/discover/flat/ddl/procedures.sql), but no direct automated assertions were found. |
| 20 | `usp_Union` | Gap | - | Same as above. |
| 21 | `usp_Intersect` | Gap | - | Same as above. |
| 22 | `usp_Except` | Gap | - | Same as above. |
| 23 | `usp_UnionAllInCTE` | Gap | - | Same as above. |
| 24 | `usp_InnerJoin` | Gap | - | Same as above. |
| 25 | `usp_FullOuterJoin` | Gap | - | Same as above. |
| 26 | `usp_CrossJoin` | Gap | - | Same as above. |
| 27 | `usp_CrossApply` | Gap | - | Same as above. |
| 28 | `usp_OuterApply` | Gap | - | Same as above. |
| 29 | `usp_SelfJoin` | Gap | - | Same as above. |
| 30 | `usp_DerivedTable` | Gap | - | Same as above. |
| 31 | `usp_ScalarSubquery` | Gap | - | Same as above. |
| 32 | `usp_ExistsSubquery` | Gap | - | Same as above. |
| 33 | `usp_NotExistsSubquery` | Gap | - | Same as above. |
| 34 | `usp_InSubquery` | Gap | - | Same as above. |
| 35 | `usp_NotInSubquery` | Gap | - | Same as above. |
| 36 | `usp_RecursiveCTE` | Gap | - | Same as above. |
| 37 | `usp_UpdateWithCTE` | Gap | - | Same as above. |
| 38 | `usp_DeleteWithCTE` | Gap | - | Same as above. |
| 39 | `usp_MergeWithCTE` | Gap | - | Same as above. |
| 40 | `usp_GroupingSets` | Gap | - | Same as above. |
| 41 | `usp_Cube` | Gap | - | Same as above. |
| 42 | `usp_Rollup` | Gap | - | Same as above. |
| 43 | `usp_Pivot` | Gap | - | Same as above. |
| 44 | `usp_Unpivot` | Gap | - | Same as above. |

### Skip-only Statements

| Statement type | Status | Recommended coverage type | Evidence |
|---|---|---|---|
| `TRUNCATE` split behavior | Covered | Unit | [`tests/unit/test_discover.py`](../../../tests/unit/test_discover.py), [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py) |
| `DROP/CREATE INDEX` skip behavior | Covered | Unit | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py) |
| `SET` | Gap | Unit | Empty-proc and harness-adjacent cases exist, but no direct skip classification assertion was found. |
| `DECLARE` | Gap | Unit | Appears in fixture procedures, but no direct skip classification assertion was found. |
| `RETURN` | Gap | - | No direct automated assertion was found. |
| `PRINT` | Gap | - | No direct automated assertion was found. |
| `RAISERROR` | Gap | - | No direct automated assertion was found. |
| `THROW` | Gap | - | No direct automated assertion was found. |
| `BEGIN/COMMIT/ROLLBACK` | Gap | - | No direct automated assertion was found. |

### Enrichment-resolved Patterns

| Pattern(s) | Fixture(s) | Status | Recommended coverage type | Evidence |
|---|---|---|---|---|
| 49, 53 | `usp_ExecSimple` | Covered | Unit | [`tests/unit/test_discover.py`](../../../tests/unit/test_discover.py), [`tests/unit/test_catalog_enrich.py`](../../../tests/unit/test_catalog_enrich.py) |
| 50 | `usp_ExecBracketed` | Gap | Unit | Routing fixture exists and catalog fixture is present, but no direct automated assertion was found. |
| 51, 52 | `usp_ExecWithParams` | Gap | Unit | Routing fixture exists and catalog fixture is present, but no direct automated assertion was found. |
| 54 | `usp_ExecWithReturn` | Gap | Unit | Routing fixture exists and catalog fixture is present, but no direct automated assertion was found. |
| 57 static `sp_executesql` | `usp_ExecSpExecutesql` | Gap | Unit | Routing assertions exist in [`tests/unit/test_catalog.py`](../../../tests/unit/test_catalog.py), but no end-to-end deterministic `discover show` or enrichment assertion was found. |

### Claude-assisted and Out-of-scope Patterns

| Pattern(s) | Fixture(s) | Status | Recommended coverage type | Evidence |
|---|---|---|---|---|
| 45 | `usp_ConditionalMerge` | Covered | Unit | [`tests/unit/test_discover.py`](../../../tests/unit/test_discover.py), [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py) |
| 46 | `usp_TryCatchLoad` | Covered | Unit | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py), documented xfail for extraction limits |
| 47 | `usp_WhileLoop` | Covered | Unit | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py), documented xfail for extraction limits |
| 48 | `usp_NestedControlFlow` | Covered | Unit | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py), documented xfail for extraction limits |
| 55 cross-db `EXEC` | none in current fixture set | Gap | - | Cross-database reference handling exists in DMF-processing tests, but the documented `EXEC OtherDB.dbo.usp_Load` scenario does not have direct automated coverage. |
| 56 linked-server `EXEC` | none in current fixture set | Gap | - | No direct automated coverage was found. |
| 58 dynamic `sp_executesql @sql` | `usp_ExecSpExecutesql` | Intentional limitation | Unit | [`tests/unit/test_catalog.py`](../../../tests/unit/test_catalog.py) covers the current routing gap; [T-SQL Parse Classification](../tsql-parse-classification/README.md) documents it as a known limitation. |
| 59, 60 | `usp_ExecDynamic` | Covered | Unit | [`tests/unit/test_discover.py`](../../../tests/unit/test_discover.py), [`tests/unit/test_catalog.py`](../../../tests/unit/test_catalog.py), [`tests/unit/test_catalog_enrich.py`](../../../tests/unit/test_catalog_enrich.py) |

### Statement-level Integration Coverage

| Area | Status | Recommended coverage type | Evidence | Main gap |
|---|---|---|---|
| Docker SQL Server-backed statement classification boundaries | Gap | Integration | Current integration marker coverage is concentrated in [`tests/unit/test_test_harness_integration.py`](../../../tests/unit/test_test_harness_integration.py). | No SQL Server-backed tests currently assert `setup-ddl`, `catalog_enrich`, or `discover show` behavior at the live-system boundaries. |

## Phase Coverage

The phase matrix below maps the major migration phases to their recommended automated coverage.

Recommended ownership:

- Scoping should have targeted Promptfoo scenarios for each distinct outcome shape.
- Profiling, test generation/review, and model generation/review should have Promptfoo coverage across the full statement-family set that materially changes LLM behavior.
- Each phase should still have unit coverage for deterministic contracts and write-back behavior where applicable.
- Integration coverage should be limited to boundary seams that depend on live SQL Server, sandbox execution, or cross-process behavior.

| Phase | Status | Recommended coverage type | Evidence | Main gaps |
|---|---|---|---|---|
| Scoping | Gap | Representative unit, selected eval, selected integration | [`docs/design/eval-harness/README.md`](../eval-harness/README.md), [`tests/evals/packages/scoping/skill-analyzing-object.yaml`](../../../tests/evals/packages/scoping/skill-analyzing-object.yaml) | Eval coverage exists for representative scenarios, but there is no durable audit tying statement classes to scoping outcomes and no focused Python suite for the recommended representative buckets. |
| Profiling | Gap | Representative unit, selected eval | [`tests/unit/test_profile.py`](../../../tests/unit/test_profile.py), [`tests/evals/packages/profiler/skill-profiling-table.yaml`](../../../tests/evals/packages/profiler/skill-profiling-table.yaml) | Rich context and write-back are covered, but representative coverage by statement-class bucket is not explicitly mapped. |
| Model generation | Gap | Representative unit, selected eval | [`tests/unit/test_migrate.py`](../../../tests/unit/test_migrate.py), [`tests/evals/packages/model-generator/skill-generating-model.yaml`](../../../tests/evals/packages/model-generator/skill-generating-model.yaml) | Current tests cover context assembly and artifact writing, but not a representative matrix across deterministic, enrichment-resolved, and Claude-assisted inputs. |
| Ground-truth generation | Gap | Representative unit, selected integration | [`tests/unit/test_test_harness.py`](../../../tests/unit/test_test_harness.py), [`tests/unit/test_test_harness_integration.py`](../../../tests/unit/test_test_harness_integration.py) | Ground-truth capture is covered at the sandbox/harness layer, but there is no automated phase coverage connecting classified migration inputs to generated scenarios and captured outputs. |
| Test generation | Gap | - | The design contract exists in [`docs/design/skill-contract/test-generator.md`](../skill-contract/test-generator.md). | No automated phase suite was found for `/generating-tests`, and `docs/requirements/decisions.md` currently describes the phase as not yet implemented. |
| Code review gate | Gap | - | The design contract exists in [`docs/design/skill-contract/code-reviewer.md`](../skill-contract/code-reviewer.md). | No automated phase suite was found. |

## Eval Coverage

The eval matrix below captures the recommended Promptfoo ownership model.

| Phase | Eval strategy | Status | Evidence | Main gaps |
|---|---|---|---|---|
| Scoping | Targeted scenario coverage by outcome shape | Gap | [`tests/evals/packages/scoping/skill-analyzing-object.yaml`](../../../tests/evals/packages/scoping/skill-analyzing-object.yaml) | Existing scenarios cover several outcomes, but the matrix is not yet explicit and command-level scoping coverage is absent. |
| Profiling | Full statement-family coverage | Gap | [`tests/evals/packages/profiler/skill-profiling-table.yaml`](../../../tests/evals/packages/profiler/skill-profiling-table.yaml) | Current profiler evals are few and do not cover the full statement-family set that changes LLM reasoning. |
| Test generation | Full statement-family coverage | Gap | [`docs/design/skill-contract/test-generator.md`](../skill-contract/test-generator.md) | No Promptfoo package exists yet. |
| Test review | Full statement-family coverage | Gap | [`docs/design/skill-contract/test-reviewer.md`](../skill-contract/test-reviewer.md) | No Promptfoo package exists yet. |
| Model generation | Full statement-family coverage | Gap | [`tests/evals/packages/model-generator/skill-generating-model.yaml`](../../../tests/evals/packages/model-generator/skill-generating-model.yaml) | Current model-generator evals are too narrow for full statement-family coverage. |
| Model review | Full statement-family coverage | Gap | [`docs/design/skill-contract/code-reviewer.md`](../skill-contract/code-reviewer.md) | No Promptfoo package exists yet. |
| Commands (`scope`, `profile`, `generate-model`) | Targeted phase and summary coverage | Gap | [`docs/design/command-design/README.md`](../command-design/README.md) | No command-level Promptfoo coverage was found. |

## Known Limitations vs Accidental Gaps

### Intentional known limitations

| Area | Evidence |
|---|---|
| Dynamic `sp_executesql @sql` routing gap | [T-SQL Parse Classification](../tsql-parse-classification/README.md) documents the limitation; [`tests/unit/test_catalog.py`](../../../tests/unit/test_catalog.py) locks current routing behavior. |
| sqlglot extraction inside `TRY/CATCH`, `WHILE`, and nested control flow | [`tests/unit/test_extract_refs.py`](../../../tests/unit/test_extract_refs.py) marks these with `xfail`, and [T-SQL Parse Classification](../tsql-parse-classification/README.md) documents the Claude-assisted path. |

### Gap tags

| Area | Gap |
|---|---|
| Deterministic patterns | Most documented patterns from 19 onward have fixtures but no direct automated assertions. |
| Static EXEC variants | Bracketed, params, output, and return-value EXEC variants are documented but not directly asserted. |
| SQL Server-backed statement integration | No integration suite currently ties live SQL Server extraction and enrichment behavior back to the statement matrix. |
| Phase-by-classification audit | Existing phase tests are not mapped to representative deterministic, enrichment-resolved, and Claude-assisted cases. |
| Test generation phase | No automated suite currently validates the phase described in the design contract. |
| Promptfoo ownership model | Current eval docs and issue scope do not yet clearly separate scenario-first scoping coverage from statement-family-first downstream eval coverage. |

## How To Maintain This Matrix

- Update this document when new statement-pattern tests, new phase suites, or new intentional limitations are added.
- Prefer file-level evidence links over prose summaries.
- When a gap is closed, replace the gap note with the specific automated coverage that proves it.
