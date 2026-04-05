# Coverage Matrix

Statement coverage is tracked by phase so gaps can be filed directly against the workflow stage that is missing coverage.

The phase documents in this directory are the source of truth for current automated coverage:

- [Statement Inventory](statement-inventory.md) — canonical list of statement patterns and classification buckets from the parse-classification design
- [Scoping Coverage](scoping.md)
- [Profiling Coverage](profiling.md)
- [Ground-Truth Generation Coverage](ground-truth-generation.md)
- [Test Generation Coverage](test-generation.md)
- [Test Review Coverage](test-review.md)
- [Refactoring Coverage](refactoring.md)
- [Model Generation Coverage](model-generation.md)
- [Code Review Coverage](code-review.md)

Rules:

- Rows are statement-by-statement, keyed to the pattern numbers in [T-SQL Parse Classification](../tsql-parse-classification/README.md).
- Cells are marked `Yes` only when the repo has explicit automated coverage for that statement in that phase.
- Cells are marked `Gap` when that layer should cover the statement in that phase but no explicit automated coverage exists yet.
- Cells are marked `N/A` when that layer is not the right place to cover the statement in that phase.
- The matrix is coverage-oriented, not evidence-oriented. It answers which statements are tested by which layer.
- `Unit`, `Integration`, and `Promptfoo` are separate because they cover different failure modes and should produce separate gap issues.
- `Promptfoo=Yes` means a promptfoo eval exercises this statement pattern in this phase's context — not necessarily that the LLM is invoked for that pattern specifically. For phases with mixed deterministic/LLM paths (such as scoping), a deterministic pattern can be marked `Yes` if it appears inside procedures that the LLM analyzes during that phase.
