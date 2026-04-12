# Design Docs

Each design topic gets its own subdirectory with a `README.md`.

## What Goes In `design`

Use `docs/design/` for architecture and implementation design details.

### Design Index

- [Skill Contract](skill-contract/README.md) — per-table processing rules for each pipeline stage (scoping, profiler, test-generator, test-reviewer, model-generator, code-reviewer)
- [T-SQL Parse Classification](tsql-parse-classification/README.md) — exhaustive pattern list, deterministic (sqlglot) vs Claude-assisted routing
- [CLI Design](cli-design/README.md) — framework choice, registered commands, I/O contract, exit codes, and testability pattern
- [Eval Harness](eval-harness/README.md) — Promptfoo-based non-interactive testing for agents and skills against the MigrationTest schema
- [Command Design](command-design/README.md) — plugin command lifecycle, sub-agent spawning, run log structure, and relationship to skills
- [Coverage Matrix](coverage-matrix/README.md) — statement-by-statement coverage by phase and by test layer
- [Catalog Enrichment Diagnostics](catalog-enrich/README.md) — exhaustive warning/error scenarios for view, function, and procedure catalog entries

## What Goes In `references`

Use `docs/reference/` for supporting operational/reference material that is not a
design decision artifact.

See [Reference README](../reference/README.md).
