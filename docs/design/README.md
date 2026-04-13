# Design Docs

Use `docs/design/` only for durable agent-facing design context that is not clear from code, tests, `repo-map.json`, or the skill/reference files under `plugin/skills/`.

Keep out of this directory:

- command inventories and file-layout walkthroughs
- scenario lists, coverage matrices, and current test/package counts
- duplicated skill contracts or prompt references
- design proposals that are no longer implemented

If information is mainly operational or contributor-facing, put it in `docs/reference/` instead.

## Design Index

- [Skill Contract](skill-contract/README.md) — per-table processing rules for each pipeline stage (scoping, profiler, test-generator, test-reviewer, model-generator, code-reviewer)
- [T-SQL Parse Classification](tsql-parse-classification/README.md) — exhaustive pattern list, deterministic (sqlglot) vs Claude-assisted routing
- [CLI Design](cli-design/README.md) — framework choice, registered commands, I/O contract, exit codes, and testability pattern
- [DB Operations API](db-operations-api/README.md) — manifest runtime contract, adapter interfaces, orchestration boundaries, and MigrationTest fixture rules
- [Eval Harness](eval-harness/README.md) — Promptfoo-based non-interactive testing for agents and skills against the MigrationTest schema
- [Command Design](command-design/README.md) — plugin command lifecycle, sub-agent spawning, run log structure, and relationship to skills
- [Coverage Matrix](coverage-matrix/README.md) — statement-by-statement coverage by phase and by test layer
- [Catalog Enrichment Diagnostics](catalog-enrich/README.md) — exhaustive warning/error scenarios for view, function, and procedure catalog entries

When adding one, use `docs/design/<topic>/README.md` and record only the durable decision and the reason it matters to agents.
