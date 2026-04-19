# Design Docs

Use `docs/design/` only for durable agent-facing design context that is not clear from code, tests, `repo-map.json`, or the skill/reference files under `skills/`.

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
- [Init ad-migration Prereqs](init-ad-migration-prereqs/README.md) — separates MCP startup readiness from live connection readiness and assigns repo-wide vs machine-local env ownership
- [Homebrew CLI Publishing](homebrew-cli-publishing/README.md) — public CLI packaging split, Homebrew tap scope, and init-driven install contract for macOS
- [Reset Migration Global Mode](reset-migration-global/README.md) — full-project migration-state reset that preserves init scaffolding and requires destructive confirmation
- [Refactor Mart](refactor-mart/README.md) — mart-driven two-wave refactor plan, markdown candidate contract, dependency gating, and `stg`/`int` execution split
- [dbt Project Standards](dbt-project-standards/README.md) — generated dbt layer layout, bronze staging wrappers, marts placement, and skill-facing source/reference rules
- [Source YAML Catalog Enrichment](source-yaml-catalog-enrichment/README.md) — conservative source-column, test, relationship, and freshness generation from normalized table catalog metadata
- [Seed Table Catalog State](seed-table-catalog-state/README.md) — explicit catalog ownership for source-backed vs seed-backed writerless tables
- [Status Summary Contract](status-summary-contract/README.md) — summary `/status` dashboard scope, diagnostics table, and detailed-status boundary
- [Integration Test Contract](integration-test-contract/README.md) — source fixture schema rules for live integration tests and the target setup exception
- [Sandbox PDB Alignment](sandbox-pdb-alignment/README.md) — Oracle sandbox uses a PDB (not a user/schema) to match SQL Server's database-level isolation and enable multi-schema support
- [Selected Writer DDL Slice](selected-writer-ddl-slice/README.md) — LLM-facing contexts expose only target-specific SQL for sliced multi-table writers
- [Readiness Error Guard](readiness-error-guard/README.md) — object-scoped readiness fails on unresolved catalog errors while allowing warnings
- [Preserve-Catalog Reset](preserve-catalog-reset/README.md) — reset mode that clears generated target/dbt state while preserving extraction, scope, and profile work
- [Target-Normalized Catalog Types](target-normalized-catalog-types/README.md) — target-aware catalog type field used by target DDL, source/staging YAML, and dbt contracts
- [Setup-Target Staging Contracts](setup-target-staging-contracts/README.md) — setup-target creates contracted staging wrappers, full-shape passthrough unit tests, and validates the staging layer
- [Replicate Source Tables](replicate-source-tables/README.md) — deterministic CLI boundary for copying capped source data into target-side source tables before user-run dbt execution
- [dbt Generation Execution Policy](dbt-generation-execution-policy/README.md) — generate-model sub-agent orchestration, direct-parent empty materialization, and scoped dbt unit-test execution
- [Migrate Mart Coordinator](migrate-mart-coordinator/README.md) — whole-scope mart plan and coordinator workflow with deterministic worktrees, stage PRs, and crash recovery

When adding one, use `docs/design/<topic>/README.md` and record only the durable decision and the reason it matters to agents.
