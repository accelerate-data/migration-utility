# Design Docs

Each design topic gets its own subdirectory with a `README.md`.

## What Goes In `requirements`

Use `docs/requirements/` for discovery and planning artifacts:

- problem framing and scope assumptions
- research notes and evaluation artifacts
- build plans and decision logs

See [Requirements README](../requirements/README.md).

## What Goes In `design`

Use `docs/design/` for architecture and implementation design details.

### Design Index

- [Overall Design](overall-design/README.md) — end-to-end user flow, architecture decisions, and open issues
- [Test Scenario Design](test-scenario/README.md)
- [Skill Contract](skill-contract/README.md) — per-table processing rules for each pipeline stage (scoping, profiler, test-generator, test-reviewer, model-generator, code-reviewer)
- [Setup DDL](setup-ddl/README.md) — step-by-step logic for DDL extraction, catalog signal queries, DMF reference extraction, and AST enrichment
- [T-SQL Parse Classification](tsql-parse-classification/README.md) — exhaustive pattern list, deterministic (sqlglot) vs Claude-assisted routing
- [T-SQL Routing Fallback](tsql-routing-fallback/README.md) — staged routing, recursive control-flow segmentation, and narrower Claude escalation
- [CLI Design](cli-design/README.md) — framework choice, registered commands, I/O contract, exit codes, and testability pattern
- [Eval Harness](eval-harness/README.md) — Promptfoo-based non-interactive testing for agents and skills against the MigrationTest schema
- [Command Design](command-design/README.md) — plugin command lifecycle, sub-agent spawning, run log structure, and relationship to skills

## What Goes In `references`

Use `docs/reference/` for supporting operational/reference material that is not a
design decision artifact.

See [Reference README](../reference/README.md).
