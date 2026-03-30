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
- [Agent Contract](agent-contract/README.md) — structured agent output contracts and FDE review requirements
- [Setup DDL](setup-ddl/README.md) — step-by-step logic for DDL extraction, catalog signal queries, DMF reference extraction, and AST enrichment
- [SP → dbt Migration Plugin](sp-to-dbt-plugin/README.md) — skill architecture, per-skill contracts, and implementation wave plan
- [T-SQL Parse Classification](tsql-parse-classification/README.md) — exhaustive pattern list, deterministic (sqlglot) vs Claude-assisted routing
- [CLI Design](cli-design/README.md) — framework choice, registered commands, I/O contract, exit codes, and testability pattern

## What Goes In `references`

Use `docs/reference/` for supporting operational/reference material that is not a
design decision artifact.

See [Reference README](../reference/README.md).
