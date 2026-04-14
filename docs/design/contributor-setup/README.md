# Contributor Setup

Define contributor onboarding around one human entrypoint and one agent-executable repo script.

## Decisions

- `README.md` is the canonical contributor onboarding path for developing `migration-utility`.
- Contributor onboarding targets macOS and Unix-like environments only. Windows is rejected by the repo bootstrap flow.
- Contributor setup for this repo stays separate from customer or migration-project setup flows.
- The repo provides one agent-facing script in `scripts/` as the deterministic contributor readiness/bootstrap path.
- The script owns repo-local bootstrap and readiness checks. It does not install machine-level tools.
- Default script behavior is mutating `fix`; `show` is the non-mutating status mode.
- The script emits human-readable progress plus trailing JSON so a coding agent can resume deterministically after partial failure.

## Script Contract

The contributor bootstrap script:

- detects platform support
- checks required and optional machine-level tools
- classifies missing prerequisites as agent-fixable, manual action, or blocked
- bootstraps repo-local environments and eval dependencies
- verifies Docker installation and daemon readiness
- verifies required contributor containers can start or are healthy
- verifies both SQL Server and Oracle maintainer paths individually
- reports overall readiness as `ready`, `partially_ready`, or `blocked`

Maintainer readiness requires:

- Docker working end to end
- repo-local bootstrap succeeding
- at least one of the SQL Server or Oracle maintainer paths working end to end

## Documentation Ownership

- `README.md` explains contributor setup for humans on a fresh laptop, including required vs optional tools, supported platforms, script usage, and readiness meanings.
- Existing detailed setup docs remain subordinate references and are linked from `README.md` instead of acting as competing onboarding paths.

## Why This Matters

Contributors currently have to assemble maintainer setup from multiple documents, and agents lack a single executable contract for repo bootstrap. A repo-owned script plus a single canonical `README.md` path gives both humans and agents one stable onboarding flow.
