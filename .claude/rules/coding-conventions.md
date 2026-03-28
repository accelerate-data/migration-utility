# Coding Conventions

This is the canonical source for naming, markdown, and error-handling conventions.

## Python (Orchestrator + Agents)

- Files: `snake_case` (`candidacy_agent.py`, `plan_parser.py`)
- Classes: `PascalCase` (`CandidacyAgent`, `PlanState`)
- Functions/variables: `snake_case` (`get_stored_procs`, `artifact_tier`)
- Constants: `UPPER_SNAKE_CASE` (`MAX_PARALLEL_AGENTS`, `PLAN_FILE`)
- Type annotations required on all function signatures
- Use `pathlib.Path`, not `os.path`
- Never use bare `except:` — always catch specific exception types

## TypeScript (Tauri Frontend)

- Files: `kebab-case` (`scope-selector.tsx`, `candidacy-table.tsx`)
- Components: `PascalCase` (`ScopeSelector`, `CandidacyTable`)
- Functions/variables: `camelCase` (`getStoredProcs`, `artifactTier`)
- Constants: `UPPER_SNAKE_CASE` (`MAX_TABLE_ROWS`)

## Rust (Tauri Backend)

- Follow standard Rust conventions (enforced by `clippy`)
- Every `#[tauri::command]` logs `info!` on entry (with key params) and `error!` on failure
- Use `thiserror` for error types; propagate with `?`

## Database Query Conventions

- Source-system SQL (SQL Server/Fabric discovery/import and all source-related queries) lives in `app/src-tauri/sql/source/**`, not inline in Rust logic.
- SQLite mutations must use bound parameters, not string-concatenated SQL.
- Schema/data changes must keep migrations and tests in sync.
- Usage/log snapshot tables should not use foreign keys to mutable entities; keep records as immutable point-in-time data unaffected by parent deletes.

## Logging

Canonical logging policy is in `.claude/rules/logging-policy.md`.

## Markdown

All `.md` files must pass `markdownlint` before committing. Config is at `.markdownlint.json`.

```bash
markdownlint <file-or-dir>
```

Never hard-wrap prose at 80 characters (or any fixed column). Let each sentence or clause flow as a single long line — editors and renderers handle wrapping. Hard wraps only inside code blocks or tables where alignment matters.

## Error Handling

- Validate at system boundaries: Fabric API responses, ADF JSON parsing, dbt-core-mcp tool results, plan.md reads
- Trust internal Agent SDK guarantees — don't wrap them
- Python: raise typed exceptions (`class CandidacyError(Exception): ...`), never swallow silently
- TypeScript: typed errors from Tauri commands, surface to user via error state
- Agent tool errors: log and mark the affected model `BLOCKED` in `plan.md` — don't crash the orchestrator
