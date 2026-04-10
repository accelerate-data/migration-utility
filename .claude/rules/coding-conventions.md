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

## TypeScript

- Files: `kebab-case` (`scope-selector.tsx`, `candidacy-table.tsx`)
- Components: `PascalCase` (`ScopeSelector`, `CandidacyTable`)
- Functions/variables: `camelCase` (`getStoredProcs`, `artifactTier`)
- Constants: `UPPER_SNAKE_CASE` (`MAX_TABLE_ROWS`)

## Logging

Canonical logging policy is in `.claude/rules/logging-policy.md`.

## Markdown

All `.md` files must pass `markdownlint` before committing. Config is at `.markdownlint.json`.

```bash
markdownlint <file-or-dir>
```

Never hard-wrap prose at 80 characters (or any fixed column). Let each sentence or clause flow as a single long line — editors and renderers handle wrapping. Hard wraps only inside code blocks or tables where alignment matters.

## Testing

- All `run_*` functions must return a Pydantic model instance (defined in `output_models.py` with `extra="forbid"`). The model enforces the output contract at construction time — no separate schema validation step needed.
- If no matching Pydantic output model exists for a new `run_*` function, create one in `output_models.py` before the test is merged

## Error Handling

- Validate at system boundaries: Fabric API responses, ADF JSON parsing, dbt-core-mcp tool results, plan.md reads
- Trust internal Agent SDK guarantees — don't wrap them
- Python: raise typed exceptions (`class CandidacyError(Exception): ...`), never swallow silently
- TypeScript: typed errors, surface to user via error state
- Agent tool errors: log and mark the affected model `BLOCKED` in `plan.md` — don't crash the orchestrator
