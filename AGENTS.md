# Migration Utility

Tauri desktop app + headless GitHub Actions pipeline that migrates Microsoft Fabric Warehouse stored procedures to dbt models on Vibedata's platform. Targets silver and gold transformations only (bronze is out of scope).

**Maintenance rule:** This file contains architecture, conventions, and guidelines — not product details. Do not add counts, feature descriptions, or any fact that can be discovered by reading code. If it will go stale when the code changes, it doesn't belong here — point to the source file instead.

## Instruction Hierarchy

Use this precedence when maintaining agent guidance:

1. `AGENTS.md` (canonical, cross-agent source of truth)
2. `.claude/rules/*.md` (shared detailed rules; agent-agnostic content)
3. `.claude/skills/*/SKILL.md` (workflow playbooks)
4. Agent-specific adapter files (for example `CLAUDE.md`) that reference canonical docs

Adapter files must not duplicate canonical policy unless they are adding agent-specific behavior.

## Architecture

| Layer | Technology |
|---|---|
| Desktop framework | Tauri v2 |
| Frontend | React 19, TypeScript strict, Vite |
| Styling | Tailwind CSS 4, shadcn/ui |
| State | Zustand |
| Icons | Lucide React |
| Agent sidecar | Node.js + TypeScript + `@anthropic-ai/claude-agent-sdk` |
| Database | SQLite (`rusqlite` bundled) |
| Rust errors | `thiserror` |
| Orchestrator | Python + Claude Agent SDK (`uv` for deps) |
| dbt integration | dbt-core-mcp (MCP server) |
| Runtime | GitHub Actions (headless execution, session resumption) |
| Execution state | `plan.md` in migration repo (git-backed) |

**Source scope:** Fabric Warehouse (T-SQL stored procedures via ADF pipelines). Lakehouse/Spark is post-MVP.

## Repository Folder Map

Use this map before reasoning about implementation location:

- `app/src/` — frontend runtime code (React/TypeScript surfaces, components, stores, hooks).
- `app/src-tauri/src/` — Rust backend runtime code (Tauri commands, DB, logging, startup wiring).
- `app/sidecar/` — Node/TypeScript sidecar runtime code.
- `app/e2e/` — Playwright E2E tests only.
- `app/src/__tests__/` and `app/sidecar/__tests__/` — unit/integration tests only.
- `docs/` — documentation and design/reference material only; do not treat as executable source unless explicitly asked.
- `scripts/` — developer/automation scripts.
- `orchestrator/` — Python orchestration runtime + tests.

## Dev Commands

```bash
# Tauri app (run from app/)
npm run dev                    # Dev mode (hot reload)
npm run build                  # Production build

# Rust tests
cargo test --manifest-path app/src-tauri/Cargo.toml   # all Rust tests
cargo test --manifest-path app/src-tauri/Cargo.toml db # module filter

# Sidecar tests (run from app/sidecar/)
npx vitest run

# Python orchestrator (run from orchestrator/)
cd orchestrator && uv sync
uv run pytest                  # All tests
uv run pytest tests/unit       # Unit tests only
```

## Testing

### When to write tests

**Tauri app:**

1. New Rust command with testable logic → `#[cfg(test)]` tests
2. New state logic (store actions, derived state) → store unit tests (once stores exist)
3. New UI interaction → component test
4. New page or major flow → E2E test (happy path)
5. Bug fix → regression test

**Python orchestrator/agents:**

1. New agent tool or orchestration logic → pytest unit test
2. New `plan.md` parsing/state logic → pytest unit test
3. Bug fix → regression test

Purely cosmetic changes or simple wiring don't require tests. If unclear, ask the user.

### Test discipline

Before writing any test code, read existing tests for the files you changed:

1. Update tests that broke due to your changes
2. Remove tests that are now redundant
3. Add new tests only for genuinely new behavior
4. Never add tests just to increase count — every test must catch a real regression

### Choosing which tests to run

Determine what you changed, then pick the right runner:

| What changed | Tests to run |
|---|---|
| Rust command or `db.rs` | `cargo test --manifest-path app/src-tauri/Cargo.toml <module>` |
| SQL query-pack files (`app/src-tauri/sql/source/**`) or `source_sql.rs` | `cargo test --manifest-path app/src-tauri/Cargo.toml source_sql` and `cargo test --manifest-path app/src-tauri/Cargo.toml source_sql -- --ignored` (requires local SQL Server per `docs/reference/setup-docker/README.md`) |
| Frontend store / hook | `npm run test:unit` |
| Frontend component / page | `npm run test:integration` + E2E tag from `app/tests/TEST_MANIFEST.md` |
| Rust command | `cargo test <module>` + E2E tag from `app/tests/TEST_MANIFEST.md` |
| Node sidecar (`app/sidecar/`) | `cd app/sidecar && npx vitest run` |
| Python orchestrator / agents | `cd orchestrator && uv run pytest <module>` |
| Unsure | all of the above |

Run `npx tsc --noEmit` from `app/` first — catches type errors in files you didn't directly touch.

When a change depends on local infrastructure (for example SQL Server-backed ignored tests), document in the PR which commands were run and which were not run.

## Design Docs

Design notes live in `docs/design/`. Each topic gets its own subdirectory with a `README.md`
(e.g. `docs/design/orchestrator-design/README.md`). The index at `docs/design/README.md` must be
updated when adding a new subdirectory.

Write design docs concisely — state the decision and the reason, not the reasoning process. One
sentence beats a paragraph. Avoid restating what the code already makes obvious.

## Code Style

- Granular commits: one concern per commit, run tests before each
- Stage specific files — use `git add <file>` not `git add .`
- All `.md` files must pass `markdownlint` before committing (`markdownlint <file>`)
- Verify before committing: `cd app && npx tsc --noEmit` + `cargo check --manifest-path app/src-tauri/Cargo.toml`
- Canonical naming and error-handling conventions live in `.claude/rules/coding-conventions.md`

### Frontend (`app/src/`)

For AD brand rules, component constraints, and state indicator conventions, see:

- `.claude/rules/frontend-design.md`

### Rust backend (`app/src-tauri/`)

Command conventions, error types, and Rust-specific testing guidance live in `.claude/rules/rust-backend.md`.

### Sidecar (`app/sidecar/`)

Protocol and sidecar-specific constraints live in `.claude/rules/agent-sidecar.md`.

### Error handling

See `.claude/rules/coding-conventions.md` for canonical error-handling policy.

## Issue Management

- **PR title format:** `VU-XXX: short description`
- **PR body link:** `Fixes VU-XXX`
- **Worktrees:** `../worktrees/<branchName>` relative to repo root. Full rules: `.claude/rules/git-workflow.md`.

## Skills

Use these repo-local skills when requests match:

- `.claude/skills/create-linear-issue/SKILL.md` — create/log/file a Linear issue, bug, feature, or ticket decomposition
- `.claude/skills/implement-linear-issue/SKILL.md` — implement/fix/work on a Linear issue (e.g. `VU-123`)
- `.claude/skills/close-linear-issue/SKILL.md` — close/complete/ship/merge a Linear issue
- `.claude/skills/tauri/SKILL.md` — Tauri-specific implementation or debugging
- `.claude/skills/front-end-design/SKILL.md` — frontend UI/UX implementation and design polish
- `.claude/skills/shadcn-ui/SKILL.md` — shadcn/ui component work
- `.claude/skills/explaining-code/SKILL.md` — explain code with diagrams/teaching-style breakdowns

## Logging

Every new feature must include logging. Canonical logging conventions and log-level guidance live in `.claude/rules/logging-policy.md`.

## Gotchas

- **Agent SDK has no team tools:** The Claude Agent SDK (Python) does NOT support TeamCreate, TaskCreate, or SendMessage. Use the `Task` tool for sub-agents. Multiple `Task` calls in the same turn run in parallel.
- **Parallel worktrees:** `npm run dev` auto-assigns a free port — safe to run multiple Tauri instances simultaneously.
