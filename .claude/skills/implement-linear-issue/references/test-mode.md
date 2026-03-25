# Test Mode Classification

Determines whether a change needs **mock mode** or **full mode** for manual testing.

## Decision Rule

Run `git diff --name-only main` in the worktree. If **ANY** changed file matches a full-mode path, recommend full mode. Otherwise recommend mock mode.

## Full Mode (`npm run dev`)

Changes that affect agent execution or output quality. Requires an API key.

| Path Pattern | Why |
|---|---|
| `agent-sources/plugins/*/agents/` | Agent prompt content |
| `agent-sources/plugins/*/skills/` | Agent skill definitions |
| `agent-sources/CLAUDE.md` | Agent instructions |
| SDK config or model selection logic | Affects which model runs |

## Mock Mode (`MOCK_AGENTS=true npm run dev`)

Everything else. Replays bundled templates (~1s per step, no API spend).

- Frontend: components, pages, styles, routing (`app/src/`)
- Stores, hooks, utilities (`app/src/stores/`, `app/src/hooks/`, `app/src/lib/`)
- Rust commands (`app/src-tauri/src/commands/`)
- Tests and test infrastructure
- Scripts, docs, config files

## Launch Commands

**Mock mode** (from worktree):

```bash
cd ../worktrees/<branch>/app && MOCK_AGENTS=true npm run dev
```

**Full mode** (from worktree):

```bash
cd ../worktrees/<branch>/app && npm run dev
```

## Examples

| Changed Files | Mode | Reason |
|---|---|---|
| `app/src/pages/dashboard.tsx` | Mock | Pure UI |
| `app/src-tauri/src/commands/settings.rs` | Mock | Non-agent Rust command |
| `agent-sources/plugins/ad-migration/agents/scoping-agent.md` | Full | Agent prompt change |
| `agent-sources/plugins/ad-migration/skills/scoping-writers/SKILL.md` | Full | Agent skill change |
