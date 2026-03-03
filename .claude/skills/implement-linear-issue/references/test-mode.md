# Test Mode Classification

Determines whether a change needs **mock mode** or **full mode** for manual testing.

## Decision Rule

Run `git diff --name-only main` in the worktree. If **ANY** changed file matches a full-mode path, recommend full mode. Otherwise recommend mock mode.

## Full Mode (`npm run dev`)

Changes that affect agent execution or output quality. Requires an API key.

| Path Pattern | Why |
|---|---|
| `agents/` | Agent prompt content |
| `agent-sources/workspace/CLAUDE.md` | Agent instructions |
| `app/sidecar/*.ts` (except `mock-agent.ts`, `mock-templates/`) | Sidecar runtime logic |
| `app/src-tauri/src/agents/` | Rust agent orchestration (sidecar.rs, sidecar_pool.rs) |
| `app/src-tauri/src/commands/workflow.rs` (agent-execution paths) | Workflow step dispatch |
| `app/sidecar/mock-templates/` | Mock replay data itself |
| SDK config or model selection logic | Affects which model runs |

## Mock Mode (`MOCK_AGENTS=true npm run dev`)

Everything else. Replays bundled JSONL templates (~1s per step, no API spend).

- Frontend: components, pages, styles, routing (`app/src/`)
- Stores, hooks, utilities (`app/src/stores/`, `app/src/hooks/`, `app/src/lib/`)
- Non-agent Rust commands: settings, skills, files, git, dashboard, usage
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
| `app/src/pages/dashboard.tsx`, `app/src/stores/skill-store.ts` | Mock | Pure UI + state |
| `app/src-tauri/src/commands/settings.rs` | Mock | Non-agent Rust command |
| `agents/confirm-decisions.md` | Full | Agent prompt change |
| `app/sidecar/run-agent.ts` | Full | Sidecar runtime |
| `app/src/components/workflow/AgentOutputPanel.tsx` | Mock | UI display of agent output (mock replays work) |
| `app/src-tauri/src/agents/sidecar.rs` | Full | Agent process management |
| `app/sidecar/mock-templates/step0-research.jsonl` | Full | Mock data itself changed |
