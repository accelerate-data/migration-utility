# Application State Design

Canonical application state for stage-gated offline migration with DB-first state management.

## Core Model

The app runs as four gated stages per batch item:

1. `scoping`
2. `profiling`
3. `planning`
4. `running` (migration + testing)

The database is canonical. Agent output files are import artifacts and audit evidence.

## Canonical Sources

- Canonical runtime state: SQLite tables (stage runs, stage items, overrides, approvals).
- Import source: `artifacts/<run_id>/<stage>/output.json`.
- Export source for next stage: DB effective values (`COALESCE(fde_value, agent_value)`).
- File views are read-only mirrors; they never override DB directly.

## State Layers Per Item

Each stage item keeps three value layers:

1. `agent_value` - immutable value imported from agent output.
2. `fde_value` - optional user override.
3. `effective_value` - computed value used by downstream stage artifacts.

Rule: `effective_value = COALESCE(fde_value, agent_value)`.

## Recommended Tables

### `workflow_runs`

- `id`
- `batch_id`
- `status` (`active`, `completed`, `failed`, `cancelled`)
- `created_at`, `updated_at`

### `stage_runs`

- `id`
- `workflow_run_id`
- `stage` (`scoping`, `profiling`, `planning`, `running`)
- `status` (`queued`, `running`, `partial`, `passed`, `failed`, `cancelled`)
- `trigger_mode` (`normal`, `delta`, `override`)
- `override_reason` (nullable)
- `started_at`, `completed_at`

### `stage_items`

- `id`
- `stage_run_id`
- `table_ref`
- `status` (`pending`, `running`, `passed`, `failed`, `blocked`, `stale`)
- `agent_payload_json`
- `effective_payload_json`
- `upstream_fingerprint`
- `input_fingerprint`
- `stale_reason` (nullable)

### `stage_item_overrides`

- `id`
- `stage_item_id`
- `field_path`
- `agent_value_json`
- `fde_value_json`
- `edited_by`
- `edited_at`

### `stage_item_approvals`

- `id`
- `stage_item_id`
- `approval_state` (`pending`, `approved`, `rejected`)
- `approved_by`
- `approved_at`
- `notes`

### `rerun_selection`

- `id`
- `workflow_run_id`
- `stage`
- `table_ref`
- `selection_source` (`edited_filter`, `stale_filter`, `failed_filter`, `manual`)
- `selected_at`

## Stage Gates

Gate policy is warning-first with override support.

- Default submission: `safe delta` (only items passing gate checks).
- Override submission: allows selected warning items; requires override reason and audit record.

Gate checks include:

- Upstream stage approval missing.
- Upstream-derived stale fingerprints.
- Required effective fields missing.

## Delta Rerun Logic

Delta is default for reruns.

An item is delta-eligible if any are true:

- Edited by FDE since last stage run.
- Marked stale from upstream fingerprint mismatch.
- Prior run status `failed` or `partial`.
- Manually selected by FDE.

Rerun payload is generated from selected items only.

## State Machines

### Workflow-level machine

- `setup_required` -> `scope_ready` -> `stage_active` -> `stage_waiting_approval` -> `stage_active` (next stage) -> `completed`
- Any stage can transition to `failed`.
- `reset` transitions to `setup_required`.

### Stage-level machine

- `queued` -> `running` -> `partial|passed|failed|cancelled`
- `partial` can be retriggered with delta selection.
- `passed` waits for FDE approval before opening next stage.

### Item-level machine

- `pending` -> `running` -> `passed|failed|blocked|stale`
- `stale` requires rerun before downstream stage submission.
- `blocked` can move to `pending` after dependencies are satisfied.

## Transitions

| Trigger | Scope | Transition | Notes |
|---|---|---|---|
| Import stage output JSON | stage run | `running -> partial, passed, failed` | Writes `agent_payload_json` only |
| FDE edit | stage item | marks item dirty | Writes override row; recomputes effective value |
| Approve item/stage | stage item/run | approval state updated | Required for gate satisfaction |
| Submit stage run (safe delta) | stage run | creates new queued run | Includes only gate-safe selected items |
| Submit stage run (override) | stage run | creates new queued run | Requires `override_reason` |
| Upstream approved rerun | dependent items | mark `stale` | Fingerprint mismatch propagation |

## Routing and Locking

- Scoping/profiling/planning surfaces are editable from DB state.
- Running surface is read-only.
- While a stage run is `running`, that stage's edit controls are disabled.
- Other stages remain viewable; downstream stage trigger remains gated.

## Artifact Flow

1. Agent writes stage `output.json`.
2. Importer validates schema and upserts `agent_payload_json` rows.
3. UI edits store `fde_value` overrides in DB.
4. Exporter composes next stage `input.json` from effective values.
5. Triggered stage writes new `output.json`; cycle repeats.

## Non-Goals

- Canonical state in markdown.
- In-place mutation of imported agent payload.
- Full rerun by default when delta is available.

## Implementation References

- [offline-stage-gated-mockup.html](/Users/hbanerjee/src/migration-utility/docs/design/ui-patterns/offline-stage-gated-mockup.html)
- [README.md](/Users/hbanerjee/src/migration-utility/docs/design/agent-contract/README.md)
- [scoping-agent.md](/Users/hbanerjee/src/migration-utility/docs/design/agent-contract/scoping-agent.md)
- [profiler-agent.md](/Users/hbanerjee/src/migration-utility/docs/design/agent-contract/profiler-agent.md)
- [planner-agent.md](/Users/hbanerjee/src/migration-utility/docs/design/agent-contract/planner-agent.md)
