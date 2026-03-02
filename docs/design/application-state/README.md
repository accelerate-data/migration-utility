# Application State Design

Canonical app state machine for surface routing and edit locks.

## Source of Truth

`app_phase` is the single source of truth for lifecycle state. All routing,
surface locks, and edit guards derive from it.

Persisted in SQLite `settings` keys:

- `app_phase` (`setup_required` | `scope_editable` | `plan_editable` | `ready_to_run` | `running_locked`)
- `app_settings` (`github_oauth_token`, `anthropic_api_key`)
- `workspaces` existence (`is_source_applied`)

Implementation references:

- [db.rs](/Users/hbanerjee/src/migration-utility/app/src-tauri/src/db.rs)
- [types.rs](/Users/hbanerjee/src/migration-utility/app/src-tauri/src/types.rs)
- [settings.rs](/Users/hbanerjee/src/migration-utility/app/src-tauri/src/commands/settings.rs)

## Phase Definitions

- `setup_required`: Missing at least one prerequisite (`github auth`, `anthropic key`, or applied source/workspace).
- `scope_editable`: Prerequisites complete; user is configuring scope.
- `plan_editable`: Scope finalized; user is editing the migration plan.
- `ready_to_run`: Plan finalized; migration can be launched.
- `running_locked`: Migration running; all surfaces are read-only.

## Phase Transitions

| Action | Resulting phase |
|---|---|
| Finalize Scope | `plan_editable` |
| Finalize Plan | `ready_to_run` |
| Launch Migration | `running_locked` |
| Workspace apply / reset | `scope_editable` |

Transitions use `app_set_phase` which writes the requested phase then calls
`reconcile_and_persist_app_phase`.

## Reconciliation Logic

`reconcile_and_persist_app_phase` in `db.rs`:

1. If prerequisites missing: return `setup_required` **without** writing to DB
   (preserves the intended phase for when prerequisites are restored).
2. Else: use persisted phase (default `scope_editable` if null).

This means phase is only written to DB when prerequisites are satisfied.
Workspace apply / reset explicitly write `scope_editable` before reconciling.

## Transition Entry Points

Commands that call reconciliation:

- `app_hydrate_phase`
- `app_set_phase` (writes requested phase first)
- `save_anthropic_api_key`
- `github_poll_for_token`
- `github_logout`
- `workspace_apply_and_clone` (writes `scope_editable` first)
- `workspace_reset_state` (writes `scope_editable` first)

## Frontend Routing and Locks

Default route by phase:

- `setup_required` -> `/home`
- `scope_editable` -> `/scope`
- `plan_editable` -> `/plan`
- `ready_to_run` -> `/monitor`
- `running_locked` -> `/monitor`

Surface availability:

- `settings`: always enabled
- `home`: always enabled
- `scope`: enabled for all non-setup phases
- `plan`: enabled in `plan_editable`, `ready_to_run`, `running_locked`
- `monitor`: enabled in `ready_to_run`, `running_locked`

Read-only behavior:

- Scope surface: read-only in all phases except `scope_editable`
- Plan surface: read-only in `running_locked`

Implementation references:

- [workflow-store.ts](/Users/hbanerjee/src/migration-utility/app/src/stores/workflow-store.ts)
- [App.tsx](/Users/hbanerjee/src/migration-utility/app/src/App.tsx)
- [icon-nav.tsx](/Users/hbanerjee/src/migration-utility/app/src/components/icon-nav.tsx)

## Operational Invariants

- Losing prerequisites always forces `setup_required` effective phase without
  overwriting the intended persisted phase.
- The intended phase is automatically restored when prerequisites are regained.
- Workspace apply and reset both write `scope_editable` before reconciliation.
