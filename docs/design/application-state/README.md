# Application State Design

Canonical app state machine for surface routing and edit locks.

## Source of Truth

`app_phase` is the single source of truth for lifecycle state. All routing,
surface locks, and edit guards derive from it.

Persisted in SQLite `settings` key `app_phase`:
`setup_required` | `scope_editable` | `plan_editable` | `ready_to_run` | `running_locked`

Implementation references:

- [db.rs](/Users/hbanerjee/src/migration-utility/app/src-tauri/src/db.rs)
- [types.rs](/Users/hbanerjee/src/migration-utility/app/src-tauri/src/types.rs)
- [settings.rs](/Users/hbanerjee/src/migration-utility/app/src-tauri/src/commands/settings.rs)

## Phase Definitions

- `setup_required`: No workspace applied yet (initial state and after reset).
- `scope_editable`: Source applied; user is configuring scope.
- `plan_editable`: Scope finalized; user is editing the migration plan.
- `ready_to_run`: Plan finalized; migration can be launched.
- `running_locked`: Migration running; all surfaces are read-only.

## Phase Transitions

All transitions are explicit writes — no dynamic inference from prerequisites.

| Action | Command | Resulting phase |
|---|---|---|
| Apply source | `workspace_apply_and_clone` | `scope_editable` |
| Reset / delete source | `workspace_reset_state` | `setup_required` |
| Finalize Scope | `app_set_phase('plan_editable')` | `plan_editable` |
| Finalize Plan | `app_set_phase('ready_to_run')` | `ready_to_run` |
| Launch Migration | `app_set_phase('running_locked')` | `running_locked` |

`app_set_phase` cannot be called with `setup_required` (rejected server-side).

## Reconciliation

`reconcile_and_persist_app_phase` reads the persisted phase and returns it,
defaulting to `setup_required` if no phase has been written yet. It performs
no inference and no writes — it is a pure read.

Called by `app_hydrate_phase` (startup) and `app_set_phase` (returns new state).

## Fact Fields

`AppPhaseState` includes `hasGithubAuth`, `hasAnthropicKey`, `isSourceApplied`
as informational fields for the Home/Setup screen. They do **not** drive
phase transitions.

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
