# Overall Design

End-to-end design for the Migration Utility: a Tauri desktop app that drives a GitHub Actions pipeline migrating Microsoft Fabric Warehouse stored procedures to dbt models on Vibedata's platform.

Currently Supports: SQL Server.

---

## Prerequisites

The user must have the following before launching the app:

1. **Docker Desktop** — manages a local SQL Server container for DacPac inspection.
2. **GitHub account** — migration repo lives here; agent runs execute as GitHub Actions. The app forces login if not set.

These checks are done at startup:

1. The app checks Docker at startup via a splash screen. If not present, ask the user to install and click retry. The app will not proceed past the splash screen until Docker is set up. Clicking cancel closes the app gracefully.
2. GitHub is checked on the main app startup (after the splash screen closes). If not set up, force the user to log in and select an empty migration repo. The app does a startup check to see if the local clone matches remote and pulls if not.

---

## High-Level Architecture

```text
Desktop App (Tauri)
  ├── Home — project status
  └── Settings — GitHub OAuth, migration repo, local clone path, log level

          │  workflow_dispatch (GitHub API, OAuth token)
          ▼
  GitHub Actions runner
    ├── Restores DacPac → SQL Server Docker container (cached)
    └── Runs agent → commits output JSON to migration repo
```

All agent execution happens in GitHub Actions. The desktop app is the control plane: it submits runs and syncs artifacts.

---

## GitHub Authentication

**Method:** Classic OAuth App (`workflow` + `repo` scopes).

Required scopes:

| Scope | Why |
|---|---|
| `repo` | Clone repo, read/write contents and LFS, manage secrets |
| `workflow` | Trigger `workflow_dispatch` |

Token storage: SQLite. Silent refresh via the OAuth refresh token flow must be implemented before re-prompting the user. The app handles 401 responses by refreshing before re-prompting the user.

---

## Migration Repository

One migration repo per installation, set by the user in Settings. The app creates it fresh (existing repos with content are not supported). All projects share this single repo. Project slug is kebab-case of user-entered project name, deduplicated with a short hash suffix on collision.

Directory layout inside the repo:

```text
{project-slug}/
  artifacts/
    dacpac/
      {dacpac-filename}          # stored via Git LFS
      metadata.json              # auto-generated at project creation
    {action}/
      {run_id}.json              # immutable agent output, committed by GH Actions
```

`{action}` is one of: `scoping-agent`, `profiler-agent`, `decomposer-agent`, `planner-agent`, `test-generator-agent`, `migrator-agent`.

**Git LFS:** The app enables LFS on the repo via GitHub API at project creation before the first DacPac push. No manual setup required.

**Local clone:** The user provides a local path in Settings (e.g. `~/migration-utility`). The app clones the migration repo to this path at project initialization.

---

## App Log

| Platform | Path |
|---|---|
| macOS | `~/Library/Logs/com.vibedata.migration-utility/app.log` |
| Windows | `C:\Users\{user}\AppData\Roaming\com.vibedata.migration-utility\logs\app.log` |

---

## SQL Server Credentials

User enters the SA password at project creation. Stored in local SQLite (`projects` table) and pushed to GitHub Secrets (`SA_PASSWORD_<SLUG_UPPER>`) via the GitHub API at project creation time.

---

## Single-Instance Enforcement

Only one instance of the app may run at a time. This is implemented using `tauri-plugin-single-instance`.

---

## Docker Management

The app manages the local SQL Server container via Docker CLI (`docker` subprocess from Rust via `std::process::Command`).

Lifecycle:

| Event | Docker CLI call |
|---|---|
| Startup check | `docker info` |
| Project init | `docker run --env SA_PASSWORD=... -p 1433:1433 -d mcr.microsoft.com/mssql/server` |
| Project delete | `docker stop {container}` + `docker rm {container}` |

If port 1433 is already bound, the app surfaces a clear error and asks the user to free the port before retrying.

---

## DacPac Caching in GitHub Actions

Each GH Actions run needs SQL Server with the project database restored. Full DacPac restores can take 5–15 minutes and are unacceptable on every run.

**LFS caching:** The `.git/lfs` object directory is cached in GH Actions keyed on the SHA256 hash of the DacPac file. Avoids re-downloading from GitHub LFS on every run.

**Restore caching:** After the first restore, the MDF/LDF files are cached via `actions/cache` keyed on the DacPac SHA256 hash. Subsequent runs attach the cached files directly, skipping restore.

Cache invalidation is automatic: when the DacPac changes (hash changes), both cache entries miss and the full restore runs once to rebuild them.

---

## Agent Execution Model

One GitHub Actions workflow file per agent. Each GitHub Actions workflow run corresponds to exactly one agent and one batch of tables.

### Submission

The app triggers a run via `workflow_dispatch` (GitHub API), passing:

| Input | Description |
|---|---|
| `run_id` | UUID generated by the app at submission time |
| `action` | Agent name (e.g. `scoping-agent`) |
| `project_slug` | Identifies the project directory in the repo |
| `submitted_ts` | ISO 8601 UTC timestamp set by the app at submission |
| `items` | JSON array of item IDs to process, per the agent contract input schema |

The app records in local SQLite (`agent_runs` table):

| Field | Type | Notes |
|---|---|---|
| `id` | INTEGER | Primary key |
| `project_id` | TEXT (UUID) | References `projects.id` |
| `run_id` | TEXT (UUID) | Generated by the app at submission time |
| `action` | TEXT | Agent name (e.g. `scoping-agent`) |
| `submitted_ts` | TEXT | ISO 8601 UTC; set by the app at submission |
| `github_run_id` | TEXT | Populated when the GH Actions run is triggered |
| `status` | TEXT | `in-progress`, `success`, or `failed` |

### Workflow Execution (GH Actions Runner)

1. Clone the migration repo (LFS objects cached by DacPac SHA256).
2. Restore the SQL Server DB from MDF/LDF cache, or full DacPac restore on cache miss.
3. Build the agent input JSON from `items[]` per the agent contract input schema.
4. Run the agent.
5. Create branch `run/{run_id}`.
6. Commit output JSON to `{project-slug}/artifacts/{action}/{run_id}.json` on that branch.
7. Merge `run/{run_id}` into `main` (no conflicts — each run touches a unique file path).
8. Delete the `run/{run_id}` branch.

---

## Projects

### SQLite `projects` Table

| Field | Type | Notes |
|---|---|---|
| `id` | TEXT (UUID) | Generated by the app at creation time |
| `slug` | TEXT | Kebab-case of project name, unique |
| `name` | TEXT | User-entered |
| `sa_password` | TEXT | Stored locally; also pushed to GitHub Secrets |
| `created_at` | TEXT | ISO 8601 UTC |

### Create Project

1. User enters: project name, SQL Server version, local DacPac path, SA password, customer, system, db name, extraction datetime.
2. App generates project UUID (`id`) and slug (kebab-case, unique).
3. App creates the folder structure in the migration repo on GitHub.
4. App enables Git LFS on the repo if not already enabled.
5. App generates `metadata.json`:
   - `customer` - user-entered
   - `system` — user-entered
   - `db_name` — user-entered
   - `extraction_datetime` — user-entered
   - `tool_version` — migration utility version
   - `dacpac_sha256` — computed by the app
6. App pushes DacPac + `metadata.json` to LFS.
7. App stores SA password in SQLite and pushes it to GitHub Secrets (`SA_PASSWORD_<SLUG_UPPER>`).
8. Project is set as the active project.
9. Project initialization runs (see below).

### Select Active Project

The entire UI operates on one project at a time. The user changes the active project in Settings. On apply, project initialization runs.

### Project Initialization

Runs at startup (if a project is set) and on active project change:

1. `git pull` the migration repo (or clone if not cloned).
2. Check Docker is running.
3. Start the SQL Server container for this project if not already running.
4. Restore DacPac if the container DB is not already present.
5. Verify DB connectivity.

If any step fails, the app surfaces a blocking error with a Retry button.

### Delete Project

Deletes:

- `{project-slug}/` directory from the migration repo on GitHub (including all artifacts and runs).
- Local clone of that project's data.
- All SQLite rows for the project.
- The `SA_PASSWORD_<SLUG_UPPER>` GitHub Secret.

Confirmation dialog required. Destructive and irreversible.

---

## Status Consolidation

Triggered on surface load and on Refresh. Consolidation owns all computation — the UI is display-only.

Steps:

1. `git pull` the migration repo.
2. For each stage, scan artifact files in `{project-slug}/artifacts/{action}/`.
3. Last run per table wins — a later run supersedes all prior runs for that table.
4. Upsert one row per `(table_id, stage)` in `stage_status` with the consolidated `status`.
5. Run dirty computation as a final standalone step after all statuses are updated (see below).

### `stage_status` Table

One row per `(table_id, stage)`, created when a stage is submitted for a table:

| Field | Meaning |
|---|---|
| `status` | `in-progress`, `success`, or `failed` |
| `dirty` | `1` if this stage needs to be re-run |

No run history is maintained — each consolidation overwrites the current state.

### Dirty Computation

Runs as a standalone final step after all statuses are updated. Iterates **backwards** from the last stage to the first (migrate → test-generator → plan → decompose → profile). For each stage N:

```text
if stage_status[N].status == 'failed':
  dirty[N] = 1                          -- failed run always stays dirty

elif fde_override_exists for upstream stage (N-1):
  dirty[N] = 1                          -- FDE edit on upstream input; must re-run

elif artifact_submitted_ts[N-1] > artifact_submitted_ts[N]:
  dirty[N] = 1                          -- upstream artifact is newer; output is stale

else:
  dirty[N] = 0
```

Migrate has two upstreams (plan and test-generator); it is dirty if either upstream's artifact is newer than migrate's, or either upstream has an FDE override, or migrate's own last run failed.

Scope has no upstream and is never dirty.

No propagation pass is needed. Each stage re-run produces a new artifact timestamp; the next consolidation's backwards pass naturally marks the immediately downstream stage dirty. The cascade resolves one stage at a time across successive re-runs and consolidations.

`dirty_from` is not stored — computed on demand by the UI as `MIN(stage_order) WHERE dirty = 1` for the table.

Status refresh is manual. Each tab has a **Refresh** button that triggers consolidation. No background polling.

---

## FDE Edits

Each stage modal lets the FDE review the prior stage's output before submitting a run. Overrides are allowed for Scope, Profile, Decompose, and Plan outputs. Generate Tests and Migrate outputs are read-only — they are final artifacts.

Rules:

- Agent output JSON is immutable in git. The UI never overwrites it.
- FDE overrides are stored in local SQLite only (`fde_overrides` table).
- History of FDE overrides is not maintained — only the current override value is stored.
- Effective input to the next agent = `COALESCE(fde_value, agent_value)`.
- `fde_overrides` is keyed by `(project_id, table_id, stage, field)`. Each row stores `fde_value`, `source_run_id`, and `source_submitted_ts`.
- Saving an override sets `dirty = 1` on the downstream stage's `stage_status` row immediately. Consolidation will preserve this dirty flag (via the FDE override check) until the downstream stage is re-run successfully.

Full schema and per-stage editable field definitions: [fde-overrides.md](fde-overrides.md).

---

## Stage Surfaces

The migration UI is a single screen with six tabs: **Scope → Profile → Decompose → Plan → Generate Tests → Migrate**. The FDE can switch between tabs freely. Each tab is a funnel — it only shows tables that have a successful output from the prior stage.

### Common Layout

Each tab:

- Table list showing only tables eligible for this stage (see Stage Gate below).
- Status filter (mutually exclusive): `Pending` (never run or failed), `Success`, `In Progress`, `Dirty`. Default view is `Pending`.
- **Refresh** button — pulls repo, re-consolidates SQLite.
- Double-click a table row → modal showing the prior stage's output. FDE can edit fields for Scope, Profile, Decompose, and Plan stages; Generate Tests and Migrate modals are read-only. Changes are saved on explicit confirm.
- Right-click a table row with a run (Success, Failed, or In Progress) → context menu with **View run log** — opens the GitHub Actions run URL (`https://github.com/{owner}/{repo}/actions/runs/{github_run_id}`) in the browser via `tauri-plugin-opener`. Pending rows (never submitted) have no run; the option is absent.
- Select one or more tables → **Submit** button.
- If any selected table already has a successful result for this stage, a confirmation prompt appears before resubmission.

### Stage Gate

**Scope (tab 1):** all tables in the project are always shown. There is no prior stage.

**All other tabs (N ≥ 2):** a table is shown only if all of the following are true:

1. `stage_status[N-1].status = success` — prior stage succeeded (funnel filter).
2. `dirty = 0` OR `dirty_from = N` — either not dirty, or this is the earliest dirty stage for the table.

Tables that are dirty at an earlier stage do not appear here at all — they surface only in the tab where `dirty_from` points, with a `Dirty` status badge. The FDE works forward from the earliest dirty stage naturally without any explicit blocking message.

### Tab Details

#### 1. Scope (`scoping-agent`)

Output: `{project-slug}/artifacts/scoping-agent/{run_id}.json`

- Shows all tables in the project (no prior stage to filter on).
- No prior-stage modal — Scope is the first stage.
- Submission input: `item_id` list + `search_depth` (default 2).

#### 2. Profile (`profiler-agent`)

Output: `{project-slug}/artifacts/profiler-agent/{run_id}.json`

- Shows tables with a successful Scope output.
- Modal shows: Scope output. FDE can edit `selected_writer` before profiling.

#### 3. Decompose (`decomposer-agent`)

Output: `{project-slug}/artifacts/decomposer-agent/{run_id}.json`

- Shows tables with a successful Profile output.
- Modal shows: Profile output. FDE can edit candidate selections before decomposing.

#### 4. Plan (`planner-agent`)

Output: `{project-slug}/artifacts/planner-agent/{run_id}.json`

- Shows tables with a successful Decompose output.
- Modal shows: Decompose output. FDE can edit split points and block purposes before planning.

#### 5. Generate Tests (`test-generator-agent`)

Output: `{project-slug}/artifacts/test-generator-agent/{run_id}.json`

- Shows tables with a successful Plan output.
- Modal shows: Plan output (read-only). Generate Tests is a final output stage — no FDE overrides.

#### 6. Migrate (`migrator-agent`)

Output: `{project-slug}/artifacts/migrator-agent/{run_id}.json`

- Shows tables with successful Plan **and** Generate Tests outputs.
- Modal shows: Plan output and Generate Tests output (read-only). Migrate is a final output stage — no FDE overrides.
