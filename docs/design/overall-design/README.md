# Overall Design

End-to-end design for the Migration Utility: a Tauri desktop app that drives a GitHub Actions pipeline migrating stored procedures to dbt models on Vibedata's platform.

## Supported Sources

| Technology | `technology` value | Import format | Test generator access |
|---|---|---|---|
| SQL Server | `sql_server` | `.dacpac` | Docker + SQL Server container (GH Actions) |
| Fabric Warehouse | `fabric_warehouse` | `.zip` (DDL export) | T-SQL cloud endpoint |
| Fabric Lakehouse | `fabric_lakehouse` | `.zip` (DDL export) | Spark SQL |
| Snowflake | `snowflake` | `.zip` (DDL export) | SQL cloud connection |

---

## Prerequisites

The user must have the following before launching the app:

1. **GitHub account** — migration repo lives here; agent runs execute as GitHub Actions. The app forces login if not set.

This check is done at startup:

1. GitHub is checked on the main app startup (after the splash screen closes). If not set up, force the user to log in and select an empty migration repo. The app does a startup check to see if the local clone matches remote and pulls if not.

---

## High-Level Architecture

```text
Desktop App (Tauri)
  ├── Home — project status
  └── Settings — GitHub OAuth, migration repo, local clone path, log level

          │  workflow_dispatch (GitHub API, OAuth token)
          ▼
  GitHub Actions runner
    ├── Reads DDL from repo → runs agent (scoping, profiling, migrating, test-generating)
    └── Connects to source database via MCP → runs test generator agent
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
    source/
      {source-filename}          # stored via Git LFS (dacpac or zip)
      metadata.json              # auto-generated at project creation
    ddl/
      tables.sql                 # CREATE TABLE statements
      procedures.sql             # CREATE PROCEDURE bodies
      views.sql
      functions.sql
      indexes.sql                # CREATE INDEX + ALTER TABLE constraints
    {action}/
      {run_id}.input.json        # agent input built and committed by the app before dispatch
      {run_id}.json              # agent output, immutable, committed by GH Actions
```

`{action}` is one of: `scoping-agent`, `profiler-agent`, `migrator-agent`, `test-generator-agent`.

**Git LFS:** The app enables LFS on the repo via GitHub API at project creation before the first source file push. No manual setup required.

**Local clone:** The user provides a local path in Settings (e.g. `~/migration-utility`). The app clones the migration repo to this path at project initialization.

---

## App Log

| Platform | Path |
|---|---|
| macOS | `~/Library/Logs/com.vibedata.migration-utility/app.log` |
| Windows | `C:\Users\{user}\AppData\Roaming\com.vibedata.migration-utility\logs\app.log` |

---

## Connection Settings

> ⚠️ Open question: connection settings schema per technology. SQL Server requires an SA password or connection string. Fabric requires OAuth/service principal. Snowflake requires key-pair/OAuth. The credential shape differs per technology. Connection settings are only needed for the test-generator-agent stage.

---

## Single-Instance Enforcement

Only one instance of the app may run at a time. This is implemented using `tauri-plugin-single-instance`.

---

## DDL Extraction

The app translates the source file into structured DDL files at project creation and keeps them consistent on subsequent startups.

### SQL Server (DacPac)

A bundled .NET 8 sidecar (`dacpac-extractor`) uses `Microsoft.SqlServer.DacFx` to unpack the DacPac and script all objects. Invoked by the Tauri app as a subprocess:

```bash
dacpac-extractor {source-file} {output-dir}
```

Outputs: `tables.sql`, `procedures.sql`, `views.sql`, `functions.sql`, `indexes.sql`.

The sidecar is built for macOS arm64/x86\_64 and Windows x86\_64 and bundled in the Tauri app bundle.

### Other sources (zip)

The app unzips the archive and normalizes the contents into the same `ddl/` structure directly in Rust.

### Consistency check (splash screen)

Runs at startup (if a project is set) and on active project change, before unblocking the UI:

1. `git pull` the migration repo (or clone if absent).
2. Pull source binary from LFS if not already local.
3. Check: does `artifacts/ddl/` exist and does `source_sha256` in `metadata.json` match the hash of the local source file?
   - No → extract DDL → commit `artifacts/ddl/` to repo.
   - Yes → nothing to do.

If any step fails, the app surfaces a blocking error with a Retry button.

---

## Interactive Migration (`migrate-table`)

Single-table interactive migration via Claude Code plugin — separate from the GHA batch pipeline. Both paths share the same deterministic Python skills. See [SP → dbt Migration Plugin](../sp-to-dbt-plugin/README.md) for skill contracts, shared library, and implementation plan.

---

## Agent Execution Model

One GitHub Actions workflow file per agent. Each GitHub Actions workflow run corresponds to exactly one agent and one batch of tables.

### Submission

1. Build agent input JSON from `items[]` per the agent contract input schema.
2. Commit `{project-slug}/artifacts/{action}/{run_id}.input.json` to the migration repo and push.
3. Trigger `workflow_dispatch` (GitHub API):

| Input | Description |
|---|---|
| `run_id` | UUID generated by the app at submission time |
| `project_slug` | Identifies the project directory in the repo |
| `submitted_ts` | ISO 8601 UTC timestamp set by the app at submission |

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

### Agent Plugin

`ad-migration` is a Claude Code marketplace package containing three plugins under `workbench/`. Plugin structure, skill contracts, and local dev setup: [SP → dbt Migration Plugin](../sp-to-dbt-plugin/README.md).

### Workflow Execution (GH Actions Runner)

**All agents except test generator:**

1. Clone the migration repo.
2. Install genai-toolbox binary; start DDL file MCP in HTTP mode on `localhost:5000`.
3. Install Claude Code CLI.
4. Run agent via the `ad-migration` plugin (see [SP → dbt Migration Plugin](../sp-to-dbt-plugin/README.md) for invocation details).
5. Create branch `run/{run_id}`.
6. Commit output JSON to `{project-slug}/artifacts/{action}/{run_id}.json` on that branch.
7. Merge `run/{run_id}` into `main` (no conflicts — each run touches a unique file path).
8. Delete the `run/{run_id}` branch.

**Test generator agent (`sql_server`):**

1. Clone the migration repo.
2. Pull source file from LFS.
3. Start SQL Server Docker container; restore database from source file (MDF/LDF cache keyed on `source_sha256`).
4. Install genai-toolbox binary; start live execution MCP in HTTP mode on `localhost:5000`.
5. Install Claude Code CLI.
6. Run test generator agent (steps 4–8 above).

---

## Projects

### SQLite `projects` Table

| Field | Type | Notes |
|---|---|---|
| `id` | TEXT (UUID) | Generated by the app at creation time |
| `slug` | TEXT | Kebab-case of project name, unique |
| `name` | TEXT | User-entered |
| `technology` | TEXT | One of `sql_server`, `fabric_warehouse`, `fabric_lakehouse`, `snowflake` |
| `created_at` | TEXT | ISO 8601 UTC |

### Create Project

1. User selects technology and enters: project name, customer, system, db name, extraction datetime.
2. User uploads source file: `.dacpac` for `sql_server`; `.zip` for all other technologies.
3. App generates project UUID (`id`) and slug (kebab-case, unique).
4. App creates the folder structure in the migration repo on GitHub.
5. App enables Git LFS on the repo if not already enabled.
6. App generates `metadata.json`:
   - `technology` — user-selected
   - `customer` — user-entered
   - `system` — user-entered
   - `db_name` — user-entered
   - `extraction_datetime` — user-entered
   - `tool_version` — migration utility version
   - `source_filename` — original uploaded filename
   - `source_sha256` — computed by the app
7. App pushes source binary + `metadata.json` to LFS.
8. App extracts DDL from source file → commits `artifacts/ddl/` to repo.
9. Project is set as the active project.

### Select Active Project

The entire UI operates on one project at a time. The user changes the active project in Settings. On apply, project initialization runs.

### Project Initialization

Runs at startup (if a project is set) and on active project change. This is the splash screen consistency check described in [DDL Extraction](#ddl-extraction):

1. `git pull` the migration repo (or clone if not cloned).
2. Pull source binary from LFS if not already local.
3. Check DDL consistency: if `artifacts/ddl/` is missing or `source_sha256` does not match → re-extract DDL → commit.

If any step fails, the app surfaces a blocking error with a Retry button.

### Delete Project

Deletes:

- `{project-slug}/` directory from the migration repo on GitHub (including all artifacts and runs).
- Local clone of that project's data.
- All SQLite rows for the project.

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

Runs as a standalone final step after all statuses are updated. Iterates **backwards** from the last stage to the first (test-generator → migrate → profile). For each stage N:

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

Migrate has one upstream (profile via catalog); it is dirty if profile's artifact is newer than migrate's, or profile has an FDE override, or migrate's own last run failed.

Scope has no upstream and is never dirty.

No propagation pass is needed. Each stage re-run produces a new artifact timestamp; the next consolidation's backwards pass naturally marks the immediately downstream stage dirty. The cascade resolves one stage at a time across successive re-runs and consolidations.

`dirty_from` is not stored — computed on demand by the UI as `MIN(stage_order) WHERE dirty = 1` for the table.

Status refresh is manual. Each tab has a **Refresh** button that triggers consolidation. No background polling.

---

## FDE Edits

Each stage modal lets the FDE review the prior stage's output before submitting a run. Overrides are allowed for Scope and Profile outputs. Migrate and Generate Tests outputs are read-only — they are final artifacts.

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

The migration UI is a single screen with four tabs: **Scope → Profile → Migrate → Generate Tests**. The FDE can switch between tabs freely. Each tab is a funnel — it only shows tables that have a successful output from the prior stage.

### Common Layout

Each tab:

- Table list showing only tables eligible for this stage (see Stage Gate below).
- Status filter (mutually exclusive): `Pending` (never run or failed), `Success`, `In Progress`, `Dirty`. Default view is `Pending`.
- **Refresh** button — pulls repo, re-consolidates SQLite.
- Double-click a table row → modal showing the prior stage's output. FDE can edit fields for Scope and Profile stages; Migrate and Generate Tests modals are read-only. Changes are saved on explicit confirm.
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

#### 3. Migrate (`migrator-agent`)

Output: `{project-slug}/artifacts/migrator-agent/{run_id}.json`

- Shows tables with a successful Profile output.
- Modal shows: Profile output. FDE can review profile answers before submitting migration.

#### 4. Generate Tests (`test-generator-agent`)

Output: `{project-slug}/artifacts/test-generator-agent/{run_id}.json`

- Shows tables with a successful Migrate output.
- Modal shows: Migrate output (read-only). Generate Tests is a final output stage — no FDE overrides.
