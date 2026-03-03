# UI Design — Surfaces, Flows, and Patterns

Research-backed design for the Migration Utility Tauri app. Covers the four surfaces the FDE moves through, the journeys between them, and the screen-level patterns for each view.

**Stack:** React 19, TanStack Router, TanStack Table, Zustand, shadcn/ui, Tailwind 4, Lucide icons.

---

## Surfaces

Four surfaces reachable from the left icon nav at any time:

| Surface | Icon | Purpose |
|---------|------|---------|
| **Home** | House | Migration status at a glance. Three states: Setup required / Ready (wizard progress summary) / Active (pipeline running). Entry point that routes the FDE to wherever they need to go next. |
| **Scope** | Grid | Three-step sidebar wizard: Scope → Candidacy → Table Config. Freely navigable before launch; read-only after. |
| **Monitor** | Waveform | Two states: Ready (pre-launch summary + Launch button) / Running (agent phases grid, progress bar, log stream). |
| **Settings** | Gear | Four tabs: Connections (one-time) · Workspace (per-migration, locked during run) · Reset (destructive) · Usage (cost and token tracking). |

---

## User Flows

### Flow 1 — First launch

**Trigger:** App opens with no prior state.

Home shows a "Setup required" empty state with a single CTA: *Go to Settings*.

**Settings → Connections** (one-time, safe to update at any time):

1. Authenticate with GitHub — used to clone and push to the migration repo.
2. Add the Anthropic API key — used by headless pipeline agents during execution.

**Settings → Workspace** (set once per migration, locked after launch):

1. Enter Fabric workspace URL and service principal credentials (DEC-03).
2. Select or create the migration repo on GitHub — all utility work happens here; the production repo is never touched (DEC-05).
3. Set the local working directory. Default: `~/migration-utility`.

The app data directory (`~/.vibedata/migration-utility`) is hardcoded — stores `.claude` context, agent logs, and run history.

Once Connections and Workspace are both configured, Home transitions to the Ready state.

---

### Flow 2 — Migration setup

**Trigger:** Home is in the Ready state. FDE clicks the Scope icon.

State autosaves to SQLite on every change — the FDE can close the app between domain owner conversations and resume exactly where they left off.

**Step 1 — Scope** · *[scope.md](scope.md)*

FDE selects which tables belong to this domain migration. Tables are grouped by schema with per-schema select-all. The utility traces each selected table back to its producing stored procedure via ADF pipeline definitions and write-target SQL patterns (DEC-10) — domain owners think in tables, not code artifacts.

**Step 2 — Candidacy** · *[candidacy.md](candidacy.md)*

A candidacy agent classifies each resolved stored procedure into one of three tiers (DEC-11):

| Tier | Criteria | What the FDE does |
|------|----------|-------------------|
| **Migrate** | >70% SQL-expressible; no blocking patterns | Nothing — utility handles it automatically |
| **Review** | 40–70%; some blocking patterns | Migrate manually in parallel |
| **Reject** | <40%; blocking patterns dominate | Migrate manually in parallel |

Filterable by tier and schema. Expanding a row shows the agent's reasoning. FDE can override any classification via a Sheet drawer — overridden rows show a pencil icon.

**Step 3 — Table Config** · *[table-config.md](table-config.md)*

Master-detail split: table list left, config form right. Agent pre-populates all fields from candidacy analysis — pre-filled fields show a blue left border ("AI suggested"). Per table:

- **Table type:** Fact / Dimension / Other — drives snapshot strategy
- **Snapshot strategy:** 1-day sample (facts) or full copy (dimensions) (DEC-16)
- **Incremental column:** Inferred from stored procedure SQL; FDE confirms
- **PII columns:** Agent-recommended; masked before fixture generation (DEC-15, DEC-16)

Each table requires an explicit "Confirm" click. Once all tables are confirmed, the FDE navigates to Monitor.

---

### Flow 3 — Launch and monitor

**Trigger:** All tables confirmed in Table Config. FDE navigates to Monitor.

*[launch-monitor.md](launch-monitor.md)*

**Monitor — Ready state:** Pre-launch summary shows procedure and table counts by tier. "Launch migration" writes config to `plan.md`, commits to the migration repo, and triggers the GitHub Actions workflow (DEC-19). On launch:

- Scope surface locks — all three steps go read-only behind an amber banner.
- Monitor transitions to the Running state.

**Monitor — Running state:** The orchestrator reads `plan.md`, builds a dependency graph of Migrate-tier procedures, and spawns sub-agents in parallel (DEC-13). Shows:

- Progress bar: procedures complete vs total.
- Agent phases grid: Discovery → Candidacy → Translation → Tests → Validation.
- Log stream: real-time output from agents via Tauri events.

**BLOCKED procedures:** Models depending on an unresolved Review/Reject upstream are marked `BLOCKED` in `plan.md`. The FDE migrates the upstream manually, marks it `RESOLVED`, and relaunches — the orchestrator resumes without re-running completed procedures (DEC-12, DEC-13).

**Completion:** When all Migrate-tier procedures pass `dbt test` and `dbt build` against the production snapshot (DEC-14), the utility pushes a branch to the production repo. The FDE opens a standard PR. UAT runs in Vibedata's CI/CD — the utility does not own this step (DEC-17, DEC-08).

---

### Flow 4 — Session resumption

**Trigger:** FDE closes the app mid-setup or mid-run.

- **Mid-setup:** Home shows Ready with wizard progress summary. Scope surface restores from SQLite.
- **Mid-run:** Home shows Active with last known procedure count from `plan.md`. Monitor shows current state — real-time events resume once the GitHub Actions workflow reconnects.

SQLite owns setup state. `plan.md` owns execution state. Both are independent of the app process lifecycle (DEC-19).

---

### Flow 5 — Reset

**Trigger:** Wrong scope, wrong workspace, or post-failure clean slate.

**Settings → Reset** is a single destructive action. Clears:

- Migration repo branch (abandoned)
- Local working directory (deleted)
- All scope, candidacy, and table config selections
- Workspace configuration (Fabric URL, SP credentials, migration repo, working directory)

Kept: GitHub connection, Anthropic API key — reusable across migrations.

After reset, Home returns to the Ready state. FDE reconfigures Workspace and starts setup again.

---

## Constraints in the UI

| Constraint | Where it surfaces |
|------------|-------------------|
| One migration at a time (DEC-04) | Home shows one active migration or the setup wizard, never a list |
| Table-first scope selection (DEC-10) | Scope step picks tables; stored procedure discovery is automatic |
| FDE can override candidacy tiers | Sheet drawer in Candidacy step; pencil icon on overridden rows |
| Workspace locked during migration | Settings → Workspace fields disabled; amber lock badge |
| Scope locks after launch | Amber banner on Scope surface; all fields non-interactive |
| Review/Reject manual track runs in parallel | Candidacy shows all tiers; BLOCKED in Monitor resolves when FDE marks upstream RESOLVED |
| PII masking before fixtures | Table Config PII column confirmation required before launch |

## Interaction Rules

- Scope lock after launch: Scope remains navigable, but all three steps (Scope, Candidacy, Table Config) are read-only behind an amber lock banner until run completion or cancellation.
- Settings lock behavior: Connections remains editable at all times. Workspace fields are disabled while a run is active.
- Scope forward navigation: Block only when required state is missing (for example zero tables selected). Use inline validation; do not use blocking modals for this gate.
- Candidacy override: Override reason is required. Save stays disabled until reason is non-empty. Persist overrides only on explicit save action (no debounced writes).
- Table confirmation gate: "Confirm table" is an explicit user action that writes `confirmed_at`. Agent pre-population does not imply confirmation. Launch is blocked until all included tables are confirmed.

---

## Screen Patterns

- [Stepper / Root Layout](stepper.md) — left sidebar vertical stepper, step states, save-and-resume
- [Scope Selection](scope.md) — schema-grouped table list with per-group select-all
- [Candidacy Review](candidacy.md) — sortable/filterable table, tier badges, inline expansion, Sheet override drawer
- [Table Config](table-config.md) — master-detail split, card-based form sections, agent suggestion indicators
- [Launch Monitor](launch-monitor.md) — Airflow-style agent grid, log stream, partial failure handling
- [Usage](usage.md) — Settings → Usage tab: cost summary cards, bar charts by table/phase, time series, expandable run history

Interactive mockups: [home-mockup.html](home-mockup.html), [offline-stage-gated-mockup.html](offline-stage-gated-mockup.html)

---

## Sources

- [Airbyte Schema Configuration](https://docs.airbyte.com/platform/using-airbyte/configuring-schema)
- [Fivetran Connection Schemas](https://fivetran.com/docs/using-fivetran/fivetran-dashboard/connectors/schema)
- [dbt Cloud Run Visibility](https://docs.getdbt.com/docs/deploy/run-visibility)
- [Azure SQL Migration Extension](https://learn.microsoft.com/en-us/azure-data-studio/extensions/azure-sql-migration-extension)
- [UI Patterns for Async Workflows — LogRocket](https://blog.logrocket.com/ui-patterns-for-async-workflows-background-jobs-and-data-pipelines)
- [Wizard UI Pattern — Eleken](https://www.eleken.co/blog-posts/wizard-ui-pattern-explained)
- [Data Table Design UX Patterns — Pencil & Paper](https://www.pencilandpaper.io/articles/ux-pattern-analysis-enterprise-data-tables)
- [Airflow UI Overview](https://airflow.apache.org/docs/apache-airflow/stable/ui.html)
- [GitHub Actions Workflow Visualization](https://docs.github.com/actions/managing-workflow-runs/using-the-visualization-graph)
- [PatternFly Filters Design Guidelines](https://www.patternfly.org/patterns/filters/design-guidelines/)
- [Master-Detail Pattern — Medium](https://medium.com/@lucasurbas/case-study-master-detail-pattern-revisited-86c0ed7fc3e)
