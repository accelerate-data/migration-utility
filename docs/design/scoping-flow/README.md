# Scoping Flow

## Scope

Defines the two-step scoping flow: selecting tables from the source warehouse and configuring
migration metadata for each selected table.

## User Journey

The scoping flow is the first substantive step after workspace setup. The user selects which
tables to migrate, then provides the metadata the migration agent needs to generate correct dbt
models.

Two sub-steps, accessed via the scope step nav:

1. **Select** — choose tables from the discovered source schema
2. **Table Config** — review agent analysis and configure each selected table

The flow is non-blocking: users can move between steps freely and return to edit at any time
until scope is finalized.

## Step 1: Table Selection

The user browses the source warehouse schema (grouped by schema name) and marks tables for
migration. Only selected tables proceed to Table Config and downstream planning.

Refresh Schema re-runs workspace apply to pick up schema changes and reconciles the selection
state (keeps valid selections, removes tables that no longer exist).

## Step 2: Table Config

Master-detail layout. Left panel lists selected tables grouped by schema. Right panel shows the
config form for the active table.

### Agent Analysis

On first load, the app auto-triggers agent analysis for each table. The agent inspects the table
schema and infers:

- Table type (fact / dimension / unknown)
- Load strategy (incremental / full refresh / snapshot)
- CDC column (incremental watermark)
- Canonical date column
- Grain columns
- Relationships (foreign key edges with cardinality)
- PII columns

Analysis results are saved to the database and pre-fill the form. The user can re-trigger
analysis at any time with "Analyze again".

### Form Fields

| Field | Input type | Notes |
|---|---|---|
| Table type | Dropdown | fact, dimension, unknown |
| Load strategy | Dropdown | incremental, full\_refresh, snapshot |
| CDC column | Dropdown | populated from discovered columns |
| Canonical date column | Dropdown | populated from discovered columns |
| PII columns | Multi-select pills | searchable, removable |
| Grain columns | Multi-select pills | searchable, removable |
| Relationships | Read-only cards | from agent analysis, validated in real-time |
| Snapshot strategy | Dropdown | dimensions only |

### Agent vs Manual Provenance

Each field tracks whether its current value came from the agent or was manually edited by the
user. An **Agent** chip (seafoam) or **Manual** chip (pacific) appears next to each populated
field. Editing any field marks it as manual; the agent metadata is preserved for audit.

### Agent Analysis Rationale

A collapsible accordion below the form shows the agent's reasoning and confidence score for each
inferred field. Fields the user has manually overridden are hidden from the rationale panel.

### Relationship Validation

Relationships from agent analysis are validated in real-time:

- Parent table must exist in the selected tables list
- Child column must exist in the current table's discovered columns
- Parent column must exist in the parent table's discovered columns

Validation status (Valid / Invalid) is shown on each relationship card. Invalid relationships
surface an error count badge in the table list sidebar. Validation errors do not block approval.

### Auto-save

Every field change triggers a 500ms debounced save. No explicit save button. `confirmed_at` is
stamped on each save and gates display of the approval panel.

### Approval

After first analysis, an approval panel appears below the form. The user clicks **Approve
Configuration** to mark the table as reviewed. Approval is always available regardless of
validation state — the user decides when a table is ready.

Approved tables show a seafoam ✓ in the sidebar. The header tracks approved count alongside
configured count.

Scope is finalized from the header via **Finalize Scope**, which locks the flow and advances the
app to the Plan phase.

## Data Model

Configuration state lives in `table_config` (one row per selected table):

| Column | Purpose |
|---|---|
| `table_type`, `load_strategy`, etc. | Core migration metadata |
| `analysis_metadata_json` | Agent inference data (confidence, reasoning per field) |
| `manual_overrides_json` | Field names the user has manually edited |
| `approval_status` | `pending` or `approved` |
| `approved_at` | ISO-8601 timestamp of approval |
| `grain_columns` | Comma-separated grain column names |
| `relationships_json` | JSON array of relationship objects |
| `pii_columns` | JSON array of PII column names |

Column metadata (for dropdowns and validation) is discovered during workspace apply and stored in
`sqlserver_object_columns`.

## Constraints

- Scope and Table Config are read-only in `running_locked` phase.
- Scope is read-only after `scope_finalized = true`.
- Refresh Schema is disabled while locked.

## Implementation References

- Route: `app/src/routes/scope/`
- Components: `app/src/components/scope/`
- Tauri commands: `app/src-tauri/src/commands/migration.rs`
- DB schema: `app/src-tauri/migrations/`
- UI patterns: `docs/design/ui-patterns/table-config.md`
