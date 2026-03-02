# Table Config

Route: `/scope/config` — Scope Step 2

## Pattern

**Master-detail split layout.** Table list on the left (300px fixed), card-based form sections on
the right. Modelled on Azure Data Studio's migration wizard.

## Component Architecture

```text
ConfigStep (orchestrator)
├── ConfigStepHeader        — progress counts, Refresh Schema, Finalize Scope, tab nav
├── TableListSidebar        — schema-grouped list, approval indicators, validation badges
└── Detail panel (right)
    ├── CoreFieldsSection   — table type, load strategy, CDC column, canonical date column
    ├── PiiSection          — multi-select pills for PII columns
    ├── RelationshipsSection — grain columns multi-select + read-only relationship cards
    ├── ScdSection          — snapshot strategy (dimensions only)
    └── Agent Analysis & Approval panel (shown after first analysis)
        ├── AgentRationaleSection — collapsible accordion, confidence scores, reasoning
        └── ApprovalActions       — approve button, approval status, timestamp
```

## Layout

```text
┌─────────────────────────┬─────────────────────────────────────────┐
│ dbo (3 selected)        │ dbo.fact_sales                          │
│  fact_sales      ✓      │ Migration metadata required for build.  │
│  dim_customer    2      │                          [Analyze again] │
│  dim_product            │                                         │
│                         │ Table type:    [Fact ▼]        [Agent]  │
│                         │ Load strategy: [Incremental ▼] [Agent]  │
│                         │ CDC column:    [load_date ▼]   [Manual] │
│                         │ Date column:   [sale_date ▼]   [Agent]  │
│                         │                                         │
│                         │ PII columns:                            │
│                         │ [customer_email ×] [customer_phone ×]   │
│                         │                                         │
│                         │ Grain columns:                          │
│                         │ [order_id ×] [sale_date ×]              │
│                         │                                         │
│                         │ Relationships:                          │
│                         │ ┌─ Relationship 1 ──── ✓ Valid ───────┐ │
│                         │ │ child: customer_id                  │ │
│                         │ │ parent: dbo.dim_customer.id         │ │
│                         │ └─────────────────────────────────────┘ │
│                         │                                         │
│                         │ ▶ Agent Analysis Rationale              │
│                         │                                         │
│                         │ ┌─ Approval ──────────────────────────┐ │
│                         │ │ Pending approval  [Approve Config]  │ │
│                         │ └─────────────────────────────────────┘ │
└─────────────────────────┴─────────────────────────────────────────┘
```

## Left Panel — Table List

Each row shows the table name (`font-mono text-xs`) and two optional indicators:

| Indicator | Meaning |
|-----------|---------|
| Seafoam `✓` | `approval_status = 'approved'` |
| Red badge with count | Validation errors on relationships |

Active row gets `bg-primary/10`. Loading state shows a `Loader2` spinner.

## Right Panel — Form Sections

### CoreFieldsSection

Dropdowns for `tableType`, `loadStrategy`, `incrementalColumn` (CDC column), and `dateColumn`
(canonical date column). Each field with a value shows an **Agent** (seafoam) or **Manual**
(pacific) chip based on `manualOverridesJson`. Column dropdowns (`incrementalColumn`,
`dateColumn`) populate from `availableColumns` discovered during workspace apply, sorted
alphabetically and displayed as `column_name (data_type)`.

### PiiSection

`MultiSelectColumns` component — searchable autocomplete input with removable pills. Stores as
JSON array in `piiColumns`.

### RelationshipsSection

Two sub-sections:

- **Grain columns** — `MultiSelectColumns`, stored as comma-separated string in `grainColumns`
- **Relationships** — read-only cards from agent analysis (`relationshipsJson`), each validated
  in real-time via `migrationValidateRelationship`. Validation status shown as Valid (seafoam)
  or Invalid (destructive) chips with per-error detail text. Relationships are agent-supplied
  only; manual add/remove is not implemented (post-MVP).

### ScdSection

Single `snapshotStrategy` dropdown (`sample_1day`, `full`, `full_flagged`), disabled unless
`tableType === 'dimension'`.

### AgentRationaleSection

Collapsible `Accordion` (shadcn/ui). `analysisMetadataJson` is a flat JSON object keyed by
field name, each value having `{ value, confidence, reasoning }`. One accordion item per field.
Shows confidence score badge (seafoam ≥ 80%, pacific ≥ 60%) and reasoning text. Fields listed
in `manualOverridesJson` are hidden.

### ApprovalActions

Shown only when `confirmedAt` is set (i.e., after first save/analysis). Approve button calls
`migrationApproveTableConfig`. After approval shows timestamp and seafoam `✓ Approved` badge.
Validation error count displayed with `AlertCircle` icon. Approval is never blocked — users
can approve at any time regardless of validation state.

## Agent/Manual Chip Logic

`manualOverridesJson` is a JSON array of field names the user has edited. On every field change,
`updateDraft` appends the field name to this array. Chips are rendered by `CoreFieldsSection`
based on whether the field name is in the array.

## Autosave

500ms debounce on every field change. `confirmedAt` is set to `now()` on each save. No explicit
save button.

## Validation

Relationship validation runs automatically when `relationshipsJson` changes. Results are stored
in component state and reported to `ConfigStep` via `onValidationChange`. The sidebar shows error
counts; `ApprovalActions` shows the total. Validation errors do not block approval.

## State Indicators

Follows `.claude/rules/frontend-design.md` state indicator conventions:

| State | Color | Usage |
|-------|-------|-------|
| Approved | `var(--color-seafoam)` | Sidebar checkmark, approval badge |
| Agent-inferred | `var(--color-seafoam)` | Agent chip background |
| Manual override | `var(--color-pacific)` | Manual chip background |
| Validation error | `text-destructive` / `bg-destructive/15` | Error badges, error messages |
| Loading | `Loader2 animate-spin` | Sidebar loading state |

## Components

| Component | Use |
|-----------|-----|
| `MultiSelectColumns` | PII columns, grain columns (searchable pills) |
| `Accordion` | Agent rationale collapsible |
| `Badge` | Agent/Manual chips |
| `Button` | Approve, Analyze again, Refresh schema, Finalize scope |
| `AlertCircle` | Error state icons |
| `Loader2` | Loading state icon |

## References

- Implementation: `app/src/routes/scope/config-step.tsx`
- Components: `app/src/components/scope/`
- DB schema: `app/src-tauri/migrations/010_table_config_approval.sql`
