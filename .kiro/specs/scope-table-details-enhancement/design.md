# Design: Scope Table Details Enhancement

## Architecture Overview

This feature enhances the Scope → Table Details UI by decomposing the monolithic `config-step.tsx` into composable components and adding database support for approval workflow and manual override tracking.

## Database Schema Changes

**IMPORTANT**: This change follows the protocol in `.claude/rules/db-schema-change.md`.

### Data Ownership Decision

**Field/behavior**: Agent analysis metadata, approval workflow state, manual override tracking

**Chosen table**: `table_config`

**Rationale**: These fields describe metadata about the table configuration itself. They are 1:1 with `selected_tables`, workspace-scoped via FK chain, and represent configuration state (not source data or usage logs).

**Alternatives considered**:
- Separate `table_approvals` table: Rejected because approval state is tightly coupled to config lifecycle
- Separate `analysis_metadata` table: Rejected because metadata is always loaded/saved with config

**Workspace scope**: Workspace-scoped via FK chain through `selected_tables` → `workspaces`

### Table: `table_config`

**New columns**:
- `analysis_metadata_json TEXT` — stores agent inference data (confidence scores, reasoning)
- `approval_status TEXT CHECK(approval_status IN ('pending','approved','needs_review'))` — tracks approval state
- `approved_at TEXT` — ISO-8601 timestamp of approval
- `manual_overrides_json TEXT` — tracks which fields user manually edited

**Migration**: `app/src-tauri/migrations/010_table_config_approval.sql`

**FK policy**: Inherits `ON DELETE CASCADE` from `selected_tables` FK (config is deleted when table is removed from scope)

### Schema Design Decisions

1. **JSON columns for metadata**: Use TEXT columns with JSON content rather than separate tables because metadata structure may evolve and is always loaded/saved together with config

2. **Approval status enum**: Use CHECK constraint for type safety at DB level

3. **Nullable timestamps**: `approved_at` is NULL until approval occurs

4. **Parameterized queries**: All writes use bound parameters (`?1`, `?2`, `params![...]`) per `.claude/rules/coding-conventions.md`

## Component Architecture

### Current State
Monolithic `config-step.tsx` (~800 lines) with inline form rendering.

### Proposed Architecture

```
ConfigStep (orchestrator)
├── ConfigStepHeader (progress, actions, tabs)
├── div.grid (40/60 split)
│   ├── TableListSidebar (schema-grouped list)
│   └── TableDetailsEditor (main form)
│       ├── CoreFieldsSection (table type, load strategy, CDC, date)
│       ├── RelationshipsSection (grid editor with validation)
│       ├── PiiSection (checkbox multi-select)
│       ├── ScdSection (SCD fields for dimensions)
│       └── AgentRationaleSection (collapsible evidence)
└── ApprovalActions (approve button with validation)
```

### Component Responsibilities

**Naming convention**: All component files use `kebab-case` per `.claude/rules/coding-conventions.md`

**ConfigStep** (orchestrator):
- Manages workspace state
- Handles table selection
- Coordinates auto-save
- Delegates rendering to child components
- Logs key state transitions per `.claude/rules/logging-policy.md`

**ConfigStepHeader**:
- Shows progress (X of Y tables configured)
- Provides action buttons (Refresh Schema, Auto-Analyze)
- Renders tabs (if needed)

**TableListSidebar**:
- Groups tables by schema
- Shows approval status per table (uses state indicator colors from `.claude/rules/frontend-design.md`)
- Highlights selected table
- Shows validation error indicators

**TableDetailsEditor**:
- Orchestrates form sections
- Manages form state
- Triggers auto-save on changes
- Passes analysis metadata to sections

**CoreFieldsSection**:
- Table type dropdown
- Load strategy dropdown
- Incremental column input
- Date column input
- Shows Agent/Manual chips (colors per `.claude/rules/frontend-design.md`)
- Displays confidence scores

**RelationshipsSection**:
- Grid layout with add/remove rows
- Dropdowns for child column, parent table, parent column, cardinality
- Inline validation status chips (uses AD brand colors)
- Real-time validation

**PiiSection**:
- Checkbox multi-select for columns (shadcn/ui `Checkbox` component)
- Visual pill display of selected columns (`rounded-full` badges)
- Available columns list from schema

**ScdSection**:
- SCD type dropdown
- SCD key columns (for type 2)
- Conditional rendering based on table type

**AgentRationaleSection**:
- Collapsible accordion (shadcn/ui `Collapsible`)
- Confidence score display per field
- Reasoning text per field
- Hidden for manually-overridden fields

**ApprovalActions**:
- Approve button (shadcn/ui `Button`)
- Validation status display
- Approval timestamp display
- Disabled state when validation errors exist

## Data Flow

### 1. Load Flow
```
User selects table
  → ConfigStep calls migration_get_table_config(workspace_id, table_id)
  → Rust command queries DB
  → Returns TableConfigPayload with analysis_metadata_json, approval_status, etc.
  → ConfigStep updates state
  → Child components render with data
```

### 2. Edit Flow
```
User edits field
  → Component calls onChange handler
  → ConfigStep updates local state
  → Tracks field in manualOverridesJson
  → Debounced auto-save (500ms)
  → Calls migration_save_table_config(workspace_id, table_id, payload)
  → Rust command updates DB
```

### 3. Validation Flow
```
User edits relationship
  → RelationshipsSection validates in real-time
  → Checks parent table exists in scope
  → Checks column exists in schema (if available)
  → Updates validation status
  → Blocks approval if errors exist
```

### 4. Approval Flow
```
User clicks Approve
  → ApprovalActions validates all fields
  → If errors exist, show error message and block
  → If valid, call migration_approve_table_config(workspace_id, table_id)
  → Rust command updates approval_status = 'approved', approved_at = now()
  → UI updates to show approval state
  → Table list shows green checkmark
```

## Type Definitions

### Frontend Types

**Location**: `app/src/types.ts` (per project conventions)

**Naming**: `PascalCase` for interfaces, `camelCase` for properties per `.claude/rules/coding-conventions.md`

```typescript
export interface AnalysisMetadata {
  tableType?: {
    value: string;
    confidence: number;
    reasoning: string;
  };
  loadStrategy?: {
    value: string;
    confidence: number;
    reasoning: string;
  };
  incrementalColumn?: {
    value: string;
    confidence: number;
    reasoning: string;
  };
  dateColumn?: {
    value: string;
    confidence: number;
    reasoning: string;
  };
  relationships?: Array<{
    childColumn: string;
    parentTable: string;
    parentColumn: string;
    cardinality: string;
    confidence: number;
    reasoning: string;
  }>;
}

export interface Relationship {
  childColumn: string;
  parentTable: string;
  parentColumn: string;
  cardinality: 'many_to_one' | 'one_to_one';
  validationStatus: 'valid' | 'type_mismatch' | 'not_found' | 'pending';
}

export interface TableConfigPayload {
  // Existing fields
  tableId: number;
  tableType: string | null;
  loadStrategy: string | null;
  incrementalColumn: string | null;
  dateColumn: string | null;
  relationships: string | null; // JSON string
  piiColumns: string | null; // JSON string
  scdType: string | null;
  scdKeyColumns: string | null; // JSON string
  
  // New fields
  analysisMetadata: AnalysisMetadata | null;
  approvalStatus: 'pending' | 'approved' | 'needs_review';
  approvedAt: string | null; // ISO-8601
  manualOverrides: string[]; // field names that were manually edited
}
```

### Rust Types

**Location**: `app/src-tauri/src/commands/migration.rs` or `app/src-tauri/src/types.rs`

**Error handling**: Use `thiserror` per `.claude/rules/rust-backend.md`

```rust
#[derive(Debug, Serialize, Deserialize)]
pub struct TableConfigPayload {
    pub table_id: i64,
    pub table_type: Option<String>,
    pub load_strategy: Option<String>,
    pub incremental_column: Option<String>,
    pub date_column: Option<String>,
    pub relationships: Option<String>,
    pub pii_columns: Option<String>,
    pub scd_type: Option<String>,
    pub scd_key_columns: Option<String>,
    
    // New fields
    pub analysis_metadata: Option<String>, // JSON string
    pub approval_status: String,
    pub approved_at: Option<String>,
    pub manual_overrides: Option<String>, // JSON string
}
```

## Validation Logic

### Relationship Validation

**Algorithm**:
1. Parse relationship from form
2. Check parent table exists in `selected_tables` for current workspace
3. If schema metadata available:
   - Check child column exists in current table schema
   - Check parent column exists in parent table schema
   - Check data types are compatible
4. Return validation status: `valid`, `type_mismatch`, `not_found`, or `pending`

**Implementation Location**: 
- Frontend: `app/src/lib/validation.ts` (client-side validation)
- Backend: `app/src-tauri/src/commands/migration.rs` (optional server-side validation)

### Approval Validation

**Rules**:
- All required fields must be filled
- All relationships must have `valid` status
- No validation errors exist

**Implementation**: `ApprovalActions` component checks validation before calling approve command.

## Manual Override Tracking

### Tracking Strategy

1. **Initial Load**: `manualOverrides` is empty array
2. **User Edit**: When user changes a field, add field name to `manualOverrides` array
3. **Persistence**: Save `manualOverrides` to `manual_overrides_json` column on auto-save
4. **Display**: Check if field name is in `manualOverrides` array to show Agent vs Manual chip

### Edge Cases

- **Agent re-analysis after manual edit**: Manual override flag persists, user's value takes precedence
- **User edits to match agent value**: Still marked as manual override (intent matters)
- **Clear override**: Provide "Reset to Agent Value" button (future enhancement)

## Testing Strategy

**Test discipline**: Follow `.claude/rules/testing.md` and `AGENTS.md` testing guidelines.

### Unit Tests

**Location**: `app/src/__tests__/lib/` for validation logic, `app/src/__tests__/components/` for component tests

**Validation Logic** (`app/src/lib/validation.ts`):
- Test relationship validation with various inputs
- Test approval validation rules
- Test manual override tracking

**Component Rendering** (`app/src/__tests__/components/`):
- Test each component renders correctly with different states
- Test Agent/Manual chip display logic
- Test approval button disabled state

### Integration Tests

**Location**: `app/src/__tests__/pages/`

**Form Interactions** (`app/src/__tests__/pages/config-step.test.tsx`):
- Test form field updates trigger auto-save
- Test relationship editor CRUD operations
- Test PII selection interactions
- Test approval button with validation

**Test runner**: `npm run test:integration` per `AGENTS.md`

### E2E Tests

**Location**: `app/e2e/workspace/`

**Full Workflow** (`app/e2e/workspace/config-approval.spec.ts`):
- Select table → analyze → edit → approve
- Manual override: agent value → user edit → manual chip shown
- Blocking validation: invalid relationship → approve disabled
- Refresh schema preserves approval state

**Test runner**: Playwright per `AGENTS.md`

### Rust Tests

**Location**: Inline `#[cfg(test)]` in command files per `.claude/rules/rust-backend.md`

**Database tests** (`app/src-tauri/src/db.rs`):
- Schema contract tests for new columns
- Migration idempotency tests
- FK constraint tests

**Command tests** (`app/src-tauri/src/commands/migration.rs`):
- Test `migration_approve_table_config` command
- Test workspace isolation
- Use `db::open_in_memory()` for test DB

**Test runner**: `cargo test --manifest-path app/src-tauri/Cargo.toml db` per `AGENTS.md`

### Property-Based Tests

**Framework**: fast-check (TypeScript)

**Approval State Consistency**:
```typescript
fc.assert(
  fc.property(
    fc.record({
      config: arbitraryTableConfig(),
      hasValidationErrors: fc.boolean()
    }),
    ({ config, hasValidationErrors }) => {
      const canApprove = !hasValidationErrors;
      const result = attemptApproval(config, hasValidationErrors);
      return result.approved === canApprove;
    }
  )
);
```

**Manual Override Preservation**:
```typescript
fc.assert(
  fc.property(
    fc.record({
      originalMetadata: arbitraryAnalysisMetadata(),
      fieldToEdit: fc.constantFrom('tableType', 'loadStrategy', 'incrementalColumn'),
      newValue: fc.string()
    }),
    ({ originalMetadata, fieldToEdit, newValue }) => {
      const result = applyManualOverride(originalMetadata, fieldToEdit, newValue);
      return result.analysisMetadata[fieldToEdit] === originalMetadata[fieldToEdit];
    }
  )
);
```

## Migration Strategy

### Database Migration

**Protocol**: Follow `.claude/rules/db-schema-change.md` end-to-end

**File**: `app/src-tauri/migrations/010_table_config_approval.sql`

```sql
-- Add new columns to table_config
ALTER TABLE table_config ADD COLUMN analysis_metadata_json TEXT;
ALTER TABLE table_config ADD COLUMN approval_status TEXT DEFAULT 'pending' 
  CHECK(approval_status IN ('pending','approved','needs_review'));
ALTER TABLE table_config ADD COLUMN approved_at TEXT;
ALTER TABLE table_config ADD COLUMN manual_overrides_json TEXT;

-- Create index for approval status queries
CREATE INDEX idx_table_config_approval_status ON table_config(approval_status);
```

**Registration**: Add to `MIGRATIONS` array in `app/src-tauri/src/db.rs`

**Query paths to update**:
- `app/src-tauri/src/db.rs`: `get_table_config`, `save_table_config`
- `app/src-tauri/src/commands/migration.rs`: `migration_get_table_config`, `migration_save_table_config`, `migration_approve_table_config`

### Data Migration

**Existing data**: All existing `table_config` rows get `approval_status = 'pending'` by default.

**Backward compatibility**: Old code can ignore new columns; new code handles NULL values gracefully.

### Logging

**Per `.claude/rules/logging-policy.md`**:

**Rust commands**:
- `info!` on entry: `info!("migration_approve_table_config: workspace_id={} table_id={}", workspace_id, table_id)`
- `error!` on failure: `error!("migration_approve_table_config: failed: {}", e)`
- `debug!` for intermediate steps

**Frontend**:
- `console.log` for significant actions (approve, validation)
- `console.error` for failures
- Include `runId`/`requestId` for correlation

**Sensitive data**: Never log PII column values or full table contents

## Performance Considerations

### Auto-Save Debouncing

Maintain existing 500ms debounce to avoid excessive DB writes.

### Validation Performance

- Relationship validation runs on edit (not on every keystroke)
- Cache validation results to avoid redundant checks
- Validation should complete within 100ms

### Component Rendering

- Use React.memo for expensive components
- Avoid unnecessary re-renders with proper dependency arrays
- Lazy load AgentRationaleSection (only render when expanded)

## Security Considerations

**Per `.claude/rules/coding-conventions.md` and `.claude/rules/logging-policy.md`**:

- Validate all user input before saving to DB
- Sanitize JSON strings before parsing
- Use parameterized queries (`?1`, `?2`, `params![...]`) to prevent SQL injection
- Workspace isolation enforced via FK constraints
- Never log PII column values or sensitive configuration data
- Redact sensitive fields in error messages

## Future Enhancements

1. **Bulk approval**: Approve multiple tables at once
2. **Approval lock**: Make approved tables read-only
3. **Reset to agent value**: Clear manual override and restore agent inference
4. **Approval history**: Track who approved and when
5. **Validation rules engine**: Configurable validation rules per workspace
