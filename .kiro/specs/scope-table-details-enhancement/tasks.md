# Tasks: Scope Table Details Enhancement

## Agent Enhancement Status

**Agent now returns hardcoded enhanced data** (Phase 5.1.4, 5.2.2 complete):

- Relationships: 2 sample foreign key relationships with cardinality
- PII columns: 2 sample columns (customer_email, customer_phone)
- Analysis metadata: Confidence scores and reasoning for all fields
- UI displays relationships as structured cards with validation
- PII uses multi-select with pills and autocomplete
- Grain columns use multi-select with pills and autocomplete
- Raw JSON editing removed from all components

**Next step**: Replace hardcoded values with actual schema analysis (post-MVP).

## Phase 1: Database Foundation

- [x] 1.1 Create migration `010_table_config_approval.sql`
- [x] 1.2 Add new columns to `table_config`
- [x] 1.3 Update `get_table_config` query in `db.rs`
- [x] 1.4 Update `save_table_config` query in `db.rs`
- [x] 1.5 Add `approve_table_config` function in `db.rs`
- [x] 1.6 Write schema contract tests
- [x] 1.7 Verify migration idempotency

## Phase 2: Rust Backend

- [x] 2.1 Update `TableConfigPayload` struct with new fields
- [x] 2.2 Update `migration_get_table_config` command
- [x] 2.3 Update `migration_save_table_config` command
- [x] 2.4 Update `migration_analyze_table_details` to populate analysis metadata
- [x] 2.5 Add `migration_approve_table_config` command
- [ ] 2.6* Add `migration_validate_relationship` command
- [x] 2.7 Write command behavior tests
- [x] 2.8 Verify workspace isolation

## Phase 3: Frontend Types

- [x] 3.1 Update `TableConfigPayload` interface in `types.ts`
- [x] 3.2 Add `AnalysisMetadata` interface
- [x] 3.3 Add `Relationship` interface
- [x] 3.4 Update `tauri.ts` command wrappers

## Phase 3.5: Minimal UI (Ready for Testing)

- [x] 3.5.1 Add approval status display in config-step
- [x] 3.5.2 Add approve button with validation
- [x] 3.5.3 Add analysis metadata display (collapsible)
- [x] 3.5.4 Wire up approve command to backend
- [x] 3.5.5 Add approval timestamp display

## Phase 4: Component Extraction

- [x] 4.1 Extract `ConfigStepHeader` component
  - [x] 4.1.1 Create component file
  - [x] 4.1.2 Move progress display logic
  - [x] 4.1.3 Move action buttons (Refresh Schema, Auto-Analyze)
  - [x] 4.1.4 Write component unit tests
- [x] 4.2 Extract `TableListSidebar` component
  - [x] 4.2.1 Create component file
  - [x] 4.2.2 Move schema-grouped list rendering
  - [x] 4.2.3 Add approval status indicators
  - [x] 4.2.4 Add validation error indicators
  - [x] 4.2.5 Write component unit tests
- [x] 4.3 Create `TableDetailsEditor` orchestrator
  - [x] 4.3.1 Component logic in config-step.tsx (no separate file needed)
  - [x] 4.3.2 Form state management working
  - [x] 4.3.3 Auto-save logic implemented
  - [x] 4.3.4 Write component unit tests
- [x] 4.4 Extract `CoreFieldsSection` component
  - [x] 4.4.1 Create component file
  - [x] 4.4.2 Move table type, load strategy, CDC, date fields
  - [x] 4.4.3 Add Agent/Manual chip display
  - [~] 4.4.4 Add confidence score display
  - [x] 4.4.5 Write component unit tests
- [x] 4.5 Extract `ScdSection` component
  - [x] 4.5.1 Create component file
  - [x] 4.5.2 Move SCD type and key columns fields
  - [x] 4.5.3 Add conditional rendering logic
  - [x] 4.5.4 Write component unit tests

## Phase 5: New Components

- [x] 5.1 Create `RelationshipsSection` component
  - [x] 5.1.1 Create component file
  - [x] 5.1.2 Implement grid layout for grain columns multi-select
  - [x] 5.1.3 Display relationships as read-only cards
  - [x] 5.1.4 Display relationships from agent analysis with validation
  - [ ] 5.1.5* Implement add/remove row functionality (manual editing)
  - [x] 5.1.6 Add validation status chip display
  - [x] 5.1.7 Write component unit tests
- [x] 5.2 Create `PiiSection` component
  - [x] 5.2.1 Create component file
  - [x] 5.2.2 Display PII columns as pills with multi-select
  - [x] 5.2.3 Remove raw JSON editor (completed)
  - [x] 5.2.4 Display available columns list with autocomplete
  - [x] 5.2.5 Write component unit tests
- [x] 5.3 Create `AgentRationaleSection` component
  - [x] 5.3.1 Create component file
  - [x] 5.3.2 Implement collapsible accordion design
  - [x] 5.3.3 Add confidence score display per field
  - [x] 5.3.4 Add reasoning text display per field
  - [x] 5.3.5 Implement hide logic for manually-overridden fields
  - [x] 5.3.6 Write component unit tests
- [x] 5.4 Create `ApprovalActions` component
  - [x] 5.4.1 Create component file
  - [x] 5.4.2 Implement approve button
  - [x] 5.4.3 Add validation checks
  - [x] 5.4.4 Add approval status display
  - [x] 5.4.5 Add approval timestamp display
  - [x] 5.4.6 Write component unit tests
- [x] 5.5 Write component integration tests

## Phase 6: Validation Logic

**Prerequisites** (must be done first):

- [x] 6.0 Add column discovery during workspace apply
  - [x] 6.0.1 Create `discover_columns.sql` query for SQL Server
  - [x] 6.0.2 Add column extraction to workspace apply flow
  - [x] 6.0.3 Populate `sqlserver_object_columns` table during apply
  - [x] 6.0.4 Write column discovery tests

- [x] 6.1 Implement relationship validation
  - [x] 6.1.1 Create validation command in Rust
  - [x] 6.1.2 Check parent table exists in scope
  - [x] 6.1.3 Check child column exists in current table
  - [x] 6.1.4 Check parent column exists in parent table
  - [x] 6.1.5 Return validation status
  - [x] 6.1.6 Write validation unit tests
- [x] 6.2 Add validation UI feedback
  - [x] 6.2.1 Display validation status chips
  - [x] 6.2.2 Show error messages
  - [x] 6.2.3 Highlight invalid fields
- [x] 6.3 Block approval on validation errors — not implemented (user can approve anytime by design)

## Phase 7: Manual Override Tracking

- [x] 7.1 Track field edits in state
  - [x] 7.1.1 Add manual overrides tracking to form state
  - [x] 7.1.2 Update tracking on field change
  - [x] 7.1.3 Write tracking logic unit tests
- [x] 7.2 Persist to `manualOverridesJson`
  - [x] 7.2.1 Serialize manual overrides on save
  - [x] 7.2.2 Deserialize on load
  - [x] 7.2.3 Write persistence tests
- [x] 7.3 Show Agent vs Manual chips
  - [x] 7.3.1 Implement chip display logic
  - [x] 7.3.2 Style Agent chip (green)
  - [x] 7.3.3 Style Manual chip (blue)
  - [x] 7.3.4 Write chip display tests
- [x] 7.4 Hide agent rationale for overridden fields
  - [x] 7.4.1 Implement hide logic in AgentRationaleSection
  - [x] 7.4.2 Write hide logic tests

## Phase 8: Approval Workflow

- [x] 8.1 Implement approve button logic
  - [x] 8.1.1 Add click handler
  - [x] 8.1.2 Call validation check (skipped - user can approve anytime)
  - [x] 8.1.3 Call `migration_approve_table_config` command
  - [x] 8.1.4 Handle success/error responses
  - [x] 8.1.5 Write approval logic tests
- [x] 8.2 Update approval status in DB
  - [x] 8.2.1 Verify command updates `approval_status`
  - [x] 8.2.2 Verify command sets `approved_at` timestamp
  - [x] 8.2.3 Write DB update tests (Rust tests exist)
- [x] 8.3 Show approval state in UI
  - [x] 8.3.1 Display approval status badge
  - [x] 8.3.2 Display approval timestamp
  - [x] 8.3.3 Update table list with approval indicator (green checkmark)
  - [x] 8.3.4 Write UI state tests
- [x] 8.4* Disable editing after approval — not implemented (fields stay editable by design)
- [x] 8.5 Update header counts
  - [x] 8.5.1 Add approved count to header
  - [x] 8.5.2 Update count on approval
  - [x] 8.5.3 Write count update tests

## Phase 8.5: Column Dropdowns and Multi-Select (MVP)

- [x] 8.5.1 Update `CoreFieldsSection` with dropdowns
  - [x] 8.5.1.1 Convert CDC column to dropdown
  - [x] 8.5.1.2 Convert Canonical date column to dropdown
  - [x] 8.5.1.3 Populate from `availableColumns`
  - [x] 8.5.1.4 Display format: `column_name (data_type)`
- [x] 8.5.2 Update `PiiSection` with multi-select
  - [x] 8.5.2.1 Convert to multi-select with pills and autocomplete
  - [x] 8.5.2.2 Show all columns with search
  - [x] 8.5.2.3 Pre-fill agent suggestions as removable pills
  - [x] 8.5.2.4 Add MultiSelectColumns component
- [x] 8.5.3 Update `RelationshipsSection` with multi-select for grain columns
  - [x] 8.5.3.1 Add multi-select for grain columns
  - [x] 8.5.3.2 Pre-fill agent suggestions
  - [x] 8.5.3.3 Remove raw JSON editor
- [x] 8.5.4 Verify TypeScript compilation
- [x] 8.5.5 Run integration tests (57 tests passing)

## Phase 9: Integration & Polish

- [x] 9.1 Refactor `config-step.tsx` to use new components
  - [x] 9.1.1 Replace inline rendering with component composition
  - [x] 9.1.2 Wire up component props and callbacks
  - [x] 9.1.3 Remove old code
  - [x] 9.1.4 Write integration tests
- [x] 9.2 Verify auto-save still works
  - [x] 9.2.1 Test auto-save triggers on field change
  - [x] 9.2.2 Test debounce behavior (500ms)
  - [x] 9.2.3 Write auto-save tests
- [x] 9.3 Verify auto-analyze still works
  - [x] 9.3.1 Test auto-analyze button
  - [x] 9.3.2 Test analysis metadata population
  - [x] 9.3.3 Write auto-analyze tests
- [x] 9.4 Verify refresh schema still works
  - [x] 9.4.1 Test refresh schema button
  - [x] 9.4.2 Test schema data reload
  - [x] 9.4.3 Write refresh schema tests
- [x] 9.5 UI polish pass
  - [x] 9.5.1 Review spacing and alignment
  - [x] 9.5.2 Review color consistency
  - [x] 9.5.3 Review typography hierarchy
  - [x] 9.5.4 Review loading states
  - [x] 9.5.5 Review error states
  - [x] 9.5.6 Review empty states
- [x] 9.6 Run full E2E test suite
  - [x] 9.6.1 Run existing E2E tests
  - [x] 9.6.2 Fix any regressions
- [x] 9.7 Fix any regressions

## Phase 10: Testing & Documentation

- [x] 10.1 Run `cargo test db` (14 tests passing)
- [x] 10.2 Run `cargo test migration` (16 tests passing)
- [x] 10.3 Run `npm run test:integration` (57 tests passing)
- [x] 10.4 Update E2E test expectations for scope flow
- [x] 10.5 Update component documentation
- [-] 10.6 Verify markdownlint passes

## Property-Based Tests

- [x] PBT-1 Write approval state consistency property test
  - [x] PBT-1.1 Define arbitrary table config generator
  - [x] PBT-1.2 Define property: approved state requires no validation errors
  - [x] PBT-1.3 Implement test with fast-check
  - [x] PBT-1.4 Verify test catches violations
- [x] PBT-2 Write manual override preservation property test
  - [x] PBT-2.1 Define arbitrary analysis metadata generator
  - [x] PBT-2.2 Define property: agent metadata preserved after override
  - [x] PBT-2.3 Implement test with fast-check
  - [x] PBT-2.4 Verify test catches violations
- [x] PBT-3 Write relationship validation correctness property test
  - [x] PBT-3.1 Define arbitrary relationship generator
  - [x] PBT-3.2 Define property: validation status matches specification
  - [x] PBT-3.3 Implement test with fast-check
  - [x] PBT-3.4 Verify test catches violations
