# Tasks: Scope Table Details Enhancement

## Agent Enhancement Status

**Agent now returns hardcoded enhanced data** (Phase 5.1.4, 5.2.2 complete):

- Relationships: 2 sample foreign key relationships with cardinality
- PII columns: 2 sample columns (customer_email, customer_phone)
- Analysis metadata: Confidence scores and reasoning for all fields
- UI displays relationships as structured cards and PII as pills
- Raw JSON editing available via collapsible sections

**Next step**: Replace hardcoded values with actual schema analysis (post-MVP).

## Phase 1: Database Foundation

- [x] 1.1 Create migration `010_table_config_approval.sql`
- [x] 1.2 Add new columns to `table_config`
- [x] 1.3 Update `get_table_config` query in `db.rs`
- [x] 1.4 Update `save_table_config` query in `db.rs`
- [x] 1.5 Add `approve_table_config` function in `db.rs`
- [ ] 1.6 Write schema contract tests
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
  - [ ] 4.1.4 Write component unit tests
- [x] 4.2 Extract `TableListSidebar` component
  - [x] 4.2.1 Create component file
  - [x] 4.2.2 Move schema-grouped list rendering
  - [ ] 4.2.3 Add approval status indicators
  - [ ] 4.2.4 Add validation error indicators
  - [ ] 4.2.5 Write component unit tests
- [x] 4.3 Create `TableDetailsEditor` orchestrator
  - [x] 4.3.1 Component logic in config-step.tsx (no separate file needed)
  - [x] 4.3.2 Form state management working
  - [x] 4.3.3 Auto-save logic implemented
  - [ ] 4.3.4 Write component unit tests
- [x] 4.4 Extract `CoreFieldsSection` component
  - [x] 4.4.1 Create component file
  - [x] 4.4.2 Move table type, load strategy, CDC, date fields
  - [x] 4.4.3 Add Agent/Manual chip display
  - [ ] 4.4.4 Add confidence score display
  - [ ] 4.4.5 Write component unit tests
- [x] 4.5 Extract `ScdSection` component
  - [x] 4.5.1 Create component file
  - [x] 4.5.2 Move SCD type and key columns fields
  - [x] 4.5.3 Add conditional rendering logic
  - [ ] 4.5.4 Write component unit tests

## Phase 5: New Components

- [-] 5.1 Create `RelationshipsSection` component
  - [x] 5.1.1 Create component file
  - [x] 5.1.2 Implement grid layout
  - [x] 5.1.3 Add dropdowns for child column, parent table, parent column, cardinality
  - [x] 5.1.4 Display relationships from agent analysis
  - [ ] 5.1.5* Implement add/remove row functionality (manual editing)
  - [ ] 5.1.6* Add validation status chip display
  - [ ] 5.1.7 Write component unit tests
- [-] 5.2 Create `PiiSection` component
  - [x] 5.2.1 Create component file
  - [x] 5.2.2 Display PII columns as pills
  - [x] 5.2.3 Add collapsible raw JSON editor
  - [ ] 5.2.4* Display available columns list (manual selection)
  - [ ] 5.2.5 Write component unit tests
- [-] 5.3 Create `AgentRationaleSection` component
  - [x] 5.3.1 Create component file
  - [ ] 5.3.2 Implement collapsible accordion design
  - [x] 5.3.3 Add confidence score display per field
  - [x] 5.3.4 Add reasoning text display per field
  - [ ] 5.3.5 Implement hide logic for manually-overridden fields
  - [ ] 5.3.6 Write component unit tests
- [-] 5.4 Create `ApprovalActions` component
  - [x] 5.4.1 Create component file
  - [x] 5.4.2 Implement approve button
  - [ ] 5.4.3 Add validation checks
  - [x] 5.4.4 Add approval status display
  - [x] 5.4.5 Add approval timestamp display
  - [ ] 5.4.6 Write component unit tests
- [ ] 5.5 Write component integration tests

## Phase 6: Validation Logic

- [ ] 6.1 Implement relationship validation
  - [ ] 6.1.1 Create validation utility file
  - [ ] 6.1.2 Check parent table exists in scope
  - [ ] 6.1.3 Check data type compatibility (if schema available)
  - [ ] 6.1.4 Return validation status
  - [ ] 6.1.5 Write validation unit tests
- [ ] 6.2 Add validation UI feedback
  - [ ] 6.2.1 Display validation status chips
  - [ ] 6.2.2 Show error messages
  - [ ] 6.2.3 Highlight invalid fields
- [ ] 6.3 Block approval on validation errors
  - [ ] 6.3.1 Implement approval validation check
  - [ ] 6.3.2 Disable approve button when errors exist
  - [ ] 6.3.3 Show validation error summary

## Phase 7: Manual Override Tracking

- [x] 7.1 Track field edits in state
  - [x] 7.1.1 Add manual overrides tracking to form state
  - [x] 7.1.2 Update tracking on field change
  - [ ] 7.1.3 Write tracking logic unit tests
- [x] 7.2 Persist to `manualOverridesJson`
  - [x] 7.2.1 Serialize manual overrides on save
  - [x] 7.2.2 Deserialize on load
  - [ ] 7.2.3 Write persistence tests
- [x] 7.3 Show Agent vs Manual chips
  - [x] 7.3.1 Implement chip display logic
  - [x] 7.3.2 Style Agent chip (green)
  - [x] 7.3.3 Style Manual chip (blue)
  - [ ] 7.3.4 Write chip display tests
- [ ] 7.4 Hide agent rationale for overridden fields
  - [ ] 7.4.1 Implement hide logic in AgentRationaleSection
  - [ ] 7.4.2 Write hide logic tests

## Phase 8: Approval Workflow

- [x] 8.1 Implement approve button logic
  - [x] 8.1.1 Add click handler
  - [x] 8.1.2 Call validation check (skipped - user can approve anytime)
  - [x] 8.1.3 Call `migration_approve_table_config` command
  - [x] 8.1.4 Handle success/error responses
  - [ ] 8.1.5 Write approval logic tests
- [x] 8.2 Update approval status in DB
  - [x] 8.2.1 Verify command updates `approval_status`
  - [x] 8.2.2 Verify command sets `approved_at` timestamp
  - [x] 8.2.3 Write DB update tests (Rust tests exist)
- [x] 8.3 Show approval state in UI
  - [x] 8.3.1 Display approval status badge
  - [x] 8.3.2 Display approval timestamp
  - [ ] 8.3.3 Update table list with approval indicator (deferred to Phase 9 polish)
  - [ ] 8.3.4 Write UI state tests
- [ ] 8.4* Disable editing after approval (optional feature, deferred)
  - [ ] 8.4.1 Add read-only mode to form
  - [ ] 8.4.2 Disable form fields when approved
  - [ ] 8.4.3 Write read-only mode tests
- [x] 8.5 Update header counts
  - [x] 8.5.1 Add approved count to header
  - [x] 8.5.2 Update count on approval
  - [ ] 8.5.3 Write count update tests

## Phase 9: Integration & Polish

- [ ] 9.1 Refactor `config-step.tsx` to use new components
  - [x] 9.1.1 Replace inline rendering with component composition
  - [x] 9.1.2 Wire up component props and callbacks
  - [x] 9.1.3 Remove old code
  - [ ] 9.1.4 Write integration tests
- [ ] 9.2 Verify auto-save still works
  - [ ] 9.2.1 Test auto-save triggers on field change
  - [ ] 9.2.2 Test debounce behavior (500ms)
  - [ ] 9.2.3 Write auto-save tests
- [ ] 9.3 Verify auto-analyze still works
  - [ ] 9.3.1 Test auto-analyze button
  - [ ] 9.3.2 Test analysis metadata population
  - [ ] 9.3.3 Write auto-analyze tests
- [ ] 9.4 Verify refresh schema still works
  - [ ] 9.4.1 Test refresh schema button
  - [ ] 9.4.2 Test schema data reload
  - [ ] 9.4.3 Write refresh schema tests
- [ ] 9.5 UI polish pass
  - [ ] 9.5.1 Review spacing and alignment
  - [ ] 9.5.2 Review color consistency
  - [ ] 9.5.3 Review typography hierarchy
  - [ ] 9.5.4 Review loading states
  - [ ] 9.5.5 Review error states
  - [ ] 9.5.6 Review empty states
- [ ] 9.6 Run full E2E test suite
  - [ ] 9.6.1 Run existing E2E tests
  - [ ] 9.6.2 Fix any regressions
- [ ] 9.7 Fix any regressions

## Phase 10: Testing & Documentation

- [x] 10.1 Run `cargo test db` (14 tests passing)
- [x] 10.2 Run `cargo test migration` (16 tests passing)
- [x] 10.3 Run `npm run test:integration` (57 tests passing)
- [ ] 10.4 Run E2E tests for scope flow
- [ ] 10.5 Update component documentation
- [ ] 10.6 Verify markdownlint passes

## Property-Based Tests

- [ ] PBT-1 Write approval state consistency property test
  - [ ] PBT-1.1 Define arbitrary table config generator
  - [ ] PBT-1.2 Define property: approved state requires no validation errors
  - [ ] PBT-1.3 Implement test with fast-check
  - [ ] PBT-1.4 Verify test catches violations
- [ ] PBT-2 Write manual override preservation property test
  - [ ] PBT-2.1 Define arbitrary analysis metadata generator
  - [ ] PBT-2.2 Define property: agent metadata preserved after override
  - [ ] PBT-2.3 Implement test with fast-check
  - [ ] PBT-2.4 Verify test catches violations
- [ ] PBT-3 Write relationship validation correctness property test
  - [ ] PBT-3.1 Define arbitrary relationship generator
  - [ ] PBT-3.2 Define property: validation status matches specification
  - [ ] PBT-3.3 Implement test with fast-check
  - [ ] PBT-3.4 Verify test catches violations
