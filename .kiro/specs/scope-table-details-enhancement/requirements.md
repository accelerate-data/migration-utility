# Requirements: Scope Table Details Enhancement

## Overview

Enhance the Scope → Table Details UI to match the mockup design with improved UX for reviewing and approving agent-analyzed table metadata.

## Functional Requirements

### 1. Agent Rationale Visibility

**User Story**: As a user reviewing agent-analyzed table metadata, I want to see the confidence scores and reasoning behind each inference so that I can understand and validate the agent's decisions.

**Acceptance Criteria**:
- Display confidence scores (0-100%) for each agent-inferred field
- Show reasoning/evidence text explaining why the agent made each inference
- Provide a collapsible section to avoid UI clutter
- Hide agent rationale when a field has been manually overridden

### 2. Approval Workflow

**User Story**: As a user, I want to explicitly approve table configurations after reviewing them so that I can control which tables proceed to the next stage.

**Acceptance Criteria**:
- Provide an "Approve" button for each table configuration
- Track approval state: `pending`, `approved`, `needs_review`
- Block approval when validation errors exist
- Display approval timestamp after approval
- Show approval status in the table list

### 3. Relationship Editor

**User Story**: As a user, I want to define table relationships using a structured form instead of raw JSON so that I can avoid syntax errors and get immediate validation feedback.

**Acceptance Criteria**:
- Replace JSON text input with a grid-based editor
- Provide dropdowns for: child column, parent table, parent column, cardinality
- Show inline validation status chips: Valid, Type mismatch, Not found
- Support adding and removing relationship rows
- Validate relationships in real-time

### 4. PII Column Selection

**User Story**: As a user, I want to select PII columns using checkboxes instead of typing column names so that I can avoid typos and see all available columns.

**Acceptance Criteria**:
- Replace text input with checkbox multi-select UI
- Display all available columns from the table schema
- Show selected columns as visual pills
- Support adding and removing PII columns

### 5. Manual Override Tracking

**User Story**: As a user, I want to see which fields I've manually edited versus which came from the agent so that I can track my changes and understand the data provenance.

**Acceptance Criteria**:
- Track which fields the user has manually edited
- Display "Agent" chip (green) for agent-inferred values
- Display "Manual" chip (blue) for user-edited values
- Hide agent rationale section for manually-overridden fields
- Preserve agent metadata even after manual override (for audit trail)

### 6. Field Provenance

**User Story**: As a user, I want to understand where each field value came from so that I can trust the configuration and know what to review carefully.

**Acceptance Criteria**:
- Show data source for each field (Agent vs Manual)
- Display confidence level for agent inferences
- Show "Manually configured" label for user edits
- Provide visual distinction between provenance types

## Non-Functional Requirements

### Performance

- Maintain existing auto-save behavior (500ms debounce)
- No performance degradation with new UI components
- Relationship validation should complete within 100ms

### Compatibility

- Preserve all existing functionality
- Backward compatible with existing `table_config` data
- Support migration from old format to new format

### Usability

- All new UI components follow AD brand guidelines
- Responsive layout works on standard desktop resolutions
- Keyboard navigation support for form fields

### Testing

- Unit tests for validation logic
- Integration tests for component interactions
- E2E test for full approval workflow

## Correctness Properties

### Property 1: Approval State Consistency
**Specification**: A table cannot be in `approved` state if it has validation errors.

**Test Strategy**: Property-based test that generates random table configurations with various validation states and verifies that approval is blocked when errors exist.

### Property 2: Manual Override Preservation
**Specification**: When a user manually overrides an agent-inferred value, the original agent metadata must be preserved in `analysis_metadata_json`.

**Test Strategy**: Property-based test that generates random field edits and verifies that agent metadata remains intact after override.

### Property 3: Relationship Validation Correctness
**Specification**: A relationship is valid if and only if:
1. The parent table exists in the selected tables list
2. The child column exists in the current table schema
3. The parent column exists in the parent table schema
4. Data types are compatible (if schema metadata available)

**Test Strategy**: Property-based test that generates random relationship configurations and verifies validation status matches the specification.

## Open Questions

1. **Schema metadata access** — Do we have table/column metadata for full relationship validation?
2. **PII column source** — Where does the list of available columns come from?
3. **Relationship cardinality** — Does agent infer this? How?
4. **Bulk operations** — Need bulk approve for multiple tables?
5. **Approval lock** — Can users edit after approval? Or read-only?

## Success Criteria

- Agent rationale visible with confidence scores
- Relationship editor uses dropdowns (not JSON text)
- Relationship validation shows inline status
- PII uses checkbox multi-select
- Manual edits visually distinct from agent inferences
- Approve button enforces validation rules
- Approved tables show green checkmark
- All existing functionality preserved

## References

- **Mockup**: `docs/design/ui-patterns/home-mockup.html` (Scope — Table Details section)
- **Current implementation**: `app/src/routes/scope/config-step.tsx`
- **DB schema**: `app/src-tauri/migrations/001_initial_schema.sql`
- **DB change protocol**: `.claude/rules/db-schema-change.md`
