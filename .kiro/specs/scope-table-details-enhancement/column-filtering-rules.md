# Column Display Rules for Table Details UI

This document defines how the UI should display columns for each field type in the Scope → Table Details editor.

## MVP Approach (Current)

**Show all columns without filtering** - For MVP, we display all available columns in dropdowns/selects. Future enhancement will add intelligent filtering via agent.

## Field Types and UI Controls

### 1. CDC Column (Incremental Load Watermark)
**UI Control:** Dropdown (single select)
- Show all columns
- Sort alphabetically
- Agent suggestion pre-selected
- Display format: `column_name (data_type)`

### 2. Canonical Date Column (Business Date)
**UI Control:** Dropdown (single select)
- Show all columns
- Sort alphabetically
- Agent suggestion pre-selected
- Display format: `column_name (data_type)`

### 3. Grain Columns (Table Grain / Unique Key)
**UI Control:** Multi-select dropdown or comma-separated text input
- Show all columns
- Sort alphabetically
- Agent suggestions pre-selected
- Support multiple column selection
- Display format: `column_name (data_type)`

### 4. Relationship Columns (see screenshot)
**UI Control:** Three dropdowns per relationship row

**Child Column Dropdown:**
- Show all columns from current table
- Sort alphabetically
- Display format: `column_name`

**Parent Table Dropdown:**
- Show all selected tables in scope
- Group by schema (e.g., `dbo.dim_customer`)
- Sort alphabetically

**Parent Column Dropdown:**
- Show all columns from selected parent table
- Sort alphabetically
- Display format: `column_name`

**Cardinality Dropdown:**
- Fixed options: `many_to_one`, `one_to_one`

**Validation Column:**
- Display validation status chip (Valid/Invalid/Type mismatch)
- Show error messages below if invalid

### 5. PII Columns (Personally Identifiable Information)
**UI Control:** Multi-select checkboxes
- Show all columns as checkbox list
- Agent suggestions pre-checked
- Display format: `column_name (data_type)`
- Visual indicator for checked items

### 6. SCD Columns (Slowly Changing Dimensions)
**Only shown when Table Type = "dimension"**

**SCD Type:** Dropdown with fixed options (`none`, `type_1`, `type_2`)

**Effective Date Column:** Dropdown (single select)
- Show all columns
- Sort alphabetically
- Display format: `column_name (data_type)`

**Expiry Date Column:** Dropdown (single select)
- Show all columns
- Sort alphabetically
- Display format: `column_name (data_type)`

## Implementation Notes

### Backend (Complete)

1. ✅ `ColumnMetadata` struct added to Rust types
2. ✅ `migration_get_table_config` fetches columns from `sqlserver_object_columns`
3. ✅ Returns columns as part of `TableConfigPayload`

### Frontend (To Do)

1. **Update `CoreFieldsSection`:**
   - Convert CDC column input → dropdown
   - Convert Canonical Date column input → dropdown
   - Populate from `draft.availableColumns`

2. **Update `RelationshipsSection`:**
   - Already has dropdowns (good!)
   - Populate child column dropdown from `draft.availableColumns`
   - Parent table dropdown from selected tables in scope
   - Parent column dropdown from parent table's columns (need to fetch)

3. **Update `PiiSection`:**
   - Convert from pill display → checkbox list
   - Show all columns with checkboxes
   - Pre-check agent suggestions

4. **Update grain columns:**
   - Keep as text input for MVP (comma-separated)
   - Future: convert to multi-select dropdown

5. **Update `ScdSection`:**
   - Convert effective/expiry date inputs → dropdowns
   - Populate from `draft.availableColumns`

### Display Format

All dropdowns should show: `column_name (data_type)`

Example:
```tsx
<option value="customer_id">customer_id (int)</option>
<option value="sale_date">sale_date (date)</option>
<option value="email">email (varchar)</option>
```

---

## Future Enhancement: Intelligent Filtering

When agent becomes smarter, add filtering logic to show only relevant columns for each field type. See original version of this document for detailed filtering rules.
