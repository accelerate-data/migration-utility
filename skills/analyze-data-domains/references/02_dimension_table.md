# Dimension Table

## What It Is
A companion table to the fact table that provides the descriptive context — the who, what, where, when, why, and how — for every fact row.

---

## Steps to Identify

### Step 1 — Identify Descriptive, Categorical Columns
**How to identify:**
- Look at source system records. Any column that describes **what** the transaction involved rather than measuring it → candidate dimension attribute.
- Ask: *"Do business users filter, group, or label their reports by this value?"*
- Typical signals: product name, store city, employee department, promotion type, customer segment.
- If you find yourself writing `GROUP BY product_name` or `WHERE store_region = 'South'` → those are dimension attributes.

### Step 2 — Identify the Dimension Subject
**How to identify:**
- Group related descriptive attributes by their real-world subject: all product-related attributes → Product dimension; all store-related attributes → Store dimension.
- Each dimension should describe one and only one real-world entity or concept.
- Ask: *"Who or what does this attribute describe?"*

### Step 3 — Identify Hierarchies Within the Dimension
**How to identify:**
- Look for natural drill paths where one attribute rolls up to another: SKU → Brand → Category → Department.
- Ask: *"If I know the value of column A, does it always determine the value of column B?"* If yes → B is higher in the hierarchy than A.
- All hierarchy levels belong in the same flat dimension table (not separate normalized tables).

### Step 4 — Identify Slowly Changing Attributes
**How to identify:**
- Ask for each attribute: *"Can this value change over time for the same entity?"*
- If yes → determine whether history matters (Type 2) or only the current value matters (Type 1).
- Common slowly changing attributes: customer address, employee department, product category, store manager.

### Step 5 — Identify Coded vs. Descriptive Values
**How to identify:**
- Find any column where the stored value is a code, abbreviation, or number that represents a human concept.
- Example: `status_code = 'CB4'` → decode to `status_description = 'Carbonated Beverages - 4 Pack'`.
- Every coded column is a signal that the dimension needs a decoded descriptive counterpart.

### Step 6 — Confirm the Dimension Is Not a Fact
**How to identify:**
- Dimension attributes are almost always text or low-cardinality categories.
- If you would sum or average a column → it is a fact, not a dimension attribute.
- If a column is a continuous numeric measure → it belongs in the fact table.

---

## Common Dimension Identification by Source System Column Type

| Source Column Characteristic | Likely Belongs In |
|---|---|
| Short text description or label | Dimension attribute |
| Categorical code needing decode | Dimension attribute (decoded) |
| Continuous numeric measure | Fact table |
| Yes/No or boolean flag | Dimension attribute (or Junk Dimension) |
| Hierarchical grouping | Dimension attribute (flattened) |
| Transaction control number (no attributes) | Degenerate dimension in fact table |
