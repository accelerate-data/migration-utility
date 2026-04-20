# Bridge Table (Multivalued Dimension)

## What It Is
A helper table that resolves a many-to-many relationship between a fact table and a dimension. Used when one fact row legitimately has multiple dimension members simultaneously.

---

## Steps to Identify When a Bridge Table Is Needed

### Step 1 — Identify a Many-to-Many Relationship Between Fact and Dimension
**How to identify:**
- Ask: *"Can a single fact row have more than one value for this dimension?"*
- Examples: A single patient claim can have multiple simultaneous diagnoses. A bank account can have multiple co-owners. A product can have multiple category tags.
- If the answer is YES → a direct foreign key from fact to dimension is insufficient. Bridge table needed.

### Step 2 — Confirm the Relationship Violates the Declared Grain
**How to identify:**
- If you tried to add multiple rows to the fact table (one per dimension value), would it distort the measures?
- Example: A claim for $500 with 3 diagnoses → adding 3 rows would triple-count the $500 as $1,500 total.
- If adding rows per dimension value causes measure double-counting → bridge table is the correct solution.

### Step 3 — Identify the "Group" Concept
**How to identify:**
- A bridge table works by grouping dimension members into a "group key."
- Ask: *"What is the complete set of dimension values for this one fact row?"* — That set becomes a group.
- Example: Claim #1234 has diagnoses {Diabetes, Hypertension, High Cholesterol} → this set is one diagnosis group.
- The group key links the fact table to the bridge table.

### Step 4 — Determine Whether Weighting Is Needed
**How to identify:**
- Ask: *"If I query total revenue by [dimension value], will measures be double-counted due to the many-to-many?"*
- If measures will be counted once per bridge row (one per dimension value) → double-counting occurs.
- Business decision: Add a `weighting_factor` column to the bridge table so measures can be proportionally allocated.
- Example: 3 equal diagnoses → each gets weight of 1/3. `SUM(claim_amount × weighting_factor)` avoids double-counting.

### Step 5 — Confirm the Pattern in Source Data
**How to identify:**
- Look at the source system. Is there a one-to-many table linking the main entity to the dimension?
- Example: `claim_diagnoses` table with columns `claim_id, diagnosis_code` — multiple rows per claim.
- This source-level one-to-many relationship → bridge table in the dimensional model.

---

## Bridge Table Identification Signals

| Signal | Indicates Bridge Table Needed |
|---|---|
| One fact row has multiple values of one dimension | ✓ |
| Adding rows per dimension value causes measure distortion | ✓ |
| Source has a many-to-many linking table | ✓ |
| "Multiple simultaneous [dimension members]" | ✓ |

---

## Common Bridge Table Scenarios

| Fact | Many-to-Many Dimension | Bridge Table |
|---|---|---|
| Health care claim | Diagnoses | Diagnosis group bridge |
| Bank account balance | Account holders (customers) | Account holder bridge |
| Student enrollment | Degree programs | Program group bridge |
| Insurance policy | Coverage types | Coverage group bridge |
