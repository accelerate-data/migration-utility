# Minidimension

## What It Is
A technique for handling frequently-changing attributes of a large dimension. The volatile attributes are broken out into a small, separate dimension table (the minidimension), whose key appears directly in the fact table.

---

## Steps to Identify When a Minidimension Is Needed

### Step 1 — Identify a Large Dimension with Frequently Changing Attributes
**How to identify:**
- Ask: *"How many rows does this dimension have?"* — Large means millions of rows (Customer, Account, Employee).
- Ask: *"How often do certain attributes change for each entity?"* — Frequently means monthly, weekly, or more.
- If you have a large dimension AND frequently changing attributes → SCD Type 2 would cause massive row explosion. Minidimension needed.

### Step 2 — Identify Which Specific Attributes Change Frequently
**How to identify:**
- Not all attributes change frequently. Identify WHICH ones do:
  - Customer: income band, age range, credit score tier, purchase frequency tier, loyalty status → frequently updated.
  - Customer: name, birth date, original acquisition date → rarely updated.
- Ask: *"Which attributes does the analytics team want to analyze 'as-was at time of transaction'?"* AND *"Which of those change frequently?"* → minidimension candidates.

### Step 3 — Confirm the Attributes Are Analytically Meaningful in Bands
**How to identify:**
- Minidimension attributes are typically **banded** or **bucketed** versions of continuous measures.
- Ask: *"Is this a continuous value (credit score = 723) that we want to group into bands (700–750 = 'Good')?"*
- The banding reduces the number of distinct values → makes the minidimension feasible.
- Continuous values with thousands of distinct values → band them before putting them in a minidimension.

### Step 4 — Estimate the Minidimension Row Count
**How to identify:**
- Multiply the distinct band counts for all candidate attributes.
- `income_band (4) × age_range (5) × credit_tier (5) × frequency_tier (4)` = 400 combinations.
- If this is small (under a few thousand rows) → minidimension is practical.
- If combinations explode → reduce the number of attributes or the number of bands.

### Step 5 — Confirm the Minidimension Key Goes in the Fact Table
**How to identify:**
- The minidimension foreign key must appear in the FACT TABLE (not in the main dimension table).
- At the moment a transaction occurs, the ETL captures which minidimension row describes the entity's current state and records that key in the fact row.
- This "freezes" the entity's profile as it was at transaction time — enabling "as-was" analysis.

---

## Minidimension vs. SCD Type 2 Decision

| Situation | Solution |
|---|---|
| Large dimension, infrequently changing attribute, history matters | SCD Type 2 on main dimension |
| Large dimension, frequently changing attribute, history matters | Minidimension |
| Small dimension, any change frequency | SCD Type 2 on main dimension |
| Attribute changes so fast it would be in every fact row | Store directly in fact table |
