# Fact Table

## What It Is

The central table in a dimensional schema. Stores the quantitative measurements (facts) of a business process and foreign keys to all associated dimension tables.

---

## Steps to Identify

### Step 1 — Identify the Business Event Being Measured

**How to identify:**

- Look for the source system transaction or event: a sale, a shipment, a payment, a lab test, a click.
- Ask: *"What happened that the business wants to measure?"*
- The answer is your fact table subject.

### Step 2 — Identify Which Columns Are Facts (Not Dimensions)

**How to identify:**

- Facts are **numeric** and **vary per event row** — quantity sold, revenue, cost, duration.
- If you can meaningfully sum or average a column across rows → it's a fact.
- If a column labels or categorizes (product name, store city, employee department) → it belongs in a dimension table.
- Non-additive numbers (unit price, ratios) are still facts — classify them as non-additive.

### Step 3 — Identify Fact Additivity

**How to identify:**

- **Additive**: Can be summed across ALL dimensions. Ask: "Does summing this across every dimension make sense?" (quantity sold, revenue → yes).
- **Semi-additive**: Can be summed across SOME dimensions but not time. Ask: "Does summing this across dates make sense?" (account balance → no, it's a point-in-time measure).
- **Non-additive**: Cannot be meaningfully summed across any dimension. (unit price, profit margin % → summing gives a nonsensical result).

### Step 4 — Identify Foreign Keys (Not Facts)

**How to identify:**

- Every column that is a reference to a who/what/where/when → is a foreign key to a dimension table, not a fact.
- Transaction IDs or control numbers with no descriptive attributes → degenerate dimensions (stay in fact table as-is, no separate dim table).

### Step 5 — Confirm the Fact Table Grain

**How to identify:**

- Write the grain: *"One row represents ___."*
- Verify every fact column can be populated for every row at that grain.
- If some facts are NULL for large portions of rows → you likely have mixed grains. Split into two fact tables.

---

## Fact Additivity Quick Reference

| Type | Test | Example |
|---|---|---|
| Additive | `SUM()` across all dimensions makes sense | Revenue, Units Sold |
| Semi-Additive | `SUM()` across some dims OK, not time | Account Balance, Inventory Level |
| Non-Additive | `SUM()` never makes sense | Unit Price, Margin % |

---

## Structure Reference

| Column Type | How to Identify It |
|---|---|
| Foreign Keys | References to dimension entities (date, product, store) |
| Degenerate Dimensions | Transaction IDs with no descriptive attributes |
| Additive Facts | Numeric, summable across all dimensions |
| Semi-Additive Facts | Numeric, summable across some dimensions only |
| Non-Additive Facts | Numeric ratios/prices that cannot be summed |
