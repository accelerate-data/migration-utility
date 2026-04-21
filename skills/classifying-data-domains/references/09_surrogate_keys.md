# Surrogate Keys

## What It Is

A system-generated, meaningless integer assigned by ETL as the primary key for every dimension table row. Independent of any source system key.

---

## Steps to Identify Where Surrogate Keys Are Needed

### Step 1 — Identify Every Dimension Table

**How to identify:**

- Every dimension table needs a surrogate key as its primary key — no exceptions.
- Check: if you are building a dimension table, it needs a surrogate key.

### Step 2 — Identify Natural Keys That Are Unreliable as Primary Keys

**How to identify:**

- Ask: *"Can the source system reuse or recycle this key?"* → Surrogate key needed.
- Ask: *"Can two different source systems produce the same key value for different entities?"* → Surrogate key needed to merge them.
- Ask: *"Does this dimension use SCD Type 2?"* → Multiple surrogate keys will exist for the same natural entity (one per version). Natural key cannot be the PK.
- Ask: *"Can this key format change in the source system?"* → Surrogate key insulates the warehouse.

### Step 3 — Identify the Natural Key to Retain as an Attribute

**How to identify:**

- The source system's original identifier (SKU number, employee ID, customer account number) must be KEPT — but as a regular non-key attribute, not as the primary key.
- Look for: the field that source system users and business people refer to when they identify an entity.
- Retain it for: traceability back to source, joining to operational systems for troubleshooting.

### Step 4 — Identify Edge Cases Requiring Special Surrogate Keys

**How to identify:**

- Look for fact rows where a dimension value is legitimately unknown at load time.
- Ask: *"Can a fact row arrive without a known value for a required dimension?"*
- If yes → pre-create placeholder dimension rows: "Unknown" (key = 0 or -1), "Not Applicable" (key = -2).
- Assign these placeholder keys to fact rows instead of NULL → prevents NULL foreign keys in the fact table.

### Step 5 — Identify the Date Dimension Surrogate Key Convention

**How to identify:**

- The Date dimension surrogate key is often formatted as `YYYYMMDD` integer (e.g., `20240315`).
- This is both a surrogate key AND human-readable — an exception to the "meaningless integer" rule.
- Confirm the convention your team will use before building the Date dimension.

---

## Surrogate Key Requirement Checklist

| Situation | Surrogate Key Needed? |
|---|---|
| Any dimension table | ✓ Always |
| Source key could be recycled | ✓ Yes |
| SCD Type 2 history tracking | ✓ Yes — multiple keys per entity |
| Multiple source systems feeding one dimension | ✓ Yes |
| Source key could change format | ✓ Yes |
| Fact row with unknown dimension value | ✓ Yes — placeholder key, not NULL |
