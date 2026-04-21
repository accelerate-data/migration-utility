# Junk Dimension

## What It Is

A single dimension table that consolidates multiple low-cardinality flags, indicators, and codes into one table rather than cluttering the fact table with many tiny separate foreign keys.

---

## Steps to Identify a Junk Dimension Opportunity

### Step 1 — Find Low-Cardinality Flag Columns in the Source Transaction Record

**How to identify:**

- Look for columns in the source data that have only 2–5 possible values: Yes/No, True/False, status codes, indicator flags.
- Examples: `gift_wrap_flag` (Y/N), `rush_order_flag` (Y/N), `payment_method` (Cash/Credit/Debit/Gift Card), `order_channel` (Web/Phone/In-Store/App).
- These are often boolean columns or short-code columns in the source system.

### Step 2 — Count How Many of These Low-Cardinality Columns Exist

**How to identify:**

- If you find 3 or more such columns that would otherwise go into the fact table as individual foreign keys or raw columns → junk dimension candidate.
- Ask: *"Would the fact table have many small dimension foreign keys that each point to a tiny 2-row or 3-row dimension table?"* → Yes → junk dimension.

### Step 3 — Determine Whether They Belong Together Logically

**How to identify:**

- The flags don't need to be logically related — they just need to all describe aspects of the same transaction type.
- Ask: *"Are all these flags captured at the same grain as the fact table?"* → Yes → they can be combined into a junk dimension.
- Ask: *"Are they all sourced from the same transaction record?"* → Yes → good junk dimension candidates.

### Step 4 — Estimate the Combination Count

**How to identify:**

- Multiply the distinct value counts of all the flag columns together.
- `gift_wrap (2) × payment_method (4) × order_channel (3) × rush_flag (2)` = 48 combinations.
- If the combination count is manageable (under a few thousand) → junk dimension is practical.
- If combinations explode into tens of thousands → reconsider: keep as raw columns or split into separate small dimensions.

### Step 5 — Confirm These Are Not Better as Their Own Dimensions

**How to identify:**

- Ask: *"Does any of these flags have additional descriptive attributes worth storing in a dimension table?"*
- Example: if `payment_method` has associated attributes like `payment_provider`, `fee_rate`, `processing_time_seconds` → create a proper Payment Method dimension instead.
- Junk dimensions are for flags with no additional attributes.

---

## Junk Dimension Identification Signals

| Signal | Indicates Junk Dimension |
|---|---|
| Many Y/N or boolean flag columns in source | ✓ |
| Short-code columns with 2–5 values | ✓ |
| No additional attributes beyond the flag value itself | ✓ |
| Flags would create many tiny 2–3 row dimension tables | ✓ |
| All flags are captured at the same grain as the fact | ✓ |
