# Transaction Fact Table

## What It Is

A fact table where each row represents a discrete business event or transaction as it occurred. The most common fact table type.

---

## Steps to Identify

### Step 1 — Identify That the Subject Is an Individual Event

**How to identify:**

- The source system records individual, discrete events — not summaries or status snapshots.
- Ask: *"Is there a moment in time when this event happened, and can we point to exactly when it occurred?"*
- Examples: a product was scanned, an order was placed, a payment was received, a click occurred.
- Each event either happened or it didn't — there is no "between events" state to track.

### Step 2 — Confirm Rows Are Append-Only

**How to identify:**

- In the source system, once a transaction is recorded, it is never updated (only corrections are made).
- Ask: *"After a row is inserted, would we ever go back and change it?"*
- If no → transaction fact table.
- If yes, rows are updated as a process progresses → accumulating snapshot instead.

### Step 3 — Confirm the Sparse Nature

**How to identify:**

- A row exists only when an event occurred. Many dimension combinations will have no rows for certain time periods — this is normal and expected.
- Ask: *"Does a row exist for every product × store × day combination?"*
- If not (only when a sale happened) → transaction fact table (sparse by nature).
- If yes, every combination always has a row → periodic snapshot instead.

### Step 4 — Identify the Additive Measures

**How to identify:**

- Transaction facts almost always have numeric measures that are fully additive: quantity sold, revenue amount, cost, discount.
- Ask: *"What did the business record as a measurement at the moment this event occurred?"*

### Step 5 — Identify the Degenerate Dimension

**How to identify:**

- Look for a transaction control number (receipt number, order ID, invoice number) in the source data.
- Ask: *"Is there a transaction identifier that groups related fact rows but has no descriptive attributes worth building a dimension table for?"*
- If yes → store it directly in the fact table as a degenerate dimension.

---

## Decision: Transaction vs. Other Fact Table Types

| Question | Transaction | Periodic Snapshot | Accumulating Snapshot |
|---|---|---|---|
| Does a row exist only when an event occurs? | ✓ Yes | No — rows exist for every period | No — one row per pipeline instance |
| Are rows ever updated after initial load? | Never | Never | Yes — each milestone |
| Is the grain a single event? | ✓ Yes | A time period + entity | A pipeline lifecycle |
| Are measures fully additive? | Usually yes | Often semi-additive | Mix |
