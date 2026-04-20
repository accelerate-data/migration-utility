# Factless Fact Table

## What It Is
A fact table with only foreign keys and no numeric measures. Records that events occurred, or defines a universe of valid dimension combinations for "what did not happen" analysis.

---

## Steps to Identify

### Step 1 — Identify That the Event Has No Natural Numeric Measure
**How to identify:**
- Ask: *"What number do we want to measure here?"*
- If the answer is *"Nothing — we just want to know it happened"* → factless fact table candidate.
- The count of rows itself IS the measure (use `COUNT(*)` in queries).
- Examples: student attendance (did they show up?), accident occurrence (did it happen?), promotion application (was this product on promotion?).

### Step 2 — Identify Pattern 1 — Event Recording (Something That Happened)
**How to identify:**
- The business wants to record the occurrence of an event, not measure a quantity.
- Ask: *"Is the existence of a row in this table itself meaningful?"*
- Ask: *"Would users count the number of rows to answer business questions?"*
- Examples: "How many students attended?" → count rows. "How many promotions ran?" → count rows.

### Step 3 — Identify Pattern 2 — Coverage (What Did NOT Happen)
**How to identify:**
- The business wants to find combinations that SHOULD have occurred but didn't.
- Ask: *"Do we need to find things that were expected but didn't happen?"*
- Examples: "Which promoted products had no sales?" or "Which enrolled students missed class?"
- This requires a coverage table that defines all theoretically possible combinations, then an anti-join against the actual events table.

### Step 4 — Confirm There Are Truly No Measures to Add
**How to identify:**
- Before accepting no measures, ask: *"Is there any meaningful number associated with each event?"*
- Sometimes a constant (`event_occurred = 1`) is added for ease of aggregation — this is acceptable.
- If a useful measure exists (e.g., duration of attendance, severity of accident) → it is NOT a factless table; add those measures.

---

## Key Identification Signals

| Signal | Indicates Factless Fact Table |
|---|---|
| "Did it happen?" is the question | ✓ Event factless |
| "What did NOT happen?" is the question | ✓ Coverage factless |
| COUNT(*) is the primary aggregation | ✓ |
| No natural dollar, quantity, or duration measure | ✓ |
| The event either occurred or it didn't | ✓ |
