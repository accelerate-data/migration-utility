# Periodic Snapshot Fact Table

## What It Is

A fact table where each row represents the measured status or level of something at a regular, predictable interval (daily, weekly, monthly). Rows exist even if no activity occurred.

---

## Steps to Identify

### Step 1 — Identify That the Business Needs Status at Regular Intervals

**How to identify:**

- Business users ask questions like: *"What was our inventory level at end of each week?"* or *"What was the account balance each month-end?"*
- The question is about **status** or **balance** at a point in time, not about individual events.
- Ask: *"Does the business want to see a value for every time period, even if nothing changed?"*
- If yes → periodic snapshot.

### Step 2 — Identify the Regular Interval

**How to identify:**

- Ask: *"How often does the business want to see this measurement?"*
- Look for reporting cadences: daily inventory counts, weekly headcount reports, monthly balance statements.
- The interval must be regular and predictable — not event-driven.
- The interval becomes part of the grain: "one row per [entity] per [period]."

### Step 3 — Confirm Rows Are Dense (Always Present)

**How to identify:**

- Unlike a transaction fact table (sparse — rows only when events occur), a periodic snapshot always has a row for every entity × every period.
- Ask: *"If a product had no sales this week, do we still want a row for it showing zero/current inventory?"*
- If yes → periodic snapshot (dense by design).

### Step 4 — Identify Semi-Additive Facts

**How to identify:**

- Periodic snapshot facts are often semi-additive — they can be summed across entities but NOT across time.
- Ask: *"If I sum this value across all months, does the result make business sense?"*
- Summing account balances across 12 months → nonsensical. → Semi-additive.
- Summing units received across 12 months → valid. → Additive.
- This semi-additive nature is the strongest identifier that you need a periodic snapshot.

### Step 5 — Identify Whether a Transaction Table Already Exists

**How to identify:**

- The periodic snapshot complements — it does not replace — the transaction fact table.
- Ask: *"Do we need both individual-event analysis AND period-status analysis?"*
- If yes → build both. The snapshot is derived from the transaction history.

---

## Key Identification Signals

| Signal | Indicates Periodic Snapshot |
|---|---|
| "What was the balance/level at end of [period]?" | ✓ |
| Rows needed even when no events occurred | ✓ |
| Facts cannot be summed across time | ✓ |
| Regular, scheduled reporting interval | ✓ |
| Source is an end-of-period extract, not an event log | ✓ |
