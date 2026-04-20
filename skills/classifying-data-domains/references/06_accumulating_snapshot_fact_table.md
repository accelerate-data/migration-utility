# Accumulating Snapshot Fact Table

## What It Is
A fact table where each row represents the full lifecycle of one business pipeline instance (an order, a claim, a loan application), updated each time the entity reaches a new milestone.

---

## Steps to Identify

### Step 1 — Identify a Pipeline or Workflow with Defined Milestones
**How to identify:**
- Ask: *"Does this business process have a sequence of distinct stages that every instance goes through?"*
- Look for flowcharts, process maps, or SLA definitions that describe stages with clear handoff points.
- Examples: Order → Confirmed → Shipped → Delivered → Invoiced → Paid.
- If you can list 3–8 named milestones with a clear order → accumulating snapshot candidate.

### Step 2 — Identify That One Entity Is Tracked Over Its Entire Lifetime
**How to identify:**
- Ask: *"Can I identify one 'thing' (order, claim, application) that exists from creation to completion?"*
- That one thing is the grain: one row per order, one row per claim.
- Every milestone date, duration, and status for that entity will live in its single row.

### Step 3 — Identify Multiple Date Milestones
**How to identify:**
- Count the number of meaningful dates in the lifecycle.
- Each date = a milestone = a separate date foreign key column in the fact table.
- If a fact table would have 3, 4, 5+ date foreign keys → strong signal of accumulating snapshot.
- Ask: *"Is there more than one important date associated with each pipeline instance?"*

### Step 4 — Confirm That Rows Are Updated Over Time
**How to identify:**
- Unlike transaction and periodic snapshot fact tables (append-only), accumulating snapshots require UPDATE operations.
- Ask: *"After a row is inserted, will we need to go back and fill in more data as the process progresses?"*
- If yes → accumulating snapshot.
- ETL must support UPDATE (not just INSERT) for this fact table type.

### Step 5 — Identify Lag/Duration Metrics as the Key Business Questions
**How to identify:**
- Business users ask: *"How long does it take to go from order to shipment?"* or *"What is the average cycle time from application to approval?"*
- Duration between milestones = lag facts = computed columns in the accumulating snapshot.
- If lag/duration/cycle-time analysis is the primary goal → accumulating snapshot.

### Step 6 — Identify In-Progress vs. Completed Instances
**How to identify:**
- Rows with NULL milestone date columns represent in-progress pipeline instances.
- Ask: *"Do we need to track open/pending items alongside completed ones in the same table?"*
- If yes → accumulating snapshot handles both in one table (NULL = not yet reached that milestone).

---

## Key Identification Signals

| Signal | Indicates Accumulating Snapshot |
|---|---|
| "How long does step X take?" | ✓ |
| Multiple milestone dates per entity | ✓ |
| Rows need to be updated after initial insert | ✓ |
| Business tracks pipeline/workflow stages | ✓ |
| One entity has a defined start and end point | ✓ |
| Users need to see current status of in-flight items | ✓ |
