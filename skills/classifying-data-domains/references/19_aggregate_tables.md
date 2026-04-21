# Aggregate Tables

## What They Are

Pre-summarized versions of atomic fact tables at a coarser grain, built to accelerate common summary-level queries.

---

## Steps to Identify When and What to Aggregate

### Step 1 — Identify Performance Problems on the Atomic Fact Table

**How to identify:**

- Monitor query execution times against the atomic fact table.
- Ask: *"Which queries run most frequently and take longest to execute?"*
- Ask: *"What percentage of queries actually need atomic-level detail?"*
- If most queries run at a summarized level (monthly, by category) but the atomic table has billions of rows → aggregate tables will help.

### Step 2 — Identify the Most Frequently Requested Aggregation Grain

**How to identify:**

- Analyze query logs: what GROUP BY combinations appear most often?
- Ask business users: *"What is the typical level of detail you report at?"* — "We mostly look at monthly sales by product category and region."
- Each common GROUP BY pattern is a candidate aggregate grain.
- Prioritize the grains that appear in the most frequent or most important queries.

### Step 3 — Identify Which Dimensions Can Be Rolled Up

**How to identify:**

- For each candidate aggregate grain, identify which dimensions are summarized away and which are retained:
  - Daily Date → rolled up to Monthly Date → drop `day_of_week`, `day_number_in_month`; retain `month_name`, `quarter`, `year`.
  - SKU-level Product → rolled up to Brand level → drop `sku_number`, `product_name`; retain `brand`, `category`, `department`.
- The aggregate fact table needs shrunken versions of these dimensions at the matching grain.

### Step 4 — Identify Which Facts Can Be Aggregated

**How to identify:**

- Additive facts (revenue, quantity) → SUM them into the aggregate. ✓
- Semi-additive facts (balances) → Do NOT sum across time in an aggregate. Aggregate across entities only.
- Non-additive facts (unit price, ratios) → Cannot be aggregated by summing. Exclude from aggregates or store count + total for recalculation.
- Ask: *"If I sum this fact from the atomic table, does the result equal what business users expect at the aggregate grain?"* → If yes, include it.

### Step 5 — Identify the Aggregate Navigation Requirement

**How to identify:**

- Aggregate tables are only useful if queries are automatically routed to them.
- Ask: *"Does our BI tool support aggregate navigation (automatic routing to the right table)?"*
- If yes → configure it. Business users should never need to know which table to query.
- If no → evaluate middleware options or document manual query guidelines.

---

## Aggregate Table Identification Checklist

| Signal | Action |
|---|---|
| Queries consistently slow on atomic fact table | Identify candidate aggregate grains |
| Most queries GROUP BY a coarser level (monthly, category) | Build aggregate at that grain |
| Business users report only at summary levels | Aggregate table likely high ROI |
| Additive facts present | Include in aggregate via SUM |
| Semi-additive facts present | Aggregate across entities only, not time |
| Non-additive facts present | Exclude or store components for recalculation |
