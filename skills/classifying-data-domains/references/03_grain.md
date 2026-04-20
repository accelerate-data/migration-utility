# Grain Declaration

## What It Is
The precise, one-sentence definition of what a single row in a fact table represents. It is the most critical decision in dimensional design.

---

## Steps to Identify the Grain

### Step 1 — Find the Most Granular Source Record
**How to identify:**
- Go to the source system that feeds this data mart.
- Ask: *"What is the smallest, most atomic unit of data the source system records?"*
- Look for the finest-level transaction log, scan record, or event entry.
- Examples: a single scanned product at a POS terminal; a single journal entry line; a single lab test result per patient per day.

### Step 2 — Write the Grain as One Sentence
**How to identify:**
- Complete: *"One row in this fact table represents ___."*
- The sentence must be specific enough that any two designers would place the same row count in the table.
- Test: If you and a colleague both designed the table from this sentence, would you get the same number of rows? If not, the grain is still ambiguous.

**Ambiguous:** "One row per sale."
**Specific:** "One row per individual product scanned per POS transaction per store per day."

### Step 3 — Test Every Dimension Against the Grain
**How to identify if a dimension fits the grain:**
- Ask: *"Can I assign exactly one value of this dimension to every fact row at the declared grain?"*
- If yes for every row → dimension fits.
- If some rows would have multiple values, or some rows have no applicable value → dimension does not fit this grain (redesign or choose a different grain).

### Step 4 — Test Every Fact Against the Grain
**How to identify if a fact fits the grain:**
- Ask: *"Is there exactly one value of this fact for every row at this grain?"*
- If a fact only exists at the order-header level but the grain is order-line → the fact doesn't fit. Either allocate it down to the line or put it in a separate fact table.

### Step 5 — Identify Mixed Grains (Red Flag)
**How to identify mixed grains:**
- Look at your candidate fact columns. Do some apply to only a subset of rows?
- Do certain facts make sense only at the total-order level while others make sense at the line-item level?
- High NULL density in a fact column across many rows → likely a grain mismatch.
- Fix: declare a single grain and remove facts that don't belong, or split into two separate fact tables.

---

## Grain Selection Guide

| Signal | Grain Choice |
|---|---|
| Source system records individual events | Atomic transaction grain |
| Source system provides daily/weekly summaries | Periodic snapshot grain |
| Business wants pipeline or lifecycle tracking | Accumulating snapshot grain |
| Only pre-aggregated data available | Coarser summary grain (document the limitation) |

---

## Grain Statement Examples by Industry

| Industry | Business Process | Grain Statement |
|---|---|---|
| Retail | POS Sales | One row per product per POS transaction |
| Retail | Inventory | One row per product per store per calendar day |
| Finance | GL Accounting | One row per journal entry line item |
| Health Care | Billing | One row per billed procedure per claim |
| Insurance | Claims | One row per claim (accumulating snapshot) |
| Education | Enrollment | One row per student per course section per term |
