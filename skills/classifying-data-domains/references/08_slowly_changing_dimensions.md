# Slowly Changing Dimensions (SCD)

## What It Is
A set of techniques for handling changes to dimension attribute values over time. The correct SCD type for each attribute is a business decision, not a technical one.

---

## Steps to Identify Which SCD Type Is Needed

### Step 1 — Identify Which Dimension Attributes Can Change
**How to identify:**
- For every attribute in every dimension, ask: *"Can this value change for the same entity over time?"*
- Examples that change: customer address, employee department, product category, store manager, sales territory.
- Examples that don't change: customer birth date, original account open date, product creation date.

### Step 2 — For Each Changing Attribute, Identify Whether History Matters
**How to identify for Type 1 vs. Type 2:**
- Ask the business: *"If this attribute changes, do you want historical reports to show the old value or the new value?"*
- *"We want history to reflect what was true at the time"* → **Type 2** (new row per change).
- *"We only care about the current correct value; old value is irrelevant"* → **Type 1** (overwrite).
- A correction (typo, data entry error) → always **Type 1**.
- A legitimate business change that affects how we analyze history → **Type 2**.

### Step 3 — Identify Type 2 Scenarios
**How to identify:**
- Ask: *"Would changing this attribute rewrite history in a way that misleads business users?"*
- Example: A product moves from the Education category to the Strategy category. If we overwrite (Type 1), all historical Education sales will now appear under Strategy — misleading.
- Fix: Type 2 — add a new row. Historical sales stay linked to Education; future sales link to Strategy.
- Signal: Any attribute where business users would say *"tell me how it was classified at the time of the transaction."*

### Step 4 — Identify Type 3 Scenarios
**How to identify:**
- Ask: *"Does the business need to analyze data under BOTH the old AND the new value simultaneously?"*
- Scenario: A territory reorganization where management wants to compare performance under old vs. new territory lines for a transition period.
- Signal: A specific, one-time organizational change where both before and after views are needed side-by-side.
- Limitation: Only tracks ONE prior value — not suitable for attributes that change many times.

### Step 5 — Identify Rapidly Changing Attributes (Not Suitable for Type 2)
**How to identify:**
- Ask: *"How often does this attribute change per entity per year?"*
- If an attribute changes dozens or hundreds of times (e.g., a customer's behavioral score updates weekly) → Type 2 would cause dimension table explosion.
- Signal: High change frequency + the attribute is on a large dimension (millions of rows) → use a **Minidimension** instead.

### Step 6 — Identify Type 0 Attributes
**How to identify:**
- Ask: *"Should this attribute ever change after initial load, by business definition?"*
- Examples: original application date, founding year, birth date.
- If the business says *"this value should never change"* → Type 0.

---

## SCD Type Decision Tree

```
Is the change a data correction?
  → YES: Type 1 (overwrite)
  → NO:
    Does history matter for this attribute?
      → NO: Type 1 (overwrite)
      → YES:
        Does the business need both old and new values simultaneously?
          → YES (one-time restructure): Type 3 (new column)
          → NO:
            Does the attribute change very frequently on a large dimension?
              → YES: Minidimension
              → NO: Type 2 (new row)
```

---

## SCD Type Quick Reference

| SCD Type | When to Use | How to Identify |
|---|---|---|
| Type 0 | Attribute must never change | Business says "freeze at load time" |
| Type 1 | Correction or history not needed | Overwrite is acceptable; no historical analysis needed |
| Type 2 | Legitimate change, history matters | Users need "as-was" analysis at transaction time |
| Type 3 | Need both old and new simultaneously | One-time restructure with transition period |
| Minidimension | Rapidly changing attributes on large dimension | High change frequency, large dimension row count |
