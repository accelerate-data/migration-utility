# Degenerate Dimension

## What It Is
A dimension value stored directly in the fact table as a column — with no separate dimension table — because the key has no useful descriptive attributes beyond itself.

---

## Steps to Identify a Degenerate Dimension

### Step 1 — Find Transaction Control Numbers in the Source Data
**How to identify:**
- Look at source system transaction records for ID fields that exist to identify and group a transaction.
- Common examples: POS receipt number, order number, invoice ID, bill of lading, batch number, claim number.
- These are operational control numbers — they exist in the source system's header records.

### Step 2 — Ask Whether It Has Any Descriptive Attributes
**How to identify:**
- Ask: *"If I built a dimension table for this value, what attributes would it have beyond the key itself?"*
- If the answer is *"just the key number — nothing else to describe"* → degenerate dimension (no separate table needed).
- If the answer is *"it has a type, a status, a channel"* → create a proper dimension table for those attributes.

### Step 3 — Confirm It Is Used for Grouping or Filtering in Queries
**How to identify:**
- Ask: *"Would users ever want to GROUP BY or filter by this value?"*
- Example: *"Show me all line items from transaction #00293847"* → users need this value for grouping.
- If users would never reference it in a query → consider excluding it.

### Step 4 — Confirm It Is Not a Surrogate Key or Foreign Key
**How to identify:**
- A degenerate dimension is stored as a raw value (VARCHAR or the source's native type), NOT as a surrogate key integer.
- It does NOT reference another table — there is no dimension table for it.
- It is NOT a numeric measure — it does not get summed or averaged.

---

## Degenerate Dimension vs. Proper Dimension — Decision Test

| Question | Answer | Decision |
|---|---|---|
| Does it have any descriptive attributes beyond the key? | No | Degenerate dimension |
| Does it have any descriptive attributes beyond the key? | Yes | Build a proper dimension table |
| Would users GROUP BY or filter by this value? | Yes | Include as degenerate dimension |
| Would users GROUP BY or filter by this value? | No | Consider excluding entirely |
| Is it a transaction control number from the source? | Yes | Strong degenerate dimension signal |

---

## Common Degenerate Dimensions by Industry

| Industry | Degenerate Dimension |
|---|---|
| Retail | POS receipt / transaction number |
| Order Management | Order number, line number |
| Finance | Journal entry number, voucher ID |
| Health Care | Claim number, encounter ID |
| Manufacturing | Work order number, batch ID |
| Shipping | Bill of lading, tracking number |
