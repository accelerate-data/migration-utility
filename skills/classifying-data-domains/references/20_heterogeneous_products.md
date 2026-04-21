# Heterogeneous Product Dimension

## What It Is

A design pattern for modeling entities (products, policies, accounts) where different types share some common attributes but also have completely different type-specific attributes.

---

## Steps to Identify When Heterogeneous Product Design Is Needed

### Step 1 — Identify Multiple Entity Types Under One Umbrella

**How to identify:**

- Ask: *"Does our 'product' (or 'account', 'policy') dimension cover multiple fundamentally different types?"*
- Look for a type or category code that segments entities into very different kinds: Checking vs. Mortgage vs. Auto Loan; Life vs. Auto vs. Home insurance; Mobile vs. Broadband vs. TV plan.
- If the answer is yes and the types are structurally different → heterogeneous product design may be needed.

### Step 2 — Identify Type-Specific vs. Common Attributes

**How to identify:**

- For each entity type, list its descriptive attributes.
- Separate: *"Which attributes apply to ALL types?"* vs. *"Which attributes apply ONLY to one type?"*
- Common attributes (product name, product type code, effective date) → go in the core dimension table.
- Type-specific attributes (loan term, LTV ratio for mortgages; overdraft limit for checking; vehicle make for auto loans) → go in extension tables.

### Step 3 — Identify the Null Density Problem

**How to identify:**

- Draft a single flat dimension table with ALL attributes for ALL types.
- Count the percentage of cells that would be NULL (attributes not applicable for a given type).
- If NULL density is high (50%+ of cells are NULL for most rows) → heterogeneous design justified.
- If types are similar enough that most attributes apply to most rows → a single table with nulls may be acceptable.

### Step 4 — Identify the Type Code Column

**How to identify:**

- Find the column that discriminates between product types: `product_type_code`, `account_type`, `policy_line`.
- This column will be the filter used in queries to join to the appropriate extension table.
- Ask: *"How many distinct values does this type code have?"* → Each value is a candidate extension table.

### Step 5 — Decide Between Core+Extension vs. Single Wide Table

**How to identify the right approach:**

- **Use Core + Extension** when: 3+ distinct types, each with 5+ unique attributes, high NULL density in a flat table.
- **Use Single Wide Table** when: 1–2 types, few type-specific attributes, NULL density is manageable.
- Ask: *"Would a single wide table be so sparse that it confuses users and wastes storage?"* → If yes, use core + extension.

---

## Heterogeneous Dimension Identification Signals

| Signal | Indicates Heterogeneous Design Needed |
|---|---|
| A type code that splits entities into very different kinds | ✓ |
| Type-specific attributes that only apply to one entity type | ✓ |
| High NULL density in a single flat dimension table | ✓ |
| 3+ entity types each with distinct attribute sets | ✓ |
| Source system has separate tables per product type | ✓ |

---

## Common Heterogeneous Dimension Scenarios

| Industry | Umbrella Entity | Types |
|---|---|---|
| Financial Services | Account | Checking, Savings, Mortgage, Auto Loan, Investment |
| Insurance | Policy | Auto, Home, Life, Health, Commercial |
| Telecommunications | Product | Mobile, Broadband, TV, Landline |
| Health Care | Procedure | Lab Test, Radiology, Surgery, Pharmacy |
| Retail | Product | Fresh Food, Packaged, Electronics, Apparel |
