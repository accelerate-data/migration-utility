# Conformed Dimensions

## What It Is

A dimension table defined identically — same attributes, same values, same meaning — and shared across multiple fact tables and data marts. The foundation of enterprise data warehouse integration.

---

## Steps to Identify Conformed Dimensions

### Step 1 — Identify Dimensions That Appear in Multiple Business Processes

**How to identify:**

- Look at the Bus Matrix: any dimension column with checkmarks in multiple rows is a conformed dimension candidate.
- Ask: *"Does this dimension (Date, Product, Customer, Store) describe entities that appear in more than one business process?"*
- Common universal conformed dimensions: Date, Product/Item, Customer, Location/Store, Employee.

### Step 2 — Identify Whether the Same Entity Is Described the Same Way Across Processes

**How to identify:**

- Pull the product dimension from the sales system and from the inventory system side by side.
- Ask: *"Are the same product attributes defined and labeled the same way in both?"*
- If YES and identical → already conformed (or easy to conform).
- If NO → there is definitional conflict that must be resolved before conforming.

### Step 3 — Identify Definitional Conflicts to Resolve

**How to identify:**

- Look for: different names for the same attribute (`prod_cat` vs `product_category`), different value domains (`SW` vs `Software` for the same thing), different hierarchies, different grain.
- Any conflict → must be resolved through business agreement before the dimension can be truly conformed.
- Ask business stakeholders: *"When you say 'product category', do both teams mean exactly the same thing?"*

### Step 4 — Identify Row and Column Subset Situations

**How to identify:**

- Ask: *"Does every data mart need every row and every column of this dimension?"*
- If a data mart only needs beverage products → it uses a row subset of the master product dimension. Still conformed as long as shared rows are identical.
- If a monthly budget data mart only needs category-level product data → it uses a column subset. Still conformed at the category level.

### Step 5 — Identify Drill-Across Opportunities

**How to identify:**

- Any two fact tables (from different data marts) that share a conformed dimension can be drilled-across.
- Ask: *"Do business users want to see sales revenue and inventory levels side by side for the same products?"*
- If yes → Product must be a conformed dimension between the two data marts.
- If the dimensions are NOT conformed → drill-across will produce misaligned or irreconcilable results.

---

## Conformed Dimension Identification Checklist

| Question | If YES → |
|---|---|
| Does this dimension appear in 2+ data marts? | Conformed dimension candidate |
| Do users want to combine metrics from 2+ data marts? | Conformed dimension required |
| Do reports from different data marts need to reconcile? | Conformed dimensions required |
| Same entity described differently in different systems? | Conforming effort required first |
