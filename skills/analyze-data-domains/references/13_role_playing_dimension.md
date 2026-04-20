# Role-Playing Dimension

## What It Is
A single physical dimension table referenced multiple times by the same fact table, each time under a different name (role), representing a different context for the same type of entity.

---

## Steps to Identify Role-Playing Dimensions

### Step 1 — Find Multiple Foreign Keys of the Same Type in the Fact Table
**How to identify:**
- Look at the fact table design and ask: *"Does this fact table have more than one date? More than one employee? More than one geography?"*
- Count the number of foreign keys that would point to the same dimension table type.
- Example: order_date, ship_date, delivery_date, invoice_date → four date foreign keys in one fact table.

### Step 2 — Confirm They Reference the Same Underlying Dimension
**How to identify:**
- Ask: *"Are all these keys referring to the same type of entity — just in different contexts?"*
- Order date, ship date, invoice date are all dates → they all reference the same Date dimension, just at different moments in the process.
- Salesperson, manager, support rep are all employees → they all reference the same Employee dimension, just in different roles.

### Step 3 — Identify Each Role Name
**How to identify:**
- Name each role from the business perspective:
  - Order Date, Requested Ship Date, Actual Ship Date, Invoice Date, Payment Date.
  - Sold-To Customer, Billed-To Customer.
  - Origin Airport, Destination Airport.
- The role name becomes the alias/view name used in BI tools.

### Step 4 — Confirm the Physical Table Is the Same
**How to identify:**
- There is only one `DIM_DATE` table in the database.
- There is only one `DIM_EMPLOYEE` table.
- All roles are implemented as views or aliases of this single physical table.
- This means the dimension attributes are identical for each role — only the context (the foreign key in the fact table) differs.

### Step 5 — Identify NULL Role Keys
**How to identify:**
- Ask: *"Can some role dates be unknown or not yet reached?"*
- Example: `payment_date_key` is NULL if the invoice hasn't been paid yet.
- Pre-create placeholder dimension rows ("Not Yet Applicable") and assign their surrogate key instead of storing NULL.

---

## Role-Playing Identification Signals

| Signal | Indicates Role-Playing |
|---|---|
| Fact table has 2+ date foreign keys | ✓ (most common case) |
| Fact table has 2+ employee/person foreign keys | ✓ |
| Fact table has 2+ geography foreign keys | ✓ |
| The same entity type appears in different contexts | ✓ |
| A single physical dim table, multiple fact table references | ✓ |

---

## Common Role-Playing Scenarios

| Dimension | Roles |
|---|---|
| Date | Order Date, Ship Date, Delivery Date, Invoice Date, Payment Date |
| Geography | Ship-To, Bill-To, Origin, Destination |
| Customer | Buyer, Payer, End User, Referral Source |
| Employee | Salesperson, Manager, Support Rep, Technician |
