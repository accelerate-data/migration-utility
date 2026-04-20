# Data Warehouse Bus Matrix

## What It Is
A planning grid where business processes are rows and shared dimensions are columns. A checkmark at each intersection means that dimension applies to that data mart's fact table.

---

## Steps to Identify and Build the Bus Matrix

### Step 1 — Identify All Business Processes (Rows)
**How to identify:**
- Interview business stakeholders: *"What business activities do you want to analyze?"*
- Look at operational source systems: each system that captures transactions is a business process candidate.
- Ask: *"What does this organization DO that generates measurable data?"*
- Think processes, not departments: "Invoice Processing" not "Accounts Payable Department."

### Step 2 — Identify All Shared Dimensions (Columns)
**How to identify:**
- For each business process, ask: *"What entities does this process involve — products, customers, stores, employees, dates?"*
- Pool all dimension candidates from all business processes into a master list.
- Remove duplicates — the same entity appearing in multiple processes = one conformed dimension column.
- Date is always a column — it applies to every business process.

### Step 3 — Mark the Intersections
**How to identify which checkmarks to place:**
- For each business process row, ask: *"Does this fact table use this dimension?"*
- More specifically: *"Can I assign a single value of this dimension to every row in this fact table at the declared grain?"*
- If yes → checkmark.
- If no → leave blank.

### Step 4 — Identify the Most Widely Shared Dimensions
**How to identify:**
- Count checkmarks per column. The columns with the most checkmarks are the most critical conformed dimensions to define first.
- These widely-shared dimensions (Date, Product, Customer, Store) are the backbone of the bus.

### Step 5 — Identify Drill-Across Pairs
**How to identify:**
- Scan the matrix for rows that share common checkmarks in the same columns.
- Two rows with shared checkmarks = those two data marts CAN be drilled-across on those dimensions.
- Ask: *"Do users want to combine these two marts?"* → If yes, those shared dimensions MUST be conformed.

### Step 6 — Identify Prioritization Signals
**How to identify which data mart to build first:**
- Row with the most business urgency (sponsor demand, business pain).
- Row with the most dimensions already being built (least new work).
- Row whose conformed dimensions will be reused by the most future data marts.

### Step 7 — Identify When the Matrix Is Incomplete or Stale
**How to identify:**
- New data marts are planned but not represented in the matrix → update the matrix.
- A dimension is being built that doesn't appear in the matrix → add it and check its applicability across all rows.
- Use the matrix as a living document — review it at each planning cycle.

---

## Example Matrix (Identify the Patterns)

|  | Date | Product | Store | Customer | Employee |
|---|---|---|---|---|---|
| POS Sales | ✓ | ✓ | ✓ | | |
| Inventory | ✓ | ✓ | ✓ | | |
| Order Mgmt | ✓ | ✓ | | ✓ | ✓ |
| HR | ✓ | | ✓ | | ✓ |

**What to identify from this matrix:**
- Date and Product are the most critical conformed dimensions (most checkmarks).
- POS Sales and Inventory can be drilled-across on Date, Product, and Store.
- Order Mgmt and HR share only Date → limited integration potential.
- Employee is used by Order Mgmt and HR → must be conformed between them.
