# Data Warehouse Bus Architecture

## What It Is

The integration framework for the enterprise data warehouse — an architecture that connects independently built data marts through shared conformed dimensions and conformed facts.

---

## Steps to Identify Whether Bus Architecture Is Needed & How to Apply It

### Step 1 — Identify That Multiple Data Marts Will Exist

**How to identify:**

- Ask: *"Will our data warehouse have more than one subject area or more than one data mart?"*
- Ask: *"Will multiple teams build data marts over time?"*
- If yes to either → bus architecture is needed to prevent isolated data silos.

### Step 2 — Identify the Business Processes (Rows of the Bus Matrix)

**How to identify:**

- List every business process the organization wants to analyze: sales, inventory, procurement, HR, finance, CRM, etc.
- Ask: *"What operational activities generate data we want to report on?"*
- Each process = one row in the Bus Matrix = one candidate data mart.

### Step 3 — Identify the Shared Dimensions (Columns of the Bus Matrix)

**How to identify:**

- Look across all the business processes. What descriptive entities appear in multiple processes?
- Ask: *"Does both the sales process AND the inventory process care about products? About stores? About dates?"*
- Every shared entity = one column in the Bus Matrix = one conformed dimension to define.

### Step 4 — Identify Integration Opportunities

**How to identify:**

- Any two data mart rows that share a conformed dimension column can be drilled-across.
- Ask: *"Which pairs of data marts will users want to combine in a single analysis?"*
- Example: Users want inventory turns AND sales revenue side-by-side for the same products → Product must be a conformed dimension between the Inventory and Sales data marts.

### Step 5 — Identify Which Data Mart to Build First

**How to identify:**

- Look at the Bus Matrix. Which row has:
  - The highest business urgency or business value?
  - The most checkmarks in dimensions you are already planning to build?
  - Stakeholders who are ready and engaged?
- Build that data mart first. Its conformed dimensions become the bus that the next data mart connects to.

### Step 6 — Identify When the Architecture Is Being Violated

**How to identify (anti-patterns):**

- A new team builds its own version of the Product dimension with different attribute names or different category hierarchies → bus violation.
- Two data marts produce different revenue totals for the same product and time period → conformed facts not enforced.
- Business users cannot combine metrics from two data marts → conformed dimensions missing.

---

## Bus Architecture Identification Checklist

| Question | If YES → |
|---|---|
| Will 2+ data marts be built? | Bus architecture required |
| Will users combine metrics from 2+ data marts? | Conformed dimensions required |
| Do reports from different systems need to reconcile? | Conformed facts required |
| Is a new team building a dimension that already exists? | Enforce conforming, don't allow a new version |
