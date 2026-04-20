# Date Dimension

## What It Is

A pre-built dimension table with one row per calendar day, containing rich calendar, fiscal, and business-context attributes for every date.

---

## Steps to Identify Date Dimension Requirements

### Step 1 — Identify Every Date in Every Fact Table

**How to identify:**

- Examine every fact table design. Find every date or timestamp column.
- Ask: *"Is this date stored as a raw SQL date type, or does it reference a Date dimension?"*
- Every date in every fact table should be a foreign key to the Date dimension — not a raw date column.
- If you have raw date columns in fact tables → convert them to foreign keys pointing to DIM_DATE.

### Step 2 — Identify How Many Roles the Date Plays in Each Fact Table

**How to identify:**

- Count the date foreign keys in the fact table.
- If more than one (order_date_key, ship_date_key, invoice_date_key) → each is a different role for the same DIM_DATE.
- Each role needs its own named view or alias of DIM_DATE.

### Step 3 — Identify Fiscal Calendar Requirements

**How to identify:**

- Ask: *"Does the organization use a non-standard fiscal year?"*
- Ask: *"Do reports reference fiscal quarters, fiscal months, or fiscal periods that don't align with the calendar?"*
- If yes → the Date dimension must include fiscal calendar attributes alongside the standard calendar attributes.

### Step 4 — Identify Holiday and Special Day Requirements

**How to identify:**

- Ask: *"Do reports need to distinguish weekdays vs. weekends?"* → Add `is_weekend_flag`.
- Ask: *"Do reports need to identify company holidays or public holidays?"* → Add `is_holiday_flag` and `holiday_name`.
- Ask: *"Does the business use any special trading periods or reporting seasons?"* → Add relevant flags.

### Step 5 — Identify the Required Date Range

**How to identify:**

- Ask: *"How far back does the oldest data in the warehouse go?"* → Start date of the Date dimension.
- Ask: *"How far into the future do forecasts, budgets, or planned events extend?"* → End date of the Date dimension.
- Best practice: build slightly beyond the known range on both ends to avoid ETL failures from out-of-range dates.

### Step 6 — Identify SQL Date Functions Being Used Instead of Dimension Attributes

**How to identify:**

- Look at existing reports and queries. Are they using `MONTH()`, `YEAR()`, `DATEPART()`, or custom fiscal calendar logic in SQL?
- Every such function is a signal that the Date dimension is missing the attribute those functions are trying to compute.
- Each SQL date function in a report → add the equivalent pre-computed attribute to the Date dimension.

---

## Date Dimension Attribute Identification Checklist

| Business Need | Attribute to Add |
|---|---|
| Filter by day of week | `day_of_week`, `is_weekend_flag` |
| Filter by month name | `month_name`, `month_number` |
| Filter by quarter | `quarter`, `quarter_number` |
| Fiscal reporting | `fiscal_month`, `fiscal_quarter`, `fiscal_year` |
| Holiday exclusion | `is_holiday_flag`, `holiday_name` |
| Year-over-year comparison | `year`, `day_number_in_year` |
| Week-based reporting | `week_number_in_year` |
| Rolling window queries | `is_current_month`, `days_ago` |
