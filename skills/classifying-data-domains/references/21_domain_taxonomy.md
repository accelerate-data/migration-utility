# Domain Taxonomy Reference

Contains canonical domain definitions, keyword lists, and industry-specific variants.

---

## Table of Contents

1. [Universal Business Domains](#universal-business-domains)
2. [Domain Keyword Library](#domain-keyword-library)
3. [Industry-Specific Domain Variants](#industry-specific-domain-variants)
4. [Domain Assignment Rules](#domain-assignment-rules)
5. [Cross-Domain Table Patterns](#cross-domain-table-patterns)

---

## Universal Business Domains

These domains apply across all industries. Always check for these first before applying industry-specific overrides.

| Domain | Code | Description | Typical DW Roles |
|---|---|---|---|
| **Customer** | CUST | All entities describing who buys or uses the product/service. Includes individuals, households, organisations, accounts. | Dimension, Fact (behaviour), Bridge (multi-owner) |
| **Product** | PROD | All entities describing what is sold, offered, or produced. Includes SKUs, services, policies, accounts, procedures. | Dimension, Reference |
| **Sales / Revenue** | SALE | Business process of recording what was sold, when, to whom, for how much. | Fact (transaction, snapshot), Dimension (promotion, deal) |
| **Inventory / Supply Chain** | INVT | Tracking physical goods from procurement through to delivery. Includes inventory levels, movements, and procurement events. | Fact (snapshot, transaction), Dimension (warehouse, supplier) |
| **Finance / Accounting** | FIN | General ledger, journals, budgets, profit & loss, cost centres, accounts payable/receivable. | Fact (journal entry, budget), Dimension (account, cost centre) |
| **Operations** | OPS | Operational processes not covered by Sales or Supply Chain — manufacturing, facilities, service delivery. | Fact (work order, service event), Dimension (equipment, location) |
| **Human Resources** | HR | Employees, jobs, compensation, attendance, performance, organisational structure. | Fact (headcount snapshot, payroll), Dimension (employee, job, department) |
| **Marketing** | MKT | Campaigns, promotions, channels, ad spend, lead generation, customer segments. | Fact (campaign response, spend), Dimension (campaign, channel, segment) |
| **Geography / Location** | GEO | Physical locations: stores, warehouses, regions, territories, postal codes, countries. | Dimension (shared across many domains) |
| **Date / Time** | DATE | Calendar and fiscal time periods. Always a conformed dimension. | Dimension (universal) |
| **Unclassified** | UNCL | Cannot be assigned with available evidence. Requires manual review. | Unknown |

---

## Domain Keyword Library

For each domain, tables are matched when their name contains any of the listed keywords (case-insensitive, partial match acceptable for compound words).

### Customer Domain Keywords

```text
customer, cust, client, account, holder, member, subscriber, household, contact,
individual, party, person, consumer, buyer, payer, recipient, user, profile,
loyalty, segment, tier, relationship, crm, lead, prospect
```

**Column-level reinforcement (if DDL available):**

```text
customer_id, client_id, account_number, member_id, subscriber_id, household_id,
loyalty_number, crm_id
```

---

### Product Domain Keywords

```text
product, prod, item, sku, article, catalog, catalogue, service, offering,
merchandise, good, material, component, part, variant, bundle, package,
brand, category, subcategory, department, class, subclass, hierarchy
```

**Column-level reinforcement:**

```text
product_id, sku_number, item_code, article_number, upc, ean, gtin,
product_key, brand_key, category_key
```

---

### Sales / Revenue Domain Keywords

```text
sale, sales, revenue, order, orders, invoice, invoicing, transaction, pos,
purchase, receipt, booking, reservation, contract, deal, quote, quotation,
promotion, promo, discount, coupon, basket, cart, line_item, shipment,
delivery, fulfillment
```

**Column-level reinforcement:**

```text
sales_amount, revenue_amount, order_amount, invoice_amount, discount_amount,
quantity_sold, extended_price, gross_profit
```

---

### Inventory / Supply Chain Domain Keywords

```text
inventory, invt, stock, warehouse, wh, supply, chain, procurement, procure,
purchasing, vendor, supplier, purchase_order, po, receiving, receipt,
shipment, logistics, distribution, replenishment, reorder, backorder,
on_hand, on_order, lead_time, safety_stock
```

**Column-level reinforcement:**

```text
quantity_on_hand, quantity_on_order, reorder_point, safety_stock_quantity,
supplier_id, vendor_id, purchase_order_number, warehouse_id
```

---

### Finance / Accounting Domain Keywords

```text
finance, fin, financial, accounting, acct, gl, general_ledger, journal,
ledger, account, chart_of_accounts, coa, budget, forecast, cost_centre,
cost_center, profit_loss, pnl, balance_sheet, revenue_recognition,
accounts_payable, ap, accounts_receivable, ar, cash, treasury,
currency, exchange_rate, fx, tax, vat, gst
```

**Column-level reinforcement:**

```text
journal_entry_id, voucher_number, gl_account_code, cost_centre_code,
budget_amount, actual_amount, variance_amount, debit_amount, credit_amount
```

---

### Human Resources Domain Keywords

```text
hr, human_resource, employee, emp, staff, worker, headcount, payroll,
salary, compensation, benefit, job, role, position, department, dept,
division, organisation, org, attendance, absence, leave, performance,
review, appraisal, hire, termination, onboard, training
```

**Column-level reinforcement:**

```text
employee_id, emp_id, job_code, department_id, manager_id, hire_date,
termination_date, salary_amount, headcount
```

---

### Marketing Domain Keywords

```text
marketing, mkt, campaign, channel, segment, audience, lead, prospect,
impression, click, conversion, attribution, spend, media, ad, advert,
advertisement, email, social, digital, content, landing, funnel, pipeline,
acquisition, retention, churn
```

**Column-level reinforcement:**

```text
campaign_id, channel_id, segment_id, impression_count, click_count,
conversion_count, spend_amount, cost_per_acquisition
```

---

### Geography / Location Domain Keywords

```text
geography, geo, location, loc, store, shop, branch, outlet, site, facility,
region, territory, district, zone, area, country, state, province, city,
postal, zipcode, zip, address, warehouse, wh, plant
```

**Column-level reinforcement:**

```text
location_id, store_id, region_code, territory_code, country_code, state_code,
city_name, postal_code, latitude, longitude
```

---

### Date / Time Domain Keywords

```text
date, time, calendar, fiscal, period, day, week, month, quarter, year,
hour, minute, holiday, weekday, weekend
```

**Column-level reinforcement:**

```text
date_key, calendar_date, fiscal_period, fiscal_year, day_of_week,
month_name, quarter_number, is_holiday, is_weekend
```

---

### Reference / Lookup Domain Keywords

```text
lookup, lkp, reference, ref, code, type, status, category, reason,
classification, decode, mapping, enumeration, enum, picklist, list,
master_data
```

**Structural reinforcement:**

- ≤ 5 columns
- One code column + one description column
- No FK constraints

---

### Staging / ETL Domain Keywords

```text
stg, stage, staging, raw, land, landing, src, source, inbound, extract,
temp, tmp, work, wrk, error, reject, audit, batch, watermark, control,
etl, pipeline, load, incremental
```

---

## Industry-Specific Domain Variants

When `industry_context` is provided, apply the additional domain and keyword overrides below.

### Retail

Add domains:

- **Promotion** (keywords: `promo`, `promotion`, `coupon`, `discount`, `offer`, `circular`, `flyer`, `ad_item`)
- **Basket / Market Basket** (keywords: `basket`, `cart`, `market_basket`, `affinity`)

Rename / specialise:

- Product domain → include `planogram`, `merchandise`, `assortment`
- Location domain → include `store`, `outlet`, `format`, `banner`, `chain`

---

### Banking / Financial Services

Add domains:

- **Account** (keywords: `account`, `acct`, `deposit`, `loan`, `mortgage`, `credit`, `debit`, `card`, `liability`, `asset`)
- **Transaction** (keywords: `transaction`, `txn`, `transfer`, `payment`, `settlement`, `clearing`, `wire`)
- **Risk / Compliance** (keywords: `risk`, `compliance`, `aml`, `kyc`, `fraud`, `limit`, `exposure`, `collateral`, `provision`)
- **Instrument / Product** (keywords: `instrument`, `security`, `equity`, `bond`, `derivative`, `fund`, `portfolio`)

Specialise:

- Customer domain → include `household`, `corporate`, `counterparty`, `obligor`

---

### Insurance

Add domains:

- **Policy** (keywords: `policy`, `coverage`, `premium`, `endorsement`, `rider`, `term`, `renewal`, `lapse`)
- **Claims** (keywords: `claim`, `loss`, `incident`, `accident`, `indemnity`, `settlement`, `subrogation`, `reserve`)
- **Underwriting** (keywords: `underwriting`, `risk_score`, `exposure`, `actuarial`, `rate`, `rating`)
- **Agent / Distribution** (keywords: `agent`, `broker`, `producer`, `commission`, `channel`)

---

### Health Care

Add domains:

- **Clinical / Patient** (keywords: `patient`, `encounter`, `visit`, `admission`, `discharge`, `diagnosis`, `procedure`, `treatment`, `medication`, `prescription`, `lab`, `test`, `result`, `provider`, `physician`)
- **Claims / Billing** (keywords: `claim`, `billing`, `charge`, `reimbursement`, `adjudication`, `denial`, `icd`, `cpt`, `drg`, `revenue_code`)
- **Facility** (keywords: `facility`, `hospital`, `clinic`, `ward`, `bed`, `unit`, `department`)

---

### Telecommunications

Add domains:

- **Network** (keywords: `network`, `cell`, `tower`, `site`, `equipment`, `device`, `sim`, `imei`, `node`, `circuit`)
- **Usage / Billing** (keywords: `cdr`, `call_detail`, `usage`, `data_usage`, `roaming`, `rating`, `billing_cycle`)
- **Service / Plan** (keywords: `plan`, `subscription`, `bundle`, `tariff`, `contract`, `activation`, `churn`, `port`)

---

### Education

Add domains:

- **Academic** (keywords: `student`, `course`, `enrollment`, `registration`, `grade`, `curriculum`, `program`, `degree`, `faculty`, `instructor`, `class`, `section`, `term`, `semester`)
- **Admissions** (keywords: `application`, `applicant`, `admission`, `acceptance`, `waitlist`, `offer`)

---

## Domain Assignment Rules

### Rule 1 — Prefix/Suffix First

If the table name contains a clear domain keyword as a prefix or suffix segment, assign that domain at `high` confidence before checking any other signals.

Examples:

- `DIM_CUSTOMER_PROFILE` → Customer (keyword: `CUSTOMER`)
- `FACT_HR_HEADCOUNT` → HR (keyword: `HR`)
- `STG_FINANCIAL_TRANSACTIONS` → Finance domain (prefix `STG_` → Staging role; keyword `FINANCIAL` → Finance domain)

### Rule 2 — Column Signals Reinforce or Override

If the table name is ambiguous (no domain keyword), use column-level reinforcement signals from the keyword library above. A single strong column-level signal upgrades confidence from `low` to `medium`.

### Rule 3 — FK Graph Inheritance

If Table A has a FK to Table B, and Table B has a confirmed domain assignment, Table A inherits Table B's domain as a `low`-confidence candidate. This is an inheritance suggestion only — override with name/column signals if available.

### Rule 4 — Staging Is a Role, Not a Functional Domain

Staging tables use the Staging role. Assign their primary functional domain from subject-matter evidence in the table name, columns, schema, or dependencies. If no subject-matter owner is visible, assign `Unclassified` with low confidence.

### Rule 5 — Reference Is a Role, Not a Functional Domain

Reference / Lookup tables (`LKP_`, `REF_`, `CODE_`) use the Reference role. Assign their primary functional domain from business stewardship evidence such as schema, subject-matter terms, columns, or dependencies. For example, `LKP_CURRENCY` belongs to Finance when the DDL places it in finance stewardship.

### Rule 6 — Date/Time Domain Is Always Conformed

Tables matching the Date/Time domain are always conformed dimensions. Flag them as `conformed_dimension: true` and do not assign them to any subject-matter domain.

---

## Cross-Domain Table Patterns

These table name patterns commonly appear at domain boundaries and require careful classification:

| Pattern | Common Interpretation |
|---|---|
| `FACT_CUSTOMER_ORDERS` | Sales domain (Fact), Customer secondary |
| `DIM_PRODUCT_PROMOTION` | Marketing domain (Dim), Product secondary |
| `BRIDGE_ACCOUNT_HOLDER` | Customer domain (Bridge) |
| `XREF_PATIENT_DIAGNOSIS` | Clinical domain (Bridge) |
| `LKP_CURRENCY` | Finance domain (Reference) |
| `DIM_SALES_TERRITORY` | Geography domain (Dim), Sales secondary |
| `FACT_POLICY_CLAIMS` | Claims domain (Fact), Policy secondary |
| `AGG_MONTHLY_REVENUE_BY_CHANNEL` | Sales domain (Aggregate), Marketing secondary |
