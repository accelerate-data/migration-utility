"""Extract AdventureWorks data from SQL Server and emit INSERT statements for all dialects.

Despite the filename (kept for consistency with the issue description), this script
reads directly from a live SQL Server with AdventureWorks2022 installed — not from CSVs.

Usage:
    cd plugin/lib
    SA_PASSWORD=<password> uv run python ../../test-fixtures/scripts/csv_to_inserts.py \
        --host localhost --port 1433 \
        --output-dir ../../test-fixtures/data/baseline

Emits: sqlserver.sql, oracle.sql, postgres.sql in the output directory.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import random
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyodbc

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Row cap per table (keeps fixture small)
# ---------------------------------------------------------------------------
ROW_CAP = 5000

# ---------------------------------------------------------------------------
# SQL queries to extract data from AdventureWorks2022
# ---------------------------------------------------------------------------
EXTRACT_QUERIES: dict[str, str] = {
    "stg_customer": """
        SELECT TOP {cap}
            CustomerID AS customer_id,
            PersonID AS person_id,
            StoreID AS store_id,
            TerritoryID AS territory_id,
            AccountNumber AS account_number,
            ModifiedDate AS modified_date
        FROM Sales.Customer
        ORDER BY CustomerID
    """,
    "stg_person": """
        SELECT TOP {cap}
            p.BusinessEntityID AS business_entity_id,
            p.PersonType AS person_type,
            p.Title AS title,
            p.FirstName AS first_name,
            p.MiddleName AS middle_name,
            p.LastName AS last_name,
            p.Suffix AS suffix,
            p.EmailPromotion AS email_promotion,
            p.ModifiedDate AS modified_date
        FROM Person.Person p
        WHERE p.BusinessEntityID IN (
            SELECT PersonID FROM Sales.Customer WHERE PersonID IS NOT NULL
        )
        ORDER BY p.BusinessEntityID
    """,
    "stg_product": """
        SELECT TOP {cap}
            ProductID AS product_id,
            Name AS product_name,
            ProductNumber AS product_number,
            MakeFlag AS make_flag,
            FinishedGoodsFlag AS finished_goods_flag,
            Color AS color,
            SafetyStockLevel AS safety_stock_level,
            ReorderPoint AS reorder_point,
            StandardCost AS standard_cost,
            ListPrice AS list_price,
            Size AS product_size,
            CAST(Weight AS FLOAT) AS weight,
            DaysToManufacture AS days_to_manufacture,
            ProductLine AS product_line,
            Class AS class,
            Style AS style,
            ProductSubcategoryID AS product_subcategory_id,
            ProductModelID AS product_model_id,
            SellStartDate AS sell_start_date,
            SellEndDate AS sell_end_date,
            DiscontinuedDate AS discontinued_date,
            ModifiedDate AS modified_date
        FROM Production.Product
        ORDER BY ProductID
    """,
    "stg_product_subcategory": """
        SELECT
            ProductSubcategoryID AS product_subcategory_id,
            ProductCategoryID AS product_category_id,
            Name AS subcategory_name,
            ModifiedDate AS modified_date
        FROM Production.ProductSubcategory
        ORDER BY ProductSubcategoryID
    """,
    "stg_product_category": """
        SELECT
            ProductCategoryID AS product_category_id,
            Name AS category_name,
            ModifiedDate AS modified_date
        FROM Production.ProductCategory
        ORDER BY ProductCategoryID
    """,
    "stg_sales_order_header": """
        SELECT TOP {cap}
            SalesOrderID AS sales_order_id,
            RevisionNumber AS revision_number,
            OrderDate AS order_date,
            DueDate AS due_date,
            ShipDate AS ship_date,
            Status AS status,
            OnlineOrderFlag AS online_order_flag,
            SalesOrderNumber AS sales_order_number,
            CustomerID AS customer_id,
            SalesPersonID AS sales_person_id,
            TerritoryID AS territory_id,
            BillToAddressID AS bill_to_address_id,
            ShipToAddressID AS ship_to_address_id,
            ShipMethodID AS ship_method_id,
            CreditCardID AS credit_card_id,
            SubTotal AS sub_total,
            TaxAmt AS tax_amt,
            Freight AS freight,
            TotalDue AS total_due,
            ModifiedDate AS modified_date
        FROM Sales.SalesOrderHeader
        ORDER BY SalesOrderID
    """,
    "stg_sales_order_detail": """
        SELECT TOP {cap}
            sod.SalesOrderID AS sales_order_id,
            sod.SalesOrderDetailID AS sales_order_detail_id,
            sod.CarrierTrackingNumber AS carrier_tracking_number,
            sod.OrderQty AS order_qty,
            sod.ProductID AS product_id,
            sod.SpecialOfferID AS special_offer_id,
            sod.UnitPrice AS unit_price,
            sod.UnitPriceDiscount AS unit_price_discount,
            sod.LineTotal AS line_total,
            sod.ModifiedDate AS modified_date
        FROM Sales.SalesOrderDetail sod
        INNER JOIN (
            SELECT TOP {cap} SalesOrderID
            FROM Sales.SalesOrderHeader
            ORDER BY SalesOrderID
        ) h ON sod.SalesOrderID = h.SalesOrderID
        ORDER BY sod.SalesOrderID, sod.SalesOrderDetailID
    """,
    "stg_address": """
        SELECT TOP {cap}
            AddressID AS address_id,
            AddressLine1 AS address_line_1,
            AddressLine2 AS address_line_2,
            City AS city,
            StateProvinceID AS state_province_id,
            PostalCode AS postal_code,
            ModifiedDate AS modified_date
        FROM Person.Address
        ORDER BY AddressID
    """,
    "stg_credit_card": """
        SELECT TOP {cap}
            CreditCardID AS credit_card_id,
            CardType AS card_type,
            CardNumber AS card_number,
            ExpMonth AS exp_month,
            ExpYear AS exp_year,
            ModifiedDate AS modified_date
        FROM Sales.CreditCard
        ORDER BY CreditCardID
    """,
    "stg_employee": """
        SELECT TOP {cap}
            e.BusinessEntityID AS business_entity_id,
            e.NationalIDNumber AS national_id_number,
            e.LoginID AS login_id,
            e.JobTitle AS job_title,
            e.BirthDate AS birth_date,
            e.Gender AS gender,
            e.HireDate AS hire_date,
            e.SalariedFlag AS salaried_flag,
            e.VacationHours AS vacation_hours,
            e.SickLeaveHours AS sick_leave_hours,
            e.CurrentFlag AS current_flag,
            p.FirstName AS first_name,
            p.LastName AS last_name,
            e.ModifiedDate AS modified_date
        FROM HumanResources.Employee e
        INNER JOIN Person.Person p ON e.BusinessEntityID = p.BusinessEntityID
        ORDER BY e.BusinessEntityID
    """,
}

# Order-status reference data (not extracted — static)
ORDER_STATUS_ROWS = [
    (1, "In Process"),
    (2, "Approved"),
    (3, "Backordered"),
    (4, "Rejected"),
    (5, "Shipped"),
    (6, "Cancelled"),
]

# ---------------------------------------------------------------------------
# Dialect-specific formatting
# ---------------------------------------------------------------------------

def _esc(val: str) -> str:
    """Escape single quotes in a string value."""
    return val.replace("'", "''")


def _format_val_sqlserver(val: Any) -> str:
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "1" if val else "0"
    if isinstance(val, (int, Decimal, float)):
        return str(val)
    if isinstance(val, datetime):
        return f"'{val.strftime('%Y-%m-%dT%H:%M:%S.%f')[:23]}'"
    if isinstance(val, date):
        return f"'{val.strftime('%Y-%m-%d')}'"
    return f"N'{_esc(str(val))}'"


def _format_val_oracle(val: Any) -> str:
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "1" if val else "0"
    if isinstance(val, (int, Decimal, float)):
        return str(val)
    if isinstance(val, datetime):
        return f"TO_TIMESTAMP('{val.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
    if isinstance(val, date):
        return f"TO_DATE('{val.strftime('%Y-%m-%d')}', 'YYYY-MM-DD')"
    return f"'{_esc(str(val))}'"


def _format_val_postgres(val: Any) -> str:
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    if isinstance(val, (int, Decimal, float)):
        return str(val)
    if isinstance(val, datetime):
        return f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'"
    if isinstance(val, date):
        return f"'{val.strftime('%Y-%m-%d')}'"
    return f"'{_esc(str(val))}'"


FORMATTERS = {
    "sqlserver": _format_val_sqlserver,
    "oracle": _format_val_oracle,
    "postgres": _format_val_postgres,
}

# Schema prefixes per dialect
SCHEMA_PREFIX = {
    "sqlserver": {"staging": "staging.", "dim": "dim.", "fact": "fact."},
    "oracle": {"staging": "", "dim": "", "fact": ""},
    "postgres": {"staging": "staging.", "dim": "dim.", "fact": "fact."},
}


def _table_layer(table_name: str) -> str:
    if table_name.startswith("stg_") or table_name.startswith("vw_"):
        return "staging"
    if table_name.startswith("dim_"):
        return "dim"
    if table_name.startswith("fct_"):
        return "fact"
    return "staging"


def _full_table(table_name: str, dialect: str) -> str:
    layer = _table_layer(table_name)
    return f"{SCHEMA_PREFIX[dialect][layer]}{table_name}"


# ---------------------------------------------------------------------------
# INSERT generation
# ---------------------------------------------------------------------------

def _generate_inserts(
    table_name: str,
    columns: list[str],
    rows: list[tuple],
    dialect: str,
) -> list[str]:
    fmt = FORMATTERS[dialect]
    full_name = _full_table(table_name, dialect)
    col_list = ", ".join(columns)
    lines: list[str] = []

    for row in rows:
        vals = ", ".join(fmt(v) for v in row)
        lines.append(f"INSERT INTO {full_name} ({col_list}) VALUES ({vals});")

    return lines


# ---------------------------------------------------------------------------
# Dimension population helpers
# ---------------------------------------------------------------------------

def _build_dim_customer(stg_customer: list[dict], stg_person: list[dict]) -> tuple[list[str], list[tuple]]:
    person_map = {p["business_entity_id"]: p for p in stg_person}
    columns = ["customer_id", "person_id", "store_id", "full_name", "store_name", "territory_id", "valid_from", "valid_to", "is_current"]
    rows = []
    for c in stg_customer:
        p = person_map.get(c["person_id"])
        full_name = None
        if p:
            parts = [p["first_name"], p.get("middle_name"), p["last_name"]]
            full_name = " ".join(x for x in parts if x)
        rows.append((
            c["customer_id"], c["person_id"], c["store_id"],
            full_name, None, c["territory_id"],
            datetime(2011, 5, 31), None, True,
        ))
    return columns, rows


def _build_dim_product(
    stg_product: list[dict],
    stg_subcat: list[dict],
    stg_cat: list[dict],
) -> tuple[list[str], list[tuple]]:
    subcat_map = {s["product_subcategory_id"]: s for s in stg_subcat}
    cat_map = {c["product_category_id"]: c for c in stg_cat}
    columns = [
        "product_id", "product_name", "product_number", "color", "class",
        "product_line", "standard_cost", "list_price", "product_subcategory",
        "product_category", "sell_start_date", "sell_end_date",
        "valid_from", "valid_to", "is_current",
    ]
    rows = []
    for p in stg_product:
        subcat = subcat_map.get(p["product_subcategory_id"])
        cat = cat_map.get(subcat["product_category_id"]) if subcat else None
        rows.append((
            p["product_id"], p["product_name"], p["product_number"],
            p["color"], p["class"], p["product_line"],
            p["standard_cost"], p["list_price"],
            subcat["subcategory_name"] if subcat else None,
            cat["category_name"] if cat else None,
            p["sell_start_date"], p["sell_end_date"],
            datetime(2011, 5, 31), None, True,
        ))
    return columns, rows


def _build_dim_date(min_date: date, max_date: date) -> tuple[list[str], list[tuple]]:
    columns = [
        "date_key", "date_day", "day_of_week", "day_of_week_name",
        "day_of_month", "day_of_year", "week_of_year", "month_number",
        "month_name", "quarter_number", "year_number", "is_weekend",
    ]
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    month_names = ["January", "February", "March", "April", "May", "June",
                   "July", "August", "September", "October", "November", "December"]
    rows = []
    d = min_date
    while d <= max_date:
        dow = d.weekday()  # 0=Mon
        rows.append((
            int(d.strftime("%Y%m%d")),
            d,
            dow + 1,
            day_names[dow],
            d.day,
            d.timetuple().tm_yday,
            int(d.strftime("%W")) + 1,
            d.month,
            month_names[d.month - 1],
            (d.month - 1) // 3 + 1,
            d.year,
            dow >= 5,
        ))
        d += timedelta(days=1)
    return columns, rows


def _build_dim_employee(stg_employee: list[dict]) -> tuple[list[str], list[tuple]]:
    columns = [
        "employee_id", "national_id_number", "first_name", "last_name",
        "job_title", "birth_date", "gender", "hire_date",
        "salaried_flag", "current_flag", "valid_from", "valid_to", "is_current",
    ]
    rows = []
    for e in stg_employee:
        rows.append((
            e["business_entity_id"], e["national_id_number"],
            e["first_name"], e["last_name"], e["job_title"],
            e["birth_date"], e["gender"], e["hire_date"],
            e["salaried_flag"], e["current_flag"],
            datetime(2011, 5, 31), None, True,
        ))
    return columns, rows


def _build_dim_product_category(stg_cat: list[dict]) -> tuple[list[str], list[tuple]]:
    columns = ["product_category_id", "category_name", "valid_from", "valid_to", "is_current"]
    rows = [(c["product_category_id"], c["category_name"], datetime(2011, 5, 31), None, True) for c in stg_cat]
    return columns, rows


def _build_dim_address(stg_address: list[dict]) -> tuple[list[str], list[tuple]]:
    columns = ["address_id", "address_line_1", "city", "state_province_id", "postal_code", "valid_from", "valid_to", "is_current"]
    rows = [(a["address_id"], a["address_line_1"], a["city"], a["state_province_id"], a["postal_code"], datetime(2011, 5, 31), None, True) for a in stg_address]
    return columns, rows


def _build_dim_credit_card(stg_cc: list[dict]) -> tuple[list[str], list[tuple]]:
    columns = ["credit_card_id", "card_type", "exp_month", "exp_year", "valid_from", "valid_to", "is_current"]
    rows = [(c["credit_card_id"], c["card_type"], c["exp_month"], c["exp_year"], datetime(2011, 5, 31), None, True) for c in stg_cc]
    return columns, rows


def _build_dim_order_status() -> tuple[list[str], list[tuple]]:
    columns = ["order_status", "order_status_name"]
    return columns, ORDER_STATUS_ROWS


def _build_stg_returns(stg_details: list[dict], seed: int = 42) -> tuple[list[str], list[tuple]]:
    """Generate synthetic returns for ~5% of order details."""
    rng = random.Random(seed)
    reasons = [
        "Defective product", "Wrong item shipped", "Customer changed mind",
        "Damaged in transit", "Size/fit issue",
    ]
    columns = ["return_id", "sales_order_id", "sales_order_detail_id", "return_date", "return_qty", "return_reason"]
    sample = rng.sample(stg_details, min(len(stg_details) // 20, 250))
    rows = []
    for i, d in enumerate(sample, start=1):
        return_date = d["modified_date"] + timedelta(days=rng.randint(1, 30)) if isinstance(d["modified_date"], datetime) else datetime(2014, 1, 15)
        rows.append((
            i,
            d["sales_order_id"],
            d["sales_order_detail_id"],
            return_date,
            min(d["order_qty"], rng.randint(1, 3)),
            rng.choice(reasons),
        ))
    return columns, rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _connect(host: str, port: int, database: str = "AdventureWorks2022") -> pyodbc.Connection:
    password = os.environ.get("SA_PASSWORD", "")
    if not password:
        raise ValueError("SA_PASSWORD environment variable is required")
    drivers = [d for d in pyodbc.drivers() if "ODBC Driver" in d and "SQL Server" in d]
    if not drivers:
        raise RuntimeError("No SQL Server ODBC driver found")
    driver = sorted(drivers)[-1]  # highest version
    conn_str = (
        f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={database};"
        f"UID=SA;PWD={password};TrustServerCertificate=yes"
    )
    return pyodbc.connect(conn_str)


def _fetch(conn: pyodbc.Connection, query: str) -> tuple[list[str], list[tuple], list[dict]]:
    cur = conn.cursor()
    cur.execute(query)
    columns = [desc[0] for desc in cur.description]
    raw_rows = cur.fetchall()
    rows = [tuple(r) for r in raw_rows]
    dicts = [dict(zip(columns, r)) for r in rows]
    return columns, rows, dicts


def _write_file(path: Path, header: str, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(header)
        f.write("\n".join(lines))
        f.write("\n")
    logger.info("event=write_file path=%s lines=%d", path, len(lines))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Extract AW data and emit fixture INSERTs")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=1433)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--cap", type=int, default=ROW_CAP)
    args = parser.parse_args()

    conn = _connect(args.host, args.port)
    logger.info("event=connected host=%s port=%d", args.host, args.port)

    # ------------------------------------------------------------------
    # Extract staging data
    # ------------------------------------------------------------------
    staging_data: dict[str, tuple[list[str], list[tuple], list[dict]]] = {}
    for table, query in EXTRACT_QUERIES.items():
        q = query.format(cap=args.cap)
        cols, rows, dicts = _fetch(conn, q)
        staging_data[table] = (cols, rows, dicts)
        logger.info("event=extracted table=%s rows=%d", table, len(rows))

    conn.close()

    # ------------------------------------------------------------------
    # Build synthetic stg_returns
    # ------------------------------------------------------------------
    _, _, detail_dicts = staging_data["stg_sales_order_detail"]
    ret_cols, ret_rows = _build_stg_returns(detail_dicts)
    staging_data["stg_returns"] = (ret_cols, ret_rows, [dict(zip(ret_cols, r)) for r in ret_rows])
    logger.info("event=generated table=stg_returns rows=%d", len(ret_rows))

    # ------------------------------------------------------------------
    # Build dimension data
    # ------------------------------------------------------------------
    _, _, cust_dicts = staging_data["stg_customer"]
    _, _, person_dicts = staging_data["stg_person"]
    _, _, prod_dicts = staging_data["stg_product"]
    _, _, subcat_dicts = staging_data["stg_product_subcategory"]
    _, _, cat_dicts = staging_data["stg_product_category"]
    _, _, emp_dicts = staging_data["stg_employee"]
    _, _, addr_dicts = staging_data["stg_address"]
    _, _, cc_dicts = staging_data["stg_credit_card"]

    # Determine date range from sales orders
    _, _, soh_dicts = staging_data["stg_sales_order_header"]
    order_dates = [d["order_date"] for d in soh_dicts if d["order_date"]]
    if order_dates:
        min_dt = min(order_dates)
        max_dt = max(order_dates)
        min_date = min_dt.date() if isinstance(min_dt, datetime) else min_dt
        max_date = max_dt.date() if isinstance(max_dt, datetime) else max_dt
        # Extend range slightly
        min_date = date(min_date.year, 1, 1)
        max_date = date(max_date.year, 12, 31)
    else:
        min_date = date(2011, 1, 1)
        max_date = date(2014, 12, 31)

    dim_tables: dict[str, tuple[list[str], list[tuple]]] = {
        "dim_customer": _build_dim_customer(cust_dicts, person_dicts),
        "dim_product": _build_dim_product(prod_dicts, subcat_dicts, cat_dicts),
        "dim_date": _build_dim_date(min_date, max_date),
        "dim_employee": _build_dim_employee(emp_dicts),
        "dim_product_category": _build_dim_product_category(cat_dicts),
        "dim_address": _build_dim_address(addr_dicts),
        "dim_credit_card": _build_dim_credit_card(cc_dicts),
        "dim_order_status": _build_dim_order_status(),
    }

    # ------------------------------------------------------------------
    # Emit per dialect
    # ------------------------------------------------------------------
    for dialect in ("sqlserver", "oracle", "postgres"):
        all_lines: list[str] = []

        header = f"-- Kimball DW Fixture — Baseline Seed ({dialect})\n"
        header += f"-- Generated by csv_to_inserts.py\n"
        header += f"-- Row cap: {args.cap}\n\n"

        if dialect == "sqlserver":
            header += "USE KimballFixture;\nGO\n\n"

        # Staging tables
        for table in EXTRACT_QUERIES:
            cols, rows, _ = staging_data[table]
            inserts = _generate_inserts(table, cols, rows, dialect)
            all_lines.append(f"-- {table}: {len(rows)} rows")
            all_lines.extend(inserts)
            all_lines.append("")

        # stg_returns
        ret_cols_list, ret_rows_list, _ = staging_data["stg_returns"]
        inserts = _generate_inserts("stg_returns", ret_cols_list, ret_rows_list, dialect)
        all_lines.append(f"-- stg_returns: {len(ret_rows_list)} rows")
        all_lines.extend(inserts)
        all_lines.append("")

        # Dimension tables
        for table, (cols, rows) in dim_tables.items():
            inserts = _generate_inserts(table, cols, rows, dialect)
            all_lines.append(f"-- {table}: {len(rows)} rows")
            all_lines.extend(inserts)
            all_lines.append("")

        out_path = args.output_dir / f"{dialect}.sql"
        _write_file(out_path, header, all_lines)
        logger.info("event=dialect_complete dialect=%s path=%s", dialect, out_path)

    logger.info("event=done")


if __name__ == "__main__":
    main()
