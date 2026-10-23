"""Lightweight, standard-library-only e-commerce medallion pipeline."""
from __future__ import annotations

import argparse
import csv
import shutil
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

INPUT_FILES = ("orders.csv", "order_items.csv", "products.csv", "customers.csv", "payments.csv")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return [{k: (v or "").strip() for k, v in row.items()} for row in csv.DictReader(f)]


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def money(value: str) -> Decimal:
    try:
        return Decimal(value or "0").quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return Decimal("0.00")


def integer(value: str, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def parse_date(value: str):
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None


def main(input_dir: str, output_dir: str) -> None:
    source = Path(input_dir)
    output = Path(output_dir)
    missing = [name for name in INPUT_FILES if not (source / name).is_file()]
    if missing:
        raise FileNotFoundError(
            f"Missing required input file(s) in {source}: {', '.join(missing)}"
        )

    bronze = output / "bronze"
    silver = output / "silver"
    gold = output / "gold"
    for folder in (bronze, silver, gold):
        folder.mkdir(parents=True, exist_ok=True)

    # Bronze: preserve raw files as source snapshots.
    for name in INPUT_FILES:
        shutil.copy2(source / name, bronze / name)

    raw_orders = read_csv(bronze / "orders.csv")
    raw_items = read_csv(bronze / "order_items.csv")
    raw_products = read_csv(bronze / "products.csv")
    raw_customers = read_csv(bronze / "customers.csv")
    raw_payments = read_csv(bronze / "payments.csv")

    # Silver: normalize and validate each entity.
    orders = []
    for row in raw_orders:
        dt = parse_date(row.get("order_date", ""))
        order_id = row.get("order_id", "")
        customer_id = row.get("customer_id", "")
        if not order_id or not customer_id or dt is None:
            continue
        orders.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "order_status": row.get("order_status", "").lower() or "unknown",
            "order_date": dt.isoformat(),
        })

    items = []
    for row in raw_items:
        order_id, product_id = row.get("order_id", ""), row.get("product_id", "")
        qty = integer(row.get("quantity", ""))
        if not order_id or not product_id or qty <= 0:
            continue
        items.append({
            "order_id": order_id,
            "product_id": product_id,
            "quantity": qty,
            "price": money(row.get("price", "")),
            "freight_value": money(row.get("freight_value", "")),
        })

    products = [
        {"product_id": r["product_id"], "product_name": r["product_name"] or "Unknown",
         "category": r["category"] or "Uncategorized"}
        for r in raw_products if r.get("product_id")
    ]
    customers = [
        {"customer_id": r["customer_id"], "customer_city": r.get("customer_city", "") or "Unknown",
         "customer_state": r.get("customer_state", "") or "Unknown"}
        for r in raw_customers if r.get("customer_id")
    ]
    payments = [
        {"order_id": r["order_id"], "payment_type": r.get("payment_type", "").lower() or "unknown",
         "payment_value": money(r.get("payment_value", ""))}
        for r in raw_payments if r.get("order_id")
    ]

    write_csv(silver / "orders_clean.csv", orders,
              ["order_id", "customer_id", "order_status", "order_date"])
    write_csv(silver / "order_items_clean.csv",
              [{**r, "price": f'{r["price"]:.2f}', "freight_value": f'{r["freight_value"]:.2f}'} for r in items],
              ["order_id", "product_id", "quantity", "price", "freight_value"])
    write_csv(silver / "products_clean.csv", products, ["product_id", "product_name", "category"])
    write_csv(silver / "customers_clean.csv", customers, ["customer_id", "customer_city", "customer_state"])
    write_csv(silver / "payments_clean.csv",
              [{**r, "payment_value": f'{r["payment_value"]:.2f}'} for r in payments],
              ["order_id", "payment_type", "payment_value"])

    order_by_id = {r["order_id"]: r for r in orders}
    product_by_id = {r["product_id"]: r for r in products}
    customer_by_id = {r["customer_id"]: r for r in customers}
    payment_by_order = defaultdict(list)
    for payment in payments:
        payment_by_order[payment["order_id"]].append(payment)

    # Enriched order-item grain. Cancelled/unavailable orders remain in Silver but are excluded from Gold.
    enriched = []
    for item in items:
        order = order_by_id.get(item["order_id"])
        if not order:
            continue
        product = product_by_id.get(item["product_id"], {"product_name": "Unknown", "category": "Uncategorized"})
        customer = customer_by_id.get(order["customer_id"], {"customer_city": "Unknown", "customer_state": "Unknown"})
        item_revenue = item["price"] * item["quantity"] + item["freight_value"]
        enriched.append({
            "order_id": order["order_id"],
            "customer_id": order["customer_id"],
            "order_date": order["order_date"],
            "order_status": order["order_status"],
            "product_id": item["product_id"],
            "product_name": product["product_name"],
            "category": product["category"],
            "quantity": item["quantity"],
            "price": f'{item["price"]:.2f}',
            "freight_value": f'{item["freight_value"]:.2f}',
            "item_revenue": f'{item_revenue:.2f}',
            "customer_city": customer["customer_city"],
            "customer_state": customer["customer_state"],
        })
    write_csv(silver / "order_enriched.csv", enriched, [
        "order_id", "customer_id", "order_date", "order_status", "product_id",
        "product_name", "category", "quantity", "price", "freight_value",
        "item_revenue", "customer_city", "customer_state"
    ])

    eligible_statuses = {"delivered", "shipped", "invoiced", "processing"}
    eligible = [r for r in enriched if r["order_status"] in eligible_statuses]
    monthly = defaultdict(lambda: {"revenue": Decimal("0"), "orders": set(), "units": 0})
    category = defaultdict(lambda: {"revenue": Decimal("0"), "orders": set(), "units": 0})
    product = defaultdict(lambda: {"revenue": Decimal("0"), "orders": set(), "units": 0})
    customer = defaultdict(lambda: {"revenue": Decimal("0"), "orders": set()})

    for row in eligible:
        rev = money(row["item_revenue"])
        month = row["order_date"][:7]
        monthly[month]["revenue"] += rev
        monthly[month]["orders"].add(row["order_id"])
        monthly[month]["units"] += integer(str(row["quantity"]))
        category[row["category"]]["revenue"] += rev
        category[row["category"]]["orders"].add(row["order_id"])
        category[row["category"]]["units"] += integer(str(row["quantity"]))
        product[row["product_id"]]["revenue"] += rev
        product[row["product_id"]]["orders"].add(row["order_id"])
        product[row["product_id"]]["units"] += integer(str(row["quantity"]))
        customer[row["customer_id"]]["revenue"] += rev
        customer[row["customer_id"]]["orders"].add(row["order_id"])

    monthly_rows = [{
        "month": key, "revenue": f'{val["revenue"]:.2f}',
        "order_count": len(val["orders"]), "units_sold": val["units"]
    } for key, val in sorted(monthly.items())]
    category_rows = [{
        "category": key, "revenue": f'{val["revenue"]:.2f}',
        "order_count": len(val["orders"]), "units_sold": val["units"]
    } for key, val in sorted(category.items(), key=lambda x: x[1]["revenue"], reverse=True)]
    product_rows = []
    for product_id, val in sorted(product.items(), key=lambda x: x[1]["revenue"], reverse=True):
        prod = product_by_id.get(product_id, {"product_name": "Unknown", "category": "Uncategorized"})
        product_rows.append({
            "product_id": product_id, "product_name": prod["product_name"], "category": prod["category"],
            "revenue": f'{val["revenue"]:.2f}', "order_count": len(val["orders"]), "units_sold": val["units"]
        })
    customer_rows = []
    for customer_id, val in sorted(customer.items(), key=lambda x: x[1]["revenue"], reverse=True):
        customer_rows.append({
            "customer_id": customer_id, "orders": len(val["orders"]), "revenue": f'{val["revenue"]:.2f}',
            "average_order_value": f'{(val["revenue"] / len(val["orders"])):.2f}' if val["orders"] else "0.00"
        })

    payment_totals = defaultdict(lambda: {"value": Decimal("0"), "count": 0})
    eligible_order_ids = {r["order_id"] for r in orders if r["order_status"] in eligible_statuses}
    for payment in payments:
        if payment["order_id"] in eligible_order_ids:
            payment_totals[payment["payment_type"]]["value"] += payment["payment_value"]
            payment_totals[payment["payment_type"]]["count"] += 1
    payment_rows = [{
        "payment_type": key, "payment_value": f'{val["value"]:.2f}', "payment_count": val["count"]
    } for key, val in sorted(payment_totals.items(), key=lambda x: x[1]["value"], reverse=True)]

    write_csv(gold / "sales_by_month.csv", monthly_rows, ["month", "revenue", "order_count", "units_sold"])
    write_csv(gold / "sales_by_category.csv", category_rows, ["category", "revenue", "order_count", "units_sold"])
    write_csv(gold / "top_products.csv", product_rows,
              ["product_id", "product_name", "category", "revenue", "order_count", "units_sold"])
    write_csv(gold / "customer_summary.csv", customer_rows,
              ["customer_id", "orders", "revenue", "average_order_value"])
    write_csv(gold / "payment_summary.csv", payment_rows, ["payment_type", "payment_value", "payment_count"])

    print("Pipeline completed successfully.")
    print(f"Bronze: {bronze}")
    print(f"Silver: {silver}")
    print(f"Gold:   {gold}")
    print(f"Rows processed: orders={len(orders)}, items={len(items)}, enriched_items={len(enriched)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the lightweight e-commerce medallion pipeline.")
    parser.add_argument("--input", default="data/sample", help="Folder containing the five source CSV files.")
    parser.add_argument("--output", default="output", help="Folder where Bronze/Silver/Gold CSVs are written.")
    args = parser.parse_args()
    main(args.input, args.output)
