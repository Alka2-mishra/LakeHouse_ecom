# E-commerce Medallion Pipeline (Minimal Local Edition)

A small, end-to-end portfolio project demonstrating **Bronze → Silver → Gold** data processing and Power BI-ready analytics.

This edition is intentionally lightweight: it runs on **Windows with Python only**, requires no Linux/WSL, Java, Spark, Delta Lake, or large downloads, and uses only Python's standard library. It is a practical local implementation of medallion-style data engineering—not a claim that local CSV files are Delta tables.

## Architecture

```text
data/sample/*.csv
       |
       v
Bronze: raw source snapshots
       |
       v
Silver: validated and cleaned CSVs
       |
       v
Gold: business-ready aggregate CSVs
       |
       +--> Power BI Desktop
```

- **Bronze** preserves source CSV files as received.
- **Silver** normalizes types, removes invalid rows, handles missing values, and joins orders to customers/products/items/payments.
- **Gold** creates sales-by-month, sales-by-category, top-products, customer-summary, and payment-method datasets.
- **Power BI** imports the Gold CSV files to create a dashboard.

## Requirements

- Python 3.10 or newer
- Power BI Desktop (optional, for the dashboard)
- No third-party Python packages

## Run on Windows

Open PowerShell in the project folder:

```powershell
python --version
python src/run_pipeline.py --input data/sample --output output
```

Expected result:

```text
Pipeline completed successfully.
Bronze: output/bronze
Silver: output/silver
Gold: output/gold
```

To run the lightweight validation tests:

```powershell
python -m unittest discover -s tests -v
```

## Output folders

```text
output/
├── bronze/                  # copied source snapshots
├── silver/
│   ├── orders_clean.csv
│   ├── order_items_clean.csv
│   ├── products_clean.csv
│   ├── customers_clean.csv
│   ├── payments_clean.csv
│   └── order_enriched.csv
└── gold/
    ├── sales_by_month.csv
    ├── sales_by_category.csv
    ├── top_products.csv
    ├── customer_summary.csv
    └── payment_summary.csv
```

Generated output is excluded from Git by `.gitignore`; commit the code and sample input data, not generated files.

## Power BI dashboard

1. Run the pipeline first.
2. Open Power BI Desktop → **Get data** → **Text/CSV**.
3. Import these files from `output/gold/`:
   - `sales_by_month.csv`
   - `sales_by_category.csv`
   - `top_products.csv`
   - `customer_summary.csv`
   - `payment_summary.csv`
4. Suggested visuals:
   - **Cards:** total revenue, number of orders, average order value.
   - **Line chart:** `sales_by_month[month]` vs `sales_by_month[revenue]`.
   - **Bar chart:** `sales_by_category[category]` vs `sales_by_category[revenue]`.
   - **Bar chart:** `top_products[product_name]` vs `top_products[revenue]`.
   - **Donut chart:** `payment_summary[payment_type]` vs `payment_summary[payments]`.
   - **Table:** customer summary with orders and revenue.
5. Format `revenue` and `average_order_value` as currency.

## Data and assumptions

The included data is small, synthetic demo data so the project runs immediately and stays tiny on GitHub. It is not real customer data. The pipeline expects these CSV filenames: `orders.csv`, `order_items.csv`, `products.csv`, `customers.csv`, and `payments.csv`.

Revenue is calculated from order item price plus freight. Cancelled/unavailable orders are excluded from revenue summaries. The demo intentionally favors readability over distributed processing.

## Project structure

```text
src/run_pipeline.py      # complete ETL pipeline
data/sample/             # tiny demo source files
tests/test_pipeline.py   # basic output tests
powerbi/dashboard_guide.md
```

## How to describe this project on a resume

**E-commerce Medallion Data Pipeline | Python, CSV, Power BI**
- Built a Bronze–Silver–Gold ETL pipeline to ingest, clean, join, and aggregate e-commerce order data.
- Produced Power BI-ready sales, product, customer, and payment datasets with data-quality checks.
- Designed a lightweight, reproducible local workflow requiring no external data services.

**Honesty note:** this minimal edition uses CSV files and Python's standard library to avoid heavyweight local setup. It demonstrates the medallion pattern but does not use Apache Spark or Delta Lake. If you later want to demonstrate Spark/Delta specifically, add it as an optional cloud or separate environment extension.
