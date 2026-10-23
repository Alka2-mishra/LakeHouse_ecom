# Power BI Dashboard Guide

Run the pipeline before opening Power BI:

```powershell
python src/run_pipeline.py --input data/sample --output output
```

In Power BI Desktop, use **Home → Get data → Text/CSV** and import the five CSVs in `output/gold/`.

## Recommended layout

- KPI cards: revenue, orders, average order value (derive from `sales_by_month` or use the data model).
- Line chart: `sales_by_month[month]` → `sales_by_month[revenue]`.
- Clustered bar: `sales_by_category[category]` → `sales_by_category[revenue]`.
- Top-N bar: `top_products[product_name]` → `top_products[revenue]`.
- Donut: `payment_summary[payment_type]` → `payment_summary[payment_count]`.
- Table: `customer_summary[customer_id]`, `orders`, `revenue`, `average_order_value`.

## Suggested DAX measures

```DAX
Total Revenue = SUM(sales_by_month[revenue])
Total Orders = SUM(sales_by_month[order_count])
Average Order Value = DIVIDE([Total Revenue], [Total Orders], 0)
```

The sample data is synthetic. Replace `data/sample` with a directory containing the same five CSV filenames to process another dataset.
