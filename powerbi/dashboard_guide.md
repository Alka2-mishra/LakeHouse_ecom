# Power BI Dashboard Guide

## Refresh the data

Run from the repository root:

```powershell
python src/run_pipeline.py --input data/sample --output output --config pipeline_config.json
```

The CSV exports are Spark folders (each has one or more `part-*.csv` files and a header), not individual CSV files. In Power BI Desktop choose **Get data → Folder**, select a Gold export folder such as `output/gold/sales_by_month_csv`, then combine files. Repeat for:

- `sales_by_month_csv`
- `sales_by_category_csv`
- `top_products_csv`
- `customer_summary_csv`
- `payment_summary_csv`

Set revenue and payment value columns to decimal/currency types. Set `month` to text or a first-of-month date before sorting chronologically.

## Model and visuals

These Gold tables are already aggregated. They do not require relationships for the suggested overview page:

- KPI cards: total sales, order count, average order value.
- Line chart: `sales_by_month[month]` and `[revenue]`.
- Bar chart: `sales_by_category[category]` and `[revenue]`.
- Top products bar: `top_products[product_name]` and `[revenue]`; apply a Top N filter of 10 by revenue.
- Donut: `payment_summary[payment_type]` and `[payment_count]`.
- Customer table: `customer_summary[customer_id]`, `[orders]`, `[revenue]`, `[average_order_value]`.

Suggested measures:

```DAX
Total Revenue = SUM(sales_by_month[revenue])
Total Orders = SUM(sales_by_month[order_count])
Average Order Value = DIVIDE([Total Revenue], [Total Orders], 0)
```

## Portfolio publishing checklist

1. Refresh using the real intended input data, and confirm `output/metrics.json` reports `success`.
2. Review `output/quality/summary_csv/` and resolve or explain any rejects.
3. Save the Power BI report as `powerbi/ecommerce_dashboard.pbix` if the repository size and licensing allow it.
4. Capture the overview page and one data quality or drill-down view; save PNG files under `powerbi/screenshots/`.
5. Publish the screenshots with a caption, the synthetic/real data note, and key counts from `metrics.json`. Do not publish customer-identifying data.

The repository includes the dashboard design and Power BI-ready exports. A `.pbix` and Desktop screenshots must be produced in Power BI Desktop after refreshing the local outputs; those are not fabricated by the ETL pipeline.
