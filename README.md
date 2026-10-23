# E-commerce Lakehouse

A local, production-minded **PySpark Bronze → Silver → Gold** project over a small synthetic e-commerce dataset. It demonstrates distributed DataFrame transformations, data quality rejects, rerunnable outputs, Parquet analytics, integration tests, CI, and Power BI-ready exports.

## Architecture

```text
CSV source files
  └─ Bronze: immutable per-run source snapshots
       └─ Silver: typed, deduplicated, validated Parquet tables
            ├─ quality/rejected_records: rejected source rows + reason
            └─ Gold: aggregated Parquet tables + CSV export folders → Power BI
                 └─ metrics.json + logs/pipeline.log
```

All layers are built in a temporary sibling directory. A successful run swaps it into place; a failed run leaves the last successful output intact. Gold calculations reuse a cached eligible fact DataFrame, broadcast the small dimensions, and use adaptive query execution. Gold monthly sales are partitioned by month.

## Requirements and run

- Python 3.10+
- Java 17
- PySpark 3.5.x (`pip install -r requirements.txt`)

On Windows, local Spark writes may additionally require the Hadoop `winutils.exe` binary matching the Hadoop version bundled with PySpark. GitHub Actions runs the integration suite on Linux and does not need that Windows helper.

```powershell
python -m pip install -r requirements.txt
python src/run_pipeline.py --input data/sample --output output --config pipeline_config.json
```

Set `PIPELINE_SPARK_MASTER` or `PIPELINE_SHUFFLE_PARTITIONS` to override the matching JSON values. For larger deployments, provide a Spark master in configuration and an output filesystem that supports directory rename semantics.

## Data quality

The pipeline checks required identifiers, parseable order dates, positive quantities, nonnegative prices/freight/payment values, duplicate business keys, and missing order/product/customer references. Accepted rows become Silver tables; rejected rows retain the original fields plus `_reject_reason`. The grouped counts are in `output/quality/summary_csv/` and full rejected rows are in Parquet at `output/quality/rejected_records/`.

## Output contract

- `output/bronze/*.csv`: source snapshots.
- `output/silver/{orders,order_items,products,customers,payments}/`: typed Parquet tables.
- `output/silver/order_enriched/`: materialized enriched item-grain Parquet view.
- `output/gold/<table>.parquet/`: analytics tables.
- `output/gold/<table>_csv/`: Spark CSV export folder, containing a header and one or more `part-*.csv` files. Import each folder with Power BI's **Folder** connector.
- `output/metrics.json`, `output/logs/pipeline.log`: row counts, quality counts, run status, duration, and execution log.

Gold tables: `sales_by_month`, `sales_by_category`, `top_products`, `customer_summary`, `payment_summary`.

## Tests and CI

```powershell
python -m unittest discover -s tests -v
```

The integration tests run Spark locally, verify Parquet/CSV/quality/metrics output, and exercise safe reruns. GitHub Actions installs Java 17 and PySpark, then runs the same test suite for pushes and pull requests.

## Power BI and portfolio

Follow [the dashboard guide](powerbi/dashboard_guide.md) and see [sample results](powerbi/sample_results.md). The checked-in source data is synthetic demo data; generated output is intentionally ignored by Git. Run the pipeline before taking screenshots, then save report screenshots in `powerbi/screenshots/` and include a short caption and the run's `metrics.json` summary when publishing the portfolio. No real customer data is included.

## Assumptions

Revenue is item price × quantity + freight. Only delivered, shipped, invoiced, and processing orders contribute to Gold sales. Cancelled/unavailable orders remain valid Silver orders but are not included in sales. Payment totals sum payment records associated with eligible orders.
