"""Configurable PySpark Bronze/Silver/Gold pipeline for the demo store."""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
import time
import uuid
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

INPUT_FILES = ("orders.csv", "order_items.csv", "products.csv", "customers.csv", "payments.csv")
VALID_STATUSES = ("delivered", "shipped", "invoiced", "processing")
SCHEMAS = {
    "orders": T.StructType([T.StructField("order_id", T.StringType()), T.StructField("customer_id", T.StringType()), T.StructField("order_status", T.StringType()), T.StructField("order_date", T.StringType())]),
    "order_items": T.StructType([T.StructField("order_id", T.StringType()), T.StructField("product_id", T.StringType()), T.StructField("quantity", T.StringType()), T.StructField("price", T.StringType()), T.StructField("freight_value", T.StringType())]),
    "products": T.StructType([T.StructField("product_id", T.StringType()), T.StructField("product_name", T.StringType()), T.StructField("category", T.StringType())]),
    "customers": T.StructType([T.StructField("customer_id", T.StringType()), T.StructField("customer_city", T.StringType()), T.StructField("customer_state", T.StringType())]),
    "payments": T.StructType([T.StructField("order_id", T.StringType()), T.StructField("payment_type", T.StringType()), T.StructField("payment_value", T.StringType())]),
}


def configure_logging(path: Path) -> logging.Logger:
    path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("ecommerce_pipeline")
    logger.setLevel(logging.INFO)
    for old_handler in logger.handlers[:]:
        logger.removeHandler(old_handler)
        old_handler.close()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    for handler in (logging.StreamHandler(sys.stdout), logging.FileHandler(path, encoding="utf-8")):
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def read_source(spark: SparkSession, path: Path, schema: T.StructType) -> DataFrame:
    return (spark.read.option("header", True).option("mode", "PERMISSIVE")
            .option("encoding", "UTF-8").schema(schema).csv(str(path)))


def nonblank(column: str):
    return F.col(column).isNotNull() & (F.trim(F.col(column)) != "")


def split_quality(df: DataFrame, valid, reasons) -> tuple[DataFrame, DataFrame]:
    reason_expr = F.concat_ws(";", *[
        F.when(condition, F.lit(reason)) for condition, reason in reasons
    ])
    tagged = df.withColumn("_reject_reason", reason_expr)
    accepted = tagged.filter(valid).drop("_reject_reason")
    rejected = tagged.filter(~valid)
    return accepted, rejected


def deduplicate(df: DataFrame, keys: list[str]) -> tuple[DataFrame, DataFrame]:
    from pyspark.sql import Window
    tie_breakers = [F.col(column).asc_nulls_first() for column in df.columns]
    ranked = df.withColumn("_duplicate_rank", F.row_number().over(Window.partitionBy(*keys).orderBy(*tie_breakers)))
    duplicates = ranked.filter(F.col("_duplicate_rank") > 1).drop("_duplicate_rank").withColumn("_reject_reason", F.lit("duplicate_key"))
    unique = ranked.filter(F.col("_duplicate_rank") == 1).drop("_duplicate_rank")
    return unique, duplicates


def write_parquet(df: DataFrame, path: Path, partitions: list[str] | None = None) -> None:
    writer = df.write.mode("overwrite")
    if partitions:
        writer = writer.partitionBy(*partitions)
    writer.parquet(str(path))


def run_pipeline(input_dir: str, output_dir: str, config: dict | None = None) -> dict:
    config = config or {}
    source = Path(input_dir).resolve()
    target = Path(output_dir).resolve()
    missing = [name for name in INPUT_FILES if not (source / name).is_file()]
    if missing:
        raise FileNotFoundError(f"Missing required input file(s) in {source}: {', '.join(missing)}")
    target.parent.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex[:10]
    stage = target.parent / f".{target.name}.staging-{run_id}"
    backup = target.parent / f".{target.name}.backup-{run_id}"
    stage.mkdir()
    logger = configure_logging(stage / "logs" / "pipeline.log")
    started = time.monotonic()
    spark = (SparkSession.builder.appName(config.get("app_name", "ecommerce-medallion"))
             .master(config.get("master", "local[*]"))
             .config("spark.sql.shuffle.partitions", str(config.get("shuffle_partitions", 4)))
             .config("spark.sql.adaptive.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel(config.get("spark_log_level", "WARN"))
    metrics: dict = {"run_id": run_id, "status": "running", "input_dir": str(source), "tables": {}, "quality": {}}
    try:
        bronze = stage / "bronze"
        bronze.mkdir()
        for name in INPUT_FILES:
            shutil.copy2(source / name, bronze / name)

        raw = {name: read_source(spark, bronze / f"{name}.csv", schema)
               for name, schema in SCHEMAS.items()}
        for name, frame in raw.items():
            metrics["tables"][f"bronze_{name}"] = frame.count()

        rejects: list[DataFrame] = []
        # Canonicalize strings before validation and typed casts.
        orders0 = raw["orders"].select(*[F.trim(F.col(c)).alias(c) for c in raw["orders"].columns])
        orders0 = orders0.withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
        orders, bad = split_quality(orders0,
            nonblank("order_id") & nonblank("customer_id") & F.col("order_date").isNotNull(),
            [(~nonblank("order_id"), "missing_order_id"), (~nonblank("customer_id"), "missing_customer_id"), (F.col("order_date").isNull(), "invalid_order_date")])
        orders, dup = deduplicate(orders, ["order_id"]); rejects += [bad, dup]
        orders = orders.withColumn("order_status", F.lower(F.when(F.trim("order_status") == "", F.lit("unknown")).otherwise(F.coalesce("order_status", F.lit("unknown")))))

        items0 = raw["order_items"].select(*[F.trim(F.col(c)).alias(c) for c in raw["order_items"].columns])
        items0 = (items0.withColumn("quantity", F.col("quantity").cast("int"))
                  .withColumn("price", F.col("price").cast("decimal(12,2)"))
                  .withColumn("freight_value", F.col("freight_value").cast("decimal(12,2)")))
        item_valid = (nonblank("order_id") & nonblank("product_id") & (F.col("quantity") > 0) &
                      F.col("price").isNotNull() & (F.col("price") >= 0) & F.col("freight_value").isNotNull() & (F.col("freight_value") >= 0))
        items, bad = split_quality(items0, item_valid, [
            (~nonblank("order_id"), "missing_order_id"), (~nonblank("product_id"), "missing_product_id"),
            (F.col("quantity").isNull() | (F.col("quantity") <= 0), "invalid_quantity"),
            (F.col("price").isNull() | (F.col("price") < 0), "invalid_price"),
            (F.col("freight_value").isNull() | (F.col("freight_value") < 0), "invalid_freight")])
        items, dup = deduplicate(items, ["order_id", "product_id"]); rejects += [bad, dup]

        products0 = raw["products"].select(*[F.trim(F.col(c)).alias(c) for c in raw["products"].columns])
        products, bad = split_quality(products0, nonblank("product_id"), [(~nonblank("product_id"), "missing_product_id")])
        products, dup = deduplicate(products, ["product_id"]); rejects += [bad, dup]
        products = products.withColumn("product_name", F.when(F.trim("product_name") == "", F.lit("Unknown")).otherwise(F.coalesce("product_name", F.lit("Unknown")))).withColumn("category", F.when(F.trim("category") == "", F.lit("Uncategorized")).otherwise(F.coalesce("category", F.lit("Uncategorized"))))

        customers0 = raw["customers"].select(*[F.trim(F.col(c)).alias(c) for c in raw["customers"].columns])
        customers, bad = split_quality(customers0, nonblank("customer_id"), [(~nonblank("customer_id"), "missing_customer_id")])
        customers, dup = deduplicate(customers, ["customer_id"]); rejects += [bad, dup]
        for col in ("customer_city", "customer_state"):
            customers = customers.withColumn(col, F.when(F.trim(col) == "", F.lit("Unknown")).otherwise(F.coalesce(col, F.lit("Unknown"))))

        payments0 = raw["payments"].select(*[F.trim(F.col(c)).alias(c) for c in raw["payments"].columns]).withColumn("payment_value", F.col("payment_value").cast("decimal(12,2)"))
        payments, bad = split_quality(payments0, nonblank("order_id") & F.col("payment_value").isNotNull() & (F.col("payment_value") >= 0),
            [(~nonblank("order_id"), "missing_order_id"), (F.col("payment_value").isNull() | (F.col("payment_value") < 0), "invalid_payment_value")])
        payments = payments.withColumn("payment_type", F.lower(F.when(F.trim("payment_type") == "", F.lit("unknown")).otherwise(F.coalesce("payment_type", F.lit("unknown")))))
        rejects.append(bad)

        # Reject valid-shaped facts whose foreign keys do not resolve.
        order_keys = orders.select("order_id").distinct()
        product_keys = products.select("product_id").distinct()
        customer_keys = customers.select("customer_id").distinct()
        dangling_items = (items.join(order_keys, "order_id", "left_anti").withColumn("_reject_reason", F.lit("missing_order_reference"))
                         .unionByName(items.join(product_keys, "product_id", "left_anti").withColumn("_reject_reason", F.lit("missing_product_reference")), allowMissingColumns=True))
        items = items.join(order_keys, "order_id", "inner").join(product_keys, "product_id", "inner")
        dangling_orders = orders.join(customer_keys, "customer_id", "left_anti").withColumn("_reject_reason", F.lit("missing_customer_reference"))
        orders = orders.join(customer_keys, "customer_id", "inner")
        rejects += [dangling_items, dangling_orders]
        valid_payment_orders = orders.select("order_id").distinct()
        rejects.append(payments.join(valid_payment_orders, "order_id", "left_anti").withColumn("_reject_reason", F.lit("missing_order_reference")))
        payments = payments.join(valid_payment_orders, "order_id", "inner")

        silver = stage / "silver"
        silver.mkdir()
        clean = {"orders": orders, "order_items": items, "products": products, "customers": customers, "payments": payments}
        for name, frame in clean.items():
            count = frame.count()
            metrics["tables"][f"silver_{name}"] = count
            write_parquet(frame, silver / name)
        rejected = rejects[0]
        for frame in rejects[1:]:
            rejected = rejected.unionByName(frame, allowMissingColumns=True)
        quality = rejected.groupBy("_reject_reason").count().withColumnRenamed("_reject_reason", "reason").withColumnRenamed("count", "rejected_rows")
        metrics["quality"]["rejected_rows"] = rejected.count()
        metrics["quality"]["reasons"] = {r["reason"]: r["rejected_rows"] for r in quality.collect()}
        write_parquet(rejected, stage / "quality" / "rejected_records")
        quality.coalesce(1).write.mode("overwrite").option("header", True).csv(str(stage / "quality" / "summary_csv"))

        # Broadcast dimension lookups and retain a single cached eligible fact frame for all Gold aggregates.
        enriched = (items.join(orders, "order_id").join(F.broadcast(products), "product_id")
                   .join(F.broadcast(customers), "customer_id")
                   .withColumn("item_revenue", F.col("price") * F.col("quantity") + F.col("freight_value")))
        write_parquet(enriched, silver / "order_enriched")
        metrics["tables"]["silver_order_enriched"] = enriched.count()
        eligible = enriched.filter(F.col("order_status").isin(*VALID_STATUSES)).cache()
        eligible.count()
        gold = stage / "gold"
        gold.mkdir()
        gold_tables = {
            "sales_by_month": eligible.groupBy(F.date_format("order_date", "yyyy-MM").alias("month")).agg(F.sum("item_revenue").alias("revenue"), F.countDistinct("order_id").alias("order_count"), F.sum("quantity").alias("units_sold")),
            "sales_by_category": eligible.groupBy("category").agg(F.sum("item_revenue").alias("revenue"), F.countDistinct("order_id").alias("order_count"), F.sum("quantity").alias("units_sold")),
            "top_products": eligible.groupBy("product_id", "product_name", "category").agg(F.sum("item_revenue").alias("revenue"), F.countDistinct("order_id").alias("order_count"), F.sum("quantity").alias("units_sold")),
            "customer_summary": eligible.groupBy("customer_id").agg(F.countDistinct("order_id").alias("orders"), F.sum("item_revenue").alias("revenue")).withColumn("average_order_value", F.col("revenue") / F.col("orders")),
        }
        payments_gold = payments.join(orders.filter(F.col("order_status").isin(*VALID_STATUSES)).select("order_id"), "order_id").groupBy("payment_type").agg(F.sum("payment_value").alias("payment_value"), F.count(F.lit(1)).alias("payment_count"))
        gold_tables["payment_summary"] = payments_gold
        for name, frame in gold_tables.items():
            if name != "sales_by_month":
                frame = frame.orderBy(F.desc("revenue") if "revenue" in frame.columns else F.desc("payment_value"))
            count = frame.count()
            metrics["tables"][f"gold_{name}"] = count
            write_parquet(frame, gold / f"{name}.parquet", ["month"] if name == "sales_by_month" else None)
            frame.coalesce(1).write.mode("overwrite").option("header", True).option("encoding", "UTF-8").csv(str(gold / f"{name}_csv"))
        eligible.unpersist()
        metrics["status"] = "success"
        metrics["duration_seconds"] = round(time.monotonic() - started, 3)
        (stage / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
        logger.info("Pipeline completed: %s", json.dumps(metrics, sort_keys=True))
        spark.stop()
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
            handler.close()
        if target.exists():
            target.rename(backup)
        try:
            stage.rename(target)
        except Exception:
            if backup.exists():
                backup.rename(target)
            raise
        if backup.exists():
            shutil.rmtree(backup)
        return metrics
    except Exception:
        metrics["status"] = "failed"
        metrics["duration_seconds"] = round(time.monotonic() - started, 3)
        logger.exception("Pipeline failed")
        try:
            spark.stop()
        finally:
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)
                handler.close()
            shutil.rmtree(stage, ignore_errors=True)
        raise


def load_config(path: str | None) -> dict:
    config = json.loads(Path(path).read_text(encoding="utf-8")) if path else {}
    for key, env in (("master", "PIPELINE_SPARK_MASTER"), ("shuffle_partitions", "PIPELINE_SHUFFLE_PARTITIONS")):
        if env in os.environ:
            config[key] = int(os.environ[env]) if key == "shuffle_partitions" else os.environ[env]
    return config


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the PySpark e-commerce medallion pipeline.")
    parser.add_argument("--input", default="data/sample")
    parser.add_argument("--output", default="output")
    parser.add_argument("--config", help="Optional JSON Spark and pipeline configuration.")
    args = parser.parse_args()
    run_pipeline(args.input, args.output, load_config(args.config))


if __name__ == "__main__":
    main()
