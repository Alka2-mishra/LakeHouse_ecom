import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import SparkSession

from src.run_pipeline import run_pipeline

ROOT = Path(__file__).resolve().parents[1]


@unittest.skipIf(os.name == "nt" and not shutil.which("winutils.exe"),
                 "Local Spark file writes on Windows require Hadoop winutils.exe; CI runs these integration tests on Linux.")
class PipelineIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        cls.spark = (SparkSession.builder.master("local[2]").appName("pipeline-tests")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_pipeline_writes_parquet_csv_quality_and_metrics(self):
        with tempfile.TemporaryDirectory() as temp:
            out = Path(temp) / "output"
            metrics = run_pipeline(str(ROOT / "data" / "sample"), str(out), {"master": "local[2]", "shuffle_partitions": 2})
            self.assertEqual(metrics["status"], "success")
            self.assertTrue((out / "silver" / "orders" / "_SUCCESS").is_file())
            self.assertTrue((out / "gold" / "sales_by_month.parquet" / "_SUCCESS").is_file())
            self.assertTrue((out / "gold" / "sales_by_month_csv").is_dir())
            self.assertTrue((out / "quality" / "rejected_records" / "_SUCCESS").is_file())
            saved = json.loads((out / "metrics.json").read_text(encoding="utf-8"))
            self.assertEqual(saved["status"], "success")
            self.assertGreater(metrics["tables"]["gold_sales_by_month"], 0)

    def test_safe_rerun_replaces_previous_output(self):
        with tempfile.TemporaryDirectory() as temp:
            out = Path(temp) / "output"
            run_pipeline(str(ROOT / "data" / "sample"), str(out), {"master": "local[2]"})
            (out / "stale.marker").write_text("stale")
            run_pipeline(str(ROOT / "data" / "sample"), str(out), {"master": "local[2]"})
            self.assertFalse((out / "stale.marker").exists())


if __name__ == "__main__":
    unittest.main()
