import csv
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class PipelineTest(unittest.TestCase):
    def test_pipeline_creates_gold_outputs(self):
        with tempfile.TemporaryDirectory() as temp:
            out = Path(temp) / "output"
            subprocess.run(
                [sys.executable, str(ROOT / "src" / "run_pipeline.py"),
                 "--input", str(ROOT / "data" / "sample"), "--output", str(out)],
                check=True, capture_output=True, text=True
            )
            expected = [
                "sales_by_month.csv", "sales_by_category.csv", "top_products.csv",
                "customer_summary.csv", "payment_summary.csv"
            ]
            for filename in expected:
                self.assertTrue((out / "gold" / filename).is_file(), filename)

            with (out / "gold" / "sales_by_month.csv").open(newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            self.assertGreaterEqual(len(rows), 1)
            self.assertTrue(all(float(row["revenue"]) >= 0 for row in rows))


if __name__ == "__main__":
    unittest.main()
