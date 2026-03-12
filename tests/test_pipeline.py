from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from retail_analytics.pipeline import DataQualityError, run_pipeline


class PipelineTests(unittest.TestCase):
    def test_pipeline_generates_gold_metrics_and_rejections(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_csv = root / "sales.csv"
            input_csv.write_text(
                "\n".join(
                    [
                        "order_id,order_date,store_id,product_id,category,quantity,unit_price",
                        "1,2026-01-01,S1,P1,Electronics,2,100.0",
                        "1,2026-01-01,S1,P1,Electronics,2,100.0",
                        "1,2026-01-02,S1,P1,Electronics,1,100.0",
                        "2,2026-01-10,S1,P2,Grocery,4,5.0",
                        "3,invalid,S2,P3,Apparel,1,20.0",
                    ]
                ),
                encoding="utf-8",
            )

            metrics = run_pipeline(input_csv=input_csv, output_dir=root)
            self.assertEqual(metrics.raw_rows, 5)
            self.assertEqual(metrics.valid_rows, 3)
            self.assertEqual(metrics.invalid_rows, 1)
            self.assertEqual(metrics.duplicate_rows_removed, 1)
            self.assertEqual(metrics.total_revenue, "320.00")

            gold_path = root / "gold" / "sales_gold_monthly_category.csv"
            with gold_path.open(encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            self.assertEqual(len(rows), 2)

            rejection_path = root / "silver" / "sales_rejections.csv"
            with rejection_path.open(encoding="utf-8") as f:
                rejected = list(csv.DictReader(f))
            self.assertEqual(len(rejected), 1)
            self.assertIn("time data", rejected[0]["reason"])

            metrics_data = json.loads((root / "pipeline_metrics.json").read_text(encoding="utf-8"))
            self.assertEqual(metrics_data["total_revenue"], "320.00")
            self.assertIn("processed_at_utc", metrics_data)

    def test_strict_mode_raises_on_invalid_data(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_csv = root / "sales.csv"
            input_csv.write_text(
                "\n".join(
                    [
                        "order_id,order_date,store_id,product_id,category,quantity,unit_price",
                        "1,2026-01-01,S1,P1,Electronics,2,100.0",
                        "2,not-a-date,S1,P2,Grocery,4,5.0",
                    ]
                ),
                encoding="utf-8",
            )

            with self.assertRaises(DataQualityError):
                run_pipeline(input_csv=input_csv, output_dir=root, strict=True)

    def test_missing_columns_fail_fast(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_csv = root / "bad.csv"
            input_csv.write_text("order_id,order_date\n1,2026-01-01\n", encoding="utf-8")

            with self.assertRaises(DataQualityError):
                run_pipeline(input_csv=input_csv, output_dir=root)


if __name__ == "__main__":
    unittest.main()
