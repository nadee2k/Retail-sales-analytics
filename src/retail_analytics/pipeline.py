from __future__ import annotations

import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

REQUIRED_COLUMNS = [
    "order_id",
    "order_date",
    "store_id",
    "product_id",
    "category",
    "quantity",
    "unit_price",
]


@dataclass
class PipelineMetrics:
    raw_rows: int
    valid_rows: int
    duplicate_rows_removed: int
    invalid_rows: int
    total_revenue: float


class DataQualityError(ValueError):
    """Raised when required data contracts are violated."""


def _read_raw_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        rows = list(reader)
        if not reader.fieldnames:
            raise DataQualityError("Input file has no header row")

    missing = [col for col in REQUIRED_COLUMNS if col not in reader.fieldnames]
    if missing:
        raise DataQualityError(f"Missing required columns: {', '.join(missing)}")

    return rows


def _validate_and_standardize(rows: Iterable[dict[str, str]]) -> tuple[list[dict[str, str]], int]:
    dedupe_set: set[tuple[str, str]] = set()
    valid: list[dict[str, str]] = []
    invalid_count = 0

    for row in rows:
        key = (row["order_id"].strip(), row["product_id"].strip())
        if key in dedupe_set:
            continue

        try:
            order_date = datetime.strptime(row["order_date"], "%Y-%m-%d").date().isoformat()
            quantity = int(row["quantity"])
            unit_price = float(row["unit_price"])
            if quantity <= 0 or unit_price < 0:
                raise ValueError("quantity and price must be positive")
        except Exception:
            invalid_count += 1
            continue

        dedupe_set.add(key)
        valid.append(
            {
                "order_id": row["order_id"].strip(),
                "order_date": order_date,
                "store_id": row["store_id"].strip(),
                "product_id": row["product_id"].strip(),
                "category": row["category"].strip().lower(),
                "quantity": str(quantity),
                "unit_price": f"{unit_price:.2f}",
                "revenue": f"{quantity * unit_price:.2f}",
                "year_month": order_date[:7],
            }
        )

    return valid, invalid_count


def _write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _aggregate_gold(silver_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    agg: dict[tuple[str, str], dict[str, float]] = defaultdict(lambda: {"units": 0.0, "revenue": 0.0})
    for row in silver_rows:
        key = (row["year_month"], row["category"])
        agg[key]["units"] += float(row["quantity"])
        agg[key]["revenue"] += float(row["revenue"])

    output = []
    for (year_month, category), values in sorted(agg.items()):
        output.append(
            {
                "year_month": year_month,
                "category": category,
                "units": str(int(values["units"])),
                "revenue": f"{values['revenue']:.2f}",
                "avg_unit_price": f"{(values['revenue'] / values['units']):.2f}",
            }
        )
    return output


def run_pipeline(input_csv: Path, output_dir: Path) -> PipelineMetrics:
    """Run a Bronze/Silver/Gold retail batch pipeline over a CSV source."""
    raw_rows = _read_raw_csv(input_csv)

    bronze_fields = list(raw_rows[0].keys()) if raw_rows else REQUIRED_COLUMNS
    bronze_path = output_dir / "bronze" / "sales_bronze.csv"
    _write_csv(bronze_path, raw_rows, bronze_fields)

    silver_rows, invalid_rows = _validate_and_standardize(raw_rows)
    silver_fields = [
        "order_id",
        "order_date",
        "year_month",
        "store_id",
        "product_id",
        "category",
        "quantity",
        "unit_price",
        "revenue",
    ]
    silver_path = output_dir / "silver" / "sales_silver.csv"
    _write_csv(silver_path, silver_rows, silver_fields)

    gold_rows = _aggregate_gold(silver_rows)
    gold_fields = ["year_month", "category", "units", "revenue", "avg_unit_price"]
    gold_path = output_dir / "gold" / "sales_gold_monthly_category.csv"
    _write_csv(gold_path, gold_rows, gold_fields)

    metrics = PipelineMetrics(
        raw_rows=len(raw_rows),
        valid_rows=len(silver_rows),
        duplicate_rows_removed=max(len(raw_rows) - len(silver_rows) - invalid_rows, 0),
        invalid_rows=invalid_rows,
        total_revenue=round(sum(float(row["revenue"]) for row in silver_rows), 2),
    )

    metrics_path = output_dir / "pipeline_metrics.json"
    metrics_path.write_text(json.dumps(metrics.__dict__, indent=2), encoding="utf-8")
    return metrics
