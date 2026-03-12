from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
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

SILVER_FIELDS = [
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

GOLD_FIELDS = ["year_month", "category", "units", "revenue", "avg_unit_price"]
REJECTION_FIELDS = ["row_number", "reason", *REQUIRED_COLUMNS]


@dataclass
class PipelineMetrics:
    raw_rows: int
    valid_rows: int
    duplicate_rows_removed: int
    invalid_rows: int
    total_revenue: str
    processed_at_utc: str


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


def _write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _quantize_money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _validate_and_standardize(
    rows: Iterable[dict[str, str]],
) -> tuple[list[dict[str, str]], list[dict[str, str]], int]:
    dedupe_set: set[tuple[str, str, str, str]] = set()
    valid: list[dict[str, str]] = []
    rejected: list[dict[str, str]] = []
    duplicate_rows_removed = 0

    for row_number, row in enumerate(rows, start=2):
        dedupe_key = (
            row["order_id"].strip(),
            row["order_date"].strip(),
            row["store_id"].strip(),
            row["product_id"].strip(),
        )
        if dedupe_key in dedupe_set:
            duplicate_rows_removed += 1
            continue

        try:
            order_date = datetime.strptime(row["order_date"].strip(), "%Y-%m-%d").date().isoformat()
            quantity = int(row["quantity"].strip())
            unit_price = Decimal(row["unit_price"].strip())
            if quantity <= 0:
                raise ValueError("quantity must be > 0")
            if unit_price < 0:
                raise ValueError("unit_price must be >= 0")
            revenue = _quantize_money(unit_price * quantity)
        except (ValueError, InvalidOperation) as exc:
            rejected.append(
                {
                    "row_number": str(row_number),
                    "reason": str(exc),
                    **{k: row.get(k, "") for k in REQUIRED_COLUMNS},
                }
            )
            continue

        dedupe_set.add(dedupe_key)
        valid.append(
            {
                "order_id": dedupe_key[0],
                "order_date": order_date,
                "year_month": order_date[:7],
                "store_id": dedupe_key[2],
                "product_id": dedupe_key[3],
                "category": row["category"].strip().lower(),
                "quantity": str(quantity),
                "unit_price": f"{_quantize_money(unit_price):.2f}",
                "revenue": f"{revenue:.2f}",
            }
        )

    return valid, rejected, duplicate_rows_removed


def _aggregate_gold(silver_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[tuple[str, str], dict[str, Decimal | int]] = {}

    for row in silver_rows:
        key = (row["year_month"], row["category"])
        bucket = grouped.setdefault(key, {"units": 0, "revenue": Decimal("0")})
        bucket["units"] = int(bucket["units"]) + int(row["quantity"])
        bucket["revenue"] = Decimal(str(bucket["revenue"])) + Decimal(row["revenue"])

    output: list[dict[str, str]] = []
    for (year_month, category), values in sorted(grouped.items()):
        units = int(values["units"])
        revenue = _quantize_money(Decimal(str(values["revenue"])))
        avg = _quantize_money(revenue / units) if units else Decimal("0.00")
        output.append(
            {
                "year_month": year_month,
                "category": category,
                "units": str(units),
                "revenue": f"{revenue:.2f}",
                "avg_unit_price": f"{avg:.2f}",
            }
        )
    return output


def run_pipeline(input_csv: Path, output_dir: Path, strict: bool = False, write_rejections: bool = True) -> PipelineMetrics:
    """Run a Bronze/Silver/Gold retail batch pipeline over a CSV source."""
    raw_rows = _read_raw_csv(input_csv)

    bronze_path = output_dir / "bronze" / "sales_bronze.csv"
    bronze_fields = list(raw_rows[0].keys()) if raw_rows else REQUIRED_COLUMNS
    _write_csv(bronze_path, raw_rows, bronze_fields)

    silver_rows, rejected_rows, duplicate_rows_removed = _validate_and_standardize(raw_rows)
    silver_path = output_dir / "silver" / "sales_silver.csv"
    _write_csv(silver_path, silver_rows, SILVER_FIELDS)

    if write_rejections:
        rejection_path = output_dir / "silver" / "sales_rejections.csv"
        _write_csv(rejection_path, rejected_rows, REJECTION_FIELDS)

    gold_rows = _aggregate_gold(silver_rows)
    gold_path = output_dir / "gold" / "sales_gold_monthly_category.csv"
    _write_csv(gold_path, gold_rows, GOLD_FIELDS)

    total_revenue = _quantize_money(sum((Decimal(row["revenue"]) for row in silver_rows), start=Decimal("0")))
    metrics = PipelineMetrics(
        raw_rows=len(raw_rows),
        valid_rows=len(silver_rows),
        duplicate_rows_removed=duplicate_rows_removed,
        invalid_rows=len(rejected_rows),
        total_revenue=f"{total_revenue:.2f}",
        processed_at_utc=datetime.now(tz=timezone.utc).isoformat(),
    )

    if strict and metrics.invalid_rows > 0:
        raise DataQualityError(f"Strict mode enabled and found {metrics.invalid_rows} invalid row(s)")

    metrics_path = output_dir / "pipeline_metrics.json"
    metrics_path.write_text(json.dumps(asdict(metrics), indent=2), encoding="utf-8")
    return metrics
