from __future__ import annotations

import argparse
from pathlib import Path

from .pipeline import DataQualityError, run_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the retail data engineering pipeline.")
    parser.add_argument("--input", type=Path, required=True, help="Path to raw CSV file")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data"),
        help="Directory where bronze/silver/gold files are written",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail the run when any invalid records are found",
    )
    parser.add_argument(
        "--no-rejections",
        action="store_true",
        help="Do not write silver/sales_rejections.csv",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    try:
        metrics = run_pipeline(
            input_csv=args.input,
            output_dir=args.output_dir,
            strict=args.strict,
            write_rejections=not args.no_rejections,
        )
    except DataQualityError as exc:
        raise SystemExit(f"Pipeline failed: {exc}") from exc

    print("Pipeline completed")
    print(metrics)


if __name__ == "__main__":
    main()
