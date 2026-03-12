from __future__ import annotations

import argparse
from pathlib import Path

from .pipeline import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the retail data engineering pipeline.")
    parser.add_argument("--input", type=Path, required=True, help="Path to raw CSV file")
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data"), help="Directory where bronze/silver/gold files are written"
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    metrics = run_pipeline(input_csv=args.input, output_dir=args.output_dir)
    print("Pipeline completed")
    print(metrics)


if __name__ == "__main__":
    main()
