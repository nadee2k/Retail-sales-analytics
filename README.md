# Retail Sales Analytics

This project contains a **data-engineering focused batch pipeline** for retail transactions.

## Data engineering capabilities

- **Medallion architecture**
  - **Bronze**: raw ingestion snapshot.
  - **Silver**: validated, standardized, deduplicated records with derived columns.
  - **Gold**: monthly/category aggregate table for BI/reporting use cases.
- **Data quality controls**
  - Required schema validation.
  - Type and constraint checks (`date`, `quantity`, `unit_price`).
  - Duplicate suppression using business key `(order_id, order_date, store_id, product_id)`.
  - Rejected records sink with row numbers and error reason.
- **Operational observability**
  - Run metrics JSON output (`raw_rows`, `valid_rows`, `invalid_rows`, `total_revenue`, timestamp).
  - Strict mode to fail job execution when bad records are present.

## Run locally

```bash
PYTHONPATH=src python -m retail_analytics.cli --input data/raw/sample_sales.csv --output-dir data
```

### Optional runtime flags

```bash
PYTHONPATH=src python -m retail_analytics.cli --input data/raw/sample_sales.csv --output-dir data --strict
PYTHONPATH=src python -m retail_analytics.cli --input data/raw/sample_sales.csv --output-dir data --no-rejections
```

## Output artifacts

- `data/bronze/sales_bronze.csv`
- `data/silver/sales_silver.csv`
- `data/silver/sales_rejections.csv`
- `data/gold/sales_gold_monthly_category.csv`
- `data/pipeline_metrics.json`

## Testing

```bash
PYTHONPATH=src python -m unittest discover -s tests
```
