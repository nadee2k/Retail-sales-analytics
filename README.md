<<<<<<< HEAD
# Retail Sales Analytics

This project now includes a **data-engineering focused batch pipeline** for retail transactions.

## What was added

- Medallion-style layers:
  - **Bronze**: immutable raw ingestion snapshot.
  - **Silver**: validated, deduplicated, standardized records with derived fields.
  - **Gold**: monthly/category aggregates for analytics dashboards.
- Data quality rules:
  - Required schema validation.
  - Date/type/constraint checks.
  - Record-level invalid row filtering.
  - Duplicate suppression on `(order_id, product_id)`.
- Pipeline metrics written to JSON for observability.

## Run locally

```bash
python -m retail_analytics.cli --input data/raw/sample_sales.csv --output-dir data
```

Or install as a package and run:

```bash
pip install -e .
retail-pipeline --input data/raw/sample_sales.csv --output-dir data
```

## Output artifacts

- `data/bronze/sales_bronze.csv`
- `data/silver/sales_silver.csv`
- `data/gold/sales_gold_monthly_category.csv`
- `data/pipeline_metrics.json`

## Testing

```bash
python -m unittest discover -s tests
```
=======
📌 Problem Statement

Retail businesses generate large volumes of transactional data but often lack structured analytical reporting to monitor KPIs such as revenue growth, customer retention, and product performance.

This project performs end-to-end data analysis to:

Identify revenue trends and seasonality

Analyze customer purchasing behavior

Evaluate product and regional performance

Generate KPI dashboards for decision-making

🎯 Objectives

Perform comprehensive Exploratory Data Analysis (EDA)

Build SQL-based analytical queries for KPI computation

Conduct customer segmentation using RFM analysis

Perform statistical testing on regional performance

Develop interactive dashboard for executive reporting
>>>>>>> d2b2a0b (Build power bi project)
