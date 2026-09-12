# Shipment Analytics Pipeline

End-to-end data engineering pipeline built with PostgreSQL, PySpark, Parquet, dbt, and Apache Airflow.

## Architecture

PostgreSQL → PySpark → Parquet → dbt → Analytics Mart → Airflow

## Tech Stack

- PostgreSQL
- PySpark
- Apache Airflow
- dbt
- Parquet
- Python
- SQL

## Pipeline

1. Extract shipment data from PostgreSQL.
2. Transform and validate data using PySpark.
3. Generate analytics and store processed data as Parquet.
4. Build dbt staging and intermediate models.
5. Create shipment performance analytics mart.
6. Run dbt data-quality tests.
7. Orchestrate the complete workflow with Airflow.

## Dataset

- 500,000 shipments
- 50,000 customers
- 5 carriers
- 5 warehouses

## Data Quality

PySpark validates:

- Source vs processed record counts
- Missing customers
- Missing warehouses
- Missing carriers

Current validation:

- Source records: 500,000
- Processed records: 500,000
- Missing customers: 0
- Missing warehouses: 0
- Missing carriers: 0

## dbt

Models:

```text
staging
├── stg_shipments
├── stg_customers
├── stg_carriers
└── stg_warehouses

intermediate
└── int_shipments_enriched

marts
└── fct_shipment_performance

dbt tests: 11 passed

Airflow

DAG:

run_spark_etl
      ↓
   run_dbt
      ↓
   test_dbt

Latest end-to-end DAG execution: Success

Analytics

The pipeline produces shipment-performance analytics including:

Total shipments
Delivered shipments
Delayed shipments
Cancelled shipments
Average shipping cost
Total shipping cost
Average delivery delay
Delay rate
Regional performance
Carrier performance
Project Structure
Shipment Analytics Pipeline/
├── airflow/
│   └── dags/
├── data/
│   └── processed/
├── dbt/
│   └── shipment_analytics/
├── src/
│   └── spark_transform.py
├── README.md
└── .gitignore

