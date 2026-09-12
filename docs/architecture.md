# Shipment Analytics Pipeline Architecture

PostgreSQL → PySpark ETL → Data Quality Checks → Parquet → dbt Staging → dbt Intermediate → Analytics Mart → Regional / Carrier Analytics

Apache Airflow orchestrates the pipeline.

GitHub Actions validates Python syntax and the dbt project.
