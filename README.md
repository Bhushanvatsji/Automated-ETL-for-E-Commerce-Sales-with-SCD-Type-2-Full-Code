# Automated ETL for E-Commerce (SCD Type 2)


This project demonstrates an end-to-end automated ETL pipeline using Airflow, dbt, and SQL MERGE for SCD Type 2, intended for a Snowflake / BigQuery / Redshift-like warehouse.


## How to run (local / dev)
1. Create a Python virtualenv and install dependencies:
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
# Architecture


- Sources: Postgres/MySQL/CSV
- Orchestration: Apache Airflow
- Staging & Transform: dbt + Python transforms
- Warehouse: Snowflake (or BigQuery / Redshift)
- Data Quality: Great Expectations
- BI: Power BI / Tableau


Flow: Extract -> Bronze (raw) -> Silver (staging cleansed) -> Merge SCD dims ->
