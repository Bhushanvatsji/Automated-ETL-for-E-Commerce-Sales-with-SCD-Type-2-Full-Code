from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
import os


# Configuration - replace with connection IDs configured in Airflow
SRC_CONN_ID = 'postgres_source'
WAREHOUSE_CONN_ID = 'snowflake_dw'


DAG_ID = 'etl_scd_ecommerce'


default_args = {
'owner': 'data-engineer',
'depends_on_past': False,
'retries': 1,
'retry_delay': timedelta(minutes=5),
}


with DAG(
DAG_ID,
default_args=default_args,
start_date=datetime(2023, 1, 1),
schedule_interval='@daily',
catchup=False,
max_active_runs=1,
) as dag:


def extract_table(table_name, path):
hook = PostgresHook(postgres_conn_id=SRC_CONN_ID)
sql = f"SELECT * FROM {table_name};"
df = hook.get_pandas_df(sql)
os.makedirs(os.path.dirname(path), exist_ok=True)
df.to_csv(path, index=False)
return path


def transform_stage_customers(**context):
path = '/tmp/bronze/customers.csv'
df = pd.read_csv(path)
# simple cleaning
df['email'] = df['email'].str.lower().fillna('')
df['full_name'] = df['first_name'].fillna('') + ' ' + df['last_name'].fillna('')
out = '/tmp/silver/stage_customers.csv'
os.makedirs(os.path.dirname(out), exist_ok=True)
df.to_csv(out, index=False)
return out


def load_scd_dim_customer(**context):
# Load staged customers CSV into staging table in warehouse and run MERGE
sf = SnowflakeHook(snowflake_conn_id=WAREHOUSE_CONN_ID)
staging_path = '/tmp/silver/stage_customers.csv'
df = pd.read_csv(staging_path)
# For demo: push to a temp table and run MERGE SQL
# Replace with Bulk load or COPY INTO for production
conn = sf.get_conn()
cur = conn.cursor()
cur.execute('CREATE OR REPLACE TABLE stage_customer_tmp (customer_id VARCHAR, email VARCHAR, full_name VARCHAR, address VARCHAR, updated_at TIMESTAMP_NTZ)');
# Insert rows
extract_customers >> transform_customers >> merge_customer_dim >> load_fact
