-- dbt staging model for customers (example for Snowflake/BigQuery)
with raw as (
select * from {{ source('bronze','customers') }}
)


select
customer_id,
lower(email) as email,
coalesce(first_name,'') || ' ' || coalesce(last_name,'') as full_name,
address,
updated_at
from raw
