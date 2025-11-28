-- materialized as table; this model demonstrates SCD Type 2 is handled outside by MERGE in this project
select * from {{ ref('stage_customers') }}
