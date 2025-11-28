-- Merge SCD Type 2 for dim_customer (Snowflake-like SQL)
MERGE INTO analytics.dim_customer AS tgt
USING analytics.stage_customer_tmp AS src
ON tgt.customer_id = src.customer_id
AND tgt.is_current = 1


-- If matched and data changed, expire old row
WHEN MATCHED AND (
COALESCE(tgt.email, '') <> COALESCE(src.email, '') OR
COALESCE(tgt.full_name, '') <> COALESCE(src.full_name, '') OR
COALESCE(tgt.address, '') <> COALESCE(src.address, '')
) THEN
UPDATE SET tgt.end_date = current_timestamp(), tgt.is_current = 0


-- Insert new row for new customers or changed customers
WHEN NOT MATCHED THEN
INSERT (customer_sk, customer_id, email, full_name, address, start_date, end_date, is_current)
VALUES (analytics.dim_customer_seq.nextval(), src.customer_id, src.email, src.full_name, src.address, current_timestamp(), NULL, 1);
