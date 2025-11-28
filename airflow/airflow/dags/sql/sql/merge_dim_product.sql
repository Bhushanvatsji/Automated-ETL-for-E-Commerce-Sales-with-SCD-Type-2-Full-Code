MERGE INTO analytics.dim_product AS tgt
USING analytics.stage_product_tmp AS src
ON tgt.product_id = src.product_id
AND tgt.is_current = 1


WHEN MATCHED AND (
COALESCE(tgt.name, '') <> COALESCE(src.name, '') OR
COALESCE(tgt.price, 0) <> COALESCE(src.price, 0)
) THEN
UPDATE SET tgt.end_date = current_timestamp(), tgt.is_current = 0


WHEN NOT MATCHED THEN
INSERT (product_sk, product_id, name, category, price, start_date, end_date, is_current)
VALUES (analytics.dim_product_seq.nextval(), src.product_id, src.name, src.category, src.price, current_timestamp(), NULL, 1);
