-- Example: load fact_sales with correct dimension surrogate keys using current members
INSERT INTO analytics.fact_sales (order_id, customer_sk, product_sk, quantity, unit_price, order_ts)
SELECT
s.order_id,
c.customer_sk,
p.product_sk,
s.quantity,
s.unit_price,
s.order_ts
FROM analytics.stage_sales_tmp s
LEFT JOIN analytics.dim_customer c ON s.customer_id = c.customer_id AND c.is_current = 1
LEFT JOIN analytics.dim_product p ON s.product_id = p.product_id AND p.is_current = 1;
