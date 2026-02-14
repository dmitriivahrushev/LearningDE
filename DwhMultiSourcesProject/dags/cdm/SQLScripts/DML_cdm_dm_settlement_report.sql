/*
Скрипт наполнения витрины cdm.dm_settlement_report.
*/
BEGIN;
INSERT INTO cdm.dm_settlement_report (
    restaurant_id,
    restaurant_name,
    settlement_date,
    orders_count,
    orders_total_sum,
    orders_bonus_payment_sum,
    orders_bonus_granted_sum,
    order_processing_fee,
    restaurant_reward_sum
)
SELECT
    p.restaurant_id,
    r.restaurant_name,
    DATE_TRUNC('day', t.ts)::date AS settlement_date,
    COUNT(DISTINCT d.id) AS orders_count,
    SUM(fct.total_sum) AS orders_total_sum,
    SUM(fct.bonus_payment) AS orders_bonus_payment_sum,
    SUM(fct.bonus_grant) AS orders_bonus_granted_sum,
    ROUND(SUM(fct.total_sum) * 0.25, 2) AS order_processing_fee,
    ROUND(SUM(fct.total_sum) - SUM(fct.bonus_payment) - SUM(fct.total_sum) * 0.25, 2) AS restaurant_reward_sum
FROM dds.fct_product_sales AS fct
JOIN dds.dm_products AS p ON fct.product_id = p.id
JOIN dds.dm_restaurants AS r ON p.restaurant_id = r.id
JOIN dds.dm_orders AS d ON fct.order_id = d.id
JOIN dds.dm_timestamps AS t ON d.timestamp_id = t.id
WHERE d.order_status = 'CLOSED'
GROUP BY
    p.restaurant_id,
    r.restaurant_name,
    DATE_TRUNC('day', t.ts)
ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
SET
    restaurant_name = EXCLUDED.restaurant_name,
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
    order_processing_fee = EXCLUDED.order_processing_fee,
    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
COMMIT;