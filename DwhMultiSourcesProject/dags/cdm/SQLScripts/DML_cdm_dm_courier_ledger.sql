/*
Скрипт наполения витрины cdm.dm_courier_ledger.
*/
BEGIN;
WITH stg_courier AS 
(
SELECT 
    d.order_id AS order_key,
    d.courier_id AS courier_id,
    c."name" AS courier_name,
    CAST(EXTRACT('YEAR' FROM d.order_ts) || '-01-01' AS DATE) AS settlement_year,
    EXTRACT('MONTH' FROM d.order_ts) AS settlement_month,
    COUNT(*) AS orders_count,
    SUM(d.sum) AS orders_total_sum,
    AVG(d.rate) AS rate_avg,
    SUM(d.sum) * 0.25 AS order_processing_fee,
    CASE 
    	    WHEN AVG(d.rate) < 4 THEN GREATEST(SUM(d.sum) *0.05, 100)
    	    WHEN AVG(d.rate) >= 4 AND AVG(d.rate) < 4.5 THEN GREATEST(SUM(d.sum) *0.07, 150)
    	    WHEN AVG(d.rate) >= 4.5 AND AVG(d.rate) < 4.9 THEN GREATEST(SUM(d.sum) *0.08, 175) 
    	    WHEN AVG(d.rate) >= 4.9 THEN GREATEST(SUM(d.sum) *0.10, 200)
    END AS courier_order_sum,
    SUM(d.tip_sum) AS courier_tips_sum    
FROM dds.dm_delivery AS d
JOIN dds.dm_couriers AS c ON d.courier_id = c.id
GROUP BY d.order_id, d.courier_id, courier_name, settlement_year, settlement_month
)
INSERT INTO cdm.dm_courier_ledger 
(
    courier_id, 
    courier_name, 
    settlement_year, 
    settlement_month, 
    orders_count, 
    orders_total_sum,
    rate_avg,
    order_processing_fee,
    courier_order_sum,
    courier_tips_sum,
    courier_reward_sum
)
SELECT 
    courier_id,
    courier_name,
    settlement_year,
    settlement_month,
    orders_count,
    orders_total_sum,
    rate_avg,
    order_processing_fee,
    courier_order_sum,
    courier_tips_sum,
    courier_order_sum + courier_tips_sum * 0.95 AS courier_reward_sum
FROM stg_courier AS sc 
JOIN dds.dm_orders AS o ON sc.order_key = o.order_key;
COMMIT;
