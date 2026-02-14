/*
Скрипт наполняющий dds.fct_product_sales.
*/
BEGIN;
WITH last_loaded AS (
    SELECT COALESCE((workflow_settings::json ->> 'last_loaded_id')::int, 0) AS last_id
    FROM dds.srv_wf_settings
    WHERE workflow_key = 'load_fct_product_sales'
),
orders_data AS (
    SELECT 
        o.id AS stg_id,
        (o.object_value::json ->> '_id') AS order_key,
        (o.object_value::json -> 'order_items')::json AS order_items
    FROM stg.ordersystem_orders o, last_loaded
    WHERE o.id > last_loaded.last_id
),
exploded_items AS (
    SELECT 
        od.stg_id,
        od.order_key,
        jsonb_array_elements(od.order_items::jsonb) AS item
    FROM orders_data od
),
parsed_items AS (
    SELECT 
        ei.stg_id,
        ei.order_key,
        (ei.item ->> 'id') AS product_source_id,
        SUM((ei.item ->> 'quantity')::int) AS quantity,
        CAST(MAX((ei.item ->> 'price')::numeric) AS numeric(19, 5)) AS price,
        SUM(((ei.item ->> 'quantity')::int * (ei.item ->> 'price')::numeric(19,5)))::numeric(19,5) AS total_sum
    FROM exploded_items ei
    GROUP BY ei.stg_id, ei.order_key, (ei.item ->> 'id')
),
items_linked AS (
    SELECT 
        pi.order_key,
        dp.id AS product_id,
        dmo.id AS order_id,
        pi.quantity,
        pi.price,
        pi.total_sum
    FROM parsed_items pi
     JOIN dds.dm_products dp ON dp.product_id = pi.product_source_id AND dp.active_to = '2099-12-31'
     JOIN dds.dm_orders dmo ON dmo.order_key = pi.order_key
),
bonus_events AS (
    SELECT *
    FROM stg.bonussystem_events
    WHERE event_type = 'bonus_transaction'
),
bonuses_parsed AS (
    SELECT 
        (be.event_value::json ->> 'order_id') AS order_key,
        jsonb_array_elements((be.event_value::json -> 'product_payments')::jsonb) AS pp
    FROM bonus_events be
),
bonuses_extracted AS (
    SELECT 
        bp.order_key,
        (bp.pp ->> 'product_id') AS product_source_id,
        (bp.pp ->> 'bonus_payment')::numeric AS bonus_payment,
        (bp.pp ->> 'bonus_grant')::numeric AS bonus_grant
    FROM bonuses_parsed bp
),
bonuses_agg AS (
    SELECT
        bp.order_key,
        dp.id AS product_id,
        SUM(bp.bonus_payment) AS bonus_payment,
        SUM(bp.bonus_grant) AS bonus_grant
    FROM bonuses_extracted bp
    JOIN dds.dm_products dp
      ON dp.product_id = bp.product_source_id
     AND dp.active_to = '2099-12-31'
    GROUP BY bp.order_key, dp.id
)
INSERT INTO dds.fct_product_sales (
    product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant
)
SELECT 
    il.product_id,
    il.order_id,
    il.quantity AS count,
    il.price,
    il.total_sum,
    COALESCE(ba.bonus_payment, 0) AS bonus_payment,
    COALESCE(ba.bonus_grant, 0) AS bonus_grant
FROM items_linked il
 JOIN bonuses_agg ba
  ON il.order_key = ba.order_key AND il.product_id = ba.product_id
WHERE il.product_id IS NOT NULL AND il.order_id IS NOT NULL;


WITH max_loaded AS (
    SELECT MAX(id) AS max_id
    FROM stg.ordersystem_orders
)
UPDATE dds.srv_wf_settings
SET workflow_settings = jsonb_build_object('last_loaded_id', max_loaded.max_id)
FROM max_loaded
WHERE workflow_key = 'load_fct_product_sales';
COMMIT;
