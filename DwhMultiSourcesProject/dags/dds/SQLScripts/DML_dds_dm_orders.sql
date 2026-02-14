/*
Скрипт наполняющий dds.dm_orders.
*/
BEGIN;
WITH last_loaded AS (
    SELECT COALESCE((workflow_settings::json ->> 'last_loaded_id')::int, 0) AS last_id
    FROM dds.srv_wf_settings
    WHERE workflow_key = 'load_dm_orders'
),
orders_data AS (
    SELECT 
        o.id AS stg_id,
        (o.object_value::json ->> '_id') AS order_key,
        (o.object_value::json ->> 'final_status') AS order_status,
        (o.object_value::json -> 'restaurant' ->> 'id') AS rest_source_id,
        (o.object_value::json ->> 'date')::timestamp AS order_ts,
        (o.object_value::json -> 'user' ->> 'id') AS user_source_id
    FROM stg.ordersystem_orders o, last_loaded
    WHERE o.id > last_loaded.last_id
),
joined_data AS (
    SELECT 
        od.order_key,
        od.order_status,
        dr.id AS restaurant_id,
        dt.id AS timestamp_id,
        du.id AS user_id,
        od.stg_id
    FROM orders_data od
    LEFT JOIN dds.dm_restaurants dr ON dr.restaurant_id = od.rest_source_id AND dr.active_to = '2099-12-31'
    LEFT JOIN dds.dm_timestamps dt ON dt.ts = od.order_ts
    LEFT JOIN dds.dm_users du ON du.user_id = od.user_source_id
)
INSERT INTO dds.dm_orders (order_key, order_status, restaurant_id, timestamp_id, user_id)
SELECT 
    order_key,
    order_status,
    restaurant_id,
    timestamp_id,
    user_id
FROM joined_data
WHERE restaurant_id IS NOT NULL AND timestamp_id IS NOT NULL AND user_id IS NOT NULL;


WITH max_loaded AS (
    SELECT MAX(id) AS max_id
    FROM stg.ordersystem_orders
)
UPDATE dds.srv_wf_settings
SET workflow_settings = jsonb_build_object('last_loaded_id', max_loaded.max_id)
FROM max_loaded
WHERE workflow_key = 'load_dm_orders';
COMMIT;
