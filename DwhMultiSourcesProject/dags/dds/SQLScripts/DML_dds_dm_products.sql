/*
Скрипт наполняющий dds.dm_products.
*/
BEGIN;
WITH last_loaded AS (
    SELECT (workflow_settings::json ->> 'last_loaded_id')::int AS last_id
    FROM dds.srv_wf_settings
    WHERE workflow_key = 'load_dm_products'
),
new_data AS (
    SELECT
        (menu_item ->> '_id') AS product_id,
        (menu_item ->> 'name') AS product_name,
        (menu_item ->> 'price')::numeric AS product_price,
        (r.object_value::json ->> 'update_ts')::timestamp AS active_from,
        '2099-12-31 00:00:00'::timestamp AS active_to,
        dr.id AS restaurant_id
    FROM last_loaded
    JOIN stg.ordersystem_restaurants r ON r.id > last_loaded.last_id
    JOIN dds.dm_restaurants dr ON dr.restaurant_id = (r.object_value::json ->> '_id') AND dr.active_to = '2099-12-31 00:00:00'
    CROSS JOIN jsonb_array_elements((r.object_value::json -> 'menu')::jsonb) AS menu_item
)
INSERT INTO dds.dm_products (
    product_id,
    product_name,
    product_price,
    active_from,
    active_to,
    restaurant_id
)
SELECT
    product_id,
    product_name,
    product_price,
    active_from,
    active_to,
    restaurant_id
FROM new_data;


WITH max_loaded AS (
    SELECT MAX(id) AS max_id
    FROM stg.ordersystem_restaurants
)
UPDATE dds.srv_wf_settings
SET workflow_settings = jsonb_build_object('last_loaded_id', max_loaded.max_id)
FROM max_loaded
WHERE workflow_key = 'load_dm_products';
COMMIT;
