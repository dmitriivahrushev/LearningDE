/*
Скрипт наполняющий dds.dm_restaurants.
*/
BEGIN;
WITH last_loaded AS (
    SELECT (workflow_settings::json ->> 'last_loaded_id')::int AS last_id
    FROM dds.srv_wf_settings
    WHERE workflow_key = 'load_dm_restaurants'
),
new_data AS (
    SELECT
        id,
        object_value::json ->> '_id' AS restaurant_id,
        object_value::json ->> 'name' AS restaurant_name,
        update_ts::timestamp AS active_from
    FROM stg.ordersystem_restaurants, last_loaded
    WHERE id > last_loaded.last_id
),
closed_old AS (
    UPDATE dds.dm_restaurants dr
    SET active_to = nd.active_from
    FROM new_data nd
    WHERE dr.restaurant_id = nd.restaurant_id
      AND dr.active_to = '2099-12-31 00:00:00'
      AND (dr.restaurant_name IS DISTINCT FROM nd.restaurant_name)
    RETURNING dr.restaurant_id
)
INSERT INTO dds.dm_restaurants (
    restaurant_id,
    restaurant_name,
    active_from,
    active_to
)
SELECT
    nd.restaurant_id,
    nd.restaurant_name,
    nd.active_from,
    '2099-12-31 00:00:00'
FROM new_data nd
LEFT JOIN dds.dm_restaurants dr
  ON dr.restaurant_id = nd.restaurant_id
 AND dr.active_to = '2099-12-31 00:00:00'
WHERE dr.id IS NULL OR dr.restaurant_name IS DISTINCT FROM nd.restaurant_name;


WITH max_loaded AS (
    SELECT COALESCE(MAX(id), 0) AS max_id
    FROM dds.dm_restaurants
)
UPDATE dds.srv_wf_settings
SET workflow_settings = jsonb_build_object('last_loaded_id', max_loaded.max_id)
FROM max_loaded
WHERE workflow_key = 'load_dm_restaurants';
COMMIT;
