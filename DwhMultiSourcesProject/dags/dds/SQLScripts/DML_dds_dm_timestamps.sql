/*
Скрипт наполняющий dds.dm_timestamps.
*/
BEGIN;
WITH last_loaded AS (
    SELECT (workflow_settings::json ->> 'last_loaded_id')::int AS last_id
    FROM dds.srv_wf_settings
    WHERE workflow_key = 'load_dm_timestamps'
),
new_data AS (
    SELECT
        (object_value::json ->> 'date')::timestamp AS ts,
        EXTRACT(YEAR FROM (object_value::json ->> 'date')::timestamp) AS year,
        EXTRACT(MONTH FROM (object_value::json ->> 'date')::timestamp) AS month,
        EXTRACT(DAY FROM (object_value::json ->> 'date')::timestamp) AS day,
        ((object_value::json ->> 'date')::timestamp)::time AS time,
        ((object_value::json ->> 'date')::timestamp)::date AS date    
    FROM stg.ordersystem_orders, last_loaded
    WHERE id > last_loaded.last_id
)
INSERT INTO dds.dm_timestamps (ts, year, month, day, time, date)
SELECT * FROM new_data;


WITH max_loaded AS(
    SELECT MAX(id) AS max_id
    FROM stg.ordersystem_orders
)
UPDATE dds.srv_wf_settings
SET workflow_settings = jsonb_build_object('last_loaded_id', max_loaded.max_id)
FROM max_loaded
WHERE workflow_key = 'load_dm_timestamps';
COMMIT;
