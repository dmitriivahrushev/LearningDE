/*
Скрипт наполняющий dds.dm_couriers.
*/
BEGIN;
WITH last_load AS (
    SELECT (workflow_settings::JSON ->> 'last_loaded_id')::INT AS last_id
    FROM dds.srv_wf_settings
    WHERE workflow_key = 'load_dm_couriers'
),
stg_data AS (
    SELECT 
        object_id AS courier_id,
        object_value::JSON ->> 'name' AS name
    FROM stg.couriers, last_load 
    WHERE id > last_load.last_id
)
INSERT INTO dds.dm_couriers (courier_id, name)
SELECT courier_id, name FROM stg_data
ON CONFLICT (id) DO UPDATE
  SET courier_id = EXCLUDED.courier_id,
      name = EXCLUDED.name;

WITH max_load AS (
    SELECT MAX(id) AS max_id
    FROM stg.couriers
)
UPDATE dds.srv_wf_settings
SET workflow_settings = JSON_BUILD_OBJECT('last_loaded_id', max_load.max_id)
FROM max_load
WHERE workflow_key  = 'load_dm_couriers';
COMMIT;
