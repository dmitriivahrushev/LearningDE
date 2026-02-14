/*
Скрипт наполняющий dds.dm_delivery.
*/
BEGIN;
WITH last_load AS (
    SELECT (workflow_settings::JSON ->> 'last_loaded_id')::INT AS last_id
    FROM dds.srv_wf_settings
    WHERE workflow_key = 'load_dm_delivery'
),
stg_data AS (
    SELECT
        object_value::JSON ->> 'courier_id' AS courier_id,
        object_value::JSON ->> 'delivery_id' AS delivery_id,
        object_value::JSON ->> 'order_id' AS order_id,
        to_timestamp(object_value::JSON ->> 'order_ts', 'YYYY-MM-DD HH24:MI:SS') AS order_ts,
        object_value::JSON ->> 'address' AS address,
        to_timestamp(object_value::JSON ->> 'delivery_ts', 'YYYY-MM-DD HH24:MI:SS') AS delivery_ts,
        (object_value::JSON ->> 'rate')::SMALLINT AS rate,
        (object_value::JSON ->> 'tip_sum')::numeric(14, 2) AS tip_sum,
        (object_value::JSON ->> 'sum')::numeric(14, 2) AS sum
    FROM stg.delivery, last_load
    WHERE id > last_load.last_id
)
INSERT INTO dds.dm_delivery (delivery_id, courier_id, order_id, order_ts, address, delivery_ts, rate, tip_sum, sum)
SELECT
   stg.delivery_id,
   d.id,
   stg.order_id,
   stg.order_ts,
   stg.address,
   stg.delivery_ts,
   stg.rate,
   stg.tip_sum,
   stg.sum
FROM stg_data AS stg
JOIN dds.dm_couriers AS d ON stg.courier_id = d.courier_id;

WITH max_load AS (
    SELECT MAX(id) AS max_id
    FROM stg.delivery
)
UPDATE dds.srv_wf_settings
SET workflow_settings = json_build_array('last_loaded_id', max_load.max_id)
FROM max_load
WHERE workflow_key = 'load_dm_delivery';
COMMIT;
