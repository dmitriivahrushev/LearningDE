/*
Скрипт наполняющий dds.dm_users.
*/
BEGIN;
WITH last_loaded AS (
    SELECT (workflow_settings::json ->> 'last_loaded_id')::int AS last_id
    FROM dds.srv_wf_settings
    WHERE workflow_key = 'load_dm_users'
),
new_users AS (
    SELECT
        object_value::json ->> '_id' AS user_id,
        object_value::json ->> 'name' AS user_name,
        object_value::json ->> 'login' AS user_login
    FROM stg.ordersystem_users,
         last_loaded
    WHERE id > last_loaded.last_id
)
INSERT INTO dds.dm_users (user_id, user_name, user_login)
SELECT * FROM new_users
ON CONFLICT (id) DO UPDATE
    SET user_name = EXCLUDED.user_name,
        user_login = EXCLUDED.user_login;


WITH max_loaded AS (
    SELECT COALESCE(MAX(id), 0) AS max_id
    FROM stg.ordersystem_users 
)
UPDATE dds.srv_wf_settings
SET workflow_settings = jsonb_build_object('last_loaded_id', max_loaded.max_id)
FROM max_loaded
WHERE workflow_key = 'load_dm_users';
COMMIT;
