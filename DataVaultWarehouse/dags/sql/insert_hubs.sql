-- Наполнение Хабов h_dialogs, h_groups, h_users.
INSERT INTO STV2025061611__DWH.h_users(hk_user_id, user_id, registration_dt, load_dt, load_src)
SELECT 
    hash(u.id) AS hk_user_id,
    u.id AS user_id,
    u.registration_dt AS registration_dt,
    now() AS load_dt,
    's3' AS load_src
FROM STV2025061611__STAGING.users u
LEFT JOIN STV2025061611__DWH.h_users h ON hash(u.id) = h.hk_user_id
WHERE h.hk_user_id IS NULL;

INSERT INTO STV2025061611__DWH.h_groups (hk_group_id, group_id, registration_dt, load_dt, load_src)
SELECT 
    hash(g.id) AS hk_group_id,
    g.id AS group_id,
    g.registration_dt AS registration_dt,
    now() AS load_dt,
    's3' AS load_src
FROM STV2025061611__STAGING.groups g
LEFT JOIN STV2025061611__DWH.h_groups h ON hash(g.id) = h.hk_group_id
WHERE h.hk_group_id IS NULL;

INSERT INTO STV2025061611__DWH.h_dialogs (hk_message_id, message_id, message_ts, load_dt, load_src)
SELECT 
    hash(d.message_id) AS hk_message_id,
    d.message_id AS message_id,
    d.message_ts AS message_ts,
    now() AS load_dt,
    's3' AS load_src
FROM STV2025061611__STAGING.dialogs d
LEFT JOIN STV2025061611__DWH.h_dialogs h ON hash(d.message_id) = h.hk_message_id
WHERE h.hk_message_id IS NULL;
