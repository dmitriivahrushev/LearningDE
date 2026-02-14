-- Наполнение Линков l_admins, l_groups_dialogs, l_user_message, l_user_group_activity
INSERT INTO STV2025061611__DWH.l_admins(hk_l_admin_id, hk_group_id, hk_user_id, load_dt, load_src)
SELECT
    hash(hg.hk_group_id, hu.hk_user_id) AS hk_l_admin_id,
    hg.hk_group_id,
    hu.hk_user_id,
    now() AS load_dt,
    's3' AS load_src
FROM STV2025061611__STAGING.groups AS g
LEFT JOIN STV2025061611__DWH.h_users AS hu ON g.admin_id = hu.user_id
LEFT JOIN STV2025061611__DWH.h_groups AS hg ON g.id = hg.group_id
LEFT JOIN STV2025061611__DWH.l_admins AS la ON hash(hg.hk_group_id, hu.hk_user_id) = la.hk_l_admin_id
WHERE la.hk_l_admin_id IS NULL
AND hu.hk_user_id IS NOT NULL 
AND hg.hk_group_id IS NOT NULL;


INSERT INTO STV2025061611__DWH.l_groups_dialogs(hk_l_groups_dialogs, hk_message_id, hk_group_id, load_dt, load_src)
SELECT 
    hash(hd.hk_message_id, 
         CASE 
             WHEN hg.hk_group_id IS NULL THEN -1
             ELSE hg.hk_group_id
         END) AS hk_l_groups_dialogs,
    hd.hk_message_id,
    CASE 
        WHEN hg.hk_group_id IS NULL THEN -1
        ELSE hg.hk_group_id
    END AS hk_group_id,
    now() AS load_dt,
    's3' AS load_src
FROM STV2025061611__STAGING.dialogs AS stgd
LEFT JOIN STV2025061611__DWH.h_dialogs AS hd ON stgd.message_id = hd.message_id
LEFT JOIN STV2025061611__DWH.h_groups AS hg ON stgd.message_group = hg.group_id
LEFT JOIN STV2025061611__DWH.l_groups_dialogs AS lgd ON hash(hd.hk_message_id, 
    CASE 
        WHEN hg.hk_group_id IS NULL THEN -1
        ELSE hg.hk_group_id
    END) = lgd.hk_l_groups_dialogs
WHERE lgd.hk_l_groups_dialogs IS NULL
AND hd.hk_message_id IS NOT NULL;


INSERT INTO STV2025061611__DWH.l_user_message (hk_l_user_message, hk_user_id, hk_message_id, load_dt, load_src)
SELECT 
    hash(hu.hk_user_id, hd.hk_message_id) AS hk_l_user_message,
    hu.hk_user_id,
    hd.hk_message_id,
    now() AS load_dt,
    's3' AS load_src   
FROM STV2025061611__STAGING.dialogs AS stgd
LEFT JOIN STV2025061611__DWH.h_users AS hu ON stgd.message_from = hu.user_id
LEFT JOIN STV2025061611__DWH.h_dialogs AS hd ON stgd.message_id = hd.message_id
LEFT JOIN STV2025061611__DWH.l_user_message AS lum ON hash(hu.hk_user_id, hd.hk_message_id) = lum.hk_l_user_message
WHERE lum.hk_l_user_message IS NULL
AND hu.hk_user_id IS NOT NULL 
AND hd.hk_message_id IS NOT NULL;


INSERT INTO STV2025061611__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
SELECT DISTINCT
    hash(u.hk_user_id, g.hk_group_id) AS hk_l_user_group_activity,
    u.hk_user_id,
    g.hk_group_id,
    now() AS load_dt,
    's3' AS load_src
FROM STV2025061611__STAGING.group_log AS stggl
LEFT JOIN STV2025061611__DWH.h_users AS u ON stggl.user_id = u.user_id 
LEFT JOIN STV2025061611__DWH.h_groups AS g ON stggl.group_id = g.group_id
LEFT JOIN STV2025061611__DWH.l_user_group_activity AS luga ON hash(u.hk_user_id, g.hk_group_id) = luga.hk_l_user_group_activity
WHERE luga.hk_l_user_group_activity IS NULL
AND u.hk_user_id IS NOT NULL 
AND g.hk_group_id IS NOT NULL;
