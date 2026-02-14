/* Наполнение сателлитов 
    s_admins,
    s_user_chatinfo,
    s_group_private_status,
    s_auth_history,
    s_dialog_info,
    s_group_name,
    s_user_socdem
*/
INSERT INTO STV2025061611__DWH.s_admins(hk_admin_id, is_admin, admin_from, load_dt, load_src)
SELECT
    la.hk_l_admin_id,
    True AS is_admin,
    hg.registration_dt,
    now() as load_dt,
    's3' as load_src
FROM STV2025061611__DWH.l_admins AS la
LEFT JOIN STV2025061611__DWH.h_groups AS hg ON la.hk_group_id = hg.hk_group_id
LEFT JOIN STV2025061611__DWH.s_admins AS sa ON la.hk_l_admin_id = sa.hk_admin_id
WHERE sa.hk_admin_id IS NULL
AND hg.hk_group_id IS NOT NULL;


INSERT INTO STV2025061611__DWH.s_group_name (hk_group_id, group_name, load_dt, load_src)
SELECT 
    hg.hk_group_id,
    stggr.group_name,
    now() as load_dt,
    's3' as load_src
FROM STV2025061611__STAGING.groups AS stggr
LEFT JOIN STV2025061611__DWH.h_groups AS hg ON stggr.id = hg.group_id
LEFT JOIN STV2025061611__DWH.s_group_name AS sgn ON hg.hk_group_id = sgn.hk_group_id
WHERE sgn.hk_group_id IS NULL
AND hg.hk_group_id IS NOT NULL;


INSERT INTO STV2025061611__DWH.s_group_private_status (hk_group_id, is_private, load_dt, load_src)
SELECT 
    hg.hk_group_id,
    stggr.is_private,
    now() AS load_dt,
    's3' AS load_src
FROM STV2025061611__STAGING.groups AS stggr
LEFT JOIN STV2025061611__DWH.h_groups AS hg ON stggr.id = hg.group_id
LEFT JOIN STV2025061611__DWH.s_group_private_status AS sgps ON hg.hk_group_id = sgps.hk_group_id
WHERE sgps.hk_group_id IS NULL
AND hg.hk_group_id IS NOT NULL;


INSERT INTO STV2025061611__DWH.s_dialog_info (hk_message_id, message, message_from, message_to, load_dt, load_src)
SELECT 
    hd.hk_message_id,
    stgd.message,
    stgd.message_from,
    stgd.message_to,
    now() AS load_dt,
    's3' AS load_src
FROM STV2025061611__STAGING.dialogs AS stgd
LEFT JOIN STV2025061611__DWH.h_dialogs AS hd ON stgd.message_id = hd.message_id
LEFT JOIN STV2025061611__DWH.s_dialog_info AS sdi ON hd.hk_message_id = sdi.hk_message_id
WHERE sdi.hk_message_id IS NULL
AND hd.hk_message_id IS NOT NULL;


INSERT INTO STV2025061611__DWH.s_user_socdem (hk_user_id, country, age, load_dt, load_src)
SELECT
    hu.hk_user_id,
    stgu.country,
    stgu.age,
    now() AS load_dt,
    's3' AS load_src
FROM STV2025061611__STAGING.users AS stgu
LEFT JOIN STV2025061611__DWH.h_users AS hu ON stgu.id = hu.user_id
LEFT JOIN STV2025061611__DWH.s_user_socdem AS sus ON hu.hk_user_id = sus.hk_user_id
WHERE sus.hk_user_id IS NULL
AND hu.hk_user_id IS NOT NULL;


INSERT INTO STV2025061611__DWH.s_user_chatinfo (hk_user_id, chat_name, load_dt, load_src)
SELECT 
    hu.hk_user_id,
    stgu.chat_name,
    now() AS load_dt,
    's3' AS load_src
FROM STV2025061611__STAGING.users AS stgu
LEFT JOIN STV2025061611__DWH.h_users AS hu ON stgu.id = hu.user_id
LEFT JOIN STV2025061611__DWH.s_user_chatinfo AS suc ON hu.hk_user_id = suc.hk_user_id
WHERE suc.hk_user_id IS NULL
AND hu.hk_user_id IS NOT NULL;


INSERT INTO STV2025061611__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
SELECT 
    luga.hk_l_user_group_activity,
    gl.user_id_from,
    gl.event,
    gl.datetime,
    now() AS load_dt,
    's3' AS load_src
FROM STV2025061611__STAGING.group_log AS gl
LEFT JOIN STV2025061611__DWH.h_groups AS hg ON gl.group_id = hg.group_id
LEFT JOIN STV2025061611__DWH.h_users AS hu ON gl.user_id = hu.user_id
LEFT JOIN STV2025061611__DWH.l_user_group_activity AS luga ON hg.hk_group_id = luga.hk_group_id AND hu.hk_user_id = luga.hk_user_id
LEFT JOIN STV2025061611__DWH.s_auth_history AS sah ON luga.hk_l_user_group_activity = sah.hk_l_user_group_activity
WHERE sah.hk_l_user_group_activity IS NULL
AND luga.hk_l_user_group_activity IS NOT NULL
AND hg.hk_group_id IS NOT NULL
AND hu.hk_user_id IS NOT NULL;
