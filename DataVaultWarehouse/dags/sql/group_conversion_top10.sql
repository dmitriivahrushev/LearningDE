-- Создание VIEW для финального запроса.
DROP VIEW IF EXISTS STV2025061611__DWH.group_conversion_top10;
CREATE VIEW STV2025061611__DWH.group_conversion_top10 AS 
WITH user_group_log AS 
(
SELECT 
    g.hk_group_id AS hk_group_id,
    COUNT(DISTINCT um.hk_user_id) AS cnt_users_in_group_with_messages
FROM STV2025061611__DWH.l_user_message um
JOIN STV2025061611__DWH.h_dialogs d ON um.hk_message_id = d.hk_message_id
JOIN STV2025061611__DWH.l_groups_dialogs gd ON d.hk_message_id = gd.hk_message_id
JOIN STV2025061611__DWH.h_groups g ON gd.hk_group_id = g.hk_group_id
GROUP BY g.hk_group_id
),
user_group_messages AS 
(
SELECT 
    hg.hk_group_id,
    COUNT(DISTINCT l.hk_user_id) AS cnt_added_users,
    hu.registration_dt
FROM STV2025061611__DWH.h_groups AS hg
JOIN STV2025061611__DWH.l_user_group_activity AS l ON hg.hk_group_id = l.hk_group_id
JOIN STV2025061611__DWH.h_users AS hu ON  l.hk_user_id = hu.hk_user_id
JOIN STV2025061611__DWH.s_auth_history AS sah ON l.hk_l_user_group_activity = sah.hk_l_user_group_activity
WHERE sah.event = 'add' AND hu.registration_dt IS NOT NULL
GROUP BY hg.hk_group_id, hu.registration_dt
ORDER BY hu.registration_dt ASC
LIMIT 10
)
SELECT 
    ugl.hk_group_id AS hk_group_id,
    ugm.cnt_added_users AS cnt_added_users,
    ugl.cnt_users_in_group_with_messages AS cnt_users_in_group_with_messages,
    ROUND((cnt_added_users::NUMERIC / cnt_users_in_group_with_messages::numeric) * 100, 2) AS group_conversion
FROM user_group_log AS ugl
JOIN user_group_messages AS ugm ON ugl.hk_group_id = ugm.hk_group_id
ORDER BY group_conversion;
