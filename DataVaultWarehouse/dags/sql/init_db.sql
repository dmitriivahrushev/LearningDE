-- Создание таблиц в схеме STV2025061611__STAGING
CREATE TABLE IF NOT EXISTS STV2025061611__STAGING.users
(
    id                 INT NOT NULL,
    chat_name          VARCHAR(200),
    registration_dt    TIMESTAMP,
    country            VARCHAR(200),
    age                INT
);

CREATE PROJECTION IF NOT EXISTS STV2025061611__STAGING.users 
(
 id,
 chat_name,
 registration_dt,
 country,
 age
)
AS
SELECT 
    users.id,
    users.chat_name,
    users.registration_dt,
    users.country,
    users.age
FROM STV2025061611__STAGING.users
ORDER BY users.id
SEGMENTED BY hash(users.id) ALL NODES KSAFE 1;


CREATE TABLE IF NOT EXISTS STV2025061611__STAGING.groups
(
    id                 INT NOT NULL,
    admin_id           INT,
    group_name         VARCHAR(100),
    registration_dt    TIMESTAMP,
    is_private         BOOLEAN
)
PARTITION BY ((groups.registration_dt)::DATE) 
GROUP BY (CASE WHEN ("datediff"('year', (groups.registration_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (groups.registration_dt)::date))::date WHEN ("datediff"('month', (groups.registration_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (groups.registration_dt)::date))::date ELSE (groups.registration_dt)::date END);

CREATE PROJECTION IF NOT EXISTS STV2025061611__STAGING.groups  
(
 id,
 admin_id,
 group_name,
 registration_dt,
 is_private
)
AS
SELECT 
    groups.id,
    groups.admin_id,
    groups.group_name,
    groups.registration_dt,
    groups.is_private
FROM STV2025061611__STAGING.groups
ORDER BY groups.id, groups.admin_id
SEGMENTED BY hash(groups.id) ALL NODES KSAFE 1;


CREATE TABLE IF NOT EXISTS STV2025061611__STAGING.dialogs
(
    message_id       INT NOT NULL,
    message_ts       TIMESTAMP,
    message_from     INT,
    message_to       INT,
    message          VARCHAR(1000),
    message_group    INT
)
PARTITION BY ((dialogs.message_ts)::date) 
GROUP BY (CASE WHEN ("datediff"('year', (dialogs.message_ts)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (dialogs.message_ts)::date))::date WHEN ("datediff"('month', (dialogs.message_ts)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (dialogs.message_ts)::date))::date ELSE (dialogs.message_ts)::date END);

CREATE PROJECTION IF NOT EXISTS STV2025061611__STAGING.dialogs 
(
 message_id,
 message_ts,
 message_from,
 message_to,
 message,
 message_group
)
AS
SELECT 
    dialogs.message_id,
    dialogs.message_ts,
    dialogs.message_from,
    dialogs.message_to,
    dialogs.message,
    dialogs.message_group
FROM STV2025061611__STAGING.dialogs
ORDER BY dialogs.message_id
SEGMENTED BY hash(dialogs.message_id) ALL NODES KSAFE 1;


CREATE TABLE IF NOT EXISTS STV2025061611__STAGING.group_log
(
    group_id        INT NOT NULL,
    user_id         INT,
    user_id_from    INT,
    event           VARCHAR(10),
    "datetime"      TIMESTAMP
)
PARTITION BY ((group_log."datetime")::date) 
GROUP BY (CASE WHEN ("datediff"('year', (group_log."datetime")::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (group_log."datetime")::date))::date WHEN ("datediff"('month', (group_log."datetime")::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (group_log."datetime")::date))::date ELSE (group_log."datetime")::date END);

CREATE PROJECTION IF NOT EXISTS STV2025061611__STAGING.group_log 
(
 group_id,
 user_id,
 user_id_from,
 event,
 "datetime"
)
AS
SELECT 
    group_log.group_id,
    group_log.user_id,
    group_log.user_id_from,
    group_log.event,
    group_log."datetime"
FROM STV2025061611__STAGING.group_log
ORDER BY group_log.group_id, group_log."datetime"
SEGMENTED BY hash(group_log.group_id) ALL NODES KSAFE 1;


-- Создание таблиц в схеме STV2025061611__DWH
-- Хабы h_dialogs, h_groups, h_users
CREATE TABLE IF NOT EXISTS STV2025061611__DWH.h_dialogs
(
    hk_message_id    INT PRIMARY KEY,
    message_id       INT,
    message_ts       TIMESTAMP,
    load_dt          TIMESTAMP,
    load_src         VARCHAR(20)
)
PARTITION BY ((h_dialogs.load_dt)::date) 
GROUP BY (CASE WHEN ("datediff"('year', (h_dialogs.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (h_dialogs.load_dt)::date))::date WHEN ("datediff"('month', (h_dialogs.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (h_dialogs.load_dt)::date))::date ELSE (h_dialogs.load_dt)::date END);

CREATE PROJECTION IF NOT EXISTS STV2025061611__DWH.h_dialogs 
(
 hk_message_id,
 message_id,
 message_ts,
 load_dt,
 load_src
)
AS
SELECT 
    h_dialogs.hk_message_id,
    h_dialogs.message_id,
    h_dialogs.message_ts,
    h_dialogs.load_dt,
    h_dialogs.load_src
FROM STV2025061611__DWH.h_dialogs
ORDER BY h_dialogs.load_dt
SEGMENTED BY h_dialogs.hk_message_id ALL NODES KSAFE 1;


CREATE TABLE IF NOT EXISTS STV2025061611__DWH.h_groups
(
    hk_group_id        INT PRIMARY KEY,
    group_id           INT,
    registration_dt    TIMESTAMP,
    load_dt            TIMESTAMP,
    load_src           VARCHAR(20)
)
PARTITION BY ((h_groups.load_dt)::date) 
GROUP BY (CASE WHEN ("datediff"('year', (h_groups.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (h_groups.load_dt)::date))::date WHEN ("datediff"('month', (h_groups.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (h_groups.load_dt)::date))::date ELSE (h_groups.load_dt)::date END);

CREATE PROJECTION IF NOT EXISTS STV2025061611__DWH.h_groups 
(
 hk_group_id,
 group_id,
 registration_dt,
 load_dt,
 load_src
)
AS
SELECT 
    h_groups.hk_group_id,
    h_groups.group_id,
    h_groups.registration_dt,
    h_groups.load_dt,
    h_groups.load_src
FROM STV2025061611__DWH.h_groups
ORDER BY h_groups.load_dt
SEGMENTED BY h_groups.hk_group_id ALL NODES KSAFE 1;


CREATE TABLE IF NOT EXISTS STV2025061611__DWH.h_users
(
    hk_user_id         INT PRIMARY KEY ,
    user_id            INT,
    registration_dt    TIMESTAMP,
    load_dt            TIMESTAMP,
    load_src           VARCHAR(20)
)
PARTITION BY ((h_users.load_dt)::date) 
GROUP BY (CASE WHEN ("datediff"('year', (h_users.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (h_users.load_dt)::date))::date WHEN ("datediff"('month', (h_users.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (h_users.load_dt)::date))::date ELSE (h_users.load_dt)::date END);

CREATE PROJECTION IF NOT EXISTS STV2025061611__DWH.h_users 
(
 hk_user_id,
 user_id,
 registration_dt,
 load_dt,
 load_src
)
AS
SELECT 
    h_users.hk_user_id,
    h_users.user_id,
    h_users.registration_dt,
    h_users.load_dt,
    h_users.load_src
FROM STV2025061611__DWH.h_users
ORDER BY h_users.load_dt
SEGMENTED BY h_users.hk_user_id ALL NODES KSAFE 1;


-- Линки l_admins, l_groups_dialogs, l_user_group_activity, l_user_message
CREATE TABLE IF NOT EXISTS STV2025061611__DWH.l_user_message
(
hk_l_user_message    BIGINT PRIMARY KEY,
hk_user_id           BIGINT NOT NULL 
    CONSTRAINT fk_l_user_message_user 
    REFERENCES STV2025061611__DWH.h_users (hk_user_id),
hk_message_id        BIGINT NOT NULL 
    CONSTRAINT fk_l_user_message_message 
    REFERENCES STV2025061611__DWH.h_dialogs (hk_message_id),
load_dt              DATETIME,
load_src             VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_user_id ALL nodes
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2);


CREATE TABLE IF NOT EXISTS STV2025061611__DWH.l_admins
(
    hk_l_admin_id    BIGINT PRIMARY KEY,
    hk_user_id       BIGINT NOT NULL 
        CONSTRAINT fk_l_admins_h_users 
        REFERENCES STV2025061611__DWH.h_users (hk_user_id),
    hk_group_id      BIGINT NOT NULL
        CONSTRAINT fk_l_admins_h_groups
        REFERENCES STV2025061611__DWH.h_groups (hk_group_id),
    load_dt              DATETIME,
    load_src             VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_l_admin_id ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2);


CREATE TABLE IF NOT EXISTS STV2025061611__DWH.l_groups_dialogs
(
    hk_l_groups_dialogs    BIGINT PRIMARY KEY,
    hk_message_id          BIGINT NOT NULL
        CONSTRAINT fk_l_groups_dialogs_h_dialogs
        REFERENCES STV2025061611__DWH.h_dialogs (hk_message_id),
    hk_group_id            BIGINT NOT NULL
        CONSTRAINT fk_l_groups_dialogs_h_groups
        REFERENCES STV2025061611__DWH.h_groups (hk_group_id),
    load_dt                DATETIME,
    load_src               VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_l_groups_dialogs ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2);


CREATE TABLE IF NOT EXISTS STV2025061611__DWH.l_user_group_activity
(
    hk_l_user_group_activity    INT,
    hk_user_id                  INT,
    hk_group_id                 INT,
    load_dt                     TIMESTAMP,
    load_src                    VARCHAR(20),
    CONSTRAINT pk_l_user_group_activity PRIMARY KEY (hk_l_user_group_activity),
    CONSTRAINT fk_user_group_activity_h_users FOREIGN KEY (hk_user_id) REFERENCES STV2025061611__DWH.h_users (hk_user_id),
    CONSTRAINT fk_user_group_activity_h_groups FOREIGN KEY (hk_group_id) REFERENCES STV2025061611__DWH.h_groups(hk_group_id)
)
ORDER BY load_dt
SEGMENTED BY hk_user_id ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2);


/* Сателлиты
    s_admins,
    s_user_chatinfo,
    s_group_private_status,
    s_auth_history,
    s_dialog_info,
    s_group_name,
    s_user_socdem,
*/
CREATE TABLE IF NOT EXISTS STV2025061611__DWH.s_admins
(
hk_admin_id    BIGINT NOT NULL,
is_admin       BOOLEAN,
admin_from     DATETIME,
load_dt        DATETIME,
load_src       VARCHAR(20),
CONSTRAINT fk_s_admins_l_admins FOREIGN KEY (hk_admin_id) REFERENCES STV2025061611__DWH.l_admins (hk_l_admin_id)
)
ORDER BY load_dt
SEGMENTED BY hk_admin_id ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2);


CREATE TABLE IF NOT EXISTS STV2025061611__DWH.s_group_name
(
    hk_group_id     BIGINT NOT NULL,
    group_name      VARCHAR(100),
    load_dt         DATETIME,
    load_src        VARCHAR(20),
    CONSTRAINT fk_s_group_name_l_admins FOREIGN KEY (hk_group_id) REFERENCES STV2025061611__DWH.h_groups (hk_group_id)
)
ORDER BY load_dt
SEGMENTED BY hk_group_id ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2);


CREATE TABLE IF NOT EXISTS STV2025061611__DWH.s_group_private_status
(
    hk_group_id     BIGINT NOT NULL,
    is_private      BOOLEAN,
    load_dt         TIMESTAMP,
    load_src        VARCHAR
)
ORDER BY load_dt
SEGMENTED BY hk_group_id ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2);


CREATE TABLE IF NOT EXISTS STV2025061611__DWH.s_dialog_info
(
    hk_message_id    BIGINT NOT NULL,
    message          VARCHAR(1000),
    message_from     INT,
    message_to       INT,
    load_dt          TIMESTAMP,
    load_src         VARCHAR(20),
    CONSTRAINT pk_s_dialog_info_h_dialogs FOREIGN KEY (hk_message_id) REFERENCES STV2025061611__DWH.h_dialogs (hk_message_id)
)
ORDER BY load_dt
SEGMENTED BY hk_message_id ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2);


CREATE TABLE IF NOT EXISTS STV2025061611__DWH.s_user_socdem
(
    hk_user_id    BIGINT NOT NULL,
    country       VARCHAR(100),
    age           INT,
    load_dt       TIMESTAMP,
    load_src      VARCHAR(20),
    CONSTRAINT fk_s_user_socdem_h_users FOREIGN KEY (hk_user_id) REFERENCES STV2025061611__DWH.h_users
)
ORDER BY load_dt
SEGMENTED BY hk_user_id ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2);


CREATE TABLE IF NOT EXISTS STV2025061611__DWH.s_user_chatinfo
(
    hk_user_id    BIGINT NOT NULL,
    chat_name     VARCHAR(1000),
    load_dt       TIMESTAMP,
    load_src      VARCHAR(20),
    CONSTRAINT fk_s_user_chatinfo_h_users FOREIGN KEY (hk_user_id) REFERENCES STV2025061611__DWH.h_users (hk_user_id)
)
ORDER BY load_dt
SEGMENTED BY hk_user_id ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2);


CREATE TABLE IF NOT EXISTS STV2025061611__DWH.s_auth_history
(
    hk_l_user_group_activity INT NOT NULL,
    user_id_from INT,
    event VARCHAR(10),
    event_dt TIMESTAMP,
    load_dt TIMESTAMP,
    load_src VARCHAR(20),
    CONSTRAINT fk_s_auth_history_i_user_group_activity FOREIGN KEY (hk_l_user_group_activity) REFERENCES STV2025061611__DWH.l_user_group_activity (hk_l_user_group_activity)
)
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2);
