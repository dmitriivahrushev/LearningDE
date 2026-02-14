/*
Создание схемы и таблиц в stg.
*/
BEGIN;
CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.srv_wf_settings 
(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    workflow_key varchar NOT NULL UNIQUE,
    workflow_settings JSON NOT NULL
);

CREATE TABLE IF NOT EXISTS stg.ordersystem_users
(
    id SERIAL PRIMARY KEY NOT NULL,
    object_id VARCHAR NOT NULL,
    object_value TEXT NOT NULL,
    update_ts TIMESTAMP NOT NULL
);
ALTER TABLE stg.ordersystem_users 
ADD CONSTRAINT ordersystem_users_object_id_uindex UNIQUE (object_id);

CREATE TABLE IF NOT EXISTS stg.ordersystem_restaurants
(
    id SERIAL PRIMARY KEY NOT NULL,
    object_id VARCHAR NOT NULL,
    object_value TEXT NOT NULL,
    update_ts TIMESTAMP NOT NULL
);
ALTER TABLE stg.ordersystem_restaurants 
ADD CONSTRAINT ordersystem_restaurants_object_id_uindex UNIQUE (object_id);

CREATE TABLE IF NOT EXISTS stg.ordersystem_orders
(
    id SERIAL PRIMARY KEY NOT NULL,
    object_id VARCHAR NOT NULL,
    object_value TEXT NOT NULL,
    update_ts TIMESTAMP NOT NULL
);
ALTER TABLE stg.ordersystem_orders 
ADD CONSTRAINT ordersystem_orders_object_id_uindex UNIQUE (object_id);

CREATE TABLE IF NOT EXISTS stg.delivery
(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    object_id varchar NOT NULL,
	object_value text NOT NULL,
	CONSTRAINT delivery_object_id_uindex UNIQUE (object_id)
);

CREATE TABLE IF NOT EXISTS stg.couriers
(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    object_id varchar NOT NULL,
	object_value text NOT NULL,
	CONSTRAINT couriers_object_id_uindex UNIQUE (object_id)
);

CREATE TABLE IF NOT EXISTS stg.bonussystem_users 
(
	id int4 NOT NULL,
	order_user_id text NOT NULL,
	CONSTRAINT bonussystem_users_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS stg.bonussystem_ranks 
(
	id int4 NOT NULL,
	"name" varchar(2048) NOT NULL,
	bonus_percent numeric(19, 5) DEFAULT 0 NOT NULL,
	min_payment_threshold numeric(19, 5) DEFAULT 0 NOT NULL,
	CONSTRAINT bonussystem_ranks_pkey PRIMARY KEY (id),
	CONSTRAINT ranks_bonus_percent_check CHECK ((bonus_percent >= (0)::numeric)),
	CONSTRAINT ranks_bonus_percent_check1 CHECK ((bonus_percent >= (0)::numeric))
);

CREATE TABLE IF NOT EXISTS stg.bonussystem_events 
(
	id int4 NOT NULL,
	event_ts timestamp NOT NULL,
	event_type varchar NOT NULL,
	event_value text NOT NULL,
	CONSTRAINT bonussystem_events_pkey PRIMARY KEY (id)
);
COMMIT;
