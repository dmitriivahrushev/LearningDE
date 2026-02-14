/*
Создание схемы и таблиц в dds.
*/
BEGIN;
CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.srv_wf_settings 
(
	id int4 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL,
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT dds_wf_settings_pkey PRIMARY KEY (id),
	CONSTRAINT dds_wf_settings_workflow_key_key UNIQUE (workflow_key)
);
INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
VALUES ('load_dm_users', '{"last_loaded_id": 0}');

INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
VALUES ('load_dm_timestamps', '{"last_loaded_id": 0}');

INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
VALUES ('load_dm_restaurants', '{"last_loaded_id": 0}');

INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
VALUES ('load_dm_couriers', '{"last_loaded_id": 0}');

INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
VALUES ('load_dm_products', '{"last_loaded_id": 0}');

INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
VALUES ('load_dm_delivery', '{"last_loaded_id": 0}');

INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
VALUES ('load_dm_orders', '{"last_loaded_id": 0}');

INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
VALUES ('load_fct_product_sales', '{"last_loaded_id": 0}');


CREATE TABLE IF NOT EXISTS dds.dm_restaurants 
(
	id serial4 NOT NULL,
	restaurant_id varchar NOT NULL,
	restaurant_name varchar NOT NULL,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL,
	CONSTRAINT dm_restaurants_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS dds.dm_timestamps 
(
	id serial4 NOT NULL,
	ts timestamp NOT NULL,
	"year" int2 NOT NULL,
	"month" int2 NOT NULL,
	"day" int2 NOT NULL,
	"time" time NOT NULL,
	"date" date NOT NULL,
	CONSTRAINT dm_timestamps_day_check CHECK (((day >= 1) AND (day <= 31))),
	CONSTRAINT dm_timestamps_month_check CHECK (((month >= 1) AND (month <= 12))),
	CONSTRAINT dm_timestamps_pkey PRIMARY KEY (id),
	CONSTRAINT dm_timestamps_year_check CHECK (((year >= 2022) AND (year < 2500)))
);

CREATE TABLE IF NOT EXISTS dds.dm_users 
(
	id serial4 NOT NULL,
	user_id varchar NOT NULL,
	user_name varchar NOT NULL,
	user_login varchar NOT NULL,
	CONSTRAINT dm_users_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS dds.dm_couriers
(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    courier_id VARCHAR NOT NULL,
    name VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.dm_delivery
(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_id VARCHAR NOT NULL,
    courier_id INTEGER NOT NULL,
    order_id VARCHAR NOT NULL,
    order_ts TIMESTAMP NOT NULL,      
    address VARCHAR NOT NULL,
    delivery_ts TIMESTAMP NOT NULL,
    rate SMALLINT NOT NULL DEFAULT 0 CHECK ((rate >=0) AND (rate <= 5)),
    tip_sum NUMERIC(14, 2) DEFAULT 0 NOT NULL,
    sum NUMERIC(14, 2) DEFAULT 0 NOT NULL
);
ALTER TABLE dds.dm_delivery
ADD CONSTRAINT courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers (id);

CREATE TABLE IF NOT EXISTS dds.dm_orders 
(
	id serial4 NOT NULL,
	user_id int4 NOT NULL,
	restaurant_id int4 NOT NULL,
	timestamp_id int4 NOT NULL,
	order_key varchar NOT NULL,
	order_status varchar NOT NULL,
	CONSTRAINT dm_orders_pkey PRIMARY KEY (id)
);
ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES dds.dm_users(id);
ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_restaurants_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id);
ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_timestamps_timestamp_id_fkey FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id);

CREATE TABLE IF NOT EXISTS dds.dm_products 
(
	id serial4 NOT NULL,
	restaurant_id int4 NOT NULL,
	product_id varchar NOT NULL,
	product_name varchar NOT NULL,
	product_price numeric(14, 2) DEFAULT 0 NOT NULL,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL,
	CONSTRAINT dm_products_pkey PRIMARY KEY (id),
	CONSTRAINT dm_products_product_price_check CHECK (((product_price < 999000000000.99) AND (product_price >= (0)::numeric)))
);
ALTER TABLE dds.dm_products ADD CONSTRAINT dm_products_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id);

CREATE TABLE IF NOT EXISTS dds.fct_product_sales 
(
	id serial4 NOT NULL,
	product_id int4 NOT NULL,
	order_id int4 NOT NULL,
	count int4 DEFAULT 0 NOT NULL,
	price numeric(14, 2) DEFAULT 0 NOT NULL,
	total_sum numeric(14, 2) DEFAULT 0 NOT NULL,
	bonus_payment numeric(14, 2) DEFAULT 0 NOT NULL,
	bonus_grant numeric(14, 2) DEFAULT 0 NOT NULL,
	CONSTRAINT fct_product_sales_bonus_grant_check CHECK ((bonus_grant >= (0)::numeric)),
	CONSTRAINT fct_product_sales_bonus_payment_check CHECK ((bonus_payment >= (0)::numeric)),
	CONSTRAINT fct_product_sales_count_check CHECK ((count >= 0)),
	CONSTRAINT fct_product_sales_pkey PRIMARY KEY (id),
	CONSTRAINT fct_product_sales_price_check CHECK ((price >= (0)::numeric)),
	CONSTRAINT fct_product_sales_total_sum_check CHECK ((total_sum >= (0)::numeric))
);
ALTER TABLE dds.fct_product_sales ADD CONSTRAINT fct_product_sales_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id);
ALTER TABLE dds.fct_product_sales ADD CONSTRAINT fct_product_sales_product_id_fkey FOREIGN KEY (product_id) REFERENCES dds.dm_products(id);
COMMIT;
