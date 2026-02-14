/*
Создание схемы и таблиц в cdm.
*/
BEGIN;
CREATE SCHEMA IF NOT EXISTS cdm;

CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger 
(
    id SERIAL PRIMARY KEY NOT NULL,
    courier_id VARCHAR NOT NULL,
    courier_name VARCHAR NOT NULL,
    settlement_year DATE NOT NULL CHECK (settlement_year < '2500-01-01'::date),
    settlement_month SMALLINT NOT NULL CHECK ((settlement_month >= 1) AND (settlement_month <= 12)),
    orders_count INTEGER DEFAULT 0 NOT NULL CHECK(orders_count >= 0),
    orders_total_sum NUMERIC (14, 2) DEFAULT 0 NOT NULL,
    rate_avg DECIMAL (3, 2) DEFAULT 0 NOT NULL CHECK(rate_avg >= 0),
    order_processing_fee NUMERIC (14, 2) DEFAULT 0 NOT NULL,
    courier_order_sum NUMERIC (14, 2) DEFAULT 0 NOT NULL,
    courier_tips_sum NUMERIC (14, 2) DEFAULT 0 NOT NULL,
    courier_reward_sum NUMERIC (14, 2) DEFAULT 0 NOT NULL
);

CREATE TABLE IF NOT EXISTS cdm.dm_settlement_report 
(
	id serial4 NOT NULL,
	restaurant_id varchar NOT NULL,
	restaurant_name varchar NOT NULL,
	settlement_date date NOT NULL,
	orders_count int4 DEFAULT 0 NOT NULL,
	orders_total_sum numeric(14, 2) DEFAULT 0 NOT NULL,
	orders_bonus_payment_sum numeric(14, 2) DEFAULT 0 NOT NULL,
	orders_bonus_granted_sum numeric(14, 2) DEFAULT 0 NOT NULL,
	order_processing_fee numeric(14, 2) DEFAULT 0 NOT NULL,
	restaurant_reward_sum numeric(14, 2) DEFAULT 0 NOT NULL,
	CONSTRAINT dm_settlement_report_pkey PRIMARY KEY (id),
	CONSTRAINT dm_settlement_report_settlement_date_check CHECK (((settlement_date >= '2022-01-01'::date) AND (settlement_date < '2500-01-01'::date))),
	CONSTRAINT order_processing_fee_check CHECK ((order_processing_fee >= (0)::numeric)),
	CONSTRAINT orders_bonus_granted_sum_check CHECK ((orders_bonus_granted_sum >= (0)::numeric)),
	CONSTRAINT orders_bonus_payment_sum_check CHECK ((orders_bonus_payment_sum >= (0)::numeric)),
	CONSTRAINT orders_count_check CHECK ((orders_count >= 0)),
	CONSTRAINT orders_total_sum_check CHECK ((orders_total_sum >= (0)::numeric)),
	CONSTRAINT restaurant_reward_sum_check CHECK ((restaurant_reward_sum >= (0)::numeric)),
	CONSTRAINT restaurant_unique UNIQUE (restaurant_id, settlement_date)
);
COMMIT;
