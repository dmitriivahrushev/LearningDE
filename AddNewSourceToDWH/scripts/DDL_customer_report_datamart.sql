/* DDL для витрины customer_report_datamart. */
BEGIN;
DROP TABLE IF EXISTS dwh.customer_report_datamart;
CREATE TABLE dwh.customer_report_datamart (
    id int8 GENERATED ALWAYS AS IDENTITY NOT NULL,
    customer_id INT8 NOT NULL,
    customer_name VARCHAR NOT NULL,
    customer_address VARCHAR NOT NULL,
    customer_birthday DATE NOT NULL,
    customer_email VARCHAR NOT NULL,
    paid_amount_customer NUMERIC(15, 2),
    platform_money INT8,
    orders_per_month INT8,
    avg_price_order_per_month NUMERIC(10, 2),
    median_time_order_completed NUMERIC(10, 1),
    top_product_category_per_month VARCHAR,
    top_craftsman_id INT8,
    count_order_created INT8,
    count_order_in_progress INT8,
    count_order_delivery INT8,
    count_order_done INT8,
    count_order_not_done INT8,
    report_period VARCHAR,
    CONSTRAINT customer_report_datamart_pk PRIMARY KEY (id)
);
COMMENT ON TABLE dwh.customer_report_datamart IS 'Отчет по заказчикам.';
COMMENT ON COLUMN dwh.customer_report_datamart.id IS 'Идентификатор записи.';
COMMENT ON COLUMN dwh.customer_report_datamart.customer_id IS 'Идентификатор заказчика.';
COMMENT ON COLUMN dwh.customer_report_datamart.customer_name IS 'ФИО заказчика.';
COMMENT ON COLUMN dwh.customer_report_datamart.customer_address IS 'Адрес заказчика.';
COMMENT ON COLUMN dwh.customer_report_datamart.customer_birthday IS 'Дата рождения заказчика.';
COMMENT ON COLUMN dwh.customer_report_datamart.customer_email IS 'Электронная почта заказчика.';
COMMENT ON COLUMN dwh.customer_report_datamart.paid_amount_customer IS 'Сумма, которую потратил заказчик.';
COMMENT ON COLUMN dwh.customer_report_datamart.platform_money IS 'Заработок платформы за месяц — это 10 % от суммы, потраченной заказчиком.';
COMMENT ON COLUMN dwh.customer_report_datamart.orders_per_month IS 'Количество заказов у заказчика за месяц.';
COMMENT ON COLUMN dwh.customer_report_datamart.avg_price_order_per_month IS 'Средняя стоимость одного заказа у заказчика за месяц.';
COMMENT ON COLUMN dwh.customer_report_datamart.median_time_order_completed IS 'Медианное время в днях от момента создания заказа до его завершения за месяц.';
COMMENT ON COLUMN dwh.customer_report_datamart.top_product_category_per_month IS 'Самая популярная категория товаров у этого заказчика за месяц.';
COMMENT ON COLUMN dwh.customer_report_datamart.top_craftsman_id IS 'Идентификатор самого популярного мастера ручной работы у заказчика.';
COMMENT ON COLUMN dwh.customer_report_datamart.count_order_created IS 'Количество созданных заказов за месяц.';
COMMENT ON COLUMN dwh.customer_report_datamart.count_order_in_progress IS 'Количество заказов в процессе изготовки за месяц.';
COMMENT ON COLUMN dwh.customer_report_datamart.count_order_delivery IS 'Количество заказов в доставке за месяц.';
COMMENT ON COLUMN dwh.customer_report_datamart.count_order_done IS 'Количество завершённых заказов за месяц.';
COMMENT ON COLUMN dwh.customer_report_datamart.count_order_not_done IS 'Количество незавершённых заказов за месяц.';
COMMENT ON COLUMN dwh.customer_report_datamart.report_period IS 'Отчётный период, год и месяц.';


/* Таблица с датой загрузки данных. */
DROP TABLE IF EXISTS dwh.load_dates_customer_report_datamart;
CREATE TABLE dwh.load_dates_customer_report_datamart (
	id int8 GENERATED ALWAYS AS IDENTITY NOT NULL,
	load_dttm date NOT NULL,
	CONSTRAINT load_dates_customer_report_datamart_pk PRIMARY KEY (id)
);
COMMENT ON TABLE dwh.load_dates_customer_report_datamart IS 'Таблица с датой загрузки данных.';
COMMENT ON COLUMN dwh.load_dates_customer_report_datamart.id IS 'Идентификатор записи.';
COMMENT ON COLUMN dwh.load_dates_customer_report_datamart.load_dttm IS 'Дата загрузки данных.';
COMMIT;