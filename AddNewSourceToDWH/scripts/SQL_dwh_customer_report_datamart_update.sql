/* Cкрипт для инкрементального обновления витрины. */
BEGIN;
WITH
-- Выборка новых данных.
dwh_delta AS (
    SELECT 
       crd.customer_id AS exists_customer_id,
       dcs.customer_id AS customer_id,
       dcs.customer_name AS customer_name,
       dcs.customer_address AS customer_address,
       dcs.customer_birthday AS customer_birthday,
       dcs.customer_email AS customer_email,
       dcs.load_dttm AS customers_load_dttm,
       dp.product_price AS product_price,
       dp.product_type AS product_type,
       dp.load_dttm AS products_load_dttm,
       fo.order_id AS order_id,
       fo.order_completion_date - fo.order_created_date AS diff_order_date,
       fo.order_status AS order_status,
       TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period,
       dc.craftsman_id AS craftsman_id,
       dc.load_dttm AS craftsman_load_dttm    
    FROM dwh.f_order fo
    INNER JOIN dwh.d_craftsman dc ON fo.craftsman_id = dc.craftsman_id 
    INNER JOIN dwh.d_customer dcs ON fo.customer_id = dcs.customer_id 
    INNER JOIN dwh.d_product dp ON fo.product_id = dp.product_id
    LEFT JOIN dwh.customer_report_datamart crd ON fo.customer_id = crd.customer_id
    WHERE (fo.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
          (dc.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
          (dcs.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
          (dp.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart)) 
),

-- Выборка данных для обновления.
dwh_update_delta AS (
    SELECT 
        exists_customer_id AS customer_id
    FROM dwh_delta
    WHERE exists_customer_id IS NOT NULL
),

-- Выборка дланных для вставки. 
dwh_delta_insert_result AS (
    SELECT 
        customer_id,
        customer_name,
        customer_address,
        customer_birthday,
        customer_email,
        SUM(product_price) AS paid_amount_customer,
        ROUND(SUM(product_price * 0.1)::NUMERIC, 2) AS platform_money,
        COUNT(DISTINCT order_id) AS orders_per_month,
        AVG(product_price) AS avg_price_order_per_month,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY diff_order_date) AS median_time_order_completed,
        MODE() WITHIN GROUP (ORDER BY product_type) AS top_product_category_per_month,
        MODE() WITHIN GROUP (ORDER BY craftsman_id) AS top_craftsman_id,
        COUNT(order_id) FILTER (WHERE order_status = 'created') AS count_order_created,
        COUNT(order_id) FILTER (WHERE order_status = 'in progress') AS count_order_in_progress,
        COUNT(order_id) FILTER (WHERE order_status = 'delivery') AS count_order_delivery,
        COUNT(order_id) FILTER (WHERE order_status = 'done') AS count_order_done,
        COUNT(order_id) FILTER (WHERE order_status != 'done') AS count_order_not_done,
        report_period
    FROM dwh_delta
    WHERE exists_customer_id IS NULL
    GROUP BY customer_id, report_period, customer_name, customer_address, customer_birthday, customer_email
),

-- Выборка данных для обновления.
dwh_delta_update_result AS (
    SELECT 
        customer_id,
        customer_name,
        customer_address,
        customer_birthday,
        customer_email,
        SUM(product_price) AS paid_amount_customer,
        ROUND(SUM(product_price * 0.1)::NUMERIC, 2) AS platform_money,
        COUNT(DISTINCT order_id) AS orders_per_month,
        AVG(product_price) AS avg_price_order_per_month,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY diff_order_date) AS median_time_order_completed,
        MODE() WITHIN GROUP (ORDER BY product_type) AS top_product_category_per_month,
        MODE() WITHIN GROUP (ORDER BY craftsman_id) AS top_craftsman_id,
        COUNT(order_id) FILTER (WHERE order_status = 'created') AS count_order_created,
        COUNT(order_id) FILTER (WHERE order_status = 'in progress') AS count_order_in_progress,
        COUNT(order_id) FILTER (WHERE order_status = 'delivery') AS count_order_delivery,
        COUNT(order_id) FILTER (WHERE order_status = 'done') AS count_order_done,
        COUNT(order_id) FILTER (WHERE order_status != 'done') AS count_order_not_done,
        report_period
    FROM dwh_delta
    WHERE exists_customer_id IS NOT NULL
    GROUP BY customer_id, report_period, customer_name, customer_address, customer_birthday, customer_email
),

-- Вставка новых данных.
insert_delta AS (
    INSERT INTO dwh.customer_report_datamart( 
        customer_id,
        customer_name,
        customer_address,
        customer_birthday,
        customer_email,
        paid_amount_customer,
        platform_money,
        orders_per_month,
        avg_price_order_per_month,
        median_time_order_completed,
        top_product_category_per_month,
        top_craftsman_id,
        count_order_created,
        count_order_in_progress,
        count_order_delivery,
        count_order_done,
        count_order_not_done,
        report_period
    )
    SELECT 
        customer_id,
        customer_name,
        customer_address,
        customer_birthday,
        customer_email,
        paid_amount_customer,
        platform_money,
        orders_per_month,
        avg_price_order_per_month,
        median_time_order_completed,
        top_product_category_per_month,
        top_craftsman_id,
        count_order_created,
        count_order_in_progress,
        count_order_delivery,
        count_order_done,
        count_order_not_done,
        report_period
    FROM dwh_delta_insert_result 
),

-- Обновление существующих данных.
update_delta AS ( 
    UPDATE dwh.customer_report_datamart
    SET 
       customer_id = updt.customer_id,
       customer_name = updt.customer_name,
       customer_address = updt.customer_address,
       customer_birthday = updt.customer_birthday,
       customer_email = updt.customer_email,
       paid_amount_customer = updt.paid_amount_customer,
       platform_money = updt.platform_money,
       orders_per_month = updt.orders_per_month,
       avg_price_order_per_month = updt.avg_price_order_per_month,
       median_time_order_completed = updt.median_time_order_completed,
       top_product_category_per_month = updt.top_product_category_per_month,
       top_craftsman_id = updt.top_craftsman_id,
       count_order_created = updt.count_order_created,
       count_order_in_progress = updt.count_order_in_progress,
       count_order_delivery = updt.count_order_delivery,
       count_order_done = updt.count_order_done,
       count_order_not_done = updt.count_order_not_done,
       report_period = updt.report_period
     FROM dwh_delta_update_result updt
     WHERE dwh.customer_report_datamart.customer_id = updt.customer_id   
),

-- Добавление даты загрузки.
insert_load_date AS ( 
    INSERT INTO dwh.load_dates_customer_report_datamart (
        load_dttm
    )
    SELECT GREATEST(COALESCE(MAX(craftsman_load_dttm), NOW()), 
                    COALESCE(MAX(customers_load_dttm), NOW()), 
                    COALESCE(MAX(products_load_dttm), NOW())) 
        FROM dwh_delta
)
SELECT 'increment datamart';
COMMIT;