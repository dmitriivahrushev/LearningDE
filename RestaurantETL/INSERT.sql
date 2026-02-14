--Наполнение данными таблиц в схеме cafe.

INSERT INTO cafe.managers (manager, manager_phone)
SELECT DISTINCT manager, manager_phone
FROM raw_data.sales;

INSERT INTO cafe.restaurants (cafe_name, type, menu)
SELECT DISTINCT 
    raw.cafe_name, 
    type::cafe.restaurant_type,
    menu
FROM raw_data.sales AS raw
INNER JOIN raw_data.menu AS m ON m.cafe_name = raw.cafe_name;

INSERT INTO cafe.sales (restaurant_uuid, date, avg_check)
SELECT r.restaurant_uuid, report_date, avg_check
FROM raw_data.sales AS raw
LEFT JOIN cafe.restaurants AS r ON r.cafe_name = raw.cafe_name;

INSERT INTO cafe.restaurant_manager_work_dates (
    restaurant_uuid,
    manager_uuid,
    start_date,
    end_date
)
SELECT r.restaurant_uuid, m.manager_uuid, MIN(report_date), MAX(report_date)
FROM raw_data.sales AS raw
INNER JOIN cafe.restaurants AS r ON r.cafe_name = raw.cafe_name
INNER JOIN cafe.managers AS m ON m.manager = raw.manager
GROUP BY r.restaurant_uuid, m.manager_uuid;