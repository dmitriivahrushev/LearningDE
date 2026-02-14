/*
Задание 2.
Создайте материализованное представление, которое покажет, 
как изменяется средний чек для каждого заведения от года к году за все года за исключением 2023 года. 
Все столбцы со средним чеком округлите до второго знака после запятой.*/

/*
Исправления.
Добавлено партиционирование PARTITION BY cafe_name для группировки по каждому ресторану.
Сортировка ORDER BY year обеспечивает правильное упорядочение данных по годам.
*/ 

DROP MATERIALIZED VIEW IF EXISTS cafe.avg_check_change_by_year;
CREATE MATERIALIZED VIEW cafe.avg_check_change_by_year AS
WITH avg_check_years AS (
SELECT 
    EXTRACT(YEAR FROM date) AS year, 
    cafe_name, 
    type, 
    avg(avg_check) AS avg_check
FROM cafe.sales AS s
INNER JOIN cafe.restaurants AS r ON r.restaurant_uuid = s.restaurant_uuid
WHERE EXTRACT(YEAR FROM date) != 2023
GROUP BY year, cafe_name, type
ORDER BY type
)
SELECT 
    year, 
    cafe_name, 
    type, 
    ROUND(avg_check, 2) AS current_avg_check, 
    ROUND(LAG(avg_check, 1) OVER(PARTITION BY cafe_name ORDER BY year), 2) AS prev_avg_check,
    ROUND((avg_check - LAG(avg_check, 1) OVER(PARTITION BY cafe_name ORDER BY year)) / LAG(avg_check, 1) OVER(PARTITION BY cafe_name ORDER BY year) * 100, 2) AS check_change_percentage
FROM avg_check_years;