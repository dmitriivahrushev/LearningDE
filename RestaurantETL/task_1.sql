/*
Задание 1.
Чтобы выдать премию менеджерам, нужно понять, у каких заведений самый высокий средний чек. 
Создайте представление, которое покажет топ-3 заведения внутри каждого типа заведений по среднему чеку за все даты. 
Столбец со средним чеком округлите до второго знака после запятой.*/

CREATE OR REPLACE VIEW cafe.top3_avg_checks AS
WITH avg_checks AS (
SELECT 
    r.cafe_name AS cafe_name,      
    r.type AS cafe_type,     
    ROUND(avg(avg_check), 2) AS avg_check
FROM cafe.sales as s
INNER JOIN cafe.restaurants as r on r.restaurant_uuid = s.restaurant_uuid
GROUP BY r.cafe_name, r.type
),
top_rank AS (
SELECT 
    cafe_name, 
    cafe_type, 
    avg_check, 
    row_number() OVER (PARTITION BY cafe_type ORDER BY avg_check DESC) AS cafe_rank
FROM avg_checks
)
SELECT 
    cafe_name, 
    cafe_type, 
    avg_check
FROM top_rank
WHERE cafe_rank <= 3;