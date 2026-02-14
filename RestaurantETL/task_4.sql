/*
Задание 4.
Найдите пиццерию с самым большим количеством пицц в меню. Если таких пиццерий несколько, выведите все.*/

WITH menu AS (
    SELECT 
        cafe_name, 
        jsonb_each_text(menu -> 'Пицца') AS pizza
    FROM cafe.restaurants 
    WHERE type = 'pizzeria'
),
counted_pizzas AS (
    SELECT 
        cafe_name, 
        COUNT(pizza) AS pizza_count
    FROM menu
    GROUP BY cafe_name
)
SELECT 
    cafe_name, 
    pizza_count
FROM counted_pizzas
WHERE pizza_count = (SELECT MAX(pizza_count) FROM counted_pizzas);