/*Задание 5.
Найдите самую дорогую пиццу для каждой пиццерии.
Вот формат итоговой таблицы, числа и названия — для наглядности:*/

WITH menu AS (
    SELECT 
        r.cafe_name AS cafe_name,
        'Пицца' AS type,
        j.key AS pizza_name,
        CAST(j.value AS INT) AS price
    FROM 
        cafe.restaurants AS r
    JOIN 
        jsonb_each_text(r.menu -> 'Пицца') AS j ON true
),
pizzas_price_rank AS (
    SELECT 
        cafe_name, 
        type, 
        pizza_name,
        price, 
        ROW_NUMBER() OVER(PARTITION BY cafe_name ORDER BY price DESC) AS price_rnk
    FROM 
        menu
)
SELECT 
    cafe_name, 
    type, 
    pizza_name,
    price      
FROM 
    pizzas_price_rank
WHERE 
    price_rnk = 1
ORDER BY price DESC;
