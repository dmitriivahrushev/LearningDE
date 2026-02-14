/* Запросы на получение данных. */

---- Задание 1. Напишите запрос, который выведет процент моделей машин, у которых нет параметра `gasoline_consumption`.
SELECT 
    ROUND(
        (
            COUNT(CASE WHEN s.gasoline_consumption IS NULL THEN 1 END)::DECIMAL /
            COUNT(c.model_id)
        ) * 100,
        0
    ) AS electric_cars_percentage
FROM car_shop.car_models c
LEFT JOIN car_shop.specifications s ON c.spec_id = s.spec_id;


---- Задание 2. Напишите запрос, который покажет название бренда и среднюю цену его автомобилей в разбивке по всем годам с учётом скидки.
SELECT
    brand AS brand_name,
    EXTRACT(YEAR FROM deal_date) AS year,
    AVG(price) AS price_avg
FROM car_shop.sales
GROUP BY brand, deal_date
ORDER BY brand ASC, deal_date ASC;


---- Задание 3. Посчитайте среднюю цену всех автомобилей с разбивкой по месяцам в 2022 году с учётом скидки.
SELECT
    EXTRACT(MONTH FROM deal_date) AS month,
    EXTRACT(YEAR FROM deal_date) AS year,
    ROUND(AVG(price), 2) AS price_avg
FROM car_shop.sales
WHERE EXTRACT(YEAR FROM deal_date) = 2022
GROUP BY month, YEAR
ORDER BY month ASC;


---- Задание 4. Напишите запрос, который выведет список купленных машин у каждого пользователя.
SELECT name AS person, STRING_AGG(brand, ',') AS cars
FROM car_shop.sales AS s
JOIN car_shop.car_models m ON m.model_id = s.model_id
JOIN car_shop.customers c ON c.customer_id = s.customer_id
GROUP BY name;


---- Задание 5. Напишите запрос, который покажет количество всех пользователей из США.
SELECT COUNT(DISTINCT name) AS persons_from_usa_count
FROM car_shop.customers
WHERE phone LIKE '+1%';


---- Задание 6. Напишите запрос, который вернёт самую большую и самую маленькую цену
---- продажи автомобиля с разбивкой по стране без учёта скидки. 
---- Цена в колонке price дана с учётом скидки.
SELECT 
    mc.brand_origin,
    MAX(CAST(price / (1 - discount / 100) AS DECIMAL(12, 2))) AS price_max_without_discount,
    MIN(CAST(price / (1 - discount / 100) AS DECIMAL(12, 2))) AS price_min_without_discount
FROM car_shop.sales AS s
JOIN car_shop.manufacturer_countries AS mc ON s.origin_id = mc.origin_id
GROUP BY mc.brand_origin;