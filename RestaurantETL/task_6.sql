/*
Задание 6.
В Gastro Hub решили проверить новую продуктовую гипотезу и поднять цены на капучино. 
Маркетологи компании собрали совещание, чтобы обсудить, на сколько стоит поднять цены. 
В это время для отчётности использовать старые цены нельзя. После обсуждения решили увеличить цены на капучино на 20%.
Обновите данные по ценам так, чтобы до завершения обновления никто не вносил других изменений в цены этих заведений. 
В заведениях, где цены не меняются, данные о меню должны остаться в полном доступе.*/

/*
Пояснение принятых решений:
Блок транзакций — начинается с BEGIN и заканчивается COMMIT - этот блок гарантирует атомарность выполнения операций. 
Первый CTE (current_price_cappuccino) -> получает текущие данные о стоимости капучино.
Второй CTE (new_price_cappucino) -> вычисляет новую стоимость капучино.
Основной запрос -> обновляет таблицу restaurants с новыми ценами.*/

BEGIN;

WITH current_price_cappuccino AS (
SELECT cafe_name, j.KEY AS coffee_name, j.value::DECIMAL AS price
FROM cafe.restaurants r
JOIN jsonb_each_text(r.menu -> 'Кофе') AS j ON TRUE
WHERE TYPE ='coffee_shop' AND j.KEY = 'Капучино'
FOR UPDATE --Блокирует строки для последующих изменений.
),
new_price_cappucino AS (
SELECT cafe_name, coffee_name, price + (price * 0.20) AS new_price
FROM current_price_cappuccino
)
UPDATE cafe.restaurants AS r
SET menu = jsonb_set(r.menu, '{Кофе, Капучино}', to_jsonb(np.new_price))
FROM new_price_cappucino AS np
WHERE r.cafe_name = np.cafe_name;

COMMIT;