/*
Задание 7
Руководство GastroHub приняло решение сделать единый номер телефонов для всех менеджеров. 
Новый номер — 8-800-2500-***, где порядковый номер менеджера выставляется по алфавиту, начиная с номера 100. 
Старый и новый номер нужно будет хранить в массиве, где первый элемент массива — новый номер, а второй — старый.*/

/*
Скрипт:
Блокирует таблицу cafe.managers, чтобы предотвратить параллельные изменения.
Создает новый столбец manager_new_phone типа JSON.
Ранжирует менеджеров: Присваивает менеджерам уникальные номера по алфавиту.
Генерирует новые телефоны: 
Формирует новые телефонные номера формата 8-800-2500-XXX и сохраняет их вместе со старыми номерами в новом столбце.
Удаляет старый столбец manager_phone.
Переименовывает manager_new_phone обратно в manager_phone.
Завершает транзакцию: Подтверждает изменения и снимает блокировку с таблицы.
*/

BEGIN;

LOCK TABLE cafe.managers IN EXCLUSIVE MODE;
ALTER TABLE cafe.managers ADD manager_new_phone JSON;

WITH ranked_managers AS (
    SELECT *, row_number() OVER(ORDER BY manager ASC) AS rank
    FROM cafe.managers 
)
UPDATE cafe.managers AS m
SET manager_new_phone = JSON_BUILD_ARRAY(
    CONCAT('8-800-2500-', LPAD((r.rank + 99)::text, 3, '0')), 
    m.manager_phone
)
FROM ranked_managers AS r
WHERE m.manager = r.manager; 

ALTER TABLE cafe.managers DROP COLUMN manager_phone;
ALTER TABLE cafe.managers RENAME manager_new_phone TO manager_phone;

COMMIT;