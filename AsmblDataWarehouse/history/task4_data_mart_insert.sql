/*Создание схемы и таблиц + наполнение.
  data_mart: Схема для витрин данных.
  data_mart.product_mart: Таблица для аналитики.
*/

BEGIN; 
CREATE SCHEMA IF NOT EXISTS data_mart;
CREATE TABLE data_mart.product_mart (
    id INT GENERATED ALWAYS AS IDENTITY NOT NULL,
    product_pn VARCHAR NOT NULL,
    product_name VARCHAR NOT NULL,
    quantity INTEGER NOT NULL,
    date TIMESTAMP NOT NULL
);
COMMENT ON TABLE data_mart.product_mart IS 'Витрина с данными по выработке.';
COMMENT ON COLUMN data_mart.product_mart.id IS 'Уникальный идентификатор.';
COMMENT ON COLUMN data_mart.product_mart.product_pn IS 'PN продукта.';
COMMENT ON COLUMN data_mart.product_mart.product_name IS 'Имя продукта.';
COMMENT ON COLUMN data_mart.product_mart.quantity IS 'Количество выпущенного продукта.';
COMMENT ON COLUMN data_mart.product_mart.date IS 'Дата выпуска продукта.';


INSERT INTO data_mart.product_mart (product_pn, product_name, quantity, date)
SELECT
    pt.product_pn,
    pt.product_name,
    quantity,
    date
FROM core.production p
JOIN core.product_type pt USING(product_id);
COMMIT;