/* Заполнение таблиц данными из raw_data. */


BEGIN;
INSERT INTO car_shop.specifications(gasoline_consumption)
SELECT DISTINCT
    CASE 
      WHEN 
        gasoline_consumption = 'null' OR gasoline_consumption IS NULL THEN NULL
        ELSE CAST(gasoline_consumption AS decimal(3,1)) 
    END AS gasoline_consumption
FROM raw_data.sales;


INSERT INTO car_shop.car_models(
    model,
    spec_id
)
SELECT DISTINCT
    CASE WHEN r.auto LIKE '%Tesla%' THEN TRIM(SPLIT_PART(r.auto, ' ', 2) || ' ' || SPLIT_PART(r.auto, ' ', 3) , ',') ELSE TRIM(SPLIT_PART(r.auto, ' ', 2), ',')END,
    s.spec_id
FROM raw_data.sales AS r
LEFT JOIN car_shop.specifications AS s ON (s.gasoline_consumption IS NOT DISTINCT FROM NULLIF(r.gasoline_consumption, 'null')::decimal(3,1));


INSERT INTO car_shop.colors(color)
SELECT DISTINCT SPLIT_PART(r.auto, ',', 2) AS color
FROM raw_data.sales AS r;


INSERT INTO car_shop.manufacturer_countries(brand_origin)
SELECT DISTINCT 
    CASE 
    	WHEN brand_origin = 'null' THEN 'Germany' -- Заметил, что в текущем датасете у Porsche отсутствует информация о стране-производителе, поэтому добавил Германию.
    	ELSE brand_origin
    END   
FROM raw_data.sales;


INSERT INTO car_shop.customers (name, phone)
SELECT DISTINCT person_name, phone
FROM raw_data.sales;


INSERT INTO car_shop.sales(
    price,
    discount,
    deal_date,
    brand,
    color_id,
    model_id,
    origin_id,
    customer_id
)
SELECT 
    price,
    discount,
    date,
    SPLIT_PART(r.auto, ' ', 1) AS brand,
    color_id,
    model_id,
    origin_id,
    customer_id
FROM raw_data.sales AS r
LEFT JOIN car_shop.colors AS c ON c.color = SPLIT_PART(r.auto, ',', 2) 
LEFT JOIN car_shop.car_models AS cm ON cm.model = CASE WHEN r.auto LIKE '%Tesla%' THEN TRIM(SPLIT_PART(r.auto, ' ', 2) || ' ' || SPLIT_PART(r.auto, ' ', 3) , ',') ELSE TRIM(SPLIT_PART(r.auto, ' ', 2), ',')END
LEFT JOIN car_shop.manufacturer_countries AS mc ON mc.brand_origin = CASE WHEN r.brand_origin='null' THEN 'Germany' ELSE r.brand_origin END
LEFT JOIN car_shop.customers AS cus ON cus.name = r.person_name;
COMMIT;