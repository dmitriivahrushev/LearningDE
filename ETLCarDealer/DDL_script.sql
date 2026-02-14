/* Создание схем и таблиц для хранения данных.
   raw_data: Сырой слой данных.
   sales: Таблица со всеми данными.
   
   car_shop: Схема для нормализованных таблиц.
   specifications: Характеристики авто.
   car_models: Модели авто.
   colors: Цвет авто.
   manufacturer_countries: Страна производитель.
   customers: Клиенты.
   sales: Продажи.
*/


BEGIN;
CREATE SCHEMA IF NOT EXISTS raw_data; 

CREATE TABLE IF NOT EXISTS raw_data.sales (
    id int4,
    auto varchar(50),
    gasoline_consumption varchar(50),
    price float4,
    date TIMESTAMP(50),
    person_name varchar(50),
    phone varchar(50),
    discount int4,
    brand_origin varchar(50) 
);


CREATE SCHEMA IF NOT EXISTS car_shop;
CREATE TABLE IF NOT EXISTS car_shop.specifications(
    spec_id SERIAL PRIMARY KEY, 
    gasoline_consumption DECIMAL(3,1) NULL 
);

CREATE TABLE IF NOT EXISTS car_shop.car_models(
    model_id SERIAL PRIMARY KEY, 
    model VARCHAR(50), 
    spec_id INTEGER,  
    CONSTRAINT spec_id_fkey FOREIGN KEY (spec_id) REFERENCES car_shop.specifications(spec_id)   
);

CREATE TABLE IF NOT EXISTS car_shop.colors(
    color_id SERIAL PRIMARY KEY, 
    color VARCHAR(20) 
);

CREATE TABLE IF NOT EXISTS car_shop.manufacturer_countries(
    origin_id SERIAL PRIMARY KEY, 
    brand_origin VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS car_shop.customers(
    customer_id SERIAL PRIMARY KEY, 
    name VARCHAR(50), 
    phone VARCHAR(50) 
);

CREATE TABLE IF NOT EXISTS car_shop.sales(
    sale_id SERIAL PRIMARY KEY, 
    price DECIMAL(10, 2), 
    discount DECIMAL(5, 2), 
    deal_date TIMESTAMP, 
    brand VARCHAR(50), 
    origin_id INTEGER,  
    color_id INTEGER, 
    model_id INTEGER, 
    customer_id INTEGER,
    CONSTRAINT customer_id_fkey FOREIGN KEY (customer_id) REFERENCES car_shop.customers(customer_id), 
    CONSTRAINT origin_id_fkey FOREIGN KEY (origin_id) REFERENCES car_shop.manufacturer_countries(origin_id),  
    CONSTRAINT color_id_fkey FOREIGN KEY (color_id) REFERENCES car_shop.colors(color_id),  
    CONSTRAINT model_id_fkey FOREIGN KEY (model_id) REFERENCES car_shop.car_models(model_id)  
);
COMMIT;