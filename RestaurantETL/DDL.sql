--Запросы, для создания схемы сafe и таблиц в ней. 

CREATE SCHEMA IF NOT EXISTS cafe;

--Тип данных ENUM для фиксированного набора типов ресторанов. 
DROP TYPE IF EXISTS cafe.restaurant_type CASCADE;
CREATE TYPE cafe.restaurant_type AS ENUM
            ('coffee_shop', 'restaurant', 'bar', 'pizzeria');

--Таблица с информацией о ресторанах. 
CREATE TABLE IF NOT EXISTS cafe.restaurants (
    restaurant_uuid uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    cafe_name VARCHAR,
    type cafe.restaurant_type,
    menu jsonb
);

--Таблица с информацией о менеджерах.
CREATE TABLE IF NOT EXISTS cafe.managers (
    manager_uuid uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    manager VARCHAR,
    manager_phone VARCHAR(20)
);

--Таблица хранит дату начала работы в ресторане и дату окончания работы в ресторане.
CREATE TABLE IF NOT EXISTS cafe.restaurant_manager_work_dates (
    restaurant_uuid uuid,
    manager_uuid uuid,
    start_date DATE,
    end_date DATE,
    CONSTRAINT pk_restaurant_manager PRIMARY KEY (restaurant_uuid, manager_uuid),
    CONSTRAINT fk_restaurant_uuid FOREIGN KEY (restaurant_uuid) REFERENCES cafe.restaurants (restaurant_uuid),
    CONSTRAINT fk_manager_uuid FOREIGN KEY (manager_uuid) REFERENCES cafe.managers (manager_uuid),
    CHECK (start_date <= end_date)
    
);

--Таблица хранит информацию о средних чеках за определенный день.
CREATE TABLE IF NOT EXISTS cafe.sales (
    date DATE,
    restaurant_uuid uuid,
    avg_check NUMERIC(6, 2),
    CONSTRAINT pk_date_restaurant PRIMARY KEY (date, restaurant_uuid),
    CONSTRAINT fk_restaurant_uuid FOREIGN KEY (restaurant_uuid) REFERENCES cafe.restaurants(restaurant_uuid)
);