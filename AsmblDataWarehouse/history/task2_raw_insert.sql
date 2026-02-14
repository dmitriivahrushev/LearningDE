/*Создание схемы и таблиц + наполнение.
  stage: Схема для сырых данных из источника.
  stage.raw_data: Таблица для хранения сырых данных из источника.
  Информация об источнике: Данные обновляются один раз в день, каждая новая загрузка в источник удаляет старые данные.
*/

BEGIN; 
CREATE SCHEMA IF NOT EXISTS stage;


DROP TABLE IF EXISTS stage.raw_data;
CREATE TABLE stage.raw_data (
   id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
   data TEXT,
   data_load TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE stage.raw_data IS 'Сырые данные из источника.';
COMMENT ON COLUMN stage.raw_data.data IS 'Поле с данными.';
COMMENT ON COLUMN stage.raw_data.data_load IS 'Время загрузки.';


COPY stage.raw_data (data) FROM 'D:\temp\raw_data.csv' WITH (FORMAT CSV, HEADER FALSE, DELIMITER ';', ENCODING 'utf-8');
COMMIT;