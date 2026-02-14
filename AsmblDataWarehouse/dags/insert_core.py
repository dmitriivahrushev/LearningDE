from common_variables import OWNER, LAUNCH_TIME, PG_CONNECT, moscow_tz
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime as dt
from airflow import DAG


"""DAG выполняет следующие действия:
   Создание схемы и таблиц + наполнение.
   core: Схема для очищенных, нормализованных данных.
   core.product_type: Таблица для хранения типов продукта.
   core.production: Таблица для хранения общей выработки продукта.
"""


DAG_ID = 'insert_core_data'

update_core_data = """
        BEGIN; 
        CREATE SCHEMA IF NOT EXISTS core;


        CREATE TABLE IF NOT EXISTS core.product_type (
        product_id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
        product_name VARCHAR NOT NULL, 
        product_pn VARCHAR NOT NULL,
        CONSTRAINT pk_product_id PRIMARY KEY (product_id)
        );
        COMMENT ON TABLE core.product_type IS 'Виды продукта.';
        COMMENT ON COLUMN core.product_type.product_id IS 'ID Продукта.';
        COMMENT ON COLUMN core.product_type.product_name IS 'Имя Продукта';
        COMMENT ON COLUMN core.product_type.product_pn IS 'PN Продукта.';


        CREATE TABLE IF NOT EXISTS core.production (
            id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER,
            date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_product_id FOREIGN KEY (product_id) REFERENCES core.product_type (product_id) ON DELETE RESTRICT,
            CONSTRAINT pk_id PRIMARY KEY (id)
        );
        COMMENT ON TABLE core.production IS 'Произведенная продукция.';
        COMMENT ON COLUMN core.production.id IS 'Уникальный идентификатор кортежа.';
        COMMENT ON COLUMN core.production.product_id IS 'ID Продукта.';
        COMMENT ON COLUMN core.production.quantity IS 'Произведенное количество.';
        COMMENT ON COLUMN core.production.date IS 'Время производства продукта.';


        /*Алгоритм обновления данных в таблицах core слоя.
        core.product_type, core.production.
        */
        MERGE INTO core.product_type pt
        USING (
            SELECT DISTINCT 
            TRIM(REPLACE(SUBSTRING(data from '\s(.*),'), ']', ''))::VARCHAR AS product_name,
            TRIM(CONCAT(SPLIT_PART(data, ']', 1), ']'))::VARCHAR AS product_pn
            FROM stage.raw_data AS rd 
            ) AS src  
        ON pt.product_name = src.product_name AND pt.product_pn = src.product_pn
        WHEN NOT MATCHED THEN
        INSERT (product_name, product_pn) VALUES (src.product_name, src.product_pn);


        INSERT INTO core.production (product_id, quantity)
        SELECT 
            pt.product_id,
            TRIM(SPLIT_PART(data, ',', -1))::INT AS quantity
        FROM stage.raw_data AS src
        LEFT JOIN core.product_type AS pt ON TRIM(REPLACE(SUBSTRING(src.data from '\s(.*),'), ']', ''))::VARCHAR = pt.product_name AND TRIM(CONCAT(SPLIT_PART(src.data, ']', 1), ']'))::VARCHAR = pt.product_pn;
        COMMIT;
        """


args = {
    'owner': OWNER,
    'start_date': dt(2025, 6, 4, tzinfo=moscow_tz)
}

with DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule_interval=LAUNCH_TIME,
    catchup=False,
    tags=['sensor_on_insert_stage_data', 'task_update_core_data']
) as dag:
    
    sensor_on_insert_stage_data = ExternalTaskSensor(
        task_id='sensor_on_insert_stage_data',
        external_dag_id='insert_stage_data',
        external_task_id=None,
        allowed_states=['success'],
        mode='reschedule',
        timeout=36000,
        poke_interval=20
    )

    task_update_core_data = SQLExecuteQueryOperator(
        task_id='task_update_core_data',
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=update_core_data
    )

    sensor_on_insert_stage_data >> task_update_core_data





