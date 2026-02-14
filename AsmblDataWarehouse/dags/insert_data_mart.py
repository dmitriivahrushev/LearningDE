from common_variables import OWNER, PG_CONNECT, LAUNCH_TIME, moscow_tz
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime as dt
from airflow import DAG


"""DAG выполняет следующие действия:
   Создание схемы и таблиц + наполнение.
   data_mart: Схема для витрин данных.
   data_mart.product_mart: Таблица для аналитики.
"""


DAG_ID = 'insert_data_mart_data'


update_data_mart = """
                    BEGIN; 
                    CREATE SCHEMA IF NOT EXISTS data_mart;
                    CREATE TABLE IF NOT EXISTS data_mart.product_mart (
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
    tags=['sensor_insert_core_data', 'task_update_data_mart']
) as dag:
    
    sensor_insert_core_data = ExternalTaskSensor(
        task_id='sensor_insert_core_data',
        external_dag_id='insert_core_data',
        external_task_id=None,
        allowed_states=['success'],
        mode='reschedule',
        timeout=36000,
        poke_interval=20
    )

    task_update_data_mart = SQLExecuteQueryOperator(
        task_id='task_update_data_mart',
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=update_data_mart
    )

    sensor_insert_core_data >> task_update_data_mart


