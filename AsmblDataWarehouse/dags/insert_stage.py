from common_variables import OWNER, LAUNCH_TIME, PG_CONNECT, moscow_tz
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime as dt
from airflow import DAG


"""DAG выполняет следующие действия:
   sensor_on_transform_file: Ожидает выполнение дага transform_file.
   task_create_schema: Создание таблиц в БД.
   task_copy_in_postgres: Наполение таблиц данными.
"""


PATH_TO_CSV = '/opt/airflow/tmp_data/raw_data.csv'

DAG_ID = 'insert_stage_data'


create_schema = """
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

            COMMIT; 
            """


# PostgresHook для COPY из файла raw_data.csv.
def copy_to_postgres():
    hook = PostgresHook(postgres_conn_id=PG_CONNECT)
    conn = hook.get_conn()
    cur = conn.cursor()

    with open(PATH_TO_CSV, 'r') as f:
        cur.copy_expert("""COPY stage.raw_data (data) FROM STDIN WITH (FORMAT CSV, HEADER FALSE, DELIMITER ';', ENCODING 'utf-8'); """, f)
    
    conn.commit()
    cur.close()
    conn.close()


args = {
    'owner': OWNER,
    'start_date': dt(2025, 6, 4, tzinfo=moscow_tz)
}

with DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule_interval=LAUNCH_TIME,
    catchup=False,
    tags=['sensor_on_transform_file', 'task_create_schema', 'task_copy_in_postgres']
) as dag:
    
    sensor_on_transform_file = ExternalTaskSensor(
        task_id='sensor_on_transform_file',
        external_dag_id='transform_file',
        external_task_id=None,
        allowed_states=['success'],
        mode='reschedule',
        timeout=36000,
        poke_interval=20
    )

    task_create_schema = SQLExecuteQueryOperator(
        task_id='task_create_schema',
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=create_schema,
    )

    task_copy_in_postgres = PythonOperator(
        task_id='task_copy_in_postgres',
        python_callable=copy_to_postgres
    )
     
    sensor_on_transform_file >> task_create_schema >> task_copy_in_postgres
