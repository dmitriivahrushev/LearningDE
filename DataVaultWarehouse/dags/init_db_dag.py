"""
DAG для инициализации структуры базы данных Vertica.

DAG отвечает за создание всех необходимых таблиц, схем и представлений
в базе данных Vertica. Выполняет SQL скрипт инициализации при запуске.
"""
from airflow.decorators import dag, task
import pendulum
from lib.vertica_conn import get_vertica_conn
from lib.exec_sql import SqlQuery


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['vertica', 'init_db'],
    description='Инициализация БД Vertica для DWH'
)
def init_db_dag():
    """  
    Создает необходимую структуру базы данных в Vertica:
    - Схемы данных (STAGING, DWH)
    - Таблицы Data Vault (хабы, линки, сателлиты)
    - Представления для аналитики
            
    Note:
        - Использует параметры подключения из Airflow Connection 'vertica_conn'
    """
    conn_info = get_vertica_conn()
    
    @task(task_id='init_db')
    def init_tables() -> None:
        path_file = '/lessons/dags/sql/init_db.sql'
        sql_obj = SqlQuery(path_file, conn_info)
        sql_obj.init_sql()
    
    init_db_task = init_tables()
    return init_db_task

init_dag = init_db_dag()
