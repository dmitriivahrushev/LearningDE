"""
DAG для загрузки данных в STV2025061611__STAGING слой Vertica.

DAG отвечает за загрузку сырых данных из CSV файлов в промежуточный
STAGING слой базы данных Vertica.
"""
import pendulum
from airflow.decorators import dag, task
from lib.load_stg import insert_stg
from lib.vertica_conn import get_vertica_conn


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['vertica', 'insert_STV2025061611__STAGING'],
    description='Загрузка в STAGING слой Vertica'
)
def load_stg():
    """
    Tasks:
        dialogs_load: Загрузка диалогов в STV2025061611__STAGING.dialogs
        groups_load: Загрузка групп в STV2025061611__STAGING.groups
        users_load: Загрузка пользователей в STV2025061611__STAGING.users
        group_log_load: Загрузка логов групп в STV2025061611__STAGING.group_log
    """
    
    stg_dialogs = 'dialogs'
    stg_groups = 'groups'
    stg_users = 'users'
    stg_group_log = 'group_log'
    
    conn_info = get_vertica_conn()

    @task(task_id='dialogs_load')
    def dialogs_load() -> None:
        insert_stg(stg_dialogs, conn_info)
    
    dialogs_task = dialogs_load()

    @task(task_id='groups_load')
    def groups_load() -> None:
        insert_stg(stg_groups, conn_info)
    
    groups_task = groups_load()

    @task(task_id='users_load')
    def users_load() -> None:
        insert_stg(stg_users, conn_info)
    
    users_task = users_load()

    @task(task_id='group_log_load')
    def group_log_load() -> None:
        insert_stg(stg_group_log, conn_info)
    
    group_log_task = group_log_load()


    tasks = [dialogs_task, groups_task, users_task, group_log_task] 
    return tasks

dag_init = load_stg()
