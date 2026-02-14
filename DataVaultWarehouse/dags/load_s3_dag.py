"""
DAG для загрузки исходных данных из S3 хранилища.

DAG отвечает за загрузку исходных CSV файлов из S3 хранилища
Загружает четыре основных типа данных: пользователи, группы, диалоги и логи групп.
"""
import pendulum
from airflow.decorators import dag, task
from lib.save_s3_data import S3store
from lib.check_s3_data import check_files


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['s3', 'data_loading_local'],
    description='Загрузка исходных данных из S3 хранилища в локальную файловую систему'
)
def load_s3():
    """  
    Загружает набор данных из S3 хранилища в локальную директорию /data/:

    Tasks:
        groups_load: Загрузка данных о группах из S3
        users_load: Загрузка данных о пользователях из S3
        dialogs_load: Загрузка данных о диалогах из S3
        group_log_load: Загрузка логов групп из S3
        check_data: Проверка целостности загруженных данных
    """
    groups_key = 'groups.csv'
    groups_path = '/data/groups.csv'

    users_key = 'users.csv'
    users_path = '/data/users.csv'

    dialogs_key = 'dialogs.csv'
    dialogs_path = '/data/dialogs.csv'

    group_log_key = 'group_log.csv'
    group_log_path = '/data/group_log.csv'

    @task(task_id='groups_load')
    def groups_load() -> None:
        groups = S3store(
            key=groups_key,
            file_path=groups_path
        )
        groups.save_csv()
    
    groups_task = groups_load()

    @task(task_id='users_load')
    def users_load() -> None:
        users = S3store(
            key=users_key,
            file_path=users_path
        )
        users.save_csv()
  
    users_task = users_load()

    @task(task_id='dialogs_load')
    def dialogs_load() -> None:
        dialogs = S3store(
            key=dialogs_key,
            file_path=dialogs_path
        )
        dialogs.save_csv()

    dialogs_task = dialogs_load()

    @task(task_id='group_log_load')
    def group_log_load() -> None:
        dialogs_log = S3store(
            key=group_log_key,
            file_path=group_log_path
        )
        dialogs_log.save_csv()

    group_log_task = group_log_load()

    @task(task_id='check_data')
    def check_data() -> None:
        files = ['dialogs.csv', 'groups.csv', 'users.csv', 'group_log.csv']
        check_files(files)

    check_task = check_data()


    [groups_task, users_task, dialogs_task, group_log_task] >> check_task
    return check_task

dag_init = load_s3()
