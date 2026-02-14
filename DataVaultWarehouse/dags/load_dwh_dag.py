"""
DAG для загрузки данных в Vertica слой - STV2025061611__DWH.
Выполняет последовательную загрузку данных.
Хабы → Линки → Сателлиты → Представления.
"""
from airflow.decorators import dag, task
import pendulum
from lib.vertica_conn import get_vertica_conn
from lib.exec_sql import SqlQuery


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['vertica', 'insert_STV2025061611__DWH'],
    description='Загрузка данных в слой STV2025061611__DWH'
)
def insert_dwh_dag():
    conn_info = get_vertica_conn()

    @task(task_id='insert_hubs')
    def insert_hubs() -> None:
        """
        Загружает данные в хабы.
        - h_users
        - h_groups
        - h_dialogs
        """
        path_file = '/lessons/dags/sql/insert_hubs.sql'
        hubs_obj = SqlQuery(path_file, conn_info)
        hubs_obj.init_sql()

    @task(task_id='insert_links')
    def insert_links() -> None:
        """
        Загружает данные в линки.
        - l_admins
        - l_groups_dialogs
        - l_user_message
        - l_user_group_activity
        """
        path_file = '/lessons/dags/sql/insert_links.sql'
        link_obj = SqlQuery(path_file, conn_info)
        link_obj.init_sql()

    @task(task_id='insert_sattelite')
    def insert_sattelite() -> None:
        """
        Загружает данные в сателлиты.
        - s_admins
        - s_group_name
        - s_group_private_status
        - s_dialog_info
        - s_user_socdem
        - s_user_chatinfo
        - s_auth_history
        """
        path_file = '/lessons/dags/sql/insert_sattelite.sql'
        sattelite_obj = SqlQuery(path_file, conn_info)
        sattelite_obj.init_sql()

    @task(task_id='create_view_group_conversion')
    def create_view() -> None:
        """       
        Создает представление v_group_conversion_top10, которое показывает:
        - Топ-10 самых старых групп
        - Количество добавленных пользователей
        - Количество активных пользователей
        - Процент конверсии пользователей
        """
        path_file = '/lessons/dags/sql/group_conversion_top10.sql'
        view_obj = SqlQuery(path_file, conn_info)
        view_obj.init_sql()


    init_insert_hubs = insert_hubs()    
    init_insert_links = insert_links()
    init_insert_sattelite = insert_sattelite()
    init_view_group_conversion = create_view()


    init_insert_hubs >> init_insert_links >> init_insert_sattelite >> init_view_group_conversion
    return init_view_group_conversion

init_insert_dwh_dag = insert_dwh_dag()
