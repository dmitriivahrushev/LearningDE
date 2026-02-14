from datetime import datetime, timedelta
from airflow.decorators import dag,task

from transactional_activity.utils.load_stg_to_vertica import load_staging_transaction
from transactional_activity.utils.load_stg_to_vertica import load_staging_currencies
from transactional_activity.utils.load_global_metrics import load_global_metricts
from transactional_activity.configs.load_config import Config


@dag(
    schedule_interval="0 1 * * *",
    start_date=datetime(2022, 10, 1),
    end_date=datetime(2022, 11, 11),
    max_active_runs=1,
    max_active_tasks=1,
    catchup=True,
    tags=["load_staging_to_vertica", "aggregate_global_metrics"]
)

def aggregate_global_metrics():
    """
    DAG для загрузки и агрегации данных в витрину global_metrics.
    1. Загрузка транзакций за предыдущий день в стейджинг.
    2. Загрузка курсов валют за предыдущий день в стейджинг.
    3. Агрегация и загрузка данных в витрину global_metrics.
    Фильтр в запросах - контекст Airflow, содержащий дату выполнения (ds).
    Расписание: каждый день в 01:00.
    """
    configs = Config.unpack_config()

    @task(task_id="load_transactions")
    def load_transactions(**context):
        current_date = datetime.strptime(context["ds"], "%Y-%m-%d").date()
        logical_date = current_date - timedelta(days=1)
        load_staging_transaction(configs, logical_date)

    @task(task_id="load_currencies")
    def load_currencies(**context):
        current_date = datetime.strptime(context["ds"], "%Y-%m-%d").date()
        logical_date = current_date - timedelta(days=1)
        load_staging_currencies(configs, logical_date)

    @task(task_id="load_global_metrics")
    def upload_global_metrics(**context):
        current_date = datetime.strptime(context["ds"], "%Y-%m-%d").date()
        logical_date = current_date - timedelta(days=1)
        load_global_metricts(configs, logical_date)

    (
        [load_transactions(), load_currencies()]
        >> upload_global_metrics()
    )

init_dag = aggregate_global_metrics()
