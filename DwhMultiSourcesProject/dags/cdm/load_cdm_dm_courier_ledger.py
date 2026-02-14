from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pendulum


"""
DAG наполняющий cdm_dm_courier_ledger.
"""
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2022, 5, 5, tz="UTC")
}

with DAG(
    dag_id='cdm_dm_courier_ledger_load',
    default_args=default_args,
    schedule_interval='0/15 * * * *',
    tags=['cdm', 'load_cdm_dm_courier_ledger'],
    catchup=False,
    is_paused_upon_creation=True
) as dag:

    load_cdm_dm_courier_ledger = PostgresOperator(
        task_id='load_cdm_dm_courier_ledger',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',  
        sql='/SQLScripts/DML_cdm_dm_courier_ledger.sql'
    )

    load_cdm_dm_courier_ledger
  