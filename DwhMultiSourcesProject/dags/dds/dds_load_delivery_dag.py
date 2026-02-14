from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pendulum


"""
DAG наполняющий:
     dds_dm_couriers
     dds_dm_delivery
"""
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2022, 5, 5, tz="UTC")
}

with DAG(
    dag_id='dds_delivery_load',
    default_args=default_args,
    schedule_interval='0/15 * * * *',
    tags=['dds', 'delivery'],
    catchup=False,
    is_paused_upon_creation=True
) as dag:

    load_dds_couriers = PostgresOperator(
        task_id='load_dds_couriers',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql='/SQLScripts/DML_dds_dm_couriers.sql'
    )
    
    load_dds_delivery = PostgresOperator(
        task_id='load_dds_delivery',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql='/SQLScripts/DML_dds_dm_delivery.sql'
    )

    load_dds_couriers >> load_dds_delivery
    