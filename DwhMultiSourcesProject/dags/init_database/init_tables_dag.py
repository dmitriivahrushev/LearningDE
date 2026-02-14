from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pendulum


"""
DAG инциализирующий:
    init_stg
    init_dds 
    init_cdm
"""
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2022, 5, 5, tz="UTC")
}

with DAG(
    dag_id='init_tables',
    default_args=default_args,
    schedule_interval=None,
    tags=['init', 'ddl'],
    catchup=False
) as dag:
    
    init_stg = PostgresOperator(
        task_id='init_stg',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql='/stg_init.sql'
    )
    
    init_dds = PostgresOperator(
        task_id='init_dds',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql='/dds_init.sql'
    )

    init_cdm = PostgresOperator(
        task_id='init_cdm',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql='/cdm_init.sql'
    )
    
    init_stg >> init_dds >> init_cdm
