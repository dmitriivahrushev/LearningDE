from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pendulum


"""
DAG наполняющий:
    dds_dm_users 
    dds_dm_restaurants 
    dds_dm_timestamps
    dds_dm_products 
    dds_dm_orders 
    fct_product_sales
"""
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2022, 5, 5, tz="UTC")
}

with DAG(
    dag_id='dds_sales_load',
    default_args=default_args,
    schedule_interval='0/15 * * * *',
    tags=['dds', 'sales'],
    catchup=False,
    is_paused_upon_creation=True
) as dag:

    load_dds_dm_users = PostgresOperator(
        task_id='load_dds_dm_users',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',  
        sql='/SQLScripts/DML_dds_dm_users.sql'
    )
    
    load_dds_dm_restaurants = PostgresOperator(
        task_id='load_dds_dm_restaurants',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql='/SQLScripts/DML_dds_dm_restaurants.sql'
    )

    load_dds_dm_timestamps = PostgresOperator(
        task_id='load_dds_dm_timestamps',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql='/SQLScripts/DML_dds_dm_timestamps.sql'
    )
    
    load_dds_dm_products = PostgresOperator(
        task_id='load_dds_dm_products',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql='/SQLScripts/DML_dds_dm_products.sql'
    )
    
    load_dds_dm_orders = PostgresOperator(
        task_id='load_dds_dm_orders',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql='/SQLScripts/DML_dds_dm_orders.sql'
    )
    
    load_fct_product_sales = PostgresOperator(
        task_id='load_fct_product_sales',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql='/SQLScripts/DML_fct_product_sales.sql'
    )

    (
    [load_dds_dm_users, load_dds_dm_restaurants, load_dds_dm_timestamps]
    >> load_dds_dm_products >> load_dds_dm_orders >> load_fct_product_sales
    )
