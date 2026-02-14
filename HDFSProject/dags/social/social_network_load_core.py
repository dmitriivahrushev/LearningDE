"""
DAG для построения аналитических витрин социальной сети.
Tasks:
- load_cities_events: загружает и обогащает гео-события городами.
- load_users_geo_mart: строит витрину пользовательской гео-активности.
- load_zone_geo_mart: строит витрину агрегированной активности по зонам.
- load_friends_geo_mart: строит витрину для рекомендации друзей.
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from social.config.load_cofig import Config


with DAG(
    dag_id="social_network_pipeline",
    schedule_interval=None,
    start_date=datetime(2022, 5, 5),
    catchup=False,
    tags=["CoreSocialNetwork"],
) as dag:
    configs = Config.unpack_config()
    load_cities_events = SparkSubmitOperator(
        task_id="load_cities_events",
        application="/lessons/dags/social/scripts/users_geo_core.py",
        name="load_cities_events_job",
        verbose=True,
        application_args=[
            f"/user/master/data/geo/events/date={configs.load_date}",
            "/user/dm1trydm/storage/raw_data/geo_cities/geo.csv",
            "/user/dm1trydm/storage/core/cities_events",
            configs.master,
            configs.load_date
        ]
    )

    load_users_geo_mart = SparkSubmitOperator(
        task_id="load_users_geo_mart",
        application="/lessons/dags/social/scripts/users_geo_mart.py",
        name="load_users_geo_mart_job",
        verbose=True,
        application_args=[
            f"hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net/user/dm1trydm/storage/core/cities_events/date={configs.load_date}",
            f"hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net/user/dm1trydm/storage/mart/f_user_geo_activity",
            configs.master,
            configs.load_date
        ]
    )

    load_zone_geo_mart = SparkSubmitOperator(
        task_id="load_zone_geo_mart",
        application="/lessons/dags/social/scripts/zone_geo_mart.py",
        name="load_zone_geo_mart_job",
        verbose=True,
        application_args=[
            f"hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net/user/dm1trydm/storage/core/cities_events/date={configs.load_date}",
            f"hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net/user/dm1trydm/storage/raw_data/geo_events/date={configs.load_date}",
            "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net/user/dm1trydm/storage/mart/zone_geo_mart",
            configs.master,
            configs.load_date
        ]
    )

    load_friends_geo_mart = SparkSubmitOperator(
        task_id="load_friends_geo_mart",
        application="/lessons/dags/social/scripts/friends_geo_mart.py",
        name="load_friends_geo_mart_job",
        verbose=True,
        application_args=[
            f"hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net/user/dm1trydm/storage/core/cities_events/date={configs.load_date}",
            f"hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net/user/dm1trydm/storage/raw_data/geo_events/date={configs.load_date}",
            "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net/user/dm1trydm/storage/mart/friends_geo_mart",
            configs.master,
            configs.load_date
        ]                
    )

    load_cities_events >> [load_users_geo_mart, load_zone_geo_mart, load_friends_geo_mart]
