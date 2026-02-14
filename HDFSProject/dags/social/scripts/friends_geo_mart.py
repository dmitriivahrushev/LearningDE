"""
Job для построения витрины рекомендации друзей.

Витрина содержит:
- user_left: первый пользователь.
- user_right: второй пользователь.
- processed_dttm: дата расчёта.
- zone_id: идентификатор зоны.
- local_time: локальное время.
"""
import sys
from datetime import datetime
sys.path.insert(0, "/lessons/dags")

from pyspark.sql import functions as F
from pyspark.sql.functions import radians, sin, cos, atan2, sqrt, lit

from social.lib.spark_client import SparkClient


def save_friend_geo_mart(geo_cities_path: str, 
                        activity_path: str, 
                        save_path: str, 
                        master: str, 
                        load_date: str):
    """
    Args:
        geo_cities_path: путь к данным с гео-городами.
        activity_path: путь к данным активности.
        save_path: путь для сохранения витрины.
        master: режим выполнения Spark.
        load_date: дата загрузки (для партиционирования).
    """
    spark = SparkClient(app_name="save_friend_geo_mart", master=master)
    geo_cities_df = spark.read_parquet(geo_cities_path)
    activity_df = spark.read_parquet(activity_path)

    activity_df_extr_col = activity_df.withColumn("message_from", F.col("event.message_from"))
    zone_df = geo_cities_df.alias("geo").join(
        activity_df_extr_col.alias("act"),
        F.col("geo.message_from") == F.col("act.message_from"),
        how="left"
    ).select(
        F.col("geo.message_from"),
        F.col("geo.lat"),
        F.col("geo.lon"),
        F.col("geo.message_ts"),
        F.col("geo.city_id"),
        F.col("geo.city"),
        F.col("act.event"),
        F.col("act.event_type")
    )

    subscriptions = zone_df.filter(F.col("event_type") == "subscription")
    subscriptions = subscriptions.select(
        F.col("event.subscription_user").alias("user_id"),
        F.col("event.subscription_channel").alias("channel_id"),
        F.col("city_id").alias("zone_id"),
        F.col("lat"),
        F.col("lon"),
        F.col("event.datetime").alias("datetime")
    ).filter(F.col("user_id").isNotNull() & F.col("channel_id").isNotNull())

    user_pairs = subscriptions.alias("a").join(
        subscriptions.alias("b"),
        (F.col("a.channel_id") == F.col("b.channel_id")) &
        (F.col("a.user_id") < F.col("b.user_id"))
    ).select(
        F.col("a.user_id").alias("user_left"),
        F.col("b.user_id").alias("user_right"),
        F.col("a.zone_id"),
        F.col("a.lat").alias("lat_left"),
        F.col("a.lon").alias("lon_left"),
        F.col("b.lat").alias("lat_right"),
        F.col("b.lon").alias("lon_right"),
        F.col("a.datetime").alias("processed_dttm")
    )

    user_pairs = user_pairs.withColumn("delta_lat", radians(F.col("lat_right") - F.col("lat_left"))) \
        .withColumn("delta_lon", radians(F.col("lon_right") - F.col("lon_left"))) \
        .withColumn("a", sin(F.col("delta_lat") / 2) * sin(F.col("delta_lat") / 2) +
                          cos(radians(F.col("lat_left"))) * cos(radians(F.col("lat_right"))) *
                          sin(F.col("delta_lon") / 2) * sin(F.col("delta_lon") / 2)) \
        .withColumn("c", 2 * atan2(sqrt(F.col("a")), sqrt(1 - F.col("a")))) \
        .withColumn("distance_km", lit(6371) * F.col("c")) \
        .filter(F.col("distance_km") <= 1.0)

    mart = user_pairs.select(
        F.col("user_left"),
        F.col("user_right"),
        F.col("processed_dttm"),
        F.col("zone_id"),
        F.col("processed_dttm").alias("local_time")
    )

    mart = mart.withColumn("date", F.lit(load_date))
    mart.write.mode("overwrite").partitionBy("date").parquet(save_path)

if __name__ == "__main__":
    import sys
    geo_cities_path = sys.argv[1]
    activity_path = sys.argv[2]
    save_path = sys.argv[3]
    master = sys.argv[4]
    load_date = sys.argv[5]
    save_friend_geo_mart(geo_cities_path, activity_path, save_path, master, load_date)
