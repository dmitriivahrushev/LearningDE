"""
Job для построения витрины активности по зонам (месяц/неделя).
Витрина содержит:
- month: месяц расчёта.
- week: неделя расчёта.
- zone_id: идентификатор зоны (города).
- week_message: количество сообщений за неделю.
- week_reaction: количество реакций за неделю.
- week_subscription: количество подписок за неделю.
- week_user: количество регистраций за неделю.
- month_message: количество сообщений за месяц.
- month_reaction: количество реакций за месяц.
- month_subscription: количество подписок за месяц.
- month_user: количество регистраций за месяц.
"""
import sys
sys.path.insert(0, "/lessons/dags")

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from social.lib.spark_client import SparkClient


def save_usr_geo_core(geo_cities_path: str, 
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
    spark = SparkClient(app_name="save_usr_geo_core", master=master)
    geo_cities_df = spark.read_parquet(geo_cities_path)
    activity_df = spark.read_parquet(activity_path)
    activity_df_extr_col = activity_df.withColumn("message_from", F.col("event.message_from"))
    zone_df = geo_cities_df.join(activity_df_extr_col, on="message_from", how="left").select("*")

    df = zone_df.withColumn("datetime", F.to_timestamp(F.col("event.datetime"), "yyyy-MM-dd HH:mm:ss"))

    df = df.withColumn("month", F.month("datetime")) \
           .withColumn("week", F.weekofyear("datetime"))

    monthly_agg = df.groupBy("month", "city_id", "event_type") \
                    .agg(F.count("*").alias("count")) \
                    .groupBy("month", "city_id") \
                    .pivot("event_type", ["message", "reaction", "subscription", "user"]) \
                    .sum("count") \
                    .fillna(0) \
                    .select(
                        F.col("month"),
                        F.col("city_id").alias("zone_id"),
                        F.coalesce(F.col("message"), F.lit(0)).alias("month_message"),
                        F.coalesce(F.col("reaction"), F.lit(0)).alias("month_reaction"),
                        F.coalesce(F.col("subscription"), F.lit(0)).alias("month_subscription"),
                        F.coalesce(F.col("user"), F.lit(0)).alias("month_user")
                    )

    weekly_agg = df.groupBy("week", "city_id", "event_type") \
                   .agg(F.count("*").alias("count")) \
                   .groupBy("week", "city_id") \
                   .pivot("event_type", ["message", "reaction", "subscription", "user"]) \
                   .sum("count") \
                   .fillna(0) \
                   .select(
                       F.col("week"),
                       F.col("city_id").alias("zone_id"),
                       F.coalesce(F.col("message"), F.lit(0)).alias("week_message"),
                       F.coalesce(F.col("reaction"), F.lit(0)).alias("week_reaction"),
                       F.coalesce(F.col("subscription"), F.lit(0)).alias("week_subscription"),
                       F.coalesce(F.col("user"), F.lit(0)).alias("week_user")
                   )

    mart = monthly_agg.join(weekly_agg, on="zone_id", how="outer") \
                      .select(
                          F.coalesce(F.col("month"), F.lit(0)).alias("month"),
                          F.coalesce(F.col("week"), F.lit(0)).alias("week"),
                          F.col("zone_id"),
                          F.coalesce(F.col("week_message"), F.lit(0)).alias("week_message"),
                          F.coalesce(F.col("week_reaction"), F.lit(0)).alias("week_reaction"),
                          F.coalesce(F.col("week_subscription"), F.lit(0)).alias("week_subscription"),
                          F.coalesce(F.col("week_user"), F.lit(0)).alias("week_user"),
                          F.coalesce(F.col("month_message"), F.lit(0)).alias("month_message"),
                          F.coalesce(F.col("month_reaction"), F.lit(0)).alias("month_reaction"),
                          F.coalesce(F.col("month_subscription"), F.lit(0)).alias("month_subscription"),
                          F.coalesce(F.col("month_user"), F.lit(0)).alias("month_user")
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
    save_usr_geo_core(geo_cities_path, activity_path, save_path, master, load_date)
