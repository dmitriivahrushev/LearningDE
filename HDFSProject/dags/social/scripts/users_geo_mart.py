"""
Job для построения витрины пользовательской гео-активности.
Витрина содержит:
- user_id: идентификатор пользователя.
- act_city: актуальный город.
- home_city: домашний город.
- travel_count: количество посещённых городов.
- travel_array: массив посещённых городов.
- local_time: время последнего действия.
"""
import sys

from pyspark.sql import functions as F, Window

sys.path.insert(0, "/lessons/dags")
from social.lib.spark_client import SparkClient


def save_users_geo_mart(core_events_path: str, 
                        save_path: str, 
                        master: str, 
                        load_date: str):
    """
    Функция для построения витрины пользовательской гео-активности.

    Args:
        core_events_path: путь к core-витрине событий.
        save_path: путь для сохранения витрины.
        master: режим выполнения Spark.
        load_date: дата загрузки (для партиционирования).
    """
    spark = SparkClient(app_name="save_users_geo_mart", master=master)
    events_df = spark.read_parquet(core_events_path).withColumn("message_ts", F.to_timestamp("message_ts"))

    w_last = Window.partitionBy("message_from").orderBy(F.col("message_ts").desc())
    last_city_df = (
        events_df.withColumn("rnk", F.row_number().over(w_last))
        .filter(F.col("rnk") == 1)
        .select(
            F.col("message_from").alias("user_id"),
            F.col("city").alias("act_city"),
            F.col("message_ts").alias("local_time")
        )
    )

    home_city_df = (
        events_df.groupBy("message_from", "city")
        .agg(F.count("*").alias("msg_count"))
    )
    w_home = Window.partitionBy("message_from").orderBy(F.col("msg_count").desc())
    home_city_df = (
        home_city_df.withColumn("rnk", F.row_number().over(w_home))
        .filter(F.col("rnk") == 1)
        .select(
            F.col("message_from").alias("user_id"),
            F.col("city").alias("home_city")
        )
    )

    w_travel = Window.partitionBy("message_from").orderBy("message_ts")
    travel_df = (
        events_df.withColumn("travel_array", F.collect_list("city").over(w_travel))
        .withColumn("travel_count", F.size("travel_array"))
        .select("message_from", "travel_array", "travel_count")
        .dropDuplicates(["message_from"])
    )

    users_geo_mart_df = (
        last_city_df
        .join(home_city_df, on="user_id", how="left")
        .join(travel_df, last_city_df.user_id == travel_df.message_from, "left")
        .select(
            "user_id",
            "act_city",
            "home_city",
            "travel_count",
            "travel_array",
            "local_time"
        )
    )

    users_geo_mart_df = users_geo_mart_df.withColumn("date", F.lit(load_date))
    users_geo_mart_df.write.mode("overwrite").partitionBy("date").parquet(save_path)

if __name__ == "__main__":
    import sys
    core_events_path = sys.argv[1]
    save_path = sys.argv[2]
    master = sys.argv[3]
    load_date = sys.argv[4]
    save_users_geo_mart(core_events_path, save_path, master, load_date)
