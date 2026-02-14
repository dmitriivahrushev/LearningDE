"""
Job для построения core-витрины событий с гео-информацией.
Сопоставляет события с ближайшими городами и сохраняет витрину с партиционированием по дате.
"""
import math
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

sys.path.insert(0, "/lessons/dags")
from social.lib.spark_client import SparkClient


def save_geo_events_core(events_path: str, 
                        cities_path: str,
                        save_path: str, 
                        master: str,
                        load_date: str):
    """
    Args:
        events_path: путь к сырым событиям (parquet).
        cities_path: путь к CSV-файлу с городами.
        save_path: путь для сохранения витрины.
        master: режим выполнения Spark.
        load_date: дата загрузки (для партиционирования).
    """
    client = SparkClient(app_name="save_geo_events_core", master=master)
    spark = client.get_session()
    geo_events_df = client.read_parquet(events_path).select(
        F.col("event.message_from"),
        F.col("lat"),
        F.col("lon"),
        F.col("event.message_ts")
    )

    geo_cities = client.read_csv(cities_path)
    cities_list = geo_cities.select("id", "city", "lat", "lng").collect()
    broadcast_cities = spark.sparkContext.broadcast(cities_list)

    def nearest_city(lat, lon):
        if lat is None or lon is None:
            return (None, None)

        R = 6371
        min_dist = float("inf")
        closest_city = None
        closest_id = None

        for row in broadcast_cities.value:
            city_lat = float(str(row['lat']).replace(",", "."))
            city_lon = float(str(row['lng']).replace(",", "."))

            phi1 = math.radians(lat)
            phi2 = math.radians(city_lat)
            dphi = math.radians(city_lat - lat)
            dlambda = math.radians(city_lon - lon)

            a = math.sin(dphi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2)**2
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
            distance = R * c

            if distance < min_dist:
                min_dist = distance
                closest_city = row['city']
                closest_id = row['id']

        return (closest_id, closest_city)

    city_struct = StructType([
        StructField("city_id", StringType(), True),
        StructField("city_name", StringType(), True)
    ])

    nearest_city_udf = F.udf(nearest_city, city_struct)

    geo_events_with_city = geo_events_df.withColumn(
        "nearest_city",
        nearest_city_udf(F.col("lat"), F.col("lon"))
    ).withColumn(
        "city_id", F.col("nearest_city.city_id")
    ).withColumn(
        "city", F.col("nearest_city.city_name")
    ).drop("nearest_city")

    geo_events_with_city = geo_events_with_city.withColumn("date", F.lit(load_date))
    geo_events_with_city.write.mode("overwrite").partitionBy("date").parquet(save_path)
    client.stop()

if __name__ == "__main__":
    import sys
    events_path = sys.argv[1]
    cities_path = sys.argv[2]
    save_path = sys.argv[3]
    master = sys.argv[4]
    load_date = sys.argv[5]
    save_geo_events_core(events_path, cities_path, save_path, master, load_date)
