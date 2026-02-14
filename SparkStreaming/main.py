import os
import time
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct, unix_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("restaurant_subscribe")


TOPIC_NAME = os.getenv("TOPIC_NAME", "student.persist_topic.cohort7.Dmitriidm")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091")
KAFKA_USER = os.getenv("KAFKA_USER", "de-student")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "ltcneltyn")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "student.topic.cohort7.Dmitriidm")
PG_SUBSCRIBERS_URL = os.getenv("PG_SUBSCRIBERS_URL", "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de")
PG_SUBSCRIBERS_USER = os.getenv("PG_SUBSCRIBERS_USER", "student")
PG_SUBSCRIBERS_PASS = os.getenv("PG_SUBSCRIBERS_PASS", "de-student")
PG_LOCAL_URL = os.getenv("PG_LOCAL_URL", "jdbc:postgresql://localhost:5432/de")
PG_LOCAL_USER = os.getenv("PG_LOCAL_USER", "jovyan")
PG_LOCAL_PASS = os.getenv("PG_LOCAL_PASS", "jovyan")
SPARK_JARS_PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
    "org.postgresql:postgresql:42.4.0",
])
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/tmp/checkpoints/restaurant_subscribe")


incoming_schema = StructType([
    StructField("restaurant_id", StringType(), True),
    StructField("adv_campaign_id", StringType(), True),
    StructField("adv_campaign_content", StringType(), True),
    StructField("adv_campaign_owner", StringType(), True),
    StructField("adv_campaign_owner_contact", StringType(), True),
    StructField("adv_campaign_datetime_start", LongType(), True),
    StructField("adv_campaign_datetime_end", LongType(), True),
    StructField("datetime_created", LongType(), True),
])


spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", SPARK_JARS_PACKAGES) \
    .getOrCreate()


def foreach_batch_function(df, epoch_id):
    try:
        if df is None or df.rdd.isEmpty():
            logger.info(f"Батч {epoch_id}: пустой, пропускаем обработку")
            return

        num_rows = df.count()
        logger.info(f"Батч {epoch_id}: количество строк для обработки {num_rows}")

        df.persist()
        trigger_ts = int(time.time())

        df_pg = df.withColumn("trigger_datetime_created", lit(trigger_ts).cast(IntegerType())) \
                  .withColumn("feedback", lit(None).cast(StringType()))

        needed_cols = [
            "restaurant_id", "adv_campaign_id", "adv_campaign_content", "adv_campaign_owner",
            "adv_campaign_owner_contact", "adv_campaign_datetime_start", "adv_campaign_datetime_end",
            "datetime_created", "client_id", "trigger_datetime_created", "feedback"
        ]
        df_pg_final = df_pg.select(*[c for c in needed_cols if c in df_pg.columns])

        try:
            df_pg_final.write \
                .format("jdbc") \
                .option("url", PG_LOCAL_URL) \
                .option("driver", "org.postgresql.Driver") \
                .option("dbtable", "public.subscribers_feedback") \
                .option("user", PG_LOCAL_USER) \
                .option("password", PG_LOCAL_PASS) \
                .mode("append") \
                .save()
            logger.info(f"Батч {epoch_id}: записано в Postgres (subscribers_feedback), строк: {num_rows}")
        except Exception as e:
            logger.exception(f"Батч {epoch_id}: ошибка записи в Postgres: {e}")


        kafka_cols = [c for c in df_pg.columns if c != "feedback"]
        kafka_df = df_pg.select(*kafka_cols)
        kafka_out_df = kafka_df.withColumn("value", to_json(struct(*[col(c) for c in kafka_df.columns])))
        kafka_out_df = kafka_out_df.withColumn("key",
                                               col("client_id").cast(StringType()) if "client_id" in kafka_df.columns
                                               else col("restaurant_id").cast(StringType()))

        try:
            kafka_out_df.select("key", "value") \
                .write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
                .option("kafka.security.protocol", "SASL_SSL") \
                .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
                .option("kafka.sasl.jaas.config",
                        f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{KAFKA_USER}" password="{KAFKA_PASSWORD}";') \
                .option("topic", TOPIC_NAME) \
                .save()
            logger.info(f"Батч {epoch_id}: отправлено в Kafka topic '{TOPIC_NAME}', строк: {num_rows}")
        except Exception as e:
            logger.exception(f"Батч {epoch_id}: ошибка записи в Kafka: {e}")

    finally:
        try:
            df.unpersist()
        except Exception:
            pass


logger.info("Запуск чтения из Kafka...")
kafka_read = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_INPUT_TOPIC) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{KAFKA_USER}" password="{KAFKA_PASSWORD}";') \
    .load() \
    .selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")

parsed = kafka_read.withColumn("parsed", from_json(col("value"), incoming_schema)).select(col("key"), col("parsed.*"))

current_ts_col = unix_timestamp(current_timestamp()).cast(LongType())
filtered = parsed.filter(
    (col("adv_campaign_datetime_start") <= current_ts_col) &
    (current_ts_col <= col("adv_campaign_datetime_end"))
)


logger.info("Загрузка таблицы subscribers_restaurants из Postgres...")
subscribers = spark.read \
    .format("jdbc") \
    .option("url", PG_SUBSCRIBERS_URL) \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "public.subscribers_restaurants") \
    .option("user", PG_SUBSCRIBERS_USER) \
    .option("password", PG_SUBSCRIBERS_PASS) \
    .load()
logger.info(f"Колонки таблицы subscribers: {subscribers.columns}")


joined = filtered.alias("k").join(
    subscribers.alias("s"),
    col("k.restaurant_id") == col("s.restaurant_id"),
    how="inner"
).select(
    col("k.key").alias("key"),
    col("k.restaurant_id"),
    col("k.adv_campaign_id"),
    col("k.adv_campaign_content"),
    col("k.adv_campaign_owner"),
    col("k.adv_campaign_owner_contact"),
    col("k.adv_campaign_datetime_start"),
    col("k.adv_campaign_datetime_end"),
    col("k.datetime_created"),
    col("s.client_id")
)

joined_with_event_ts = joined.withColumn("datetime_created_event", unix_timestamp(current_timestamp()).cast(LongType()))

result_df = joined_with_event_ts.persist()

logger.info("Запуск стриминга с foreachBatch...")
query = result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .start()

query.awaitTermination()
