from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os


os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 pyspark-shell"
)

MY_NAME = "fp_matvieienko"
OUTPUT_TOPIC = f"{MY_NAME}_enriched_athlete_avg"

kafka_config = {
    "bootstrap_servers": "77.81.230.104:9092",
    "username": "admin",
    "password": "VawEzo1ikLtrA8Ug8THa",
}
kafka_jaas_config = (
    f'org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";'
)

spark = SparkSession.builder.appName("VerifyOutput").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Схема вихідних даних (має співпадати з агрегацією)
schema = StructType([
    StructField("sport", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("country_noc", StringType(), True),
    StructField("avg_height", DoubleType(), True),
    StructField("avg_weight", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
])

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
    .option("kafka.security.protocol", "SASL_PLAINTEXT")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", kafka_jaas_config)
    .option("subscribe", OUTPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

print(f"Reading from topic: {OUTPUT_TOPIC}")
df.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .outputMode("append") \
    .start() \
    .awaitTermination()