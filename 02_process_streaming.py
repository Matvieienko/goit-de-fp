from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, avg, current_timestamp, 
    to_json, struct, from_json, expr
)
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
import os


# Налаштування пакети для Spark 4.0.1 / Scala 2.13
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 pyspark-shell"
)

MY_NAME = "fp_matvieienko"
INPUT_TOPIC = f"{MY_NAME}_athlete_event_results"
OUTPUT_TOPIC = f"{MY_NAME}_enriched_athlete_avg"

# Kafka credentials
kafka_config = {
    "bootstrap_servers": "77.81.230.104:9092",
    "username": "admin",
    "password": "VawEzo1ikLtrA8Ug8THa",
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
}
kafka_jaas_config = (
    f'org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";'
)

# JDBC credentials
jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"
jdbc_driver = "com.mysql.cj.jdbc.Driver"

jdbc_target_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_target_table = f"{MY_NAME}_enriched_athlete_avg"


# Ініціалізація Spark
spark = (
    SparkSession.builder
    .appName(f"FP_Streaming_{MY_NAME}")
    .config("spark.jars", "mysql-connector-j-8.0.32.jar")
    # ДОДАНО: щоб драйвер був видимий і для executor, і для driver
    .config("spark.driver.extraClassPath", "mysql-connector-j-8.0.32.jar")
    .config("spark.executor.extraClassPath", "mysql-connector-j-8.0.32.jar")
    .config("spark.sql.streaming.checkpointLocation", f"checkpoint_{MY_NAME}_v3")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# Читання біографічних даних (Static)
print(">>> Reading athlete_bio...")
athlete_bio_df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("driver", jdbc_driver)
    .option("dbtable", "athlete_bio")
    .option("user", jdbc_user)
    .option("password", jdbc_password)
    .option("partitionColumn", "athlete_id")
    .option("lowerBound", "1")
    .option("upperBound", "150000") 
    .option("numPartitions", "10")
    .load()
)

# Очищення даних
athlete_bio_clean = (
    athlete_bio_df
    # Прибираємо зайві символи
    .withColumn("height_temp", regexp_replace(col("height"), "[^0-9.]", ""))
    .withColumn("weight_temp", regexp_replace(col("weight"), "[^0-9.]", ""))
    # Кастимо через try_cast (поверне null замість помилки, якщо там пусто)
    .withColumn("height_clean", expr("try_cast(height_temp as double)"))
    .withColumn("weight_clean", expr("try_cast(weight_temp as double)"))
    # Фільтруємо null
    .filter(col("height_clean").isNotNull() & col("weight_clean").isNotNull())
    .select(
        col("athlete_id"), 
        col("sex"), 
        col("country_noc"), 
        col("height_clean").alias("height"), 
        col("weight_clean").alias("weight")
    )
)


# Відправка даних у Kafka (Batch -> Kafka)
print(">>> Reading athlete_event_results and writing to Kafka...")
jdbc_events_df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("driver", jdbc_driver)
    .option("dbtable", "athlete_event_results")
    .option("user", jdbc_user)
    .option("password", jdbc_password)
    .option("partitionColumn", "result_id")
    .option("lowerBound", "1")
    .option("upperBound", "300000")
    .option("numPartitions", "10")
    .load()
)

(
    jdbc_events_df
    .select("athlete_id", "sport", "medal")
    .selectExpr("CAST(athlete_id AS STRING) AS key", "to_json(struct(*)) AS value")
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option("kafka.sasl.jaas.config", kafka_jaas_config)
    .option("topic", INPUT_TOPIC)
    .save()
)
print(">>> Data written to Kafka successfully.")


# Читання з Kafka (Streaming)
event_schema = StructType([
    StructField("athlete_id", IntegerType(), True),
    StructField("sport", StringType(), True),
    StructField("medal", StringType(), True),
])

kafka_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option("kafka.sasl.jaas.config", kafka_jaas_config)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "1000") 
    .load()
)

events_df = (
    kafka_stream
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), event_schema).alias("data"))
    .select("data.*")
)


# Об'єднання та Агрегація
joined_df = events_df.join(athlete_bio_clean, "athlete_id")

aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc").agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight"),
    current_timestamp().alias("timestamp")
)

# Запис результатів (forEachBatch)
def foreach_batch_function(batch_df, batch_id):
    # Кешуємо батч, щоб не рахувати його двічі для кожного sink-а
    batch_df.persist()
    
    count = batch_df.count()
    print(f"Batch {batch_id} processing... Rows: {count}")
    
    if count > 0:
        # a) Запис в Kafka Output
        try:
            (
                batch_df.selectExpr("CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value")
                .write.format("kafka")
                .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
                .option("kafka.security.protocol", kafka_config["security_protocol"])
                .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
                .option("kafka.sasl.jaas.config", kafka_jaas_config)
                .option("topic", OUTPUT_TOPIC)
                .save()
            )
        except Exception as e:
            print(f"Error writing to Kafka: {e}")

        # b) Запис в MySQL
        try:
            (
                batch_df.write.format("jdbc")
                .option("url", jdbc_target_url)
                .option("driver", jdbc_driver)
                .option("dbtable", jdbc_target_table)
                .option("user", jdbc_user)
                .option("password", jdbc_password)
                .mode("append")
                .save()
            )
        except Exception as e:
            print(f"Error writing to MySQL: {e}")
            
    batch_df.unpersist()

print(">>> Starting Stream...")
(
    aggregated_df.writeStream
    .outputMode("complete")
    .foreachBatch(foreach_batch_function)
    .option("checkpointLocation", f"checkpoint_{MY_NAME}_v3")
    .start()
    .awaitTermination()
)