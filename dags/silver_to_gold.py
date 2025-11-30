from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, col, regexp_replace
from pyspark.sql.types import DoubleType
import os


# Створення SparkSession
spark = (
    SparkSession.builder
    .appName("SilverToGold_fp_matvieienko")
    .getOrCreate()
)

print(f"Spark version: {spark.version}")

SILVER_BASE_PATH = "/tmp/silver"
GOLD_BASE_PATH = "/tmp/gold"
OUTPUT_DIR = os.path.join(GOLD_BASE_PATH, "avg_stats")


# Читання даних із silver-шару
athlete_bio_path = os.path.join(SILVER_BASE_PATH, "athlete_bio")
athlete_event_results_path = os.path.join(SILVER_BASE_PATH, "athlete_event_results")

print(f"Reading silver table from: {athlete_bio_path}")
athlete_bio_df = spark.read.parquet(athlete_bio_path)

print(f"Reading silver table from: {athlete_event_results_path}")
athlete_event_results_df = spark.read.parquet(athlete_event_results_path)

print(f"athlete_bio rows: {athlete_bio_df.count()}")
print(f"athlete_event_results rows: {athlete_event_results_df.count()}")


# Підготовка даних
# Уникаємо конфлікту country_noc
athlete_bio_df = athlete_bio_df.withColumnRenamed("country_noc", "bio_country_noc")

# Нормалізуємо та кастимо height / weight до Double
athlete_bio_df = (
    athlete_bio_df
    # коми -> крапки, потім cast до Double
    .withColumn("height", regexp_replace(col("height"), ",", ".").cast(DoubleType()))
    .withColumn("weight", regexp_replace(col("weight"), ",", ".").cast(DoubleType()))
)

# відкидаємо рядки, де height або weight не вдалося перетворити
athlete_bio_df = athlete_bio_df.filter(
    col("height").isNotNull() & col("weight").isNotNull()
)

print(f"athlete_bio rows after numeric cast & filter: {athlete_bio_df.count()}")



# JOIN за athlete_id
joined_df = athlete_event_results_df.join(athlete_bio_df, "athlete_id")

print(f"Joined DataFrame rows: {joined_df.count()}")


# Агрегація для gold-шару
# Для кожної комбінації sport, medal, sex, country_noc
# рахуємо середній зріст і вагу + додаємо timestamp
aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc").agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight")
).withColumn(
    "timestamp", current_timestamp()
)

print(f"Aggregated rows: {aggregated_df.count()}")


# Запис у gold/avg_stats у форматі Parquet
os.makedirs(OUTPUT_DIR, exist_ok=True)

(
    aggregated_df.write
    .mode("overwrite")
    .parquet(OUTPUT_DIR)
)

print(f"Gold table 'avg_stats' saved to {OUTPUT_DIR} in Parquet format.")


# Перевірка результату (df.show)
gold_df = spark.read.parquet(OUTPUT_DIR)
print("Sample data from gold/avg_stats:")
gold_df.show(truncate=False)


# Завершення роботи Spark
spark.stop()
print("Spark session stopped.")
