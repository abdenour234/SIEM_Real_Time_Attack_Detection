from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.avro.functions import from_avro
import requests
import json

SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
TOPIC = "data.stream.normalized"

def get_avro_schema(subject):
    resp = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest")
    resp.raise_for_status()
    schema = resp.json()['schema']
    return schema

schema_str = get_avro_schema("event_normalized-value")

spark = SparkSession.builder \
    .appName("SparkKafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

df_parsed = df.select(
    from_avro(col("value"), schema_str).alias("data")
).select("data.*")


# === WRITE TO A SINGLE CSV FILE ===
def write_to_single_csv(batch_df, batch_id):
    output_path = "/app/events_output/output.csv"
    batch_df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("/app/events_output/temp_csv")

    # Spark writes a folder with part-XXXXX.csv → we rename it to output.csv
    import os
    folder = "/app/events_output/temp_csv"

    for file in os.listdir(folder):
        if file.startswith("part-") and file.endswith(".csv"):
            os.rename(os.path.join(folder, file), output_path)

    # clean temp dir
    for f in os.listdir(folder):
        os.remove(os.path.join(folder, f))


query = df_parsed.writeStream \
    .foreachBatch(write_to_single_csv) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark-checkpoints") \
    .start()

query.awaitTermination()
