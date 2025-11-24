import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, count, avg, max, current_timestamp,
    to_timestamp, udf
)
from pyspark.sql.types import BinaryType
from pyspark.sql.avro.functions import from_avro

# ===================== CONFIG ======================
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\clave_gcp\service_key.json"

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "datos_streaming_big_data"
AVRO_SCHEMA_FILE = "user_event.avsc"

PROJECT = "bigdatastreaming-479202"
DATASET = "streaming"
TABLE_RAW = f"{PROJECT}.{DATASET}.eventos_streaming_raw_avro"
TABLE_AGG = f"{PROJECT}.{DATASET}.eventos_streaming_agg_avro"

# Modo direct (BigQuery Storage Write API)
USE_STORAGE_WRITE_DIRECT = True  # Dejamos True para evitar el error del esquema gs
# Si algún día quieres volver al bucket temporal, define:
# TEMP_GCS_BUCKET = "spark-bigquery-staging-bucket"
# y pon USE_STORAGE_WRITE_DIRECT = False

CHECKPOINT_ROOT = "checkpoints_streaming_avro"
WINDOW_LENGTH = "5 minutes"
WINDOW_SLIDE = "5 minutes"
WATERMARK = "10 minutes"
TRIGGER_INTERVAL = "30 seconds"

spark = (
    SparkSession.builder
    .appName("KafkaStreamingToBigQueryAvro")
    .config(
        "spark.jars.packages",
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.40.0,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.apache.spark:spark-avro_2.12:3.5.0"
    )
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Leer schema Avro
with open(AVRO_SCHEMA_FILE, "r", encoding="utf-8") as f:
    avro_schema_str = f.read()

# Fuente Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# Quitar header Confluent (1 byte magic + 4 bytes schema id)
def strip_header(b: bytes) -> bytes:
    if b and len(b) > 5:
        return b[5:]
    return b

strip_udf = udf(strip_header, BinaryType())
payload_df = kafka_df.select(strip_udf(col("value")).alias("payload"))

# Deserializar Avro
events_df = payload_df.select(
    from_avro(col("payload"), avro_schema_str).alias("event")
).select("event.*")

# Limpieza y timestamps
clean_df = (
    events_df
    .filter(col("event_id").isNotNull())
    .filter(col("timestamp").isNotNull())
    .withColumn("event_time", to_timestamp(col("timestamp")))
    .withColumn("event_generated_at_ts", to_timestamp(col("event_generated_at")))
    .withColumn("processing_timestamp", current_timestamp())
)

# Agregación por ventana
agg_df = (
    clean_df
    .withWatermark("event_time", WATERMARK)
    .groupBy(window(col("event_time"), WINDOW_LENGTH, WINDOW_SLIDE), col("event_type"))
    .agg(
        count("*").alias("event_count"),
        avg("duration_seconds").alias("avg_duration"),
        max("duration_seconds").alias("max_duration")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("event_type"),
        col("event_count"),
        col("avg_duration"),
        col("max_duration"),
        current_timestamp().alias("processing_timestamp")
    )
)

def _write_to_bigquery(batch_df, table_name):
    writer = (
        batch_df.write
        .format("bigquery")
        .option("table", table_name)
        .mode("append")
    )
    if USE_STORAGE_WRITE_DIRECT:
        writer = writer.option("writeMethod", "direct")
    else:
        # writer = writer.option("temporaryGcsBucket", TEMP_GCS_BUCKET)
        raise ValueError("TEMP_GCS_BUCKET no definido. Activa direct o define el bucket.")
    writer.save()

def write_batch_raw(batch_df, batch_id):
    try:
        rows = batch_df.count()
        if rows > 0:
            print(f"[RAW] batch_id={batch_id} filas={rows}")
            latency_sample = (
                batch_df
                .select(
                    (col("processing_timestamp").cast("long") - col("event_time").cast("long"))
                    .alias("latency_seconds")
                )
                .where(col("event_time").isNotNull())
                .limit(5)
            )
            latency_sample.show(truncate=False)
        _write_to_bigquery(batch_df, TABLE_RAW)
    except Exception as e:
        print(f"[ERROR RAW] batch_id={batch_id} {e}")

def write_batch_agg(batch_df, batch_id):
    try:
        rows = batch_df.count()
        if rows > 0:
            print(f"[AGG] batch_id={batch_id} filas={rows}")
        _write_to_bigquery(batch_df, TABLE_AGG)
    except Exception as e:
        print(f"[ERROR AGG] batch_id={batch_id} {e}")

raw_query = (
    clean_df.writeStream
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/raw")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .foreachBatch(write_batch_raw)
    .start()
)

agg_query = (
    agg_df.writeStream
    .outputMode("update")
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/agg")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .foreachBatch(write_batch_agg)
    .start()
)

print("Streaming Avro iniciado. Ctrl+C para detener.")
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("Interrupción recibida. Deteniendo consultas...")
    raw_query.stop()
    agg_query.stop()
    spark.stop()
    print("Finalizado correctamente.")