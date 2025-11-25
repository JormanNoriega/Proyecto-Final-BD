import os
import signal
import sys
import json
import io
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, count, avg, max, current_timestamp,
    to_timestamp, udf
)
from pyspark.sql.types import (
    BinaryType, StructType, StructField, StringType, 
    LongType, IntegerType
)
from avro.io import BinaryDecoder, DatumReader
import avro.schema

# ===================== CONFIG ======================
# Credenciales GCP (asegúrate que el JSON existe y tiene permisos suficientes)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\clave_gcp\service_key.json"

# Kafka
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "datos_streaming_big_data"

# Avro
AVRO_SCHEMA_FILE = "user_event.avsc"

# BigQuery destino
BQ_PROJECT = "bigdatastreaming-479202"
BQ_DATASET = "streaming"
BQ_TABLE_RAW = f"{BQ_PROJECT}.{BQ_DATASET}.eventos_streaming_raw_avro"
BQ_TABLE_AGG = f"{BQ_PROJECT}.{BQ_DATASET}.eventos_streaming_agg_avro"

# Streaming
CHECKPOINT_ROOT = "checkpoints_streaming_avro"
WATERMARK = "10 minutes"
WINDOW_LENGTH = "5 minutes"
WINDOW_SLIDE = "5 minutes"
TRIGGER_INTERVAL = "30 seconds"

# ===================== SPARK SESSION ======================
spark = (
    SparkSession.builder
    .appName("KafkaStreamingToBigQueryAvro")
    .config(
        "spark.jars.packages",
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.40.0,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
    )
    # Opcional: si instalaste Python 3.11, fuerza su uso para los workers
    .config("spark.executorEnv.PYSPARK_PYTHON", "C:/Python311/python.exe")
    .config("spark.python.worker.reuse", "true")
    # Escritura directa a BigQuery (Storage Write API)
    .config("spark.bigquery.writeMethod", "direct")
    # Rendimiento y cierre ordenado
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    # Opcional: directorio temporal dedicado para Spark en Windows
    .config("spark.local.dir", "C:/spark_local_tmp")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")  # Solo mostrar errores críticos

# ===================== LEER SCHEMA AVRO ======================
with open(AVRO_SCHEMA_FILE, "r", encoding="utf-8") as f:
    avro_schema_str = f.read()

# ===================== LEER KAFKA (binary) ======================
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")  # Leer desde el principio del topic
    .option("failOnDataLoss", "false")  # Continuar aunque falten offsets antiguos
    .option("maxOffsetsPerTrigger", "50000")  # Limitar carga por batch para evitar sobrecarga
    .load()
)

# ===================== SCHEMA SPARK PARA EVENTOS ======================
event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", LongType(), True),
    StructField("session_id", LongType(), True),
    StructField("event_type", StringType(), True),
    StructField("item_id", LongType(), True),
    StructField("timestamp", StringType(), True),
    StructField("duration_seconds", IntegerType(), True),
    StructField("device", StringType(), True),
    StructField("country", StringType(), True),
    StructField("event_generated_at", StringType(), True)
])

# ===================== UDF PARA DESERIALIZAR AVRO CON PYTHON ======================
# Cargar schema de Avro
avro_schema_obj = avro.schema.parse(avro_schema_str)

def deserialize_avro(binary_value):
    """Deserializa Avro desde bytes binarios de Confluent"""
    if not binary_value or len(binary_value) <= 5:
        return None
    
    try:
        # Quitar header de Confluent (5 bytes: magic byte + schema id)
        avro_payload = binary_value[5:]
        
        # Deserializar con avro-python3
        bytes_reader = io.BytesIO(avro_payload)
        decoder = BinaryDecoder(bytes_reader)
        reader = DatumReader(avro_schema_obj)
        event = reader.read(decoder)
        
        # Retornar como tupla en el orden del schema
        return (
            event.get('event_id'),
            event.get('user_id'),
            event.get('session_id'),
            event.get('event_type'),
            event.get('item_id'),
            event.get('timestamp'),
            event.get('duration_seconds'),
            event.get('device'),
            event.get('country'),
            event.get('event_generated_at')
        )
    except Exception as e:
        # En caso de error, retornar None
        return None

# Registrar UDF
deserialize_udf = udf(deserialize_avro, event_schema)

# ===================== DESERIALIZAR AVRO ======================
events_df = (
    kafka_df
    .select(deserialize_udf(col("value")).alias("event"))
    .select("event.*")
    .filter(col("event_id").isNotNull())  # Filtrar errores de deserialización
)

# ===================== LIMPIEZA Y CAMPOS DE TIEMPO ======================
clean_df = (
    events_df
    .filter(col("timestamp").isNotNull())
    .withColumn("event_time", to_timestamp(col("timestamp")))
    .withColumn("event_generated_at_ts", to_timestamp(col("event_generated_at")))
    .withColumn("processing_timestamp", current_timestamp())
)

# ===================== AGREGACIÓN POR VENTANA ======================
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

# ===================== ESCRITURA A BIGQUERY ======================
def _write_to_bigquery(batch_df, table_name):
    (
        batch_df.write
        .format("bigquery")
        .option("table", table_name)
        .option("writeMethod", "direct")  # Forzar Storage Write API a nivel de writer
        .mode("append")
        .save()
    )

def write_batch_raw(batch_df, batch_id):
    try:
        rows = batch_df.count()
        print(f"[RAW] Micro-lote {batch_id} - Procesando {rows} filas")
        
        if rows > 0:
            # Muestra algunos registros de ejemplo
            print(f"[RAW] Muestra de datos:")
            batch_df.select("event_id", "event_type", "user_id", "timestamp").show(5, truncate=False)
            
            # Escribir a BigQuery
            print(f"[RAW] Escribiendo a BigQuery: {BQ_TABLE_RAW}")
            _write_to_bigquery(batch_df, BQ_TABLE_RAW)
            print(f"[RAW] ✓ Escritura exitosa a BigQuery")
        else:
            print(f"[RAW] No hay datos en este micro-lote")
    except Exception as e:
        print(f"[ERROR RAW] Micro-lote {batch_id}: {e}")
        import traceback
        traceback.print_exc()

def write_batch_agg(batch_df, batch_id):
    try:
        rows = batch_df.count()
        print(f"[AGG] Micro-lote {batch_id} - Procesando {rows} filas")
        
        if rows > 0:
            # Muestra agregaciones
            print(f"[AGG] Muestra de agregaciones:")
            batch_df.show(10, truncate=False)
            
            # Escribir a BigQuery
            print(f"[AGG] Escribiendo a BigQuery: {BQ_TABLE_AGG}")
            _write_to_bigquery(batch_df, BQ_TABLE_AGG)
            print(f"[AGG] ✓ Escritura exitosa a BigQuery")
        else:
            print(f"[AGG] No hay datos en este micro-lote")
    except Exception as e:
        print(f"[ERROR AGG] Micro-lote {batch_id}: {e}")
        import traceback
        traceback.print_exc()

# ===================== INICIAR STREAMS ======================
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

# ===================== APAGADO ORDENADO ======================
def graceful_shutdown(signum, frame):
    print(f"\nSeñal {signum} recibida. Deteniendo streaming queries...")
    try:
        active_queries = spark.streams.active
        for q in active_queries:
            try:
                print(f"Deteniendo {q.id}")
                q.stop()
            except Exception as e:
                print(f"Error al detener {q.id}: {e}")
    except Exception as e:
        print(f"Error al obtener queries activas: {e}")
    
    print("Deteniendo SparkSession...")
    try:
        spark.stop()
    except Exception as e:
        print(f"Error al detener SparkSession: {e}")
    print("Apagado completo.")
    sys.exit(0)

signal.signal(signal.SIGINT, graceful_shutdown)
signal.signal(signal.SIGTERM, graceful_shutdown)

print(">> Streaming Avro iniciado. Ctrl+C para detener.")
spark.streams.awaitAnyTermination()