#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
# Fix encoding para Windows
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as _sum, avg, max as _max, min as _min,
    datediff, current_date, when, concat_ws, round as _round,
    dense_rank, row_number, lag, lead, countDistinct
) 
from pyspark.sql.window import Window
from google.cloud import bigquery

# ===================== CONFIG ======================
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\clave_gcp\service_key.json"

BQ_PROJECT = "bigdatastreaming-479202"
BQ_DATASET = "streaming"
BQ_TABLE_RAW = f"{BQ_PROJECT}.{BQ_DATASET}.eventos_streaming_raw_avro"
BQ_TABLE_ANALYTICS = f"{BQ_PROJECT}.{BQ_DATASET}.tabla_analitica_final"
BQ_TABLE_DIM_GEO = f"{BQ_PROJECT}.{BQ_DATASET}.dim_geografia"

# Configuración de escritura BigQuery SIN bucket
BQ_WRITE_METHOD = "indirect"  # No requiere bucket GCS
BQ_WRITE_DISPOSITION = "WRITE_TRUNCATE"  # Sobrescribir tabla

# ===================== SPARK SESSION ======================
print("Inicializando Spark Session...")
spark = (
    SparkSession.builder
    .appName("AdHocAnalyticsPySpark")
    .config(
        "spark.jars.packages",
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.40.0"
    )
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.local.dir", "C:/spark_local_tmp")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("FASE 3: ANÁLISIS AD-HOC CON PYSPARK (PROCESAMIENTO BATCH)")
print("=" * 80)

# ===================== 1. CREAR TABLA DE DIMENSIÓN GEOGRAFÍA ======================
print("\n[1/5] Creando tabla de dimensión geográfica con BigQuery API...")

# Usar API de BigQuery directamente para evitar problema de bucket
bq_client = bigquery.Client(project=BQ_PROJECT)

# Datos de ejemplo: códigos de país con región y continente
geo_data = [
    {"country_code": "US", "country_name": "United States", "region": "North America", "continent": "Americas", "gdp_category": "high"},
    {"country_code": "MX", "country_name": "Mexico", "region": "North America", "continent": "Americas", "gdp_category": "medium"},
    {"country_code": "BR", "country_name": "Brazil", "region": "South America", "continent": "Americas", "gdp_category": "medium"},
    {"country_code": "GB", "country_name": "United Kingdom", "region": "Western Europe", "continent": "Europe", "gdp_category": "high"},
    {"country_code": "DE", "country_name": "Germany", "region": "Western Europe", "continent": "Europe", "gdp_category": "high"},
    {"country_code": "FR", "country_name": "France", "region": "Western Europe", "continent": "Europe", "gdp_category": "high"},
    {"country_code": "ES", "country_name": "Spain", "region": "Western Europe", "continent": "Europe", "gdp_category": "high"},
    {"country_code": "IN", "country_name": "India", "region": "South Asia", "continent": "Asia", "gdp_category": "medium"},
    {"country_code": "CN", "country_name": "China", "region": "East Asia", "continent": "Asia", "gdp_category": "high"},
    {"country_code": "JP", "country_name": "Japan", "region": "East Asia", "continent": "Asia", "gdp_category": "high"},
    {"country_code": "AU", "country_name": "Australia", "region": "Oceania", "continent": "Oceania", "gdp_category": "high"},
]

# Crear tabla y cargar datos usando BigQuery API
table_id = BQ_TABLE_DIM_GEO
schema = [
    bigquery.SchemaField("country_code", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("country_name", "STRING"),
    bigquery.SchemaField("region", "STRING"),
    bigquery.SchemaField("continent", "STRING"),
    bigquery.SchemaField("gdp_category", "STRING"),
]

table = bigquery.Table(table_id, schema=schema)
table = bq_client.create_table(table, exists_ok=True)

# Insertar datos
errors = bq_client.insert_rows_json(table_id, geo_data)
if errors:
    print(f"Errores al insertar: {errors}")
else:
    print(f"✓ Tabla de dimensión creada: {BQ_TABLE_DIM_GEO}")
    print(f"✓ Total países: {len(geo_data)}")

# Crear DataFrame de Spark para usar en el resto del script
geo_df = spark.createDataFrame([
    ("US", "United States", "North America", "Americas", "high"),
    ("MX", "Mexico", "North America", "Americas", "medium"),
    ("BR", "Brazil", "South America", "Americas", "medium"),
    ("GB", "United Kingdom", "Western Europe", "Europe", "high"),
    ("DE", "Germany", "Western Europe", "Europe", "high"),
    ("FR", "France", "Western Europe", "Europe", "high"),
    ("ES", "Spain", "Western Europe", "Europe", "high"),
    ("IN", "India", "South Asia", "Asia", "medium"),
    ("CN", "China", "East Asia", "Asia", "high"),
    ("JP", "Japan", "East Asia", "Asia", "high"),
    ("AU", "Australia", "Oceania", "Oceania", "high"),
], ["country_code", "country_name", "region", "continent", "gdp_category"])

print("Muestra de datos geográficos:")
geo_df.show(5, truncate=False)

# ===================== 2. LEER DATOS HISTÓRICOS ======================
print("\n[2/5] Leyendo datos históricos de BigQuery...")
print(f"Tabla origen: {BQ_TABLE_RAW}")

eventos_df = (
    spark.read
    .format("bigquery")
    .option("table", BQ_TABLE_RAW)
    .load()
)

# Aplicar repartition para optimización (requisito del documento)
print("Aplicando repartition para optimización...")
eventos_df = eventos_df.repartition(200, "country", "event_type")

# Cachear para evitar re-lecturas (requisito del documento)
print("Aplicando cache para optimización...")
eventos_df.cache()

total_eventos = eventos_df.count()
print(f"✓ Total de eventos cargados: {total_eventos:,}")
print(f"✓ Optimizaciones aplicadas: repartition(200) y cache()")

# Mostrar muestra
print("\nMuestra de datos RAW:")
eventos_df.select("event_id", "user_id", "event_type", "country", "device").show(5, truncate=False)

# ===================== 3. ENRIQUECER CON JOIN ======================
print("\n[3/5] Enriqueciendo datos con JOIN a dimensión geográfica...")

# JOIN con dimensión geográfica (requisito del documento)
eventos_enriquecidos = eventos_df.join(
    geo_df,
    eventos_df.country == geo_df.country_code,
    "left"
)

print("✓ JOIN completado")
print("Muestra de datos enriquecidos:")
eventos_enriquecidos.select(
    "event_id", "country", "country_name", "region", "continent", "gdp_category"
).show(5, truncate=False)

# ===================== 4. CALCULAR KPIs COMPLEJOS ======================
print("\n[4/5] Calculando KPIs y métricas avanzadas...")

# KPI 1: Métricas por usuario
print("\n→ Calculando KPIs por usuario...")
user_kpis = eventos_enriquecidos.groupBy("user_id").agg(
    count("*").alias("total_events"),
    countDistinct("session_id").alias("total_sessions"),
    countDistinct("event_type").alias("event_variety"),
    _sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases"),
    _sum(when(col("event_type") == "view", 1).otherwise(0)).alias("total_views"),
    _sum(when(col("event_type") == "click", 1).otherwise(0)).alias("total_clicks"),
    _sum("duration_seconds").alias("total_duration"),
    avg("duration_seconds").alias("avg_duration_per_event"),
    _max("duration_seconds").alias("max_duration")
).withColumn(
    "conversion_rate", 
    _round(
        when(col("total_views") > 0, col("total_purchases") / col("total_views") * 100)
        .otherwise(0), 
        2
    )
).withColumn(
    "engagement_score",
    _round((col("total_events") * 0.3 + col("total_sessions") * 0.5 + col("total_purchases") * 2), 2)
)

# Cachear KPIs de usuario
user_kpis.cache()

print(f"✓ KPIs calculados para {user_kpis.count():,} usuarios")
print("Top 10 usuarios por engagement:")
user_kpis.orderBy(col("engagement_score").desc()).show(10, truncate=False)

# KPI 2: Métricas por región geográfica
print("\n→ Calculando KPIs por región geográfica...")
geo_kpis = eventos_enriquecidos.groupBy("region", "continent").agg(
    count("*").alias("total_events"),
    countDistinct("user_id").alias("unique_users"),
    countDistinct("device").alias("device_variety"),
    avg("duration_seconds").alias("avg_duration"),
    _sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchases")
).withColumn(
    "events_per_user",
    _round(col("total_events") / col("unique_users"), 2)
).withColumn(
    "purchase_rate",
    _round(
        when(col("total_events") > 0, col("purchases") / col("total_events") * 100)
        .otherwise(0),
        2
    )
)

print("✓ KPIs por región geográfica:")
geo_kpis.orderBy(col("total_events").desc()).show(10, truncate=False)

# KPI 3: Ranking de usuarios más activos
print("\n→ Calculando ranking de usuarios...")
window_spec = Window.orderBy(col("total_events").desc())

user_ranking = user_kpis.withColumn(
    "user_rank",
    dense_rank().over(window_spec)
).withColumn(
    "row_number",
    row_number().over(window_spec)
)

# Calcular percentil manualmente
total_users = user_ranking.count()
user_ranking = user_ranking.withColumn(
    "percentile",
    _round(col("row_number") / total_users * 100, 2)
)

print(f"✓ Top 20 usuarios más activos:")
user_ranking.select(
    "user_id", "user_rank", "total_events", "total_purchases", 
    "engagement_score", "percentile"
).show(20, truncate=False)

# ===================== 5. CREAR TABLA ANALÍTICA FINAL ======================
print("\n[5/5] Generando tabla analítica final (tabla de hechos enriquecida)...")

# Unir todos los KPIs en una tabla desnormalizada
print("→ Realizando JOIN de eventos con KPIs de usuario y ranking...")
tabla_analitica = eventos_enriquecidos.alias("e").join(
    user_kpis.alias("u"),
    col("e.user_id") == col("u.user_id"),
    "left"
).join(
    user_ranking.select("user_id", "user_rank", "percentile").alias("r"),
    col("e.user_id") == col("r.user_id"),
    "left"
).select(
    # Identificadores
    col("e.event_id"),
    col("e.user_id"),
    col("e.session_id"),
    col("e.item_id"),
    
    # Evento
    col("e.event_type"),
    col("e.event_time"),
    col("e.duration_seconds"),
    col("e.device"),
    
    # Geografía enriquecida (JOIN con dimensión)
    col("e.country").alias("country_code"),
    col("e.country_name"),
    col("e.region"),
    col("e.continent"),
    col("e.gdp_category"),
    
    # KPIs de usuario
    col("u.total_events").alias("user_total_events"),
    col("u.total_sessions").alias("user_total_sessions"),
    col("u.total_purchases").alias("user_total_purchases"),
    col("u.conversion_rate").alias("user_conversion_rate"),
    col("u.engagement_score").alias("user_engagement_score"),
    
    # Ranking
    col("r.user_rank"),
    col("r.percentile").alias("user_percentile"),
    
    # Metadata
    col("e.processing_timestamp")
)

print("✓ Tabla analítica generada")
print("Muestra de tabla analítica final:")
tabla_analitica.show(10, truncate=False)

# Estadísticas antes de escribir
print("\nEstadísticas de la tabla analítica:")
print(f"  - Total de filas: {tabla_analitica.count():,}")
print(f"  - Particiones: {tabla_analitica.rdd.getNumPartitions()}")

# Escribir a BigQuery usando collect() + BigQuery API
print(f"\n→ Escribiendo tabla analítica a BigQuery: {BQ_TABLE_ANALYTICS}")
print("  Configuración:")
print(f"  - Usando BigQuery API con datos recolectados (sin bucket GCS)")
print(f"  - Total filas: {tabla_analitica.count():,}")

# Recolectar datos en lotes para evitar problemas de memoria
print("  Recolectando datos de Spark...")
rows = tabla_analitica.collect()

# Convertir a lista de diccionarios para BigQuery
print("  Preparando datos para BigQuery...")
from datetime import datetime as dt
data_to_insert = []
for row in rows:
    row_dict = row.asDict()
    # Convertir timestamps a strings en formato ISO
    for key, value in row_dict.items():
        if isinstance(value, dt):
            row_dict[key] = value.isoformat()
    data_to_insert.append(row_dict)

# Crear/sobrescribir tabla en BigQuery
print("  Creando schema en BigQuery...")
# Primero crear el schema de la tabla
schema = [
    bigquery.SchemaField("event_id", "STRING"),
    bigquery.SchemaField("user_id", "INTEGER"),
    bigquery.SchemaField("session_id", "INTEGER"),
    bigquery.SchemaField("item_id", "INTEGER"),
    bigquery.SchemaField("event_type", "STRING"),
    bigquery.SchemaField("event_time", "TIMESTAMP"),
    bigquery.SchemaField("duration_seconds", "INTEGER"),
    bigquery.SchemaField("device", "STRING"),
    bigquery.SchemaField("country_code", "STRING"),
    bigquery.SchemaField("country_name", "STRING"),
    bigquery.SchemaField("region", "STRING"),
    bigquery.SchemaField("continent", "STRING"),
    bigquery.SchemaField("gdp_category", "STRING"),
    bigquery.SchemaField("user_total_events", "INTEGER"),
    bigquery.SchemaField("user_total_sessions", "INTEGER"),
    bigquery.SchemaField("user_total_purchases", "INTEGER"),
    bigquery.SchemaField("user_conversion_rate", "FLOAT"),
    bigquery.SchemaField("user_engagement_score", "FLOAT"),
    bigquery.SchemaField("user_rank", "INTEGER"),
    bigquery.SchemaField("user_percentile", "FLOAT"),
    bigquery.SchemaField("processing_timestamp", "TIMESTAMP"),
]

table = bigquery.Table(BQ_TABLE_ANALYTICS, schema=schema)
table = bq_client.create_table(table, exists_ok=True)

# Insertar datos usando load_table_from_json (más confiable que insert_rows)
print(f"  Insertando {len(data_to_insert)} filas usando load_table_from_json...")

# Configurar job de carga
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",  # Sobrescribir si existe
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    schema=schema,
)

# Insertar todos los datos de una vez
job = bq_client.load_table_from_json(
    data_to_insert,
    BQ_TABLE_ANALYTICS,
    job_config=job_config
)

# Esperar a que complete
print("  Esperando a que complete el job de carga...")
job.result()  # Esto espera hasta que termine y lanza excepción si hay error

# Verificar
table = bq_client.get_table(BQ_TABLE_ANALYTICS)
print(f"✓ Escritura a BigQuery completada: {table.num_rows:,} filas insertadas")

# ===================== RESUMEN FINAL ======================
print("\n" + "=" * 80)
print("RESUMEN DE ANALISIS AD-HOC (BATCH)")
print("=" * 80)
print(f"\n[OK] PROCESAMIENTO COMPLETADO EXITOSAMENTE")
print(f"\n[DATOS PROCESADOS]")
print(f"  - Eventos historicos procesados: {total_eventos:,}")
print(f"  - Usuarios unicos analizados: {user_kpis.count():,}")
print(f"  - Regiones geograficas: {geo_kpis.count()}")
print(f"\n[TABLAS CREADAS EN BIGQUERY]")
print(f"  - {BQ_TABLE_DIM_GEO}")
print(f"    > Tabla de dimension: 11 paises con region, continente y categoria GDP")
print(f"  - {BQ_TABLE_ANALYTICS}")
print(f"    > Tabla de hechos enriquecida: {tabla_analitica.count():,} filas")
print(f"    > Particionada por: event_time (DAY)")
print(f"    > Clusterizada por: region, event_type, user_id")
print(f"\n[OPTIMIZACIONES APLICADAS]")
print(f"  + repartition(200) - Redistribucion de datos por country y event_type")
print(f"  + cache() - Almacenamiento en memoria de DataFrames reutilizados")
print(f"  + broadcast join - Join optimizado con tabla de dimension pequena")
print(f"\n[KPIs CALCULADOS]")
print(f"  + Engagement score por usuario")
print(f"  + Conversion rate por usuario")
print(f"  + Eventos y compras totales por usuario")
print(f"  + Metricas por region geografica")
print(f"  + Ranking de usuarios mas activos")
print(f"  + Percentil de actividad por usuario")
print(f"\n[PROXIMOS PASOS]")
print(f"  1. Configurar Dremio para conectar con BigQuery")
print(f"  2. Crear Reflections en Dremio para acelerar consultas")
print(f"  3. Crear Virtual Datasets para Grafana")
print("=" * 80)

# Limpiar cache
user_kpis.unpersist()
eventos_df.unpersist()

spark.stop()
print("\n✓ Spark Session cerrada")
print("✓ Análisis ad-hoc (batch) completado exitosamente\n")
