#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Verificar tabla analítica final en BigQuery
"""
from google.cloud import bigquery
import os

# Configurar credenciales
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\clave_gcp\service_key.json'

# Cliente BigQuery
bq_client = bigquery.Client(project='bigdatastreaming-479202')

print("="*80)
print("VERIFICACIÓN TABLA ANALÍTICA FINAL")
print("="*80)

# Verificar tabla_analitica_final
table_id = "bigdatastreaming-479202.streaming.tabla_analitica_final"

try:
    table = bq_client.get_table(table_id)
    print(f"\n✓ Tabla existe: {table_id}")
    print(f"  - Total filas: {table.num_rows:,}")
    print(f"  - Tamaño: {table.num_bytes / (1024*1024):.2f} MB")
    print(f"  - Creada: {table.created}")
    print(f"  - Modificada: {table.modified}")
    
    if table.time_partitioning:
        print(f"  - Particionada por: {table.time_partitioning.field} ({table.time_partitioning.type_})")
    
    if table.clustering_fields:
        print(f"  - Clusterizada por: {', '.join(table.clustering_fields)}")
    
    print(f"\n  Schema ({len(table.schema)} campos):")
    for field in table.schema[:10]:  # Mostrar primeros 10 campos
        print(f"    - {field.name}: {field.field_type}")
    if len(table.schema) > 10:
        print(f"    ... y {len(table.schema) - 10} campos más")
    
    # Consulta de muestra
    print("\n→ Consultando muestra de datos...")
    query = f"""
    SELECT 
        event_id,
        user_id,
        event_type,
        country_code,
        region,
        user_total_events,
        user_engagement_score,
        user_conversion_rate,
        user_rank
    FROM `{table_id}`
    LIMIT 5
    """
    
    results = bq_client.query(query).result()
    print("\n  Muestra de datos:")
    print("  " + "-"*78)
    for row in results:
        print(f"  User: {row.user_id} | Events: {row.user_total_events} | " + 
              f"Score: {row.user_engagement_score:.2f} | Rank: {row.user_rank}")
    
    # Estadísticas
    print("\n→ Estadísticas generales...")
    stats_query = f"""
    SELECT 
        COUNT(DISTINCT user_id) as usuarios_unicos,
        COUNT(*) as total_eventos,
        COUNT(DISTINCT region) as regiones,
        COUNT(DISTINCT country_code) as paises,
        AVG(user_engagement_score) as avg_engagement,
        AVG(user_conversion_rate) as avg_conversion
    FROM `{table_id}`
    """
    
    stats = list(bq_client.query(stats_query).result())[0]
    print(f"\n  - Usuarios únicos: {stats.usuarios_unicos:,}")
    print(f"  - Total eventos: {stats.total_eventos:,}")
    print(f"  - Regiones: {stats.regiones}")
    print(f"  - Países: {stats.paises}")
    print(f"  - Engagement promedio: {stats.avg_engagement:.4f}")
    print(f"  - Conversion rate promedio: {stats.avg_conversion:.4f}")
    
    print("\n" + "="*80)
    print("✓ TABLA ANALÍTICA VERIFICADA CORRECTAMENTE")
    print("="*80)
    print("\nPróximos pasos:")
    print("  1. Configurar Dremio para conectarse a BigQuery")
    print("  2. Crear Reflections en Dremio")
    print("  3. Crear Virtual Datasets para Grafana")
    print("="*80)
    
except Exception as e:
    print(f"\n✗ Error al verificar tabla: {e}")
    print("\nLa tabla puede no existir todavía.")
