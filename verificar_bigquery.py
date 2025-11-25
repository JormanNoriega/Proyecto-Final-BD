"""
Script para verificar la conexión y configuración de BigQuery
"""
import os
from google.cloud import bigquery

# Configurar credenciales
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\clave_gcp\service_key.json"

# Configuración
PROJECT_ID = "bigdatastreaming-479202"
DATASET_ID = "streaming"
TABLE_RAW = "eventos_streaming_raw_avro"
TABLE_AGG = "eventos_streaming_agg_avro"

print("=" * 60)
print("VERIFICACIÓN DE BIGQUERY")
print("=" * 60)

try:
    # Crear cliente
    client = bigquery.Client(project=PROJECT_ID)
    print(f"✓ Cliente BigQuery creado correctamente")
    print(f"  Proyecto: {PROJECT_ID}")
    print()
    
    # Verificar dataset
    print(f"Verificando dataset '{DATASET_ID}'...")
    try:
        dataset = client.get_dataset(f"{PROJECT_ID}.{DATASET_ID}")
        print(f"✓ Dataset existe")
        print(f"  Ubicación: {dataset.location}")
        print(f"  Creado: {dataset.created}")
    except Exception as e:
        print(f"✗ Error: Dataset no existe o no tienes acceso")
        print(f"  {e}")
        print()
        print("Creando dataset...")
        dataset = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
        dataset.location = "US"
        dataset = client.create_dataset(dataset)
        print(f"✓ Dataset creado")
    
    print()
    
    # Verificar tabla RAW
    print(f"Verificando tabla '{TABLE_RAW}'...")
    try:
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_RAW}"
        table = client.get_table(table_ref)
        print(f"✓ Tabla RAW existe")
        print(f"  Registros: {table.num_rows:,}")
        print(f"  Schema: {len(table.schema)} campos")
        
        # Mostrar últimos registros
        query = f"""
        SELECT COUNT(*) as total
        FROM `{table_ref}`
        """
        result = client.query(query).result()
        for row in result:
            print(f"  Total registros (query): {row.total:,}")
    except Exception as e:
        print(f"✗ Tabla RAW no existe")
        print(f"  {e}")
        print()
        print("La tabla se creará automáticamente cuando lleguen datos")
    
    print()
    
    # Verificar tabla AGG
    print(f"Verificando tabla '{TABLE_AGG}'...")
    try:
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_AGG}"
        table = client.get_table(table_ref)
        print(f"✓ Tabla AGG existe")
        print(f"  Registros: {table.num_rows:,}")
        print(f"  Schema: {len(table.schema)} campos")
        
        # Mostrar últimos registros
        query = f"""
        SELECT COUNT(*) as total
        FROM `{table_ref}`
        """
        result = client.query(query).result()
        for row in result:
            print(f"  Total registros (query): {row.total:,}")
    except Exception as e:
        print(f"✗ Tabla AGG no existe")
        print(f"  {e}")
        print()
        print("La tabla se creará automáticamente cuando lleguen datos")
    
    print()
    print("=" * 60)
    print("VERIFICACIÓN COMPLETA")
    print("=" * 60)
    print()
    print("Siguiente paso:")
    print("1. Si las tablas no existen, se crearán automáticamente")
    print("2. Ejecuta: python consumer_spark_streaming.py")
    print("3. En otra terminal: python producer_avro.py")
    print("4. Espera 30-60 segundos y verifica BigQuery de nuevo")
    
except Exception as e:
    print(f"✗ Error al conectar con BigQuery")
    print(f"  {e}")
    print()
    print("Verifica:")
    print("1. El archivo C:\\clave_gcp\\service_key.json existe")
    print("2. La cuenta de servicio tiene permisos:")
    print("   - BigQuery Data Editor")
    print("   - BigQuery Job User")
    print("3. El proyecto 'bigdatastreaming-479202' es correcto")
