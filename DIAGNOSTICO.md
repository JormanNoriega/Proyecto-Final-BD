# Diagnóstico y Verificación del Sistema de Streaming

## Resumen de Problemas Encontrados y Soluciones

### ✅ PROBLEMAS CORREGIDOS

#### 1. Producer (producer_avro.py)

**Error:** `UnicodeEncodeError: 'charmap' codec can't encode character '\u2248'`

**Causa:** El símbolo "≈" no es compatible con la codificación CP1252 de Windows

**Solución:** Cambiado el símbolo "≈" por "aprox" en la línea 86

#### 2. Consumer (consumer_spark_streaming.py)

**Error:** Fallo durante el shutdown con `IllegalStateException: Shutdown hooks cannot be modified during shutdown`

**Solución:** Mejorado el manejo de errores en la función `graceful_shutdown`

### ✅ ESTADO ACTUAL DEL SISTEMA

#### Kafka está funcionando correctamente:

- **Topic:** `datos_streaming_big_data`
- **Particiones:** 10 (0-9)
- **Mensajes totales:** ~136 millones
- **Distribución por partición:** ~13.6 millones por partición

#### El consumer SÍ está recibiendo datos:

- Procesó exitosamente hasta el micro-lote 15
- Los errores solo ocurren durante el apagado (problema de Spark en Windows)

## Comandos de Verificación

### 1. Verificar que Kafka tiene mensajes:

```powershell
docker exec proyectofinalbd-kafka-1 kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic datos_streaming_big_data --time -1
```

### 2. Verificar que el consumer puede leer mensajes:

```powershell
python verificar_kafka.py
```

Este script:

- Lee 5000 mensajes del topic
- Muestra los primeros 10 mensajes completos
- Reporta la velocidad de lectura
- Confirma que el consumer está funcionando

### 3. Ver mensajes en tiempo real con Kafdrop:

```
http://localhost:9000
```

### 4. Listar topics:

```powershell
docker exec proyectofinalbd-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list
```

### 5. Describir el topic:

```powershell
docker exec proyectofinalbd-kafka-1 kafka-topics --bootstrap-server localhost:9092 --describe --topic datos_streaming_big_data
```

### 6. Consumir mensajes desde la consola (formato binario Avro):

```powershell
docker exec proyectofinalbd-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic datos_streaming_big_data --max-messages 10
```

**Nota:** Verás datos binarios ya que están en formato Avro con Schema Registry

## Ejecutar el Producer

```powershell
python producer_avro.py
```

**Comportamiento esperado:**

- Envía eventos cada 5000 mensajes
- Muestra progreso: "Progreso: 5000 eventos enviados (aprox XXXX msg/s)"
- Ya NO debe fallar con UnicodeEncodeError

## Ejecutar el Consumer

```powershell
python consumer_spark_streaming.py
```

**Comportamiento esperado:**

- Inicia Spark y carga las dependencias
- Muestra: ">> Streaming Avro iniciado. Ctrl+C para detener."
- Procesa micro-lotes cada 30 segundos
- Muestra: "[RAW] Micro-lote X filas=Y"
- Muestra: "[AGG] Micro-lote X filas=Y"
- Escribe a BigQuery

**Warnings esperados (no son errores):**

- `KafkaDataConsumer is not running in UninterruptibleThread` - Es solo un warning, no afecta el funcionamiento
- Mensajes sobre `spark.local.dir` - Son informativos

## Verificar que el Consumer está procesando

### Opción 1: Ver los checkpoints

```powershell
ls checkpoints_streaming_avro/raw/commits
ls checkpoints_streaming_avro/agg/commits
```

- Cada archivo representa un micro-lote procesado
- Si ves archivos nuevos, el consumer está funcionando

### Opción 2: Ver los logs del consumer

El consumer muestra en consola:

- `[RAW] Micro-lote X filas=Y` - Procesando eventos raw
- `[AGG] Micro-lote X filas=Y` - Procesando agregaciones
- Tabla con latencias de procesamiento

### Opción 3: Consultar BigQuery

```sql
-- Ver eventos raw
SELECT COUNT(*) as total_eventos
FROM `bigdatastreaming-479202.streaming.eventos_streaming_raw_avro`;

-- Ver agregaciones
SELECT *
FROM `bigdatastreaming-479202.streaming.eventos_streaming_agg_avro`
ORDER BY window_start DESC
LIMIT 10;
```

## Troubleshooting

### El producer no envía mensajes

```powershell
# Verificar que Kafka está corriendo
docker ps

# Verificar que el Schema Registry está disponible
curl http://localhost:8081/subjects
```

### El consumer no recibe mensajes

```powershell
# 1. Verificar que hay mensajes en el topic
python verificar_kafka.py

# 2. Verificar configuración de GCP
Get-Content C:\clave_gcp\service_key.json

# 3. Ver logs detallados de Spark
# Editar consumer_spark_streaming.py línea 60:
# spark.sparkContext.setLogLevel("INFO")
```

### Error de autenticación con BigQuery

- Verificar que el archivo `C:\clave_gcp\service_key.json` existe
- Verificar que la cuenta de servicio tiene permisos:
  - BigQuery Data Editor
  - BigQuery Job User

### El consumer se detiene inmediatamente

- Verificar que `startingOffsets` está en "latest" o "earliest"
- Si usas "latest", el consumer solo leerá nuevos mensajes
- Para leer mensajes existentes, cambiar a "earliest"

## Estadísticas del Sistema

### Datos en Kafka

- **Total de mensajes:** ~136,703,208
- **Distribución:** ~13.6M por partición (10 particiones)
- **Formato:** Avro con Confluent Schema Registry
- **Schema ID:** Registrado en http://localhost:8081

### Performance Observada

- **Producer:** ~37,000 msg/s (en la última ejecución)
- **Consumer:** Depende de la velocidad de escritura a BigQuery
- **Micro-lotes:** Cada 30 segundos (configurable)

## Conclusión

✅ **Tu sistema ESTÁ funcionando correctamente:**

1. Kafka tiene 136 millones de mensajes
2. El producer puede enviar mensajes sin errores
3. El consumer puede leer y procesar mensajes
4. Los errores que ves son solo durante el apagado (problema conocido de Spark en Windows)

**Para verificar que todo funciona:** Ejecuta `python verificar_kafka.py`
