"""
Script para verificar que Kafka está recibiendo mensajes correctamente
y que el consumer puede leerlos.
"""
import time
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Configuración
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "datos_streaming_big_data"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

# Schema Registry
schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Leer schema
with open("user_event.avsc", "r", encoding="utf-8") as f:
    avro_schema_str = f.read()

avro_deserializer = AvroDeserializer(
    schema_registry_client,
    avro_schema_str
)

# Consumer
consumer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": "verification_group",
    "auto.offset.reset": "earliest",  # Leer desde el principio
}

consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC])

print(f"Conectado a Kafka: {KAFKA_BOOTSTRAP}")
print(f"Topic: {TOPIC}")
print(f"Leyendo mensajes... (Ctrl+C para detener)\n")

try:
    count = 0
    start = time.time()
    
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Fin de particion {msg.partition()}")
            else:
                print(f"Error: {msg.error()}")
            continue
        
        # Deserializar mensaje
        try:
            ctx = SerializationContext(TOPIC, MessageField.VALUE)
            event = avro_deserializer(msg.value(), ctx)
            
            count += 1
            
            # Mostrar los primeros 10 mensajes
            if count <= 10:
                print(f"\n--- Mensaje {count} ---")
                print(f"Key: {msg.key().decode('utf-8') if msg.key() else 'None'}")
                print(f"Offset: {msg.offset()}")
                print(f"Partition: {msg.partition()}")
                print(f"Event ID: {event['event_id']}")
                print(f"User ID: {event['user_id']}")
                print(f"Event Type: {event['event_type']}")
                print(f"Timestamp: {event['timestamp']}")
            
            # Cada 1000 mensajes mostrar progreso
            if count % 1000 == 0:
                elapsed = time.time() - start
                rate = count / elapsed if elapsed > 0 else 0
                print(f"\rProgreso: {count} mensajes leidos (aprox {rate:.0f} msg/s)", end="")
        
        except Exception as e:
            print(f"\nError deserializando mensaje: {e}")
            print(f"Raw value: {msg.value()[:100]}")
        
        # Detener después de 5000 mensajes para la verificación
        if count >= 5000:
            print(f"\n\n=== VERIFICACION COMPLETA ===")
            print(f"Total mensajes leidos: {count}")
            elapsed = time.time() - start
            rate = count / elapsed if elapsed > 0 else 0
            print(f"Tiempo total: {elapsed:.2f} segundos")
            print(f"Velocidad promedio: {rate:.0f} msg/s")
            print(f"\nEl consumer ESTA recibiendo datos correctamente!")
            break

except KeyboardInterrupt:
    print(f"\n\nInterrumpido por el usuario")
    print(f"Total mensajes leidos: {count}")

finally:
    consumer.close()
    print("Consumer cerrado")
