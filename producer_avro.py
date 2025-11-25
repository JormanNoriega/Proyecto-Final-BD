import csv
import time
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

# -------------------------------
# CONFIGURACIÓN KAFKA + REGISTRY
# -------------------------------
schema_registry_conf = {
    "url": "http://localhost:8081"
}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

with open("user_event.avsc", "r", encoding="utf-8") as f:
    avro_schema_str = f.read()

avro_serializer = AvroSerializer(schema_registry_client, avro_schema_str)
key_serializer = StringSerializer('utf_8')

producer_conf = {
    "bootstrap.servers": "localhost:9092",
    "key.serializer": key_serializer,
    "value.serializer": avro_serializer,
    "linger.ms": 5,
    "batch.num.messages": 5000,
    "queue.buffering.max.messages": 200000,
    # Opcional: ajustar acks si quieres durabilidad estricta:
    # "acks": "all"
}

producer = SerializingProducer(producer_conf)

# -------------------------------
# CALLBACK ENTREGA
# -------------------------------
def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Mensaje falló: {err}")
    # else:
    #     print(f"[OK] {msg.topic()}-{msg.partition()} offset={msg.offset()}")

# -------------------------------
# ENVÍO DESDE CSV
# -------------------------------
csv_file = "dataset_usuarios_50M.csv"
topic = "datos_streaming_big_data"

start = time.time()
count = 0
print("Inicio de envío de datos...")

try:
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            event = {
                "event_id": row["event_id"],
                "user_id": int(row["user_id"]),
                "session_id": int(row["session_id"]),
                "event_type": row["event_type"],
                "item_id": int(row["item_id"]),
                "timestamp": row["timestamp"],
                "duration_seconds": int(row["duration_seconds"]),
                "device": row["device"],
                "country": row["country"],
                "event_generated_at": row["timestamp"]
            }

            producer.produce(
                topic=topic,
                key=row["event_id"],
                value=event,
                on_delivery=delivery_report
            )

            producer.poll(0)
            count += 1

            if count % 5000 == 0:
                producer.flush()
                elapsed = time.time() - start
                rate = count / elapsed
                print(f"Progreso: {count} eventos enviados (aprox {rate:.0f} msg/s)")

except KeyboardInterrupt:
    print("\nInterrupción recibida. Cerrando productor...")

finally:
    print("Flushing final (enviando pendientes)...")
    producer.flush()
    elapsed = time.time() - start
    rate = count / elapsed if elapsed > 0 else 0
    print("Finalizado")
    print(f"Total registros enviados: {count}")
    print(f"Tiempo total: {elapsed:.2f} s")
    print(f"Velocidad promedio: {rate:.0f} msg/s")