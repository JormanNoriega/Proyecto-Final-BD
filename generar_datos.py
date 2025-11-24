import csv
import random
import uuid
from datetime import datetime, timedelta

# ===== CONFIGURACIÓN =====
TOTAL_REGISTROS = 50_000_000   # 50 millones
CHUNK = 1_000_000              # escribir 1 millón por bloque
ARCHIVO_SALIDA = "dataset_usuarios_50M.csv"

event_types = ["view", "click", "search", "like", "comment", "purchase"]
devices = ["mobile", "desktop", "smart_tv", "tablet", "console"]
countries = ["US", "CO", "MX", "BR", "AR", "CL", "ES", "FR"]

# ===== GENERADOR =====
def generar_registro():
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 10_000_000),  
        "session_id": random.randint(1, 20_000_000),
        "event_type": random.choice(event_types),
        "item_id": random.randint(1, 2_000_000),
        "timestamp": (datetime.now() - timedelta(seconds=random.randint(0, 60*60*24*30))).isoformat(),
        "duration_seconds": random.randint(1, 7200),
        "device": random.choice(devices),
        "country": random.choice(countries)
    }

# ===== PROCESO DE ESCRITURA =====
with open(ARCHIVO_SALIDA, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=[
        "event_id", "user_id", "session_id", "event_type",
        "item_id", "timestamp", "duration_seconds",
        "device", "country"
    ])
    
    writer.writeheader()
    
    registros_generados = 0
    while registros_generados < TOTAL_REGISTROS:
        lote = min(CHUNK, TOTAL_REGISTROS - registros_generados)
        
        filas = [generar_registro() for _ in range(lote)]
        writer.writerows(filas)
        
        registros_generados += lote
        print(f"Progreso: {registros_generados:,}/{TOTAL_REGISTROS:,} registros generados")

print("¡Dataset generado exitosamente!")
