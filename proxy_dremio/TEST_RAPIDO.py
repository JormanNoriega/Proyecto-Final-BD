#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""TEST RÁPIDO DEL PROXY"""

import requests
import json

PROXY = "http://localhost:5000"

print("=" * 70)
print("🧪 TEST PROXY DREMIO")
print("=" * 70)

# Test 1: Info
print("\n[1] Info del proxy...")
try:
    r = requests.get(f"{PROXY}/", timeout=5)
    print(f"✓ Status: {r.status_code}")
    print(f"  {r.json()}")
except Exception as e:
    print(f"✗ Error: {e}")
    print("\n⚠ Asegúrate de iniciar el proxy:")
    print("  python proxy_simple.py")
    exit(1)

# Test 2: Query simple
print("\n[2] Query simple...")
try:
    r = requests.post(
        f"{PROXY}/query",
        json={"sql": "SELECT 1 as numero, 'test' as texto"},
        timeout=30
    )
    print(f"✓ Status: {r.status_code}")
    print(f"  Resultado: {r.json()}")
except Exception as e:
    print(f"✗ Error: {e}")

# Test 3: Query al VDS
print("\n[3] Query a vds.vds_tabla_analitica_final...")
try:
    r = requests.post(
        f"{PROXY}/query",
        json={"sql": "SELECT COUNT(*) as total FROM vds.vds_tabla_analitica_final"},
        timeout=60
    )
    print(f"✓ Status: {r.status_code}")
    data = r.json()
    if isinstance(data, list) and len(data) > 0:
        print(f"  Total registros: {data[0].get('total', 'N/A'):,}")
    else:
        print(f"  Resultado: {data}")
except Exception as e:
    print(f"✗ Error: {e}")

# Test 4: Query con LIMIT
print("\n[4] Query con LIMIT 3...")
try:
    r = requests.post(
        f"{PROXY}/query",
        json={"sql": "SELECT * FROM vds.vds_tabla_analitica_final LIMIT 3"},
        timeout=60
    )
    print(f"✓ Status: {r.status_code}")
    data = r.json()
    if isinstance(data, list):
        print(f"  Filas: {len(data)}")
        if len(data) > 0:
            print(f"  Columnas: {', '.join(data[0].keys())}")
    else:
        print(f"  Resultado: {data}")
except Exception as e:
    print(f"✗ Error: {e}")

print("\n" + "=" * 70)
print("✅ TESTS COMPLETADOS")
print("=" * 70)
print("\n📊 CONFIGURACIÓN GRAFANA:")
print("  Plugin: Infinity")
print("  URL: http://localhost:5000/query")
print("  Method: POST")
print("  Headers: Content-Type: application/json")
print('  Body: {"sql": "SELECT * FROM vds.vds_tabla_analitica_final LIMIT 100"}')
print("=" * 70)
