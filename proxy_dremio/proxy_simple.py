#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PROXY SIMPLE: Grafana -> Dremio Cloud (REST API Job-based)
Usa el Job API de Dremio Cloud para ejecutar queries SQL
"""

from flask import Flask, request, jsonify
import requests
import time

app = Flask(__name__)

# ==================== CONFIGURACIÓN ====================
PROJECT_ID = "c00ea464-53ed-4e9d-adba-847689d50b3a"
DREMIO_TOKEN = "gF4E1WYOSjuqiZNMcLROvSzlbeb3ODx26RESLDCx+N1BETRdc6vB9WyPWlq7UA=="
DREMIO_API_BASE = f"https://api.dremio.cloud/v0/projects/{PROJECT_ID}"
# =======================================================

def ejecutar_query_dremio(sql):
    """Ejecuta query en Dremio Cloud usando Job API"""
    headers = {
        "Authorization": f"Bearer {DREMIO_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # 1. Crear el job
    print(f"\n📊 Ejecutando query: {sql[:80]}...")
    print(f"🌐 URL: {DREMIO_API_BASE}/sql")
    print(f"🔑 Token (primeros 10 chars): {DREMIO_TOKEN[:10]}...")
    
    try:
        response = requests.post(
            f"{DREMIO_API_BASE}/sql",
            headers=headers,
            json={"sql": sql},
            timeout=10
        )
        
        print(f"📡 Status Code: {response.status_code}")
        print(f"📡 Response: {response.text[:200]}...")
        
    except Exception as e:
        error_msg = f"Error en request: {type(e).__name__} - {str(e)}"
        print(f"❌ {error_msg}")
        raise Exception(error_msg)
    
    if response.status_code != 200:
        raise Exception(f"Error creando job: {response.status_code} - {response.text[:500]}")
    
    job_id = response.json().get("id")
    if not job_id:
        raise Exception(f"No se obtuvo job_id: {response.json()}")
    
    print(f"✓ Job: {job_id}")
    
    # 2. Esperar a que termine
    for i in range(60):
        time.sleep(1)
        status_resp = requests.get(
            f"{DREMIO_API_BASE}/job/{job_id}",
            headers=headers,
            timeout=10
        )
        
        if status_resp.status_code != 200:
            raise Exception(f"Error status: {status_resp.status_code}")
        
        job_state = status_resp.json().get("jobState")
        
        if job_state == "COMPLETED":
            print(f"✓ Completado")
            break
        elif job_state in ["FAILED", "CANCELLED"]:
            raise Exception(f"Job falló: {job_state}")
        
        if i % 5 == 0:
            print(f"⏳ Esperando... ({job_state})")
    else:
        raise Exception("Timeout")
    
    # 3. Obtener resultados
    print(f"📥 Obteniendo resultados del job {job_id}...")
    results_resp = requests.get(
        f"{DREMIO_API_BASE}/job/{job_id}/results",
        headers=headers,
        timeout=30
    )
    
    print(f"📡 Results Status: {results_resp.status_code}")
    print(f"📡 Results Response: {results_resp.text[:500]}")
    
    if results_resp.status_code != 200:
        raise Exception(f"Error resultados: {results_resp.status_code} - {results_resp.text[:500]}")
    
    try:
        data = results_resp.json()
    except Exception as e:
        raise Exception(f"Error parseando JSON: {e} - Response: {results_resp.text[:500]}")
    
    rows = data.get("rows", [])
    schema = data.get("schema", [])
    
    print(f"📋 Schema: {schema}")
    print(f"📋 Rows count: {len(rows)}")
    
    # Las filas ya vienen como diccionarios, no como arrays
    # Ejemplo: [{"numero":1,"texto":"test"}] en lugar de [[1,"test"]]
    result = rows  # Ya están en el formato correcto!
    
    print(f"✓ {len(result)} filas retornadas")
    return result

@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status": "ok",
        "service": "Dremio Cloud Proxy (Job API)",
        "project_id": PROJECT_ID
    })

@app.route("/query", methods=["POST", "GET"])
def query():
    """Endpoint para Grafana"""
    try:
        # Soportar POST (JSON) y GET (query param)
        if request.method == "POST":
            data = request.get_json() or {}
            sql = data.get("sql", "")
        else:
            sql = request.args.get("sql", "")
        
        if not sql:
            return jsonify({"error": "No SQL provided"}), 400
        
        resultado = ejecutar_query_dremio(sql)
        return jsonify(resultado)
        
    except Exception as e:
        print(f"✗ Error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    print("=" * 70)
    print("🚀 PROXY DREMIO CLOUD (Job API)")
    print("=" * 70)
    print(f"Project ID: {PROJECT_ID}")
    print(f"API Base: {DREMIO_API_BASE}")
    print(f"Proxy: http://0.0.0.0:5000")
    print(f"Para Grafana (Docker): http://host.docker.internal:5000")
    print("=" * 70)
    print("\n📊 Endpoints:")
    print("  GET  /         - Info")
    print("  POST /query    - SQL Query (body: {\"sql\": \"...\"})")
    print("  GET  /query    - SQL Query (param: ?sql=...)")
    print("  GET  /health   - Health check")
    print("\n💡 Ejemplo:")
    print('  POST http://host.docker.internal:5000/query')
    print('  Body: {"sql": "SELECT * FROM vds.vds_tabla_analitica_final LIMIT 10"}')
    print("=" * 70)
    
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
