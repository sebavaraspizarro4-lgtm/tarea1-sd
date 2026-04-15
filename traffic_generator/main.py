import numpy as np
import requests
import time
import os
import json

# Tus variables actuales
CACHE_URL = os.getenv("CACHE_SERVICE_URL", "http://response_generator:5001")
# ... resto de tus variables ...

# --- INSERTA ESTA FUNCIÓN AQUÍ ---
def enviar_con_reintento(payload):
    for i in range(15):  # Damos más intentos porque el dataset es pesado
        try:
            # El endpoint debe ser /query según el flujo del sistema [cite: 40, 64]
            response = requests.post(f"{CACHE_URL}/query", json=payload, timeout=5)
            return response
        except requests.exceptions.ConnectionError:
            print(f"Esperando al servidor (Intento {i+1})...")
            time.sleep(3)
    return None
DIST        = os.getenv("DISTRIBUTION", "zipf")
NUM_QUERIES = int(os.getenv("NUM_QUERIES", 1000))
ZIPF_S      = float(os.getenv("ZIPF_S", 1.5))

ZONES       = ["Z1", "Z2", "Z3", "Z4", "Z5"]
QTYPES      = ["Q1", "Q2", "Q3", "Q4", "Q5"]
CONF_VALUES = [0.0, 0.5, 0.7]

def zipf_weights(n, s):
    w = [1 / ((i + 1) ** s) for i in range(n)]
    total = sum(w)
    return [x / total for x in w]

def generate_query(distribution):
    if distribution == "zipf":
        weights = zipf_weights(len(ZONES), ZIPF_S)
        zone_id = np.random.choice(ZONES, p=weights)
    else:
        zone_id = np.random.choice(ZONES)

    qtype = np.random.choice(QTYPES)
    conf  = float(np.random.choice(CONF_VALUES))
    query = {"query_type": qtype, "zone_id": zone_id, "confidence_min": conf}

    if qtype == "Q4":
        otros = [z for z in ZONES if z != zone_id]
        query["zone_b"] = np.random.choice(otros)
    if qtype == "Q5":
        query["bins"] = int(np.random.choice([3, 5, 10]))

    return query

def run():
    print(f"Iniciando generador | distribucion={DIST} | consultas={NUM_QUERIES}")
    time.sleep(8)

    results = []
    for i in range(NUM_QUERIES):
        query = generate_query(DIST)
        try:
            start   = time.time()
            resp    = requests.post(f"{CACHE_URL}/query", json=query, timeout=10)
            elapsed = (time.time() - start) * 1000
            data    = resp.json()
            results.append({"query": query, "source": data.get("source"), "latency_ms": elapsed})
            if (i + 1) % 100 == 0:
                hits = sum(1 for r in results if r["source"] == "cache")
                avg  = np.mean([r["latency_ms"] for r in results])
                print(f"[{i+1}/{NUM_QUERIES}] hit_rate={hits/(i+1):.2%} latencia_avg={avg:.1f}ms")
        except Exception as e:
            print(f"Error consulta {i}: {e}")
        time.sleep(0.01)

    os.makedirs("/results", exist_ok=True)
    with open("/results/traffic_results.json", "w") as f:
        json.dump({"distribution": DIST, "results": results}, f)
    print("Resultados guardados en /results/traffic_results.json")

if __name__ == "__main__":
    run()
