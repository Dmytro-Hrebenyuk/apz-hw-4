from fastapi import FastAPI
import hazelcast
import os
import time
import httpx

app = FastAPI()

HZ_HOST = os.getenv("HZ_HOST", "hazelcast-1")
SERVICE_NAME = os.getenv("SERVICE_NAME", "logging-service-1")
SERVICE_URL = os.getenv("SERVICE_URL", "http://logging-service-1:8081")
CONFIG_URL = os.getenv("CONFIG_URL", "http://config-server:8085")

transactions_map = None


def get_map():
    global transactions_map
    if transactions_map is None:
        client = hazelcast.HazelcastClient(
            cluster_members=[f"{HZ_HOST}:5701"],
            cluster_name="dev",
        )
        transactions_map = client.get_map("transactions").blocking()
        print(f"[{SERVICE_NAME}] Connected to Hazelcast!")
    return transactions_map


@app.on_event("startup")
async def startup():
    # Connect Hazelcast
    for i in range(10):
        try:
            get_map()
            break
        except Exception as e:
            print(f"[{SERVICE_NAME}] HZ not ready ({i+1}/10): {e}")
            time.sleep(3)

    # Register in config-server
    import asyncio
    await asyncio.sleep(2)
    for attempt in range(10):
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                await client.post(f"{CONFIG_URL}/register", json={
                    "service": "logging-service",
                    "url": SERVICE_URL
                })
                print(f"[{SERVICE_NAME}] Registered in config-server as {SERVICE_URL}")
                return
        except Exception as e:
            print(f"[{SERVICE_NAME}] Config-server not ready ({attempt+1}/10): {e}")
            await asyncio.sleep(2)


@app.post("/log")
def log_transaction(msg: dict):
    tx_map = get_map()
    key = str(msg["transaction_ID"])
    tx_map.put(key, msg)
    print(f"[{SERVICE_NAME}] Stored transaction {key}: {msg}")
    return {"status": "ok", "stored_by": SERVICE_NAME}


@app.get("/user/{user_id}")
def get_transactions(user_id: int):
    tx_map = get_map()
    all_values = tx_map.values()
    user_transactions = [t for t in all_values if t["user_Id"] == user_id]
    print(f"[{SERVICE_NAME}] GET user={user_id}: {len(user_transactions)} found")
    return {"transactions": user_transactions, "served_by": SERVICE_NAME}


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME}