from fastapi import FastAPI, HTTPException
import httpx
import time
import random
import os

app = FastAPI()

CONFIG_URL = os.getenv("CONFIG_URL", "http://config-server:8085")
SERVICE_URL = os.getenv("SERVICE_URL", "http://facade-service:8080")

last_logging_call_ms = 0
last_counter_call_ms = 0


@app.on_event("startup")
async def startup():
    await asyncio_sleep_and_register()


async def asyncio_sleep_and_register():
    import asyncio
    await asyncio.sleep(2)
    for attempt in range(10):
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                await client.post(f"{CONFIG_URL}/register", json={
                    "service": "facade-service",
                    "url": SERVICE_URL
                })
                print(f"[FACADE] Registered in config-server")
                return
        except Exception as e:
            print(f"[FACADE] Config-server not ready ({attempt+1}/10): {e}")
            import asyncio
            await asyncio.sleep(2)


async def get_service_urls(service: str) -> list:
    async with httpx.AsyncClient(timeout=3.0) as client:
        resp = await client.get(f"{CONFIG_URL}/services/{service}")
        return resp.json().get("urls", [])


async def call_with_fallback(client, urls, method, path, **kwargs):
    shuffled = urls.copy()
    random.shuffle(shuffled)
    for url in shuffled:
        try:
            fn = client.post if method == "POST" else client.get
            resp = await fn(f"{url}{path}", **kwargs)
            resp.raise_for_status()
            return resp, url
        except Exception as e:
            print(f"[FACADE] {url} unavailable: {e}, trying next...")
    raise HTTPException(status_code=503, detail=f"All {path} services unavailable")


@app.post("/transaction")
async def transaction(msg: dict):
    global last_logging_call_ms, last_counter_call_ms

    user_id = msg["user_Id"]
    amount = msg["amount"]
    transaction_id = int(time.time() * 1000)

    payload = {
        "transaction_ID": transaction_id,
        "user_Id": user_id,
        "amount": amount,
    }

    logging_urls = await get_service_urls("logging-service")
    if not logging_urls:
        raise HTTPException(status_code=503, detail="No logging-service registered")

    counter_urls = await get_service_urls("counter-service")
    if not counter_urls:
        raise HTTPException(status_code=503, detail="No counter-service registered")

    async with httpx.AsyncClient(timeout=5.0) as client:
        # POST to logging-service (sync, need confirmation)
        start = time.time()
        resp, used_logging = await call_with_fallback(
            client, logging_urls, "POST", "/log", json=payload
        )
        last_logging_call_ms = (time.time() - start) * 1000
        print(f"[FACADE] Logged via {used_logging} ({last_logging_call_ms:.1f}ms)")

        # POST to counter-service — async via Hazelcast Queue (fire and forget style)
        start = time.time()
        resp, used_counter = await call_with_fallback(
            client, counter_urls, "POST", "/enqueue", json=payload
        )
        last_counter_call_ms = (time.time() - start) * 1000
        print(f"[FACADE] Enqueued to counter via {used_counter} ({last_counter_call_ms:.1f}ms)")

    return {
        "transaction_ID": transaction_id,
        "status": "queued",
        "logging_service_used": used_logging,
        "logging_ms": last_logging_call_ms,
        "counter_ms": last_counter_call_ms,
    }


@app.get("/transactions/{user_id}")
async def get_transactions(user_id: int):
    logging_urls = await get_service_urls("logging-service")
    if not logging_urls:
        raise HTTPException(status_code=503, detail="No logging-service registered")

    async with httpx.AsyncClient(timeout=5.0) as client:
        resp, used = await call_with_fallback(
            client, logging_urls, "GET", f"/user/{user_id}"
        )
        return resp.json()


@app.get("/balance/{user_id}")
async def get_balance(user_id: int):
    counter_urls = await get_service_urls("counter-service")
    if not counter_urls:
        raise HTTPException(status_code=503, detail="No counter-service registered")

    async with httpx.AsyncClient(timeout=5.0) as client:
        resp, used = await call_with_fallback(
            client, counter_urls, "GET", f"/balance/{user_id}"
        )
        return resp.json()


@app.get("/timing")
def get_timing():
    return {
        "last_logging_call_ms": last_logging_call_ms,
        "last_counter_call_ms": last_counter_call_ms,
    }