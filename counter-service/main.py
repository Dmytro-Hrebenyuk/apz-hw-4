from fastapi import FastAPI
import hazelcast
import psycopg2
import psycopg2.extras
import os
import time
import threading
import httpx

app = FastAPI()

HZ_HOST = os.getenv("HZ_HOST", "hazelcast-1")
SERVICE_NAME = os.getenv("SERVICE_NAME", "counter-service")
SERVICE_URL = os.getenv("SERVICE_URL", "http://counter-service:8082")
CONFIG_URL = os.getenv("CONFIG_URL", "http://config-server:8085")

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "counterdb")
DB_USER = os.getenv("DB_USER", "user")
DB_PASS = os.getenv("DB_PASS", "password")

QUEUE_NAME = "counter-queue"

db_conn = None


def get_conn():
    global db_conn
    if db_conn is None or db_conn.closed:
        db_conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT,
            dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        db_conn.autocommit = True
    return db_conn


def init_db():
    for i in range(10):
        try:
            c = get_conn()
            with c.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS balances (
                        user_id BIGINT PRIMARY KEY,
                        balance NUMERIC DEFAULT 0
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS transactions (
                        transaction_id BIGINT PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        amount NUMERIC NOT NULL,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
            print("[COUNTER] DB initialized")
            return
        except Exception as e:
            print(f"[COUNTER] DB not ready ({i+1}/10): {e}")
            time.sleep(3)
    raise RuntimeError("Cannot connect to PostgreSQL")


def get_hz_queue():
    for i in range(10):
        try:
            client = hazelcast.HazelcastClient(
                cluster_members=[f"{HZ_HOST}:5701"],
                cluster_name="dev",
            )
            q = client.get_queue(QUEUE_NAME).blocking()
            print(f"[COUNTER] Connected to Hazelcast Queue '{QUEUE_NAME}'")
            return q
        except Exception as e:
            print(f"[COUNTER] HZ not ready ({i+1}/10): {e}")
            time.sleep(3)
    raise RuntimeError("Cannot connect to Hazelcast")


def process_message(msg: dict):
    user_id = msg["user_Id"]
    amount = msg["amount"]
    transaction_id = msg["transaction_ID"]
    c = get_conn()
    with c.cursor() as cur:
        cur.execute(
            "INSERT INTO transactions (transaction_id, user_id, amount) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (transaction_id, user_id, amount)
        )
        cur.execute("""
            INSERT INTO balances (user_id, balance) VALUES (%s, %s)
            ON CONFLICT (user_id)
            DO UPDATE SET balance = balances.balance + EXCLUDED.balance
            RETURNING balance
        """, (user_id, amount))
        balance = cur.fetchone()[0]
    print(f"[COUNTER] Processed tx={transaction_id} user={user_id} amount={amount} => balance={balance}")


def queue_consumer():
    queue = get_hz_queue()
    print("[COUNTER] Queue consumer started, waiting for messages...")
    while True:
        try:
            msg = queue.poll(timeout=1)
            if msg is not None:
                print(f"[COUNTER] Dequeued: {msg}")
                process_message(msg)
        except Exception as e:
            print(f"[COUNTER] Consumer error: {e}")
            time.sleep(1)


@app.on_event("startup")
async def startup():
    init_db()

    t = threading.Thread(target=queue_consumer, daemon=True)
    t.start()

    import asyncio
    await asyncio.sleep(2)
    for attempt in range(10):
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                await client.post(f"{CONFIG_URL}/register", json={
                    "service": "counter-service",
                    "url": SERVICE_URL
                })
                print(f"[COUNTER] Registered in config-server as {SERVICE_URL}")
                return
        except Exception as e:
            print(f"[COUNTER] Config-server not ready ({attempt+1}/10): {e}")
            await asyncio.sleep(2)


@app.get("/balance/{user_id}")
def get_balance(user_id: int):
    c = get_conn()
    with c.cursor() as cur:
        cur.execute("SELECT balance FROM balances WHERE user_id = %s", (user_id,))
        row = cur.fetchone()
    return {"balance": float(row[0]) if row else None}


@app.get("/balances")
def get_all_balances():
    c = get_conn()
    with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT user_id, balance FROM balances")
        rows = cur.fetchall()
    return {str(r["user_id"]): float(r["balance"]) for r in rows}


@app.get("/health")
def health():
    return {"status": "ok"}