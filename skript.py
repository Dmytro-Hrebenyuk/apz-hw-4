import httpx
import time
import statistics
import asyncio

BASE_URL = "http://localhost:8080"


def send_transactions_demo():
    print("\n=== Sending 10 demo transactions ===")
    with httpx.Client() as client:
        for i in range(1, 11):
            payload = {"user_Id": 1, "amount": i * 10}
            resp = client.post(f"{BASE_URL}/transaction", json=payload)
            data = resp.json()
            print(f"msg{i}: tx_id={data['transaction_ID']} balance={data['balance']} "
                  f"logging_ms={data['logging_ms']:.1f} counter_ms={data['counter_ms']:.1f} "
                  f"via={data.get('logging_service_used','?').split('/')[-1]}")


def read_transactions(user_id=1):
    print(f"\n=== GET transactions for user {user_id} ===")
    with httpx.Client() as client:
        resp = client.get(f"{BASE_URL}/transactions/{user_id}")
        data = resp.json()
        print(f"Served by: {data.get('served_by', '?')}")
        for t in data.get("transactions", []):
            print(f"  tx={t['transaction_ID']} amount={t['amount']}")


async def send_one(client, i):
    payload = {"user_Id": 99, "amount": 1}
    start = time.time()
    resp = await client.post(f"{BASE_URL}/transaction", json=payload)
    elapsed = (time.time() - start) * 1000
    return elapsed, resp.status_code


async def performance_test(n=100, concurrency=10):
    print(f"\n=== Performance test: {n} requests, concurrency={concurrency} ===")
    semaphore = asyncio.Semaphore(concurrency)
    results = []

    async def bounded_send(client, i):
        async with semaphore:
            return await send_one(client, i)

    async with httpx.AsyncClient(timeout=10.0) as client:
        start_all = time.time()
        tasks = [bounded_send(client, i) for i in range(n)]
        raw = await asyncio.gather(*tasks, return_exceptions=True)
        total_time = time.time() - start_all

    for r in raw:
        if isinstance(r, tuple):
            results.append(r[0])

    if results:
        print(f"Total time:   {total_time:.2f}s")
        print(f"Requests OK:  {len(results)}/{n}")
        print(f"Throughput:   {len(results)/total_time:.1f} req/s")
        print(f"Latency avg:  {statistics.mean(results):.1f}ms")
        print(f"Latency p50:  {statistics.median(results):.1f}ms")
        print(f"Latency p95:  {sorted(results)[int(len(results)*0.95)]:.1f}ms")
        print(f"Latency max:  {max(results):.1f}ms")


if __name__ == "__main__":
    send_transactions_demo()

    read_transactions(user_id=1)

    asyncio.run(performance_test(n=100, concurrency=10))