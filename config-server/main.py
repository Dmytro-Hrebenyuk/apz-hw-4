from fastapi import FastAPI
from collections import defaultdict

app = FastAPI()

registry = defaultdict(list)


@app.post("/register")
def register(data: dict):
    service = data["service"]
    url = data["url"]
    if url not in registry[service]:
        registry[service].append(url)
    print(f"[CONFIG] Registered {service} -> {url}. All: {registry[service]}")
    return {"status": "ok"}


@app.get("/services/{service}")
def get_service(service: str):
    urls = registry.get(service, [])
    print(f"[CONFIG] Lookup {service} -> {urls}")
    return {"urls": urls}


@app.get("/registry")
def get_all():
    return dict(registry)