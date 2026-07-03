import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path


SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8085").rstrip("/")
SCHEMA_DIR = Path(os.getenv("SCHEMA_DIR", "/schemas"))
if not SCHEMA_DIR.exists():
    SCHEMA_DIR = Path("schemas")
SCHEMAS = [
    ("transactions.raw-value", SCHEMA_DIR / "transactions-raw-value.avsc"),
    ("transactions.replay-value", SCHEMA_DIR / "transactions-raw-value.avsc"),
    ("alerts.fraud-value", SCHEMA_DIR / "alerts-fraud-value.avsc"),
    ("transactions.aggregates-value", SCHEMA_DIR / "transactions-aggregates-value.avsc"),
    ("transactions.dlq-value", SCHEMA_DIR / "transactions-dlq-value.avsc"),
]


def request(method: str, path: str, payload: dict | None = None) -> bytes:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        f"{SCHEMA_REGISTRY_URL}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        return response.read()


def wait_until_ready() -> None:
    for _ in range(60):
        try:
            request("GET", "/subjects")
            return
        except (urllib.error.URLError, TimeoutError):
            time.sleep(2)
    raise RuntimeError(f"schema registry is not ready: {SCHEMA_REGISTRY_URL}")


def register_schema(subject: str, schema_file: Path) -> None:
    schema = schema_file.read_text(encoding="utf-8")
    response = request("POST", f"/subjects/{subject}/versions", {"schema": schema})
    print(f"{subject}: {response.decode('utf-8')}", flush=True)


def main() -> None:
    wait_until_ready()
    for subject, schema_file in SCHEMAS:
        register_schema(subject, schema_file)
    print(request("GET", "/subjects").decode("utf-8"), flush=True)


if __name__ == "__main__":
    main()
