from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


class KafkaConnectHttpError(RuntimeError):
    def __init__(self, status_code: int, path: str, detail: str) -> None:
        self.status_code = status_code
        super().__init__(
            f"Kafka Connect returned HTTP {status_code} for {path}: {detail}"
        )


CONNECT_URL = os.getenv("CONNECT_URL", "http://localhost:8083").strip().rstrip("/")
CONNECTOR_NAME = os.getenv(
    "CONNECTOR_NAME", "merchant-risk-profiles-source"
).strip()
configured_path = os.getenv("CONNECTOR_CONFIG_PATH")
if configured_path is not None:
    if not configured_path.strip():
        raise RuntimeError("CONNECTOR_CONFIG_PATH must not be blank")
    CONFIG_PATH = Path(configured_path.strip())
    if not CONFIG_PATH.is_file():
        raise RuntimeError(f"connector config file not found: {CONFIG_PATH}")
else:
    CONFIG_PATH = Path("/cdc/postgres-source-connector.json")
    if not CONFIG_PATH.is_file():
        CONFIG_PATH = Path("cdc/postgres-source-connector.json")
    if not CONFIG_PATH.is_file():
        raise RuntimeError(f"connector config file not found: {CONFIG_PATH}")


def request(method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        f"{CONNECT_URL}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            body = response.read()
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise KafkaConnectHttpError(error.code, path, detail) from error
    return None if not body else json.loads(body)


def wait_until_ready() -> None:
    for _ in range(60):
        try:
            request("GET", "/connectors")
            return
        except (urllib.error.URLError, TimeoutError):
            time.sleep(2)
    raise RuntimeError(f"Kafka Connect is not ready: {CONNECT_URL}")


def wait_until_running(connector_path: str) -> dict[str, Any]:
    latest: dict[str, Any] = {}
    for _ in range(60):
        try:
            latest = request("GET", f"{connector_path}/status")
        except KafkaConnectHttpError as error:
            if error.status_code != 404:
                raise
            time.sleep(2)
            continue
        connector_state = latest.get("connector", {}).get("state")
        task_states = [task.get("state") for task in latest.get("tasks", [])]
        if connector_state == "FAILED" or "FAILED" in task_states:
            raise RuntimeError(f"connector failed: {json.dumps(latest, ensure_ascii=False)}")
        if connector_state == "RUNNING" and task_states and all(
            state == "RUNNING" for state in task_states
        ):
            return latest
        time.sleep(2)
    raise RuntimeError(
        f"connector did not reach RUNNING state: {json.dumps(latest, ensure_ascii=False)}"
    )


def main() -> None:
    if not CONNECT_URL or not CONNECTOR_NAME:
        raise RuntimeError("CONNECT_URL and CONNECTOR_NAME must not be blank")
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    if not isinstance(config, dict):
        raise RuntimeError("connector config must be a JSON object")
    connector_class = config.get("connector.class")
    if (
        not isinstance(connector_class, str)
        or not connector_class.strip()
        or "config" in config
        or "name" in config
    ):
        raise RuntimeError("connector config must be the flat PUT /config payload")

    wait_until_ready()
    connector_path = f"/connectors/{urllib.parse.quote(CONNECTOR_NAME, safe='')}"
    request("PUT", f"{connector_path}/config", config)
    status = wait_until_running(connector_path)
    print(json.dumps(status, indent=2, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
