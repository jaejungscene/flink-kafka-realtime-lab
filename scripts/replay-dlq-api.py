from __future__ import annotations

import json
import math
import os
import urllib.error
import urllib.request
from typing import Any


API_URL = os.getenv("API_URL", "http://localhost:8000").strip().rstrip("/")


def integer_setting(name: str, fallback: int) -> int:
    raw_value = os.getenv(name)
    try:
        return fallback if raw_value is None or not raw_value.strip() else int(raw_value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer: {raw_value}") from exc


def float_setting(name: str, fallback: float) -> float:
    raw_value = os.getenv(name)
    try:
        return fallback if raw_value is None or not raw_value.strip() else float(raw_value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a number: {raw_value}") from exc


MAX_MESSAGES = integer_setting("API_REPLAY_MAX_MESSAGES", 5)
SCAN_LIMIT = integer_setting("API_REPLAY_SCAN_LIMIT", 200)
TIMEOUT_SECONDS = float_setting("API_REPLAY_TIMEOUT_SECONDS", 8.0)
API_TOKEN = os.getenv("API_TOKEN", "").strip()


def post(payload: dict[str, Any]) -> dict[str, Any]:
    headers = {"content-type": "application/json"}
    if API_TOKEN:
        headers["X-API-Token"] = API_TOKEN

    request = urllib.request.Request(
        f"{API_URL}/dlq/replay",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers=headers,
    )
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS + 2) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"replay API returned HTTP {error.code}: {detail}") from error
    except urllib.error.URLError as error:
        raise RuntimeError(f"replay API is not reachable: {error.reason}") from error


def main() -> None:
    if not API_URL:
        raise RuntimeError("API_URL must not be blank")
    if not 1 <= MAX_MESSAGES <= 200:
        raise RuntimeError("API_REPLAY_MAX_MESSAGES must be between 1 and 200")
    if not 1 <= SCAN_LIMIT <= 1000:
        raise RuntimeError("API_REPLAY_SCAN_LIMIT must be between 1 and 1000")
    if not math.isfinite(TIMEOUT_SECONDS) or not 1.0 <= TIMEOUT_SECONDS <= 30.0:
        raise RuntimeError("API_REPLAY_TIMEOUT_SECONDS must be between 1 and 30")

    preview = post(
        {
            "max_messages": MAX_MESSAGES,
            "scan_limit": max(SCAN_LIMIT, MAX_MESSAGES),
            "timeout_seconds": TIMEOUT_SECONDS,
            "from_beginning": True,
            "dry_run": True,
        }
    )
    selected = [
        {"partition": record["partition"], "offset": record["offset"]}
        for record in preview.get("records", [])
    ]
    if not selected:
        raise RuntimeError("preview returned no safely replayable DLQ records")

    result = post(
        {
            "max_messages": len(selected),
            "timeout_seconds": TIMEOUT_SECONDS,
            "dry_run": False,
            "confirm": True,
            "replay_run_id": preview["runId"],
            "records": selected,
        }
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
