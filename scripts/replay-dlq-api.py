from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any


API_URL = os.getenv("API_URL", "http://localhost:8000").rstrip("/")
MAX_MESSAGES = int(os.getenv("API_REPLAY_MAX_MESSAGES", "5"))
SCAN_LIMIT = int(os.getenv("API_REPLAY_SCAN_LIMIT", "200"))
TIMEOUT_SECONDS = float(os.getenv("API_REPLAY_TIMEOUT_SECONDS", "8"))
API_TOKEN = os.getenv("API_TOKEN", "")


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


def main() -> None:
    if MAX_MESSAGES < 1:
        raise RuntimeError("API_REPLAY_MAX_MESSAGES must be greater than 0")

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
