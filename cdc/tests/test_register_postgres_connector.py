from __future__ import annotations

import unittest
from unittest.mock import patch

from cdc import register_postgres_connector as registrar


class WaitUntilRunningTest(unittest.TestCase):
    @patch.object(registrar.time, "sleep")
    @patch.object(registrar, "request")
    def test_retries_transient_status_not_found(self, request, sleep) -> None:
        running_status = {
            "connector": {"state": "RUNNING"},
            "tasks": [{"id": 0, "state": "RUNNING"}],
        }
        request.side_effect = [
            registrar.KafkaConnectHttpError(404, "/connectors/source/status", "missing"),
            running_status,
        ]

        result = registrar.wait_until_running("/connectors/source")

        self.assertEqual(result, running_status)
        self.assertEqual(request.call_count, 2)
        sleep.assert_called_once_with(2)

    @patch.object(registrar.time, "sleep")
    @patch.object(registrar, "request")
    def test_does_not_retry_other_http_errors(self, request, sleep) -> None:
        request.side_effect = registrar.KafkaConnectHttpError(
            500, "/connectors/source/status", "internal error"
        )

        with self.assertRaises(registrar.KafkaConnectHttpError):
            registrar.wait_until_running("/connectors/source")

        request.assert_called_once_with("GET", "/connectors/source/status")
        sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
