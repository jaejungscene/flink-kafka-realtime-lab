import unittest
from unittest.mock import patch

from fastapi import HTTPException
from pydantic import ValidationError

from src import main


class ApiContractTest(unittest.TestCase):
    def setUp(self) -> None:
        main._metrics_cache = None

    def test_metrics_are_cached_between_scrapes(self) -> None:
        with patch.object(
            main,
            "_collect_kafka_metrics",
            return_value="realtime_lab_up 1\n",
        ) as collect:
            first = main.metrics()
            second = main.metrics()

        self.assertEqual(first.body, b"realtime_lab_up 1\n")
        self.assertEqual(second.body, first.body)
        collect.assert_called_once_with()

    def test_prometheus_label_values_are_escaped(self) -> None:
        self.assertEqual(main._prometheus_label('a\\b"\nc'), 'a\\\\b\\"\\nc')

    def test_execution_requires_explicit_confirmation(self) -> None:
        request = main.DlqReplayRequest(dry_run=False, replay_run_id="run-123")

        with self.assertRaises(HTTPException) as raised:
            main.replay_dlq(request)

        self.assertEqual(raised.exception.status_code, 400)

    def test_replay_run_id_is_validated(self) -> None:
        with self.assertRaises(ValidationError):
            main.DlqReplayRequest(replay_run_id="contains spaces")

    def test_api_token_is_optional_locally_and_enforced_when_configured(self) -> None:
        main._require_api_token(None)

        with patch.object(main, "API_TOKEN", "secret-token"):
            with self.assertRaises(HTTPException) as raised:
                main._require_api_token("wrong-token")
            main._require_api_token("secret-token")

        self.assertEqual(raised.exception.status_code, 401)

    def test_topics_outside_allowlist_are_not_exposed(self) -> None:
        with self.assertRaises(HTTPException) as raised:
            main._ensure_readable_topic("connect-configs")

        self.assertEqual(raised.exception.status_code, 404)


if __name__ == "__main__":
    unittest.main()
