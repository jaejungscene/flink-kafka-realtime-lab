import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException
from fastapi.testclient import TestClient
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

    def test_replay_request_rejects_unknown_fields(self) -> None:
        with self.assertRaises(ValidationError):
            main.DlqReplayRequest.model_validate({"dryRun": False})

    def test_topic_query_parameters_are_bounded(self) -> None:
        client = TestClient(main.app)
        response = client.get("/topics/transactions.raw/messages?limit=0")

        self.assertEqual(response.status_code, 422)

    def test_readiness_rejects_missing_or_failed_required_topics(self) -> None:
        metadata = SimpleNamespace(
            topics={
                "transactions.raw": SimpleNamespace(error=None),
                "transactions.dlq": SimpleNamespace(error="leader unavailable"),
            }
        )

        unavailable = main._unavailable_topics(
            metadata,
            frozenset({"transactions.raw", "transactions.dlq", "transactions.replay"}),
        )

        self.assertEqual(unavailable, ["transactions.dlq", "transactions.replay"])

    def test_readiness_requires_all_exposed_topics(self) -> None:
        metadata = SimpleNamespace(
            topics={topic: SimpleNamespace(error=None) for topic in main.READABLE_TOPICS}
        )
        admin = SimpleNamespace(list_topics=lambda timeout: metadata)

        with patch.object(main, "AdminClient", return_value=admin):
            self.assertEqual(main.ready(), {"status": "ready"})

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

    def test_selected_dlq_records_must_be_unique(self) -> None:
        duplicate = [
            main.DlqRecordRef(partition=0, offset=1),
            main.DlqRecordRef(partition=0, offset=1),
        ]

        with self.assertRaises(HTTPException) as raised:
            main.read_records_at_offsets("transactions.dlq", duplicate, 1.0)

        self.assertEqual(raised.exception.status_code, 400)

    def test_selected_dlq_records_reject_unknown_partitions(self) -> None:
        records = [main.DlqRecordRef(partition=3, offset=1)]

        with patch.object(main, "_topic_partitions", return_value=[0, 1]):
            with self.assertRaises(HTTPException) as raised:
                main.read_records_at_offsets("transactions.dlq", records, 1.0)

        self.assertEqual(raised.exception.status_code, 404)


if __name__ == "__main__":
    unittest.main()
