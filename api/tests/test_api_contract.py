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
        main._metrics_last_success_timestamp_seconds = 0.0

    def test_metrics_are_cached_between_scrapes(self) -> None:
        with patch.object(
            main,
            "_collect_kafka_metrics",
            return_value="realtime_lab_up 1\n",
        ) as collect:
            first = main.metrics()
            second = main.metrics()

        self.assertTrue(first.body.startswith(b"realtime_lab_up 1\n"))
        self.assertEqual(second.body, first.body)
        collect.assert_called_once_with()

    def test_metrics_include_collection_freshness(self) -> None:
        with (
            patch.object(
                main,
                "_collect_kafka_metrics",
                return_value="realtime_lab_kafka_up 1\n",
            ),
            patch.object(main.time, "time", return_value=1_800_000_000.0),
        ):
            payload = main.metrics().body.decode()

        self.assertIn("realtime_lab_metrics_collection_duration_seconds", payload)
        self.assertIn("realtime_lab_metrics_collection_timestamp_seconds 1800000000.000", payload)
        self.assertIn(
            "realtime_lab_metrics_last_success_timestamp_seconds 1800000000.000",
            payload,
        )

    def test_prometheus_label_values_are_escaped(self) -> None:
        self.assertEqual(main._prometheus_label('a\\b"\nc'), 'a\\\\b\\"\\nc')

    def test_metrics_expose_all_collection_statuses_when_kafka_is_down(self) -> None:
        admin = SimpleNamespace(
            list_topics=lambda timeout: (_ for _ in ()).throw(main.KafkaException("down"))
        )

        with patch.object(main, "AdminClient", return_value=admin):
            payload = main._collect_kafka_metrics()

        self.assertIn("realtime_lab_kafka_up 0", payload)
        self.assertIn("realtime_lab_metrics_partition_errors 0", payload)
        self.assertIn("realtime_lab_metrics_group_offset_errors 0", payload)

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

    def test_selected_dlq_records_are_read_in_one_partition_assignment(self) -> None:
        class FakeMessage:
            def __init__(self, partition: int, offset: int) -> None:
                self._partition = partition
                self._offset = offset

            def error(self):
                return None

            def topic(self) -> str:
                return "transactions.dlq"

            def partition(self) -> int:
                return self._partition

            def offset(self) -> int:
                return self._offset

            def key(self):
                return b"PARSE_OR_VALIDATION_ERROR"

            def value(self):
                return b'{"errorType":"PARSE_OR_VALIDATION_ERROR"}'

        class FakeConsumer:
            def __init__(self) -> None:
                self.messages = iter(
                    [FakeMessage(0, 4), FakeMessage(1, 7), FakeMessage(0, 5)]
                )
                self.assignments = []
                self.paused = []

            def get_watermark_offsets(self, topic_partition, timeout):
                return (0, 20)

            def assign(self, partitions) -> None:
                self.assignments.append(partitions)

            def poll(self, timeout):
                return next(self.messages, None)

            def pause(self, partitions) -> None:
                self.paused.extend(partitions)

            def close(self) -> None:
                pass

        consumer = FakeConsumer()
        records = [
            main.DlqRecordRef(partition=1, offset=7),
            main.DlqRecordRef(partition=0, offset=5),
        ]

        with (
            patch.object(main, "_topic_partitions", return_value=[0, 1]),
            patch.object(main, "Consumer", return_value=consumer),
        ):
            result = main.read_records_at_offsets("transactions.dlq", records, 1.0)

        self.assertEqual(len(consumer.assignments), 1)
        self.assertEqual(
            [(item.partition, item.offset) for item in consumer.assignments[0]],
            [(0, 5), (1, 7)],
        )
        self.assertEqual(
            [(item["partition"], item["offset"]) for item in result],
            [(1, 7), (0, 5)],
        )


if __name__ == "__main__":
    unittest.main()
