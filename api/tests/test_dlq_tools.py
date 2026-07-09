import unittest

from src.dlq_tools import normalize_for_replay, summarize_dlq_records


class DlqToolsTest(unittest.TestCase):
    def test_normalize_for_replay_repairs_missing_fields(self) -> None:
        event = normalize_for_replay(
            {"rawValue": '{"amount": -10, "mlFraudScore": 0.5}'},
            "transactions.dlq",
            1,
            42,
            "run-1",
        )

        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event["amount"], 0.0)
        self.assertEqual(event["userId"], "user-replayed")
        self.assertEqual(event["replayId"], "run-1-1-42")
        self.assertEqual(event["replaySourceOffset"], 42)

    def test_normalize_for_replay_rejects_malformed_raw_value(self) -> None:
        event = normalize_for_replay(
            {"rawValue": '{"broken": true'},
            "transactions.dlq",
            0,
            1,
            "run-1",
        )

        self.assertIsNone(event)

    def test_normalize_for_replay_rejects_invalid_numeric_fields(self) -> None:
        event = normalize_for_replay(
            {"rawValue": '{"amount": "bad", "mlFraudScore": 0.5, "ipRisk": 10}'},
            "transactions.dlq",
            0,
            1,
            "run-1",
        )

        self.assertIsNone(event)

    def test_summarize_dlq_records_groups_error_types_and_replayability(self) -> None:
        summary = summarize_dlq_records(
            [
                {
                    "topic": "transactions.dlq",
                    "partition": 0,
                    "offset": 1,
                    "key": "a",
                    "value": {
                        "errorType": "PARSE_OR_VALIDATION_ERROR",
                        "reason": "eventId is required",
                        "rawValue": '{"amount": 10}',
                    },
                },
                {
                    "topic": "transactions.dlq",
                    "partition": 0,
                    "offset": 2,
                    "key": "b",
                    "value": {
                        "errorType": "PARSE_OR_VALIDATION_ERROR",
                        "reason": "malformed json",
                        "rawValue": '{"broken": true',
                    },
                },
            ]
        )

        self.assertEqual(summary["scanned"], 2)
        self.assertEqual(summary["replayable"], 1)
        self.assertEqual(summary["notReplayable"], 1)
        self.assertEqual(summary["byErrorType"][0]["errorType"], "PARSE_OR_VALIDATION_ERROR")
        self.assertEqual(summary["byErrorType"][0]["count"], 2)
        self.assertEqual(summary["byErrorType"][0]["replayable"], 1)


if __name__ == "__main__":
    unittest.main()
