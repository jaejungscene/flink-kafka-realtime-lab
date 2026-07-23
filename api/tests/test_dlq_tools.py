import unittest

from realtime_lab.dlq_tools import normalize_for_replay, summarize_dlq_records


class DlqToolsTest(unittest.TestCase):
    def test_normalize_for_replay_repairs_missing_fields(self) -> None:
        event = normalize_for_replay(
            {
                "errorType": "PARSE_OR_VALIDATION_ERROR",
                "rawValue": (
                    '{"eventId":"","userId":"user-1","eventTime":1760000000000,'
                    '"amount":10,"mlFraudScore":0.5,"ipRisk":10}'
                ),
            },
            "transactions.dlq",
            1,
            42,
            "run-1",
        )

        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event["amount"], 10.0)
        self.assertEqual(event["userId"], "user-1")
        self.assertEqual(event["eventTime"], 1760000000000)
        self.assertTrue(event["eventId"].startswith("replay-"))
        self.assertEqual(event["replayId"], "run-1-1-42")
        self.assertEqual(event["replaySourceOffset"], 42)

    def test_normalize_for_replay_rejects_malformed_raw_value(self) -> None:
        event = normalize_for_replay(
            {"errorType": "PARSE_OR_VALIDATION_ERROR", "rawValue": '{"broken": true'},
            "transactions.dlq",
            0,
            1,
            "run-1",
        )

        self.assertIsNone(event)

    def test_normalize_for_replay_rejects_invalid_numeric_fields(self) -> None:
        event = normalize_for_replay(
            {
                "errorType": "PARSE_OR_VALIDATION_ERROR",
                "rawValue": (
                    '{"userId":"user-1","eventTime":1760000000000,'
                    '"amount":"bad","mlFraudScore":0.5,"ipRisk":10}'
                ),
            },
            "transactions.dlq",
            0,
            1,
            "run-1",
        )

        self.assertIsNone(event)

    def test_normalize_for_replay_rejects_unsafe_error_types_and_repairs(self) -> None:
        valid_event = (
            '{"eventId":"event-1","userId":"user-1","eventTime":1760000000000,'
            '"amount":10,"mlFraudScore":0.5,"ipRisk":10}'
        )
        for error_type in ("LATE_EVENT", "REFERENCE_DATA_PARSE_ERROR"):
            with self.subTest(error_type=error_type):
                event = normalize_for_replay(
                    {"errorType": error_type, "rawValue": valid_event},
                    "transactions.dlq",
                    0,
                    1,
                    "run-1",
                )
                self.assertIsNone(event)

        negative_amount = normalize_for_replay(
            {
                "errorType": "PARSE_OR_VALIDATION_ERROR",
                "rawValue": (
                    '{"eventId":"event-1","userId":"user-1","eventTime":1760000000000,'
                    '"amount":-1,"mlFraudScore":0.5,"ipRisk":10}'
                ),
            },
            "transactions.dlq",
            0,
            2,
            "run-1",
        )
        self.assertIsNone(negative_amount)

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
                        "rawValue": (
                            '{"eventId":"","userId":"user-1","eventTime":1760000000000,'
                            '"amount":10,"mlFraudScore":0.5,"ipRisk":10}'
                        ),
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
