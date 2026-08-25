import unittest

from src.config import ReplayerSettings


class ReplayerSettingsTest(unittest.TestCase):
    def test_loads_and_normalizes_valid_settings(self) -> None:
        settings = ReplayerSettings.from_environment(
            {"REPLAY_RUN_ID": "run-123", "KAFKA_ISOLATION_LEVEL": "READ_COMMITTED"}
        )

        self.assertEqual(settings.replay_run_id, "run-123")
        self.assertEqual(settings.isolation_level, "read_committed")
        self.assertEqual(settings.max_future_skew_millis, 300_000)
        self.assertEqual(settings.scan_timeout_seconds, 20)

    def test_requires_a_stable_replay_run_id(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "REPLAY_RUN_ID"):
            ReplayerSettings.from_environment({})

    def test_rejects_topic_collision_and_invalid_ranges(self) -> None:
        invalid_cases = (
            {
                "REPLAY_RUN_ID": "run-123",
                "DLQ_TOPIC": "same",
                "REPLAY_TOPIC": "same",
            },
            {"REPLAY_RUN_ID": "run-123", "MAX_MESSAGES": "0"},
            {"REPLAY_RUN_ID": "run-123", "MAX_FUTURE_SKEW_SECONDS": "-1"},
            {"REPLAY_RUN_ID": "run-123", "REPLAY_SCAN_TIMEOUT_SECONDS": "0"},
            {"REPLAY_RUN_ID": "run-123", "REPLAY_SCAN_TIMEOUT_SECONDS": "3601"},
        )
        for values in invalid_cases:
            with self.subTest(values=values):
                with self.assertRaises(RuntimeError):
                    ReplayerSettings.from_environment(values)

    def test_accepts_a_bounded_scan_timeout(self) -> None:
        settings = ReplayerSettings.from_environment(
            {"REPLAY_RUN_ID": "run-123", "REPLAY_SCAN_TIMEOUT_SECONDS": "120"}
        )

        self.assertEqual(settings.scan_timeout_seconds, 120)


if __name__ == "__main__":
    unittest.main()
