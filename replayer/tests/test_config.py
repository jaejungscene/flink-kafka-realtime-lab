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
        )
        for values in invalid_cases:
            with self.subTest(values=values):
                with self.assertRaises(RuntimeError):
                    ReplayerSettings.from_environment(values)


if __name__ == "__main__":
    unittest.main()
