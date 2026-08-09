import unittest

from src.config import AppSettings


class AppSettingsTest(unittest.TestCase):
    def test_defaults_are_valid_for_local_development(self) -> None:
        settings = AppSettings.from_environment({})

        self.assertEqual(settings.bootstrap_servers, "localhost:29092")
        self.assertEqual(settings.kafka_isolation_level, "read_uncommitted")
        self.assertIn(settings.dlq_topic, settings.readable_topics)

    def test_rejects_duplicate_or_unsafe_topic_configuration(self) -> None:
        invalid_environments = (
            {"METRIC_TOPICS": "transactions.raw,transactions.raw"},
            {"DLQ_TOPIC": "same", "REPLAY_TOPIC": "same"},
            {"READABLE_TOPICS": "transactions.raw"},
        )

        for environment in invalid_environments:
            with self.subTest(environment=environment):
                with self.assertRaises(RuntimeError):
                    AppSettings.from_environment(environment)

    def test_rejects_invalid_isolation_level_and_cache_duration(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "KAFKA_ISOLATION_LEVEL"):
            AppSettings.from_environment({"KAFKA_ISOLATION_LEVEL": "invalid"})

        with self.assertRaisesRegex(RuntimeError, "METRICS_CACHE_SECONDS"):
            AppSettings.from_environment({"METRICS_CACHE_SECONDS": "NaN"})


if __name__ == "__main__":
    unittest.main()
