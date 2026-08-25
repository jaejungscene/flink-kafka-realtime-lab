import unittest
from unittest.mock import patch

from src import producer as producer_module
from src.producer import (
    optional_int_setting,
    produce_with_backpressure_retry,
    wait_for_slot,
)


class FakeProducer:
    def __init__(self, queue_full_attempts: int) -> None:
        self.queue_full_attempts = queue_full_attempts
        self.produce_calls = 0
        self.poll_calls: list[float] = []

    def produce(self, _topic, *, key, value, callback) -> None:
        self.produce_calls += 1
        if self.queue_full_attempts:
            self.queue_full_attempts -= 1
            raise BufferError("queue full")
        callback(None, object())

    def poll(self, timeout: float) -> None:
        self.poll_calls.append(timeout)


class ProducerBackpressureTest(unittest.TestCase):
    def test_retries_a_temporarily_full_queue(self) -> None:
        producer = FakeProducer(queue_full_attempts=2)

        produce_with_backpressure_retry(
            producer,
            "transactions.raw",
            key="user-1",
            value="{}",
            callback=lambda _error, _message: None,
        )

        self.assertEqual(producer.produce_calls, 3)
        self.assertEqual(len(producer.poll_calls), 3)

    def test_fails_after_the_queue_timeout(self) -> None:
        producer = FakeProducer(queue_full_attempts=10)

        with patch("src.producer.time.monotonic", side_effect=[0.0, 0.0, 1.1]):
            with self.assertRaisesRegex(RuntimeError, "remained full"):
                produce_with_backpressure_retry(
                    producer,
                    "transactions.raw",
                    key="user-1",
                    value="{}",
                    callback=lambda _error, _message: None,
                    timeout_seconds=1.0,
                )

    def test_rejects_non_positive_timeout(self) -> None:
        with self.assertRaisesRegex(ValueError, "greater than 0"):
            produce_with_backpressure_retry(
                FakeProducer(queue_full_attempts=0),
                "transactions.raw",
                key="user-1",
                value="{}",
                callback=lambda _error, _message: None,
                timeout_seconds=0,
            )

    def test_optional_seed_setting_is_strictly_parsed(self) -> None:
        with patch.dict("os.environ", {"RANDOM_SEED": "42"}):
            self.assertEqual(optional_int_setting("RANDOM_SEED"), 42)
        with patch.dict("os.environ", {"RANDOM_SEED": "invalid"}):
            with self.assertRaisesRegex(RuntimeError, "must be an integer"):
                optional_int_setting("RANDOM_SEED")

    def test_seed_makes_event_ids_reproducible(self) -> None:
        with patch.object(producer_module, "RANDOM_SEED", 42):
            first = producer_module.make_event(7)
            second = producer_module.make_event(7)

        self.assertEqual(first["eventId"], second["eventId"])

    def test_pacing_uses_absolute_monotonic_deadlines(self) -> None:
        sleeps = []

        wait_for_slot(
            100.0,
            5,
            10,
            monotonic=lambda: 100.2,
            sleep=sleeps.append,
        )

        self.assertEqual(len(sleeps), 1)
        self.assertAlmostEqual(sleeps[0], 0.3)


if __name__ == "__main__":
    unittest.main()
