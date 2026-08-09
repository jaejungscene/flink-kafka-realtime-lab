import unittest

from realtime_lab.kafka_delivery import (
    KafkaDeliveryError,
    KafkaPublishRecord,
    publish_and_wait,
)


class FakeProducer:
    def __init__(
        self,
        *,
        queue_full_attempts: int = 0,
        delivery_error: str | None = None,
        undelivered: int = 0,
    ) -> None:
        self.queue_full_attempts = queue_full_attempts
        self.delivery_error = delivery_error
        self.undelivered = undelivered
        self.produce_calls = 0
        self.poll_calls = 0
        self.records: list[tuple[str, str, str]] = []

    def produce(self, topic: str, *, key: str, value: str, callback) -> None:
        self.produce_calls += 1
        if self.queue_full_attempts:
            self.queue_full_attempts -= 1
            raise BufferError("queue full")
        self.records.append((topic, key, value))
        callback(self.delivery_error, object())

    def poll(self, _timeout: float) -> None:
        self.poll_calls += 1

    def flush(self, _timeout: float) -> int:
        return self.undelivered


class KafkaDeliveryTest(unittest.TestCase):
    def test_publishes_every_record_and_waits_for_acknowledgements(self) -> None:
        producer = FakeProducer()
        records = [
            KafkaPublishRecord("replay", "user-1", '{"id":1}'),
            KafkaPublishRecord("replay", "user-2", '{"id":2}'),
        ]

        published = publish_and_wait(producer, records)

        self.assertEqual(published, 2)
        self.assertEqual(len(producer.records), 2)
        self.assertEqual(producer.poll_calls, 2)

    def test_retries_when_local_producer_queue_is_temporarily_full(self) -> None:
        producer = FakeProducer(queue_full_attempts=2)

        published = publish_and_wait(
            producer,
            [KafkaPublishRecord("replay", "user-1", "value")],
        )

        self.assertEqual(published, 1)
        self.assertEqual(producer.produce_calls, 3)
        self.assertEqual(producer.poll_calls, 3)

    def test_fails_when_callback_reports_a_delivery_error(self) -> None:
        producer = FakeProducer(delivery_error="broker unavailable")

        with self.assertRaisesRegex(KafkaDeliveryError, "broker unavailable"):
            publish_and_wait(
                producer,
                [KafkaPublishRecord("replay", "user-1", "value")],
            )

    def test_fails_when_flush_leaves_undelivered_records(self) -> None:
        producer = FakeProducer(undelivered=1)

        with self.assertRaisesRegex(KafkaDeliveryError, "undelivered=1"):
            publish_and_wait(
                producer,
                [KafkaPublishRecord("replay", "user-1", "value")],
            )

    def test_rejects_non_positive_timeout(self) -> None:
        with self.assertRaisesRegex(ValueError, "greater than 0"):
            publish_and_wait(FakeProducer(), [], timeout_seconds=0)


if __name__ == "__main__":
    unittest.main()
