"""Reusable, acknowledged UTF-8 JSON producer using kafka-python-ng."""

from __future__ import annotations

import json
import logging
from types import TracebackType
from typing import Any, Self

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import Settings

LOGGER = logging.getLogger(__name__)


class KafkaServiceError(RuntimeError):
    """Kafka connection, delivery or shutdown failed."""


def serialize_json(message: dict[str, Any]) -> bytes:
    if not isinstance(message, dict):
        raise TypeError("Kafka payload must be a dictionary")
    return json.dumps(message, ensure_ascii=False, allow_nan=False).encode("utf-8")


def serialize_key(key: str | bytes | None) -> bytes | None:
    if key is None or isinstance(key, bytes):
        return key
    if isinstance(key, str):
        return key.encode("utf-8")
    raise TypeError("Kafka key must be a string, bytes or None")


class KafkaProducerService:
    def __init__(self, settings: Settings):
        self._timeout = settings.kafka_send_timeout_seconds
        self._closed = False
        try:
            self._producer = KafkaProducer(
                **settings.kafka_client_options(),
                client_id="foresight-eth-producer",
                value_serializer=serialize_json,
                key_serializer=serialize_key,
                acks="all",
                retries=3,
                max_in_flight_requests_per_connection=1,
                max_block_ms=max(1, int(self._timeout * 1000)),
                api_version_auto_timeout_ms=5000,
            )
        except (KafkaError, OSError, ValueError) as exc:
            raise KafkaServiceError(
                "Cannot connect or authenticate to Kafka; check KAFKA_BOOTSTRAP_SERVERS, "
                "KAFKA_SECURITY_PROTOCOL, SASL credentials and broker availability"
            ) from exc

    def send(
        self, topic: str, message: dict[str, Any], key: str | bytes | None = None
    ) -> Any:
        """Wait for broker acknowledgement and return topic/partition/offset metadata."""
        if self._closed:
            raise KafkaServiceError("Cannot send with a closed Kafka producer")
        try:
            result = self._producer.send(topic, value=message, key=key).get(
                timeout=self._timeout
            )
        except (KafkaError, OSError, TypeError, ValueError) as exc:
            raise KafkaServiceError(
                f"Kafka delivery failed for topic {topic}; acknowledgement was not confirmed. "
                "A timeout can still mean the broker accepted the message."
            ) from exc
        LOGGER.info(
            "Delivered topic=%s partition=%s offset=%s",
            result.topic,
            result.partition,
            result.offset,
        )
        return result

    def flush(self) -> None:
        if not self._closed:
            try:
                self._producer.flush(timeout=self._timeout)
            except KafkaError as exc:
                raise KafkaServiceError("Kafka flush failed") from exc

    def close(self) -> None:
        if self._closed:
            return
        try:
            try:
                self.flush()
            finally:
                self._producer.close(timeout=self._timeout)
        except KafkaError as exc:
            raise KafkaServiceError("Kafka producer shutdown failed") from exc
        finally:
            self._closed = True

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            self.close()
        except KafkaServiceError:
            if exc_type is None:
                raise
            LOGGER.exception(
                "Producer shutdown also failed while handling an earlier error"
            )
