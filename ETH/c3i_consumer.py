"""Consume ETH messages and retrieve observation JPEGs for C3I."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata

from config import Settings, configure_logging
from messages import (
    CLASS_NAMES,
    SENSORS,
    ValidationError,
    validate_environment,
    validate_observation,
)
from minio_service import MinioService, MinioServiceError

LOGGER = logging.getLogger(__name__)


class ConsumerProcessingError(RuntimeError):
    """The current record was not fully processed and must not be committed."""


def _reject_constant(value: str) -> None:
    raise ValidationError(f"Non-finite JSON number {value} is not allowed")


def handle_message(
    topic: str, payload: bytes, settings: Settings, storage: MinioService
) -> Path | None:
    """Process one message and return the downloaded path for an observation."""
    message = json.loads(payload.decode("utf-8"), parse_constant=_reject_constant)
    if topic == settings.kafka_environment_topic:
        validate_environment(message)
        LOGGER.info(
            "Environment incident_id=%s timestamp=%s device_id=%s",
            message["incident_id"],
            message["timestamp"],
            message["device_id"],
        )
        for sensor in SENSORS:
            reading = message["data"][sensor]
            LOGGER.info(
                "  %s: %s %s", sensor.upper(), reading["value"], reading["unit"]
            )
    elif topic == settings.kafka_observations_topic:
        validate_observation(message)
        LOGGER.info(
            "Observation incident_id=%s timestamp=%s device_id=%s frame_id=%s num_boxes=%s",
            message["incident_id"],
            message["timestamp"],
            message["device_id"],
            message["frame_id"],
            message["detections"]["num_boxes"],
        )
        for index, box in enumerate(message["detections"]["boxes"], start=1):
            LOGGER.info(
                "  Box %s: class=%s cls_id=%s x=%s y=%s w=%s h=%s",
                index,
                CLASS_NAMES.get(box["cls_id"], "unknown"),
                box["cls_id"],
                box["x"],
                box["y"],
                box["w"],
                box["h"],
            )
        image = message["image"]
        return storage.download_image(
            image["bucket"], image["object_key"], settings.received_images_dir
        )
    else:
        raise ValidationError(f"Unsupported topic: {topic}")
    return None


def consume(consumer: Any, settings: Settings, storage: MinioService) -> None:
    for record in consumer:
        try:
            if record.value is None:
                raise ValidationError("Null Kafka values are not valid ETH messages")
            handle_message(record.topic, record.value, settings, storage)
            # Commit only this record's next offset, never all prefetched positions.
            consumer.commit(
                {
                    TopicPartition(record.topic, record.partition): OffsetAndMetadata(
                        record.offset + 1, ""
                    )
                }
            )
        except (ValueError, MinioServiceError, KafkaError, OSError) as exc:
            raise ConsumerProcessingError(
                f"Processing failed at topic={record.topic} partition={record.partition} "
                f"offset={record.offset}: {exc}. No offset advancement was confirmed. "
                "Correct the problem and restart this consumer to retry."
            ) from exc


def run_consumer(settings: Settings) -> None:
    try:
        consumer = KafkaConsumer(
            settings.kafka_environment_topic,
            settings.kafka_observations_topic,
            **settings.kafka_client_options(),
            client_id="foresight-c3i-consumer",
            group_id=settings.kafka_consumer_group,
            auto_offset_reset=settings.kafka_auto_offset_reset,
            enable_auto_commit=False,
            max_poll_records=1,
            api_version_auto_timeout_ms=5000,
        )
    except (KafkaError, OSError, ValueError) as exc:
        raise ConsumerProcessingError(
            "Cannot connect or authenticate to Kafka; check KAFKA_BOOTSTRAP_SERVERS, "
            "KAFKA_SECURITY_PROTOCOL, SASL credentials and broker availability"
        ) from exc
    try:
        LOGGER.info(
            "C3I listening on %s and %s, group=%s (Ctrl-C to stop)",
            settings.kafka_environment_topic,
            settings.kafka_observations_topic,
            settings.kafka_consumer_group,
        )
        with MinioService(settings) as storage:
            consume(consumer, settings, storage)
    finally:
        consumer.close(autocommit=False)


def main(argv: list[str] | None = None) -> int:
    argparse.ArgumentParser(description=__doc__).parse_args(argv)
    configure_logging()
    try:
        settings = Settings.from_env()
        configure_logging(settings.log_level)
        run_consumer(settings)
        return 0
    except KeyboardInterrupt:
        LOGGER.info("C3I consumer stopped")
        return 0
    except (
        KafkaError,
        ConsumerProcessingError,
        MinioServiceError,
        ValueError,
        OSError,
    ) as exc:
        LOGGER.error(
            "C3I consumer failed: %s", exc, exc_info=LOGGER.isEnabledFor(logging.DEBUG)
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
