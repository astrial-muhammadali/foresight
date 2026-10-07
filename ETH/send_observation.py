"""Upload a sample JPEG, then send its ETH detection message."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any
from uuid import uuid4

from config import PROJECT_DIR, Settings, configure_logging
from kafka_service import KafkaProducerService, KafkaServiceError
from messages import image_object_key, utc_timestamp, validate_observation
from minio_service import MinioService, MinioServiceError

LOGGER = logging.getLogger(__name__)


def build_observation_message(
    incident_id: str,
    device_id: str,
    bucket: str,
    frame_id: str | None = None,
    timestamp: str | None = None,
) -> dict[str, Any]:
    captured_at = timestamp if timestamp is not None else utc_timestamp()
    frame = frame_id if frame_id is not None else f"frame-{uuid4().hex}"
    boxes = [
        {"x": 120, "y": 85, "w": 90, "h": 210, "cls_id": 1},
        {"x": 250, "y": 180, "w": 110, "h": 130, "cls_id": 4},
    ]
    message = {
        "incident_id": incident_id,
        "timestamp": captured_at,
        "source": "ETH",
        "device_id": device_id,
        "frame_id": frame,
        "image": {
            "bucket": bucket,
            "object_key": image_object_key(device_id, frame, captured_at),
            "content_type": "image/jpeg",
        },
        "detections": {"num_boxes": len(boxes), "boxes": boxes},
    }
    validate_observation(message)
    return message


def send_observation(
    settings: Settings,
    image_path: Path,
    incident_id: str,
    device_id: str,
) -> dict[str, Any]:
    # Validate before any network side effect. Publish only after upload returns.
    message = build_observation_message(incident_id, device_id, settings.minio_bucket)
    with MinioService(settings) as storage:
        storage.upload_image(image_path, message["image"]["object_key"])
    try:
        with KafkaProducerService(settings) as producer:
            producer.send(settings.kafka_observations_topic, message, key=device_id)
    except (KafkaServiceError, KeyboardInterrupt):
        LOGGER.warning(
            "Image retained at %s/%s; Kafka delivery is unconfirmed. "
            "Inspect delivery before retrying; the image is not deleted automatically.",
            settings.minio_bucket,
            message["image"]["object_key"],
        )
        raise
    return message


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--incident-id", default="INC-2026-000031")
    parser.add_argument("--device-id", default="eth-camera-01")
    parser.add_argument("--image", type=Path, default=PROJECT_DIR / "sample.jpg")
    args = parser.parse_args(argv)
    configure_logging()
    try:
        settings = Settings.from_env()
        configure_logging(settings.log_level)
        message = send_observation(
            settings, args.image, args.incident_id, args.device_id
        )
        LOGGER.info("Observation sent frame_id=%s", message["frame_id"])
        return 0
    except (MinioServiceError, KafkaServiceError, ValueError, OSError) as exc:
        LOGGER.error(
            "Observation send failed: %s",
            exc,
            exc_info=LOGGER.isEnabledFor(logging.DEBUG),
        )
        return 1
    except KeyboardInterrupt:
        LOGGER.info("Observation send interrupted")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
