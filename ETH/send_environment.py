"""Send one sample ETH environmental message."""

from __future__ import annotations

import argparse
import logging
from typing import Any

from config import Settings, configure_logging
from kafka_service import KafkaProducerService, KafkaServiceError
from messages import utc_timestamp, validate_environment

LOGGER = logging.getLogger(__name__)


def build_environment_message(
    incident_id: str, device_id: str, timestamp: str | None = None
) -> dict[str, Any]:
    message = {
        "incident_id": incident_id,
        "timestamp": timestamp if timestamp is not None else utc_timestamp(),
        "source": "ETH",
        "device_id": device_id,
        "data": {
            "co2": {"value": 450.25, "unit": "ppm"},
            "temperature": {"value": 23.6, "unit": "C"},
            "humidity": {"value": 48.2, "unit": "%RH"},
            "pressure": {"value": 1013.25, "unit": "hPa"},
            "ch4": {"value": 2.15, "unit": "ppm"},
        },
    }
    validate_environment(message)
    return message


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--incident-id", default="INC-2026-000031")
    parser.add_argument("--device-id", default="eth-sensor-01")
    args = parser.parse_args(argv)
    configure_logging()
    try:
        settings = Settings.from_env()
        configure_logging(settings.log_level)
        message = build_environment_message(args.incident_id, args.device_id)
        with KafkaProducerService(settings) as producer:
            producer.send(settings.kafka_environment_topic, message, key=args.device_id)
        return 0
    except (KafkaServiceError, ValueError, OSError) as exc:
        LOGGER.error(
            "Environmental send failed: %s",
            exc,
            exc_info=LOGGER.isEnabledFor(logging.DEBUG),
        )
        return 1
    except KeyboardInterrupt:
        LOGGER.info("Environmental send interrupted")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
