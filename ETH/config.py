"""Environment-based configuration; importing this module does not connect to services."""

from __future__ import annotations

import logging
import math
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

PROJECT_DIR = Path(__file__).resolve().parent


class ConfigurationError(ValueError):
    """A configuration value is missing or invalid."""


def _text(name: str, default: str) -> str:
    value = os.getenv(name, default).strip()
    if not value:
        raise ConfigurationError(f"{name} must not be empty")
    return value


def _seconds(name: str, default: str) -> float:
    try:
        value = float(_text(name, default))
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be a positive number") from exc
    if not math.isfinite(value) or not 0 < value <= 60:
        raise ConfigurationError(f"{name} must be greater than 0 and at most 60")
    return value


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: tuple[str, ...]
    kafka_security_protocol: str
    kafka_sasl_mechanism: str
    kafka_sasl_username: str = field(repr=False)
    kafka_sasl_password: str = field(repr=False)
    kafka_ssl_cafile: Path | None
    kafka_environment_topic: str
    kafka_observations_topic: str
    kafka_consumer_group: str
    kafka_auto_offset_reset: str
    kafka_send_timeout_seconds: float
    minio_endpoint: str
    minio_access_key: str = field(repr=False)
    minio_secret_key: str = field(repr=False)
    minio_secure: bool
    minio_bucket: str
    minio_timeout_seconds: float
    received_images_dir: Path
    log_level: str

    @classmethod
    def from_env(cls, env_file: Path | None = PROJECT_DIR / ".env") -> Settings:
        """Load the project .env without overriding existing process variables."""
        if env_file is not None:
            # Keep passwords containing ${...} literal rather than expanding them.
            load_dotenv(env_file, override=False, encoding="utf-8", interpolate=False)
        servers = tuple(
            part.strip()
            for part in _text("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
        )
        if any(not server or "://" in server for server in servers):
            raise ConfigurationError(
                "KAFKA_BOOTSTRAP_SERVERS must list host:port addresses"
            )
        protocol = _text("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper()
        if protocol not in {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}:
            raise ConfigurationError(
                "KAFKA_SECURITY_PROTOCOL must be PLAINTEXT, SSL, SASL_PLAINTEXT or SASL_SSL"
            )
        mechanism = _text("KAFKA_SASL_MECHANISM", "PLAIN").upper()
        if mechanism not in {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}:
            raise ConfigurationError(
                "KAFKA_SASL_MECHANISM must be PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512"
            )
        # Do not strip credentials: whitespace can be part of a valid password.
        username = os.getenv("KAFKA_SASL_USERNAME", "")
        password = os.getenv("KAFKA_SASL_PASSWORD", "")
        if protocol.startswith("SASL_"):
            for name, value in (
                ("KAFKA_SASL_USERNAME", username),
                ("KAFKA_SASL_PASSWORD", password),
            ):
                if not value or "\0" in value:
                    raise ConfigurationError(
                        f"{name} must be set and must not contain NUL characters for {protocol}"
                    )
        elif username or password:
            raise ConfigurationError(
                "Kafka credentials require KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT or SASL_SSL"
            )
        ca_value = os.getenv("KAFKA_SSL_CAFILE", "").strip()
        ca_file = None
        if ca_value:
            if protocol not in {"SSL", "SASL_SSL"}:
                raise ConfigurationError("KAFKA_SSL_CAFILE requires SSL or SASL_SSL")
            ca_file = Path(ca_value).expanduser()
            if not ca_file.is_absolute():
                ca_file = PROJECT_DIR / ca_file
            ca_file = ca_file.resolve()
            if not ca_file.is_file():
                raise ConfigurationError(
                    "KAFKA_SSL_CAFILE must point to an existing CA file"
                )
        environment_topic = _text(
            "KAFKA_ENVIRONMENT_TOPIC", "foresight.eth.environment"
        )
        observations_topic = _text(
            "KAFKA_OBSERVATIONS_TOPIC", "foresight.eth.observations"
        )
        for topic in (environment_topic, observations_topic):
            if not re.fullmatch(r"[A-Za-z0-9._-]{1,249}", topic) or topic in {
                ".",
                "..",
            }:
                raise ConfigurationError("Kafka topic names contain invalid characters")
        if environment_topic == observations_topic:
            raise ConfigurationError(
                "Environment and observation topics must be different"
            )
        reset = _text("KAFKA_AUTO_OFFSET_RESET", "earliest").lower()
        if reset not in {"earliest", "latest"}:
            raise ConfigurationError(
                "KAFKA_AUTO_OFFSET_RESET must be earliest or latest"
            )
        secure = _text("MINIO_SECURE", "false").lower()
        if secure not in {"true", "false"}:
            raise ConfigurationError("MINIO_SECURE must be true or false")
        endpoint = _text("MINIO_ENDPOINT", "localhost:9000")
        if any(character in endpoint for character in "/\\@?#"):
            raise ConfigurationError(
                "MINIO_ENDPOINT must be host:port, without a URL scheme"
            )
        bucket = _text("MINIO_BUCKET", "foresight-eth-images")
        if (
            not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket)
            or any(part in bucket for part in ("..", ".-", "-."))
            or re.fullmatch(r"\d+\.\d+\.\d+\.\d+", bucket)
        ):
            raise ConfigurationError("MINIO_BUCKET must be a valid S3 bucket name")
        images_dir = Path(_text("RECEIVED_IMAGES_DIR", "received_images")).expanduser()
        if not images_dir.is_absolute():
            images_dir = PROJECT_DIR / images_dir
        level = _text("LOG_LEVEL", "INFO").upper()
        if level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ConfigurationError(
                "LOG_LEVEL must be DEBUG, INFO, WARNING, ERROR or CRITICAL"
            )
        return cls(
            kafka_bootstrap_servers=servers,
            kafka_security_protocol=protocol,
            kafka_sasl_mechanism=mechanism,
            kafka_sasl_username=username,
            kafka_sasl_password=password,
            kafka_ssl_cafile=ca_file,
            kafka_environment_topic=environment_topic,
            kafka_observations_topic=observations_topic,
            kafka_consumer_group=_text("KAFKA_CONSUMER_GROUP", "foresight-c3i"),
            kafka_auto_offset_reset=reset,
            kafka_send_timeout_seconds=_seconds("KAFKA_SEND_TIMEOUT_SECONDS", "30"),
            minio_endpoint=endpoint,
            minio_access_key=_text("MINIO_ACCESS_KEY", "minioadmin"),
            minio_secret_key=_text("MINIO_SECRET_KEY", "minioadmin"),
            minio_secure=secure == "true",
            minio_bucket=bucket,
            minio_timeout_seconds=_seconds("MINIO_TIMEOUT_SECONDS", "15"),
            received_images_dir=images_dir.resolve(),
            log_level=level,
        )

    def kafka_client_options(self) -> dict[str, Any]:
        """Shared producer/consumer connection options. Never log this dictionary."""
        options: dict[str, Any] = {
            "bootstrap_servers": list(self.kafka_bootstrap_servers),
            "security_protocol": self.kafka_security_protocol,
        }
        if self.kafka_security_protocol.startswith("SASL_"):
            options.update(
                sasl_mechanism=self.kafka_sasl_mechanism,
                sasl_plain_username=self.kafka_sasl_username,
                sasl_plain_password=self.kafka_sasl_password,
            )
        if self.kafka_security_protocol in {"SSL", "SASL_SSL"}:
            options["ssl_check_hostname"] = True
            if self.kafka_ssl_cafile is not None:
                options["ssl_cafile"] = str(self.kafka_ssl_cafile)
        return options


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(level=level, format="%(levelname)s %(name)s: %(message)s")
    logging.getLogger().setLevel(level)
    # Keep third-party connection chatter out of normal demonstration output.
    logging.getLogger("kafka").setLevel(
        logging.DEBUG if level == "DEBUG" else logging.WARNING
    )
