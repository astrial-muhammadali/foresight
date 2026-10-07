"""Read-only live Kafka diagnostic. Run explicitly; unittest discovery is offline."""

from __future__ import annotations

import argparse
import logging
import ssl
import subprocess
import sys
from pathlib import Path

# Support both `python tests/test_kafka_connection.py` and `python -m tests.test_kafka_connection`.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient
from kafka.errors import (
    AuthenticationFailedError,
    KafkaError,
    KafkaTimeoutError,
    NoBrokersAvailable,
    TopicAuthorizationFailedError,
    UnsupportedSaslMechanismError,
)

from config import PROJECT_DIR, ConfigurationError, Settings


def check_connection(settings: Settings, timeout_seconds: int) -> bool:
    """Check authenticated metadata and offset access without writing or joining a group."""
    connection = settings.kafka_client_options()
    timeout_ms = timeout_seconds * 1000
    admin = KafkaAdminClient(
        **connection,
        client_id="foresight-connection-check",
        request_timeout_ms=timeout_ms,
        api_version_auto_timeout_ms=timeout_ms,
    )
    try:
        # Listing existing topics never requests creation of a missing topic,
        # including on older brokers lacking allow_auto_topic_creation support.
        descriptions = {item["topic"]: item for item in admin.describe_topics()}
    finally:
        admin.close()

    if settings.kafka_security_protocol.startswith("SASL_"):
        print("PASS: Kafka authentication and metadata request.", flush=True)
    else:
        print(
            "PASS: Kafka metadata request (SASL authentication is disabled).",
            flush=True,
        )

    partitions = []
    healthy = True
    for topic in (settings.kafka_environment_topic, settings.kafka_observations_topic):
        description = descriptions.get(topic)
        if description is None:
            print(
                f"FAIL: {topic} does not exist or is not visible to this account.",
                flush=True,
            )
            healthy = False
            continue
        topic_partitions = description.get("partitions", [])
        if (
            description.get("error_code", 0)
            or not topic_partitions
            or any(
                partition.get("error_code", 0) or partition.get("leader", -1) < 0
                for partition in topic_partitions
            )
        ):
            print(
                f"FAIL: {topic} has unavailable partition metadata or leaders.",
                flush=True,
            )
            healthy = False
            continue
        print(
            f"PASS: {topic} is visible ({len(topic_partitions)} partition(s)).",
            flush=True,
        )
        partitions.extend(
            TopicPartition(topic, item["partition"]) for item in topic_partitions
        )

    if not healthy:
        return False

    session_ms = min(10000, timeout_ms // 2)
    consumer = KafkaConsumer(
        **connection,
        client_id="foresight-offset-check",
        group_id=None,
        enable_auto_commit=False,
        request_timeout_ms=timeout_ms,
        api_version_auto_timeout_ms=timeout_ms,
        session_timeout_ms=session_ms,
        heartbeat_interval_ms=max(1, session_ms // 3),
    )
    try:
        # ListOffsets does not fetch message bodies or change consumer positions.
        offsets = consumer.end_offsets(partitions)
        if any(
            partition not in offsets or offsets[partition] < 0
            for partition in partitions
        ):
            print(
                "FAIL: Could not obtain offsets for every configured partition.",
                flush=True,
            )
            return False
        print("PASS: Consumer connection and partition offset lookup.", flush=True)
    finally:
        consumer.close(autocommit=False)
    return True


def failure_hint(error: Exception) -> str:
    """Describe errors without exposing server responses, usernames or passwords."""
    if isinstance(error, AuthenticationFailedError):
        return "Authentication rejected. Check the local credentials against the broker's JAAS user entry."
    if isinstance(error, UnsupportedSaslMechanismError):
        return "SASL mechanism rejected. This server configuration requires PLAIN."
    if isinstance(error, TopicAuthorizationFailedError):
        return "Topic access denied. Check the Kafka account's topic permissions."
    if isinstance(error, NoBrokersAvailable):
        return (
            "No usable broker. Check bootstrap address, reachability, SASL settings and credentials; "
            "also check broker/client version compatibility."
        )
    if isinstance(error, KafkaTimeoutError):
        return "Kafka request timed out. Check the broker's advertised listeners and network access."
    if isinstance(error, ssl.SSLError):
        return "TLS connection failed. Check the listener protocol, server certificate and trusted CA."
    return f"Diagnostic failed ({type(error).__name__}). Check configuration and broker logs."


def run_worker(env_file: Path, timeout_seconds: int) -> int:
    # Kafka can log an authenticated username. Keep all SDK logs out of this
    # diagnostic, including when the application's LOG_LEVEL is DEBUG.
    logging.disable(logging.CRITICAL)
    try:
        settings = Settings.from_env(env_file)
        if not check_connection(settings, timeout_seconds):
            return 1
        print(
            "SUCCESS: All connection checks passed. No messages sent; no group offsets changed.",
            flush=True,
        )
        print(
            "Message READ/WRITE and consumer-group permissions are not tested by this diagnostic.",
            flush=True,
        )
        return 0
    except ConfigurationError as exc:
        print(f"FAIL: Configuration: {exc}", flush=True)
        return 2
    except (KafkaError, OSError, ValueError) as exc:
        print(f"FAIL: {failure_hint(exc)}", flush=True)
        return 1
    except KeyboardInterrupt:
        return 130


def _timeout(value: str) -> int:
    seconds = int(value)
    if not 5 <= seconds <= 120:
        raise argparse.ArgumentTypeError("timeout must be between 5 and 120 seconds")
    return seconds


def _print_output(output: str | bytes | None) -> None:
    if isinstance(output, bytes):
        output = output.decode("utf-8", errors="replace")
    if output:
        print(output, end="" if output.endswith("\n") else "\n", flush=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        type=Path,
        default=PROJECT_DIR / ".env",
        help="Environment file; existing process variables take precedence",
    )
    parser.add_argument(
        "--timeout",
        type=_timeout,
        default=30,
        help="Overall deadline in seconds (5-120; default: 30)",
    )
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.worker:
        return run_worker(args.env_file, args.timeout)

    print(
        f"Checking Kafka using your configured credentials (deadline: {args.timeout}s)...",
        flush=True,
    )
    # Bound even SDK metadata retries that can otherwise wait indefinitely.
    # Credentials are read by the child from .env; none appear in command arguments.
    command = [
        sys.executable,
        "-X",
        "utf8",
        str(Path(__file__).resolve()),
        "--worker",
        "--env-file",
        str(args.env_file.resolve()),
        "--timeout",
        str(args.timeout),
    ]
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=args.timeout,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            check=False,
        )
        _print_output(result.stdout)
        if result.returncode and result.stderr:
            print(
                "FAIL: Diagnostic process reported an internal error. Raw tracebacks are withheld to protect credentials."
            )
        return result.returncode
    except subprocess.TimeoutExpired as exc:
        _print_output(exc.stdout)
        print(
            "FAIL: Overall deadline exceeded. Check broker availability, advertised listeners and authentication settings."
        )
        return 124
    except OSError:
        print("FAIL: Unable to start the diagnostic using this Python interpreter.")
        return 1
    except KeyboardInterrupt:
        print("Connection check interrupted.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
