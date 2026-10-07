"""Live ETH -> Kafka/MinIO -> C3I test. Explicit execution writes sample data."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import subprocess
import sys
import time
from contextlib import ExitStack
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, for_code
from kafka.structs import OffsetAndMetadata

from c3i_consumer import handle_message
from config import PROJECT_DIR, ConfigurationError, Settings
from kafka_service import KafkaProducerService, KafkaServiceError
from minio_service import MinioService, MinioServiceError, check_jpeg
from send_environment import build_environment_message
from send_observation import build_observation_message


class WorkflowError(RuntimeError):
    """The live workflow did not meet its verification criteria."""


def new_run_id() -> str:
    return f"TEST-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}"


def safe_error(error: BaseException) -> str:
    """Only controlled messages or exception types enter the test report."""
    if isinstance(error, (WorkflowError, ConfigurationError)):
        return str(error)
    if isinstance(error, MinioServiceError):
        return "MinIO operation failed; check endpoint, credentials and bucket read/write permissions."
    if isinstance(error, (KafkaError, KafkaServiceError)):
        return f"Kafka operation failed ({type(error).__name__}); check connectivity, authentication and topic/group permissions."
    if isinstance(error, OSError):
        return f"Network or local file operation failed ({type(error).__name__})."
    return f"Workflow failed ({type(error).__name__})."


def save_report(path: Path, report: dict) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(report, indent=2, ensure_ascii=True) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def prepare_topics(settings: Settings, create_missing: bool) -> None:
    desired = (settings.kafka_environment_topic, settings.kafka_observations_topic)
    admin = KafkaAdminClient(
        **settings.kafka_client_options(),
        client_id="foresight-workflow-admin",
        request_timeout_ms=20000,
        api_version_auto_timeout_ms=10000,
    )
    try:
        visible = {item["topic"] for item in admin.describe_topics()}
        missing = [topic for topic in desired if topic not in visible]
        if missing and not create_missing:
            raise WorkflowError(
                "Missing/invisible Kafka topics: "
                + ", ".join(missing)
                + ". Use --create-topics to create missing demo topics, or check topic permissions."
            )
        if missing:
            response = admin.create_topics(
                [
                    NewTopic(topic, num_partitions=1, replication_factor=1)
                    for topic in missing
                ],
                timeout_ms=15000,
            )
            # kafka-python-ng returns per-topic errors instead of raising them.
            for topic, code, *_ in response.topic_errors:
                if code not in (
                    0,
                    36,
                ):  # TopicAlreadyExists is safe during a creation race.
                    raise WorkflowError(
                        f"Cannot create topic {topic}: {for_code(code).__name__}"
                    )
            print("Created or confirmed demo topics: " + ", ".join(missing), flush=True)

        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            metadata = {item["topic"]: item for item in admin.describe_topics()}
            if all(
                topic in metadata
                and not metadata[topic].get("error_code", 0)
                and metadata[topic].get("partitions")
                and all(
                    not part.get("error_code", 0) and part.get("leader", -1) >= 0
                    for part in metadata[topic]["partitions"]
                )
                for topic in desired
            ):
                print("PASS: Both Kafka topics have available leaders.", flush=True)
                return
            time.sleep(0.5)
        raise WorkflowError(
            "Topic metadata did not become ready; check visibility and partition leaders."
        )
    finally:
        admin.close()


def position_consumer(consumer: KafkaConsumer) -> None:
    deadline = time.monotonic() + 25
    while not consumer.assignment():
        if time.monotonic() >= deadline:
            raise WorkflowError(
                "Test consumer received no partition assignment; check group and topic read permissions."
            )
        consumer.poll(timeout_ms=500, max_records=100)
    # Resolve exact starting offsets BEFORE publishing. Lazy seek_to_end alone
    # can skip messages if its end offsets are resolved after publication.
    positions = consumer.end_offsets(list(consumer.assignment()))
    for partition, offset in positions.items():
        consumer.seek(partition, offset)


def run_workflow(
    settings: Settings,
    image_path: Path,
    count: int,
    create_topics: bool,
    run_id: str,
    receive_timeout: int = 45,
) -> dict:
    if not re.fullmatch(r"TEST-\d{8}T\d{6}Z-[0-9a-f]{8}", run_id):
        raise WorkflowError("Invalid test run identifier.")
    if not 1 <= count <= 10:
        raise WorkflowError("Sample count must be between 1 and 10.")
    directory = settings.received_images_dir / "workflow-tests" / run_id
    directory.mkdir(parents=True, exist_ok=False)
    report_path = directory / "report.json"
    group = f"foresight-workflow-{run_id}"
    report = {
        "run_id": run_id,
        "status": "running",
        "stage": "validate_image",
        "consumer_group": group,
        "requested_samples": count,
        "published": [],
        "uploaded_images": [],
        "received": [],
        "report_path": str(report_path),
    }
    save_report(report_path, report)
    print(f"Run ID: {run_id}\nReport: {report_path}", flush=True)

    def stage(name: str) -> None:
        report["stage"] = name
        save_report(report_path, report)

    try:
        check_jpeg(image_path)
        source_hash = hashlib.sha256(image_path.read_bytes()).hexdigest()
        report["source_image"] = str(image_path.resolve())
        report["source_sha256"] = source_hash
        receiver_settings = replace(settings, received_images_dir=directory / "images")
        with ExitStack() as resources:
            storage = resources.enter_context(MinioService(settings))
            stage("minio_preflight")
            storage.create_bucket_if_required()
            print("PASS: MinIO bucket is accessible.", flush=True)

            stage("kafka_topics")
            prepare_topics(settings, create_topics)
            stage("consumer_assignment")
            consumer = KafkaConsumer(
                settings.kafka_environment_topic,
                settings.kafka_observations_topic,
                **settings.kafka_client_options(),
                client_id="foresight-workflow-consumer",
                group_id=group,
                enable_auto_commit=False,
                auto_offset_reset="latest",
                request_timeout_ms=20000,
                session_timeout_ms=10000,
                api_version_auto_timeout_ms=10000,
                max_poll_records=100,
            )
            resources.callback(consumer.close, autocommit=False)
            position_consumer(consumer)
            print(f"PASS: Test consumer ready in its own group: {group}", flush=True)
            producer = resources.enter_context(KafkaProducerService(settings))
            expected = {}

            def publish(topic: str, payload: dict) -> None:
                metadata = producer.send(topic, payload, key=payload["device_id"])
                coordinate = (metadata.topic, metadata.partition, metadata.offset)
                expected[coordinate] = payload
                report["published"].append(
                    {
                        "topic": metadata.topic,
                        "partition": metadata.partition,
                        "offset": metadata.offset,
                        "payload": payload,
                    }
                )
                save_report(report_path, report)
                print(
                    f"SENT: {metadata.topic} partition={metadata.partition} offset={metadata.offset}",
                    flush=True,
                )

            suffix = run_id.rsplit("-", 1)[1]
            for index in range(1, count + 1):
                incident = f"{run_id}-{index:03}"
                environment = build_environment_message(
                    incident, f"eth-test-sensor-{suffix}"
                )
                observation = build_observation_message(
                    incident,
                    f"eth-test-camera-{suffix}",
                    settings.minio_bucket,
                )
                stage(f"publish_environment_{index}")
                publish(settings.kafka_environment_topic, environment)
                stage(f"upload_image_{index}")
                reference = storage.upload_image(
                    image_path, observation["image"]["object_key"]
                )
                report["uploaded_images"].append(reference)
                save_report(report_path, report)
                if reference != observation["image"]:
                    raise WorkflowError(
                        "Uploaded image reference differs from the observation payload."
                    )
                print(
                    f"UPLOADED: {reference['bucket']}/{reference['object_key']}",
                    flush=True,
                )
                stage(f"publish_observation_{index}")
                publish(settings.kafka_observations_topic, observation)

            stage("receive_and_verify")
            deadline = time.monotonic() + receive_timeout
            seen = set()
            last_commits = {}
            while len(seen) < len(expected):
                if time.monotonic() >= deadline:
                    raise WorkflowError(
                        f"Received {len(seen)}/{len(expected)} acknowledged test messages before timeout."
                    )
                for records in consumer.poll(timeout_ms=500, max_records=100).values():
                    for record in records:
                        coordinate = (record.topic, record.partition, record.offset)
                        if coordinate not in expected or coordinate in seen:
                            continue
                        payload = expected[coordinate]
                        if json.loads(record.value) != payload or record.key != payload[
                            "device_id"
                        ].encode("utf-8"):
                            raise WorkflowError(
                                "Received Kafka payload or message key differs from what was sent."
                            )
                        image = handle_message(
                            record.topic, record.value, receiver_settings, storage
                        )
                        received = {
                            "topic": record.topic,
                            "partition": record.partition,
                            "offset": record.offset,
                        }
                        if record.topic == settings.kafka_observations_topic:
                            if image is None:
                                raise WorkflowError(
                                    "C3I did not return a downloaded image."
                                )
                            downloaded_hash = hashlib.sha256(
                                image.read_bytes()
                            ).hexdigest()
                            if downloaded_hash != source_hash:
                                raise WorkflowError(
                                    "Downloaded image SHA-256 differs from the uploaded JPEG."
                                )
                            received.update(
                                image_path=str(image), sha256=downloaded_hash
                            )
                            print(
                                f"VERIFIED JPEG: {image.name} (SHA-256 matches)",
                                flush=True,
                            )
                        partition = TopicPartition(record.topic, record.partition)
                        consumer.commit(
                            {partition: OffsetAndMetadata(record.offset + 1, "")}
                        )
                        last_commits[partition] = record.offset + 1
                        seen.add(coordinate)
                        report["received"].append(received)
                        save_report(report_path, report)
                        print(
                            f"RECEIVED: {record.topic} partition={record.partition} offset={record.offset}; JSON and key match.",
                            flush=True,
                        )

            stage("verify_commits")
            for partition, offset in last_commits.items():
                if consumer.committed(partition) != offset:
                    raise WorkflowError(
                        "Test consumer's committed offset did not match the processed records."
                    )
            report["verified_commits"] = [
                {
                    "topic": partition.topic,
                    "partition": partition.partition,
                    "offset": offset,
                }
                for partition, offset in last_commits.items()
            ]
        report.update(status="passed", stage="complete")
        save_report(report_path, report)
        print(
            f"SUCCESS: {count} environmental messages, {count} observations and {count} matching JPEGs verified.",
            flush=True,
        )
        return report
    except (Exception, KeyboardInterrupt) as exc:
        report.update(status="failed", error=safe_error(exc))
        save_report(report_path, report)
        raise


def _timeout(value: str) -> int:
    seconds = int(value)
    if not 30 <= seconds <= 300:
        raise argparse.ArgumentTypeError("timeout must be between 30 and 300 seconds")
    return seconds


def _output(text: str | bytes | None) -> None:
    if isinstance(text, bytes):
        text = text.decode("utf-8", errors="replace")
    if text:
        print(text, end="" if text.endswith("\n") else "\n", flush=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=PROJECT_DIR / ".env")
    parser.add_argument("--image", type=Path, default=PROJECT_DIR / "sample.jpg")
    parser.add_argument(
        "--count",
        type=int,
        choices=range(1, 11),
        default=1,
        help="Number of environment/observation pairs (default: 1)",
    )
    parser.add_argument(
        "--create-topics",
        action="store_true",
        help="Create missing topics with one partition and replication factor 1",
    )
    parser.add_argument(
        "--timeout",
        type=_timeout,
        default=120,
        help="Overall deadline in seconds (default: 120)",
    )
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--run-id", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    run_id = args.run_id or new_run_id()
    if args.worker:
        logging.disable(logging.CRITICAL)
        try:
            settings = Settings.from_env(args.env_file)
            run_workflow(settings, args.image, args.count, args.create_topics, run_id)
            return 0
        except ConfigurationError as exc:
            print(f"FAIL: {safe_error(exc)}", flush=True)
            return 2
        except KeyboardInterrupt:
            return 130
        except (
            KafkaError,
            KafkaServiceError,
            MinioServiceError,
            WorkflowError,
            OSError,
            ValueError,
        ) as exc:
            print(f"FAIL: {safe_error(exc)}", flush=True)
            print(
                "Previously acknowledged messages and uploaded objects are retained; see the report.",
                flush=True,
            )
            return 1

    print(
        f"Starting live workflow: {args.count} environmental messages and {args.count} JPEG observations (deadline {args.timeout}s).",
        flush=True,
    )
    command = [
        sys.executable,
        "-X",
        "utf8",
        str(Path(__file__).resolve()),
        "--worker",
        "--env-file",
        str(args.env_file.resolve()),
        "--image",
        str(args.image.resolve()),
        "--count",
        str(args.count),
        "--timeout",
        str(args.timeout),
        "--run-id",
        run_id,
    ]
    if args.create_topics:
        command.append("--create-topics")
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
        _output(result.stdout)
        if result.returncode and result.stderr:
            print(
                "FAIL: Worker reported an internal error; raw tracebacks are withheld to protect credentials."
            )
        return result.returncode
    except subprocess.TimeoutExpired as exc:
        _output(exc.stdout)
        print(
            "FAIL: Overall deadline exceeded. Inspect the last saved report before retrying; uploaded objects and messages may exist."
        )
        return 124
    except OSError:
        print("FAIL: Could not launch the workflow worker.")
        return 1
    except KeyboardInterrupt:
        print("Workflow interrupted; previously uploaded data is retained.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
