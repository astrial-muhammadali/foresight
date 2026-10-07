"""Offline contract and failure-path tests; no running Kafka or MinIO needed."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from kafka import TopicPartition
from kafka.errors import KafkaTimeoutError, NoBrokersAvailable
from minio.error import S3Error

from c3i_consumer import ConsumerProcessingError, consume, handle_message, run_consumer
from config import PROJECT_DIR, ConfigurationError, Settings
from kafka_service import (
    KafkaProducerService,
    KafkaServiceError,
    serialize_json,
    serialize_key,
)
from messages import (
    ValidationError,
    image_object_key,
    parse_timestamp,
    validate_environment,
    validate_observation,
)
from minio_service import MinioService, MinioServiceError, check_jpeg
from send_environment import build_environment_message
from send_observation import build_observation_message, send_observation


def default_settings() -> Settings:
    with patch.dict(os.environ, {}, clear=True):
        return Settings.from_env(env_file=None)


def environment() -> dict:
    return build_environment_message(
        "INC-2026-000031", "eth-sensor-01", "2026-10-04T17:20:01Z"
    )


def observation() -> dict:
    return build_observation_message(
        "INC-2026-000031",
        "eth-camera-01",
        "foresight-eth-images",
        "frame-000123",
        "2026-10-04T17:20:10Z",
    )


class ConfigurationTests(unittest.TestCase):
    def test_local_defaults(self):
        settings = default_settings()
        self.assertEqual(settings.kafka_bootstrap_servers, ("localhost:9092",))
        self.assertEqual(settings.kafka_consumer_group, "foresight-c3i")
        self.assertEqual(settings.minio_endpoint, "localhost:9000")
        self.assertFalse(settings.minio_secure)
        self.assertEqual(settings.received_images_dir, PROJECT_DIR / "received_images")

    def test_env_file_and_shell_precedence(self):
        with tempfile.TemporaryDirectory() as directory:
            env_file = Path(directory) / ".env"
            env_file.write_text(
                "MINIO_SECURE=true\nMINIO_SECRET_KEY=file-secret\n", encoding="utf-8"
            )
            with patch.dict(
                os.environ, {"MINIO_SECRET_KEY": "shell-secret"}, clear=True
            ):
                settings = Settings.from_env(env_file)
        self.assertEqual(settings.minio_secret_key, "shell-secret")
        self.assertTrue(settings.minio_secure)
        self.assertNotIn("shell-secret", repr(settings))

    def test_invalid_settings_fail_early(self):
        for name, value in (
            ("MINIO_SECURE", "maybe"),
            ("MINIO_ENDPOINT", "http://localhost:9000"),
            ("MINIO_BUCKET", "BAD_BUCKET"),
            ("MINIO_SECRET_KEY", ""),
            ("KAFKA_BOOTSTRAP_SERVERS", ","),
            ("KAFKA_BOOTSTRAP_SERVERS", "server:9092,"),
            ("KAFKA_SEND_TIMEOUT_SECONDS", "NaN"),
            ("MINIO_TIMEOUT_SECONDS", "0"),
            ("KAFKA_AUTO_OFFSET_RESET", "first"),
            ("LOG_LEVEL", "verbose"),
            ("KAFKA_ENVIRONMENT_TOPIC", "topic with spaces"),
            ("KAFKA_ENVIRONMENT_TOPIC", "foresight.eth.observations"),
        ):
            with (
                self.subTest(name=name, value=value),
                patch.dict(os.environ, {name: value}, clear=True),
                self.assertRaises(ConfigurationError),
            ):
                Settings.from_env(env_file=None)

    def test_example_has_every_supported_setting(self):
        from dotenv import dotenv_values

        expected = {field.upper() for field in Settings.__dataclass_fields__}
        self.assertEqual(set(dotenv_values(PROJECT_DIR / ".env.example")), expected)

    def test_example_requires_private_connection_details(self):
        from dotenv import dotenv_values

        example = PROJECT_DIR / ".env.example"
        values = dotenv_values(example, interpolate=False)
        for key in (
            "KAFKA_BOOTSTRAP_SERVERS",
            "KAFKA_SASL_USERNAME",
            "KAFKA_SASL_PASSWORD",
            "MINIO_ENDPOINT",
            "MINIO_ACCESS_KEY",
            "MINIO_SECRET_KEY",
        ):
            with self.subTest(key=key):
                self.assertFalse(
                    bool(values.get(key)),
                    f"{key} must be blank in the tracked configuration template",
                )
        with (
            patch.dict(os.environ, {}, clear=True),
            self.assertRaisesRegex(ConfigurationError, "KAFKA_BOOTSTRAP_SERVERS"),
        ):
            Settings.from_env(example)


class MessageTests(unittest.TestCase):
    def test_examples_are_valid(self):
        validate_environment(environment())
        validate_observation(observation())

    def test_timestamp_is_aware_utc(self):
        result = build_environment_message("incident", "sensor")
        self.assertEqual(
            parse_timestamp(result["timestamp"]).utcoffset().total_seconds(), 0
        )
        self.assertTrue(result["timestamp"].endswith("Z"))
        parse_timestamp("2026-10-04T17:20:01+00:00")

    def test_required_fields_and_timestamp(self):
        for build, validate in (
            (environment, validate_environment),
            (observation, validate_observation),
        ):
            for field in ("incident_id", "device_id", "timestamp", "source"):
                with self.subTest(field=field, kind=build.__name__):
                    message = build()
                    del message[field]
                    with self.assertRaises(ValidationError):
                        validate(message)
        for stamp in (
            "2026-10-04",
            "2026-10-04T17:20:01",
            "2026-10-04T17:20:01+02:00",
            "2026-02-30T00:00:00Z",
            "",
        ):
            with self.subTest(stamp=stamp), self.assertRaises(ValidationError):
                parse_timestamp(stamp)

    def test_non_numeric_non_finite_and_overflow_readings(self):
        for value in (
            "450",
            True,
            None,
            [],
            float("nan"),
            float("inf"),
            -float("inf"),
            10**400,
        ):
            with self.subTest(value=value):
                message = environment()
                message["data"]["pressure"]["value"] = value
                with self.assertRaises(ValidationError):
                    validate_environment(message)

    def test_sensor_ranges_and_units(self):
        for sensor, lower, upper in (
            ("co2", 0, 5000),
            ("temperature", -40, 85),
            ("humidity", 0, 100),
            ("ch4", 1, 10000),
        ):
            for value in (lower, upper):
                message = environment()
                message["data"][sensor]["value"] = value
                validate_environment(message)
            for value in (lower - 0.01, upper + 0.01):
                with self.subTest(sensor=sensor, value=value):
                    message = environment()
                    message["data"][sensor]["value"] = value
                    with self.assertRaises(ValidationError):
                        validate_environment(message)
        message = environment()
        message["data"]["temperature"]["unit"] = "F"
        with self.assertRaises(ValidationError):
            validate_environment(message)

    def test_pressure_has_no_physical_range_constraint(self):
        for value in (-1000, 0, 100000):
            message = environment()
            message["data"]["pressure"]["value"] = value
            validate_environment(message)

    def test_box_bounds_are_inclusive_and_integers(self):
        for field, maximum in (
            ("x", 512),
            ("y", 512),
            ("w", 512),
            ("h", 512),
            ("cls_id", 255),
        ):
            for value in (0, maximum):
                message = observation()
                message["detections"]["boxes"][0][field] = value
                validate_observation(message)
            for value in (-1, maximum + 1, 1.0, True, "1", None):
                with self.subTest(field=field, value=value):
                    message = observation()
                    message["detections"]["boxes"][0][field] = value
                    with self.assertRaises(ValidationError):
                        validate_observation(message)

    def test_count_must_match_and_fit_uint8(self):
        message = observation()
        for count in (-1, 0, 1, 256, True, 2.0):
            with self.subTest(count=count):
                message["detections"]["num_boxes"] = count
                with self.assertRaises(ValidationError):
                    validate_observation(message)
        for count in (0, 255):
            message = observation()
            box = message["detections"]["boxes"][0]
            message["detections"] = {
                "num_boxes": count,
                "boxes": [deepcopy(box) for _ in range(count)],
            }
            validate_observation(message)

    def test_object_reference_matches_envelope(self):
        self.assertEqual(
            image_object_key("eth-camera-01", "frame-000123", "2026-10-04T17:20:10Z"),
            "eth-camera-01/2026/10/04/frame-000123.jpg",
        )
        for field, value in (
            ("object_key", "../secret.jpg"),
            ("content_type", "image/png"),
            ("bucket", ""),
        ):
            message = observation()
            message["image"][field] = value
            with self.assertRaises(ValidationError):
                validate_observation(message)
        with self.assertRaises(ValidationError):
            image_object_key("../camera", "frame", "2026-10-04T17:20:10Z")

    def test_frame_ids_are_unique(self):
        first = build_observation_message("incident", "camera", "bucket")
        second = build_observation_message("incident", "camera", "bucket")
        self.assertNotEqual(first["frame_id"], second["frame_id"])


class ProducerTests(unittest.TestCase):
    def test_utf8_serialization_and_keys(self):
        self.assertEqual(
            json.loads(serialize_json({"label": "Temperatur °C"})),
            {"label": "Temperatur °C"},
        )
        self.assertEqual(serialize_key("gerät"), "gerät".encode())
        self.assertEqual(serialize_key(b"device"), b"device")
        self.assertIsNone(serialize_key(None))
        for value in (float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                serialize_json({"value": value})
        with self.assertRaises(TypeError):
            serialize_json([])

    @patch("kafka_service.KafkaProducer")
    def test_acknowledgement_metadata_and_cleanup(self, factory):
        client = factory.return_value
        metadata = SimpleNamespace(topic="environment", partition=0, offset=42)
        client.send.return_value.get.return_value = metadata
        with KafkaProducerService(default_settings()) as producer:
            self.assertIs(
                producer.send("environment", environment(), key="sensor"), metadata
            )
        self.assertEqual(factory.call_args.kwargs["acks"], "all")
        client.send.return_value.get.assert_called_once_with(timeout=30)
        client.flush.assert_called_once()
        client.close.assert_called_once()

    @patch("kafka_service.KafkaProducer", side_effect=NoBrokersAvailable())
    def test_connection_error_is_clear(self, _):
        with self.assertRaisesRegex(KafkaServiceError, "Cannot connect"):
            KafkaProducerService(default_settings())

    @patch("kafka_service.KafkaProducer")
    def test_send_timeout_and_close_on_flush_failure(self, factory):
        factory.return_value.send.return_value.get.side_effect = KafkaTimeoutError()
        producer = KafkaProducerService(default_settings())
        with self.assertRaisesRegex(KafkaServiceError, "not confirmed"):
            producer.send("environment", environment())
        factory.return_value.flush.side_effect = KafkaTimeoutError()
        with self.assertRaises(KafkaServiceError):
            producer.close()
        factory.return_value.close.assert_called_once()
        producer.close()  # Idempotent after an attempted shutdown.


class StorageTests(unittest.TestCase):
    @patch("minio_service.Minio")
    def test_upload_checks_bucket_and_sets_jpeg_type(self, factory):
        factory.return_value.bucket_exists.return_value = False
        with MinioService(default_settings()) as service:
            reference = service.upload_image(
                PROJECT_DIR / "sample.jpg", "camera/2026/10/04/frame.jpg"
            )
        factory.return_value.make_bucket.assert_called_once_with(
            bucket_name="foresight-eth-images"
        )
        self.assertEqual(
            factory.return_value.fput_object.call_args.kwargs["content_type"],
            "image/jpeg",
        )
        self.assertEqual(reference["content_type"], "image/jpeg")

    @patch("minio_service.Minio")
    def test_bucket_creation_race_is_safe(self, factory):
        client = factory.return_value
        client.bucket_exists.return_value = False
        client.make_bucket.side_effect = S3Error(
            response=None,
            code="BucketAlreadyOwnedByYou",
            message="exists",
            resource="bucket",
            request_id="request",
            host_id="host",
        )
        with MinioService(default_settings()) as service:
            service.create_bucket_if_required()
        client.make_bucket.side_effect = S3Error(
            response=None,
            code="AccessDenied",
            message="denied",
            resource="bucket",
            request_id="request",
            host_id="host",
        )
        with (
            MinioService(default_settings()) as service,
            self.assertRaisesRegex(MinioServiceError, "AccessDenied"),
        ):
            service.create_bucket_if_required()

    @patch("minio_service.Minio")
    def test_invalid_jpeg_is_not_uploaded(self, factory):
        with tempfile.TemporaryDirectory() as directory:
            image = Path(directory) / "invalid.jpg"
            image.write_bytes(b"not a JPEG")
            with (
                MinioService(default_settings()) as service,
                self.assertRaises(MinioServiceError),
            ):
                service.upload_image(image, "image.jpg")
        factory.return_value.fput_object.assert_not_called()

    @patch("minio_service.Minio")
    def test_download_cannot_escape_directory_and_is_replay_safe(self, factory):
        sample = (PROJECT_DIR / "sample.jpg").read_bytes()
        factory.return_value.fget_object.side_effect = lambda **kwargs: Path(
            kwargs["file_path"]
        ).write_bytes(sample)
        with (
            tempfile.TemporaryDirectory() as directory,
            MinioService(default_settings()) as service,
        ):
            path = service.download_image(
                service.bucket, "../../CON:secret.jpg", directory
            )
            self.assertEqual(path.parent, Path(directory).resolve())
            self.assertEqual(path.read_bytes(), sample)
            self.assertEqual(
                service.download_image(
                    service.bucket, "../../CON:secret.jpg", directory
                ),
                path,
            )
            self.assertEqual(len(list(Path(directory).iterdir())), 1)
            with self.assertRaises(MinioServiceError):
                service.download_image("unapproved-bucket", "image.jpg", directory)

    @patch("minio_service.Minio")
    def test_failed_download_keeps_previous_good_image(self, factory):
        sample = (PROJECT_DIR / "sample.jpg").read_bytes()
        factory.return_value.fget_object.side_effect = lambda **kwargs: Path(
            kwargs["file_path"]
        ).write_bytes(sample)
        with (
            tempfile.TemporaryDirectory() as directory,
            MinioService(default_settings()) as service,
        ):
            path = service.download_image(service.bucket, "image.jpg", directory)
            factory.return_value.fget_object.side_effect = lambda **kwargs: Path(
                kwargs["file_path"]
            ).write_bytes(b"broken")
            with self.assertRaises(MinioServiceError):
                service.download_image(service.bucket, "image.jpg", directory)
            self.assertEqual(path.read_bytes(), sample)
            self.assertEqual(list(Path(directory).iterdir()), [path])


class FlowTests(unittest.TestCase):
    @patch("send_observation.KafkaProducerService")
    @patch("send_observation.MinioService")
    def test_upload_failure_prevents_kafka_creation(
        self, storage_factory, producer_factory
    ):
        storage_factory.return_value.__enter__.return_value.upload_image.side_effect = (
            MinioServiceError("offline")
        )
        with self.assertRaises(MinioServiceError):
            send_observation(
                default_settings(), PROJECT_DIR / "sample.jpg", "incident", "camera"
            )
        producer_factory.assert_not_called()

    @patch("send_observation.KafkaProducerService")
    @patch("send_observation.MinioService")
    def test_invalid_observation_has_no_side_effects(
        self, storage_factory, producer_factory
    ):
        with self.assertRaises(ValidationError):
            send_observation(
                default_settings(), PROJECT_DIR / "sample.jpg", "", "camera"
            )
        storage_factory.assert_not_called()
        producer_factory.assert_not_called()

    @patch("send_observation.KafkaProducerService")
    @patch("send_observation.MinioService")
    def test_upload_precedes_publication(self, storage_factory, producer_factory):
        events = []
        storage_factory.return_value.__enter__.return_value.upload_image.side_effect = (
            lambda *args: events.append("upload")
        )
        producer_factory.return_value.__enter__.return_value.send.side_effect = (
            lambda *args, **kwargs: events.append("publish")
        )
        send_observation(
            default_settings(), PROJECT_DIR / "sample.jpg", "incident", "camera"
        )
        self.assertEqual(events, ["upload", "publish"])

    def test_unknown_class_and_image_retrieval(self):
        message = observation()
        message["detections"]["boxes"][0]["cls_id"] = 255
        storage = MagicMock()
        settings = default_settings()
        with self.assertLogs("c3i_consumer", level="INFO") as logs:
            handle_message(
                settings.kafka_observations_topic,
                serialize_json(message),
                settings,
                storage,
            )
        self.assertIn("class=unknown", " ".join(logs.output))
        storage.download_image.assert_called_once_with(
            message["image"]["bucket"],
            message["image"]["object_key"],
            settings.received_images_dir,
        )

    def test_all_environment_fields_are_printed(self):
        settings = default_settings()
        storage = MagicMock()
        with self.assertLogs("c3i_consumer", level="INFO") as logs:
            handle_message(
                settings.kafka_environment_topic,
                serialize_json(environment()),
                settings,
                storage,
            )
        output = " ".join(logs.output)
        for expected in (
            "INC-2026-000031",
            "2026-10-04T17:20:01Z",
            "eth-sensor-01",
            "CO2",
            "TEMPERATURE",
            "HUMIDITY",
            "PRESSURE",
            "CH4",
        ):
            self.assertIn(expected, output)
        storage.download_image.assert_not_called()

    def test_offset_committed_only_after_processing(self):
        settings = default_settings()
        records = [
            SimpleNamespace(
                topic=settings.kafka_environment_topic,
                partition=2,
                offset=8,
                value=serialize_json(environment()),
            )
        ]
        consumer = MagicMock()
        consumer.__iter__.return_value = iter(records)
        consume(consumer, settings, MagicMock())
        offsets = consumer.commit.call_args.args[0]
        self.assertEqual(
            offsets[TopicPartition(settings.kafka_environment_topic, 2)].offset, 9
        )
        self.assertEqual(len(offsets), 1)

    def test_bad_payload_stops_without_skipping_record(self):
        settings = default_settings()
        for value in (b"broken", b"\xff", b"[]", b'{"data":NaN}', None, b"{}"):
            with self.subTest(value=value):
                consumer = MagicMock()
                consumer.__iter__.return_value = iter(
                    [
                        SimpleNamespace(
                            topic=settings.kafka_environment_topic,
                            partition=0,
                            offset=5,
                            value=value,
                        )
                    ]
                )
                with self.assertRaisesRegex(ConsumerProcessingError, "offset=5"):
                    consume(consumer, settings, MagicMock())
                consumer.commit.assert_not_called()

    def test_image_failure_stops_before_next_record(self):
        settings = default_settings()
        consumer = MagicMock()
        consumer.__iter__.return_value = iter(
            [
                SimpleNamespace(
                    topic=settings.kafka_observations_topic,
                    partition=0,
                    offset=5,
                    value=serialize_json(observation()),
                ),
                SimpleNamespace(
                    topic=settings.kafka_environment_topic,
                    partition=0,
                    offset=6,
                    value=serialize_json(environment()),
                ),
            ]
        )
        storage = MagicMock()
        storage.download_image.side_effect = MinioServiceError("missing image")
        with self.assertRaises(ConsumerProcessingError):
            consume(consumer, settings, storage)
        consumer.commit.assert_not_called()

    @patch("c3i_consumer.KafkaConsumer")
    def test_consumer_closes_without_auto_commit(self, factory):
        factory.return_value.__iter__.side_effect = KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt):
            run_consumer(default_settings())
        self.assertFalse(factory.call_args.kwargs["enable_auto_commit"])
        factory.return_value.close.assert_called_once_with(autocommit=False)

    @patch("minio_service.Minio")
    @patch("kafka_service.KafkaProducer")
    def test_offline_roundtrip_preserves_jpeg_bytes(
        self, producer_factory, storage_factory
    ):
        """Exercise real project services; substitute only the external SDK clients."""
        objects = {}
        records = []
        settings = default_settings()
        client = storage_factory.return_value
        client.bucket_exists.return_value = True

        def upload(**kwargs):
            objects[(kwargs["bucket_name"], kwargs["object_name"])] = Path(
                kwargs["file_path"]
            ).read_bytes()

        def download(**kwargs):
            Path(kwargs["file_path"]).write_bytes(
                objects[(kwargs["bucket_name"], kwargs["object_name"])]
            )

        def publish(topic, value, key):
            records.append(
                SimpleNamespace(
                    topic=topic,
                    partition=0,
                    offset=len(records),
                    value=serialize_json(value),
                )
            )
            future = MagicMock()
            future.get.return_value = records[-1]
            return future

        client.fput_object.side_effect = upload
        client.fget_object.side_effect = download
        producer_factory.return_value.send.side_effect = publish
        with tempfile.TemporaryDirectory() as directory:
            settings = replace(settings, received_images_dir=Path(directory))
            with KafkaProducerService(settings) as producer:
                producer.send(
                    settings.kafka_environment_topic, environment(), key="sensor"
                )
            message = send_observation(
                settings, PROJECT_DIR / "sample.jpg", "incident", "camera"
            )
            consumer = MagicMock()
            consumer.__iter__.return_value = iter(records)
            with MinioService(settings) as storage:
                consume(consumer, settings, storage)
            files = list(Path(directory).glob("*.jpg"))
            self.assertEqual(len(files), 1)
            self.assertEqual(
                files[0].read_bytes(), (PROJECT_DIR / "sample.jpg").read_bytes()
            )
            self.assertEqual(consumer.commit.call_count, 2)
            self.assertIn(
                (settings.minio_bucket, message["image"]["object_key"]), objects
            )
            check_jpeg(files[0])


if __name__ == "__main__":
    unittest.main()
