"""Offline regression tests for the explicitly invoked live workflow runner."""

import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from kafka import TopicPartition

from config import PROJECT_DIR, Settings
from kafka_service import serialize_json
from minio_service import MinioServiceError
from tests.test_workflow import WorkflowError, new_run_id, prepare_topics, run_workflow


def local_settings() -> Settings:
    with patch.dict(os.environ, {}, clear=True):
        return Settings.from_env(env_file=None)


class WorkflowRunnerTests(unittest.TestCase):
    def setUp(self):
        self.directory = self.enterContext(tempfile.TemporaryDirectory())
        self.settings = replace(
            local_settings(), received_images_dir=Path(self.directory)
        )
        self.run_id = new_run_id()
        self.enterContext(patch("tests.test_workflow.prepare_topics"))
        self.storage_factory = self.enterContext(
            patch("tests.test_workflow.MinioService")
        )
        self.producer_factory = self.enterContext(
            patch("tests.test_workflow.KafkaProducerService")
        )
        self.consumer_factory = self.enterContext(
            patch("tests.test_workflow.KafkaConsumer")
        )
        self.storage = self.storage_factory.return_value.__enter__.return_value
        self.producer = self.producer_factory.return_value.__enter__.return_value
        self.consumer = self.consumer_factory.return_value
        self.topics = (
            self.settings.kafka_environment_topic,
            self.settings.kafka_observations_topic,
        )
        self.records = []
        self.objects = {}
        self.events = []
        self.commits = {}
        self.offsets = {topic: 0 for topic in self.topics}
        self.partitions = {TopicPartition(topic, 0) for topic in self.topics}
        self.consumer.assignment.return_value = self.partitions
        self.consumer.end_offsets.return_value = {
            partition: 0 for partition in self.partitions
        }
        self.consumer.poll.side_effect = lambda **kwargs: {
            partition: [
                record for record in self.records if record.topic == partition.topic
            ]
            for partition in self.partitions
        }
        self.consumer.commit.side_effect = lambda offsets: self.commits.update(
            {partition: value.offset for partition, value in offsets.items()}
        )
        self.consumer.committed.side_effect = self.commits.get
        self.producer.send.side_effect = self.publish
        self.storage.upload_image.side_effect = self.upload
        self.storage.download_image.side_effect = self.download

    def publish(self, topic, payload, key):
        self.events.append(("send", topic))
        offset = self.offsets[topic]
        self.offsets[topic] += 1
        self.records.append(
            SimpleNamespace(
                topic=topic,
                partition=0,
                offset=offset,
                value=serialize_json(payload),
                key=key.encode(),
            )
        )
        return SimpleNamespace(topic=topic, partition=0, offset=offset)

    def upload(self, path, key):
        self.events.append(("upload", key))
        self.objects[key] = path.read_bytes()
        return {
            "bucket": self.settings.minio_bucket,
            "object_key": key,
            "content_type": "image/jpeg",
        }

    def download(self, bucket, key, destination):
        destination.mkdir(parents=True, exist_ok=True)
        path = destination / Path(key).name
        path.write_bytes(self.objects[key])
        return path

    def execute(self, count=1):
        with redirect_stdout(io.StringIO()):
            return run_workflow(
                self.settings, PROJECT_DIR / "sample.jpg", count, False, self.run_id
            )

    def test_roundtrip_verifies_payloads_images_and_isolated_commits(self):
        # An unrelated, malformed message must never reach the application handler.
        self.records.append(
            SimpleNamespace(
                topic=self.topics[0],
                partition=0,
                offset=999,
                value=b"not-json",
                key=None,
            )
        )
        report = self.execute(count=2)
        self.assertEqual(report["status"], "passed")
        self.assertEqual(len(report["published"]), 4)
        self.assertEqual(len(report["received"]), 4)
        self.assertEqual(len(report["uploaded_images"]), 2)
        images = [record for record in report["received"] if "image_path" in record]
        self.assertEqual(len(images), 2)
        self.assertTrue(
            all(record["sha256"] == report["source_sha256"] for record in images)
        )
        self.assertEqual(
            json.loads(Path(report["report_path"]).read_text())["status"], "passed"
        )
        self.assertNotEqual(
            self.consumer_factory.call_args.kwargs["group_id"],
            self.settings.kafka_consumer_group,
        )
        self.assertFalse(self.consumer_factory.call_args.kwargs["enable_auto_commit"])
        self.assertEqual(set(self.commits.values()), {2})
        self.consumer.close.assert_called_once_with(autocommit=False)
        self.assertEqual(
            [event[0] for event in self.events], ["send", "upload", "send"] * 2
        )

    def test_failed_upload_never_publishes_observation(self):
        self.storage.upload_image.side_effect = MinioServiceError("offline")
        with self.assertRaises(MinioServiceError):
            self.execute()
        self.assertEqual(self.offsets[self.topics[1]], 0)
        self.assertEqual(self.offsets[self.topics[0]], 1)
        report_path = (
            Path(self.directory) / "workflow-tests" / self.run_id / "report.json"
        )
        report = json.loads(report_path.read_text())
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["stage"], "upload_image_1")
        self.assertEqual(len(report["published"]), 1)

    def test_corrupted_download_fails_without_committing_observation(self):
        def wrong_image(bucket, key, destination):
            path = self.download(bucket, key, destination)
            path.write_bytes(b"different image bytes")
            return path

        self.storage.download_image.side_effect = wrong_image
        with self.assertRaisesRegex(WorkflowError, "SHA-256"):
            self.execute()
        self.assertNotIn(TopicPartition(self.topics[1], 0), self.commits)


class WorkflowTopicTests(unittest.TestCase):
    @patch("tests.test_workflow.KafkaAdminClient")
    def test_topic_creation_requires_explicit_flag(self, admin):
        admin.return_value.describe_topics.return_value = []
        with self.assertRaisesRegex(WorkflowError, "--create-topics"):
            prepare_topics(local_settings(), False)
        admin.return_value.create_topics.assert_not_called()
        admin.return_value.close.assert_called_once()

    @patch("tests.test_workflow.KafkaAdminClient")
    def test_create_topics_handles_already_existing_topic_race(self, admin):
        settings = local_settings()
        topics = (settings.kafka_environment_topic, settings.kafka_observations_topic)
        admin.return_value.describe_topics.side_effect = [
            [],
            [
                {
                    "topic": topic,
                    "partitions": [{"partition": 0, "leader": 1, "error_code": 0}],
                }
                for topic in topics
            ],
        ]
        admin.return_value.create_topics.return_value.topic_errors = [
            (topics[0], 0, None),
            (topics[1], 36, None),
        ]
        with redirect_stdout(io.StringIO()):
            prepare_topics(settings, True)
        requests = admin.return_value.create_topics.call_args.args[0]
        self.assertEqual({request.name for request in requests}, set(topics))
        self.assertTrue(
            all(
                request.num_partitions == 1 and request.replication_factor == 1
                for request in requests
            )
        )

    @patch("tests.test_workflow.KafkaAdminClient")
    def test_creation_error_in_response_is_not_mistaken_for_success(self, admin):
        admin.return_value.describe_topics.return_value = []
        admin.return_value.create_topics.return_value.topic_errors = [
            ("topic", 29, "raw-server-details")
        ]
        with self.assertRaisesRegex(
            WorkflowError, "TopicAuthorizationFailedError"
        ) as caught:
            prepare_topics(local_settings(), True)
        self.assertNotIn("raw-server-details", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
