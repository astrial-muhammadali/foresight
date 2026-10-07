"""Offline checks for the live diagnostic's safety and result reporting."""

import io
import os
import subprocess
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest.mock import patch

from kafka import TopicPartition
from kafka.errors import AuthenticationFailedError

from config import Settings
from tests.test_kafka_connection import check_connection, failure_hint, main


def settings() -> Settings:
    with patch.dict(
        os.environ,
        {
            "KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
            "KAFKA_SASL_USERNAME": "private-test-user",
            "KAFKA_SASL_PASSWORD": "private-test-password",
        },
        clear=True,
    ):
        return Settings.from_env(env_file=None)


class ConnectionDiagnosticTests(unittest.TestCase):
    @patch("tests.test_kafka_connection.KafkaConsumer")
    @patch("tests.test_kafka_connection.KafkaAdminClient")
    def test_checks_metadata_and_offsets_without_messages_or_group(
        self, admin, consumer
    ):
        configuration = settings()
        topics = (
            configuration.kafka_environment_topic,
            configuration.kafka_observations_topic,
        )
        admin.return_value.describe_topics.return_value = [
            {
                "topic": topic,
                "error_code": 0,
                "partitions": [{"partition": 0, "leader": 1, "error_code": 0}],
            }
            for topic in topics
        ]
        consumer.return_value.end_offsets.return_value = {
            TopicPartition(topic, 0): 5 for topic in topics
        }
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertTrue(check_connection(configuration, 30))
        admin.return_value.describe_topics.assert_called_once_with()
        admin.return_value.create_topics.assert_not_called()
        self.assertIsNone(consumer.call_args.kwargs["group_id"])
        self.assertFalse(consumer.call_args.kwargs["enable_auto_commit"])
        consumer.return_value.poll.assert_not_called()
        consumer.return_value.commit.assert_not_called()
        consumer.return_value.subscribe.assert_not_called()
        consumer.return_value.close.assert_called_once_with(autocommit=False)
        admin.return_value.close.assert_called_once()
        self.assertIn("PASS: Kafka authentication", output.getvalue())
        self.assertNotIn(configuration.kafka_sasl_username, output.getvalue())
        self.assertNotIn(configuration.kafka_sasl_password, output.getvalue())

    @patch("tests.test_kafka_connection.KafkaConsumer")
    @patch("tests.test_kafka_connection.KafkaAdminClient")
    def test_missing_topics_fail_without_creating_them(self, admin, consumer):
        admin.return_value.describe_topics.return_value = []
        with redirect_stdout(io.StringIO()) as output:
            self.assertFalse(check_connection(settings(), 30))
        self.assertEqual(output.getvalue().count("FAIL:"), 2)
        consumer.assert_not_called()
        admin.return_value.create_topics.assert_not_called()

    @patch("tests.test_kafka_connection.KafkaAdminClient")
    def test_auth_failure_closes_client_and_redacts_exception(self, admin):
        error = AuthenticationFailedError("private-test-password")
        admin.return_value.describe_topics.side_effect = error
        with self.assertRaises(AuthenticationFailedError):
            check_connection(settings(), 30)
        admin.return_value.close.assert_called_once()
        self.assertNotIn("private-test-password", failure_hint(error))

    @patch("tests.test_kafka_connection.subprocess.run")
    def test_worker_arguments_have_no_credentials(self, run):
        run.return_value = SimpleNamespace(returncode=0, stdout="SUCCESS\n", stderr="")
        with redirect_stdout(io.StringIO()):
            self.assertEqual(main(["--timeout", "10"]), 0)
        arguments = " ".join(run.call_args.args[0])
        self.assertNotIn("private-test-password", arguments)
        self.assertNotIn("private-test-user", arguments)
        self.assertEqual(run.call_args.kwargs["timeout"], 10)

    @patch("tests.test_kafka_connection.subprocess.run")
    def test_deadline_returns_failure_with_partial_progress(self, run):
        run.side_effect = subprocess.TimeoutExpired(
            "python", 5, output=b"PASS: metadata\n"
        )
        with redirect_stdout(io.StringIO()) as output:
            self.assertEqual(main(["--timeout", "5"]), 124)
        self.assertIn("PASS: metadata", output.getvalue())
        self.assertIn("deadline exceeded", output.getvalue())


if __name__ == "__main__":
    unittest.main()
