"""Kafka authentication configuration and SDK wiring, without network access."""

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

from c3i_consumer import ConsumerProcessingError, run_consumer
from config import ConfigurationError, Settings
from kafka_service import KafkaProducerService


def auth_environment(**overrides: str) -> dict[str, str]:
    return {
        "KAFKA_BOOTSTRAP_SERVERS": "kafka.example.invalid:29093",
        "KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
        "KAFKA_SASL_MECHANISM": "PLAIN",
        "KAFKA_SASL_USERNAME": "test-user",
        "KAFKA_SASL_PASSWORD": "test-password",
        **overrides,
    }


class KafkaAuthenticationTests(unittest.TestCase):
    def test_local_plaintext_defaults_have_no_authentication_options(self):
        with patch.dict(os.environ, {}, clear=True):
            options = Settings.from_env(env_file=None).kafka_client_options()
        self.assertEqual(
            options,
            {"bootstrap_servers": ["localhost:9092"], "security_protocol": "PLAINTEXT"},
        )

    def test_sasl_settings_match_plain_listener_and_installed_sdk(self):
        with patch.dict(os.environ, auth_environment(), clear=True):
            settings = Settings.from_env(env_file=None)
        options = settings.kafka_client_options()
        self.assertEqual(
            options,
            {
                "bootstrap_servers": ["kafka.example.invalid:29093"],
                "security_protocol": "SASL_PLAINTEXT",
                "sasl_mechanism": "PLAIN",
                "sasl_plain_username": "test-user",
                "sasl_plain_password": "test-password",
            },
        )
        for client in (KafkaProducer, KafkaConsumer):
            self.assertTrue(options.keys() <= client.DEFAULT_CONFIG.keys())
        self.assertNotIn("test-user", repr(settings))
        self.assertNotIn("test-password", repr(settings))

    def test_missing_credentials_fail_for_both_sasl_protocols(self):
        for protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
            for key in ("KAFKA_SASL_USERNAME", "KAFKA_SASL_PASSWORD"):
                with (
                    self.subTest(protocol=protocol, key=key),
                    patch.dict(
                        os.environ,
                        auth_environment(
                            **{
                                "KAFKA_SECURITY_PROTOCOL": protocol,
                                key: "",
                            }
                        ),
                        clear=True,
                    ),
                    self.assertRaisesRegex(ConfigurationError, key),
                ):
                    Settings.from_env(env_file=None)

    def test_invalid_auth_settings_do_not_echo_credentials(self):
        cases = (
            {"KAFKA_SECURITY_PROTOCOL": "PLAINTEXT"},
            {"KAFKA_SECURITY_PROTOCOL": "SSL"},
            {"KAFKA_SECURITY_PROTOCOL": "HTTPS"},
            {"KAFKA_SASL_MECHANISM": "not-supported"},
            {"KAFKA_SASL_USERNAME": "test\0user"},
        )
        for overrides in cases:
            with (
                self.subTest(overrides=overrides),
                patch("config.os.environ", auth_environment(**overrides)),
                self.assertRaises(ConfigurationError) as caught,
            ):
                Settings.from_env(env_file=None)
            self.assertNotIn("test-password", str(caught.exception))

    def test_env_credentials_are_literal_and_shell_values_take_precedence(self):
        with tempfile.TemporaryDirectory() as directory:
            env_file = Path(directory) / ".env"
            env_file.write_text(
                "KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT\n"
                "KAFKA_SASL_USERNAME=file-user\n"
                "KAFKA_SASL_PASSWORD='  p#${UNSET}=$word  '\n",
                encoding="utf-8",
            )
            with patch.dict(
                os.environ, {"KAFKA_SASL_USERNAME": "shell-user"}, clear=True
            ):
                settings = Settings.from_env(env_file)
        self.assertEqual(settings.kafka_sasl_username, "shell-user")
        self.assertEqual(settings.kafka_sasl_password, "  p#${UNSET}=$word  ")

    def test_sasl_ssl_keeps_certificate_and_hostname_verification(self):
        with tempfile.TemporaryDirectory() as directory:
            ca_file = Path(directory) / "ca.pem"
            ca_file.write_text("test CA path fixture", encoding="utf-8")
            with patch.dict(
                os.environ,
                auth_environment(
                    KAFKA_SECURITY_PROTOCOL="SASL_SSL",
                    KAFKA_SSL_CAFILE=str(ca_file),
                ),
                clear=True,
            ):
                settings = Settings.from_env(env_file=None)
        options = settings.kafka_client_options()
        self.assertTrue(options["ssl_check_hostname"])
        self.assertEqual(options["ssl_cafile"], str(ca_file.resolve()))
        self.assertEqual(options["sasl_plain_password"], "test-password")
        with patch.dict(
            os.environ, auth_environment(KAFKA_SECURITY_PROTOCOL="SASL_SSL"), clear=True
        ):
            options = Settings.from_env(env_file=None).kafka_client_options()
        self.assertTrue(options["ssl_check_hostname"])
        self.assertNotIn("ssl_cafile", options)  # Use the SDK's default trust store.

    def test_ca_requires_tls_and_an_existing_file(self):
        for protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
            with (
                tempfile.TemporaryDirectory() as directory,
                patch.dict(
                    os.environ,
                    auth_environment(
                        KAFKA_SECURITY_PROTOCOL=protocol,
                        KAFKA_SSL_CAFILE=str(Path(directory) / "missing.pem"),
                    ),
                    clear=True,
                ),
                self.assertRaisesRegex(ConfigurationError, "KAFKA_SSL_CAFILE"),
            ):
                Settings.from_env(env_file=None)

    def test_supported_scram_mechanisms(self):
        for mechanism in ("SCRAM-SHA-256", "SCRAM-SHA-512"):
            with patch.dict(
                os.environ, auth_environment(KAFKA_SASL_MECHANISM=mechanism), clear=True
            ):
                options = Settings.from_env(env_file=None).kafka_client_options()
            self.assertEqual(options["sasl_mechanism"], mechanism)

    @patch("kafka_service.KafkaProducer")
    @patch("c3i_consumer.KafkaConsumer")
    @patch("c3i_consumer.MinioService")
    def test_producer_and_consumer_receive_the_same_credentials(
        self, _, consumer, producer
    ):
        with patch.dict(os.environ, auth_environment(), clear=True):
            settings = Settings.from_env(env_file=None)
        with KafkaProducerService(settings):
            pass
        run_consumer(settings)
        for key, value in settings.kafka_client_options().items():
            self.assertEqual(producer.call_args.kwargs[key], value)
            self.assertEqual(consumer.call_args.kwargs[key], value)
        self.assertEqual(producer.call_args.kwargs["acks"], "all")
        self.assertFalse(consumer.call_args.kwargs["enable_auto_commit"])

    @patch("c3i_consumer.KafkaConsumer", side_effect=NoBrokersAvailable())
    def test_consumer_connection_error_mentions_authentication_settings(self, _):
        with patch.dict(os.environ, auth_environment(), clear=True):
            settings = Settings.from_env(env_file=None)
        with self.assertRaisesRegex(
            ConsumerProcessingError, "KAFKA_SECURITY_PROTOCOL"
        ) as caught:
            run_consumer(settings)
        self.assertNotIn(settings.kafka_sasl_password, str(caught.exception))


if __name__ == "__main__":
    unittest.main()
