"""
Tests for Configuration Module
Verifies configuration loading, validation, and environment variable handling.
"""

import os
import pytest
from unittest.mock import patch

from app.config import AppConfig, PostgresConfig, DockerConfig, load_config, validate_config


class TestPostgresConfig:
    """Test PostgresConfig dataclass defaults and field types."""

    def test_default_values(self):
        """Verify default values for optional fields."""
        config = PostgresConfig(
            primary_host="localhost",
            primary_port=5432,
            replication_user="replicator",
            replication_password="test_pass",
            replica_container="postgres-replica",
            data_directory="/var/lib/postgresql/data",
            primary_container="postgres-primary",
            replica_port=7777,
        )

        assert config.database_name == "postgres"
        assert config.backup_timeout == 3600
        assert config.retry_attempts == 5
        assert config.retry_delay == 10
        assert config.health_check_timeout == 60

    def test_custom_values(self):
        """Verify custom values override defaults."""
        config = PostgresConfig(
            primary_host="192.168.1.100",
            primary_port=5433,
            replication_user="rep_user",
            replication_password="secure_pass",
            replica_container="my-replica",
            data_directory="/data/pg",
            primary_container="my-primary",
            replica_port=8888,
            database_name="mydb",
            backup_timeout=7200,
            retry_attempts=3,
            retry_delay=30,
            health_check_timeout=120,
        )

        assert config.primary_host == "192.168.1.100"
        assert config.primary_port == 5433
        assert config.replication_user == "rep_user"
        assert config.replication_password == "secure_pass"
        assert config.replica_container == "my-replica"
        assert config.data_directory == "/data/pg"
        assert config.primary_container == "my-primary"
        assert config.replica_port == 8888
        assert config.database_name == "mydb"
        assert config.backup_timeout == 7200
        assert config.retry_attempts == 3
        assert config.retry_delay == 30
        assert config.health_check_timeout == 120


class TestDockerConfig:
    """Test DockerConfig defaults."""

    def test_default_values(self):
        config = DockerConfig()
        assert config.docker_host is None
        assert config.use_docker_sdk is True

    def test_custom_values(self):
        config = DockerConfig(docker_host="tcp://localhost:2375", use_docker_sdk=False)
        assert config.docker_host == "tcp://localhost:2375"
        assert config.use_docker_sdk is False


class TestLoadConfig:
    """Test configuration loading from environment variables."""

    @patch.dict(os.environ, {
        "PRIMARY_HOST": "172.31.37.122",
        "PRIMARY_PORT": "5432",
        "REPLICATION_USER": "replicator",
        "REPLICATION_PASSWORD": "pravz123",
        "REPLICA_CONTAINER": "postgres-replica",
        "DATA_DIRECTORY": "/var/lib/postgresql/data",
        "PRIMARY_CONTAINER": "postgres-primary",
        "REPLICA_PORT": "7777",
        "LOG_LEVEL": "DEBUG",
    }, clear=False)
    def test_load_config_from_env(self):
        """Verify config loads correctly from environment variables."""
        config = load_config()

        assert config.postgres.primary_host == "172.31.37.122"
        assert config.postgres.primary_port == 5432
        assert config.postgres.replication_user == "replicator"
        assert config.postgres.replication_password == "pravz123"
        assert config.postgres.replica_container == "postgres-replica"
        assert config.postgres.primary_container == "postgres-primary"
        assert config.postgres.replica_port == 7777
        assert config.log_level == "DEBUG"

    @patch.dict(os.environ, {
        "PRIMARY_HOST": "10.0.0.1",
        "PRIMARY_PORT": "5432",
        "REPLICATION_USER": "replicator",
        "REPLICATION_PASSWORD": "pass",
    }, clear=False)
    def test_load_config_defaults(self):
        """Verify default values are applied when optional env vars are missing."""
        config = load_config()

        assert config.postgres.replica_container == "postgres-replica"
        assert config.postgres.data_directory == "/var/lib/postgresql/data"
        assert config.postgres.primary_container == "postgres-primary"
        assert config.postgres.replica_port == 7777

    def test_load_config_missing_required(self):
        """Verify ValueError is raised when required env vars are missing."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="Missing required environment variables"):
                load_config()

    @patch.dict(os.environ, {
        "PRIMARY_HOST": "localhost",
        "PRIMARY_PORT": "5432",
        "REPLICATION_USER": "replicator",
        "REPLICATION_PASSWORD": "",
    }, clear=True)
    def test_load_config_empty_password(self):
        """Empty REPLICATION_PASSWORD should trigger missing var error."""
        with pytest.raises(ValueError, match="REPLICATION_PASSWORD"):
            load_config()


class TestValidateConfig:
    """Test configuration validation."""

    def _make_valid_config(self, **overrides):
        """Create a valid config with optional overrides."""
        pg_kwargs = {
            "primary_host": "localhost",
            "primary_port": 5432,
            "replication_user": "replicator",
            "replication_password": "test",
            "replica_container": "postgres-replica",
            "data_directory": "/var/lib/postgresql/data",
            "primary_container": "postgres-primary",
            "replica_port": 7777,
        }
        pg_kwargs.update(overrides)

        return AppConfig(
            postgres=PostgresConfig(**pg_kwargs),
            docker=DockerConfig(),
        )

    def test_valid_config(self):
        config = self._make_valid_config()
        assert validate_config(config) is True

    def test_invalid_port_zero(self):
        config = self._make_valid_config(primary_port=0)
        with pytest.raises(ValueError, match="Invalid primary port"):
            validate_config(config)

    def test_invalid_port_too_high(self):
        config = self._make_valid_config(primary_port=70000)
        with pytest.raises(ValueError, match="Invalid primary port"):
            validate_config(config)

    def test_invalid_retry_attempts(self):
        config = self._make_valid_config(retry_attempts=0)
        with pytest.raises(ValueError, match="Retry attempts must be at least 1"):
            validate_config(config)

    def test_invalid_retry_delay(self):
        config = self._make_valid_config(retry_delay=0)
        with pytest.raises(ValueError, match="Retry delay must be at least 1 second"):
            validate_config(config)

    def test_empty_data_directory(self):
        config = self._make_valid_config(data_directory="")
        with pytest.raises(ValueError, match="Data directory cannot be empty"):
            validate_config(config)

    def test_cross_server_config(self):
        """Verify VPC private IP configuration is valid."""
        config = self._make_valid_config(
            primary_host="172.31.37.122",
            primary_port=5432,
        )
        assert validate_config(config) is True
        assert config.postgres.primary_host == "172.31.37.122"
