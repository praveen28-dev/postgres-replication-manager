"""
Tests for Main Module
Verifies argument parsing, command dispatch, and workflow orchestration.
"""

import sys
import pytest
from unittest.mock import patch, MagicMock

from app.config import AppConfig, PostgresConfig, DockerConfig
from app.main import main, ReplicationManager


def make_test_config(**overrides):
    """Create a test config."""
    pg_kwargs = {
        "primary_host": "172.31.37.122",
        "primary_port": 5432,
        "replication_user": "replicator",
        "replication_password": "test_pass",
        "replica_container": "postgres-replica",
        "data_directory": "/var/lib/postgresql/data",
        "primary_container": "postgres-primary",
        "replica_port": 7777,
    }
    pg_kwargs.update(overrides)

    return AppConfig(
        postgres=PostgresConfig(**pg_kwargs),
        docker=DockerConfig(use_docker_sdk=False),
    )


class TestReplicationManagerInit:
    """Test ReplicationManager initialization."""

    def test_initialization(self):
        config = make_test_config()
        manager = ReplicationManager(config)

        assert manager.config is config
        assert manager._step_count == 0
        assert manager._lock_acquired is False
        assert manager._container_stop_count == 0
        assert manager._container_start_count == 0

    def test_hard_limits(self):
        """Verify hard limits are enforced."""
        assert ReplicationManager.MAX_WORKFLOW_RETRIES == 3
        assert ReplicationManager.MAX_CONTAINER_RESTARTS == 2

    def test_step_counter(self):
        config = make_test_config()
        manager = ReplicationManager(config)

        assert manager._next_step() == 1
        assert manager._next_step() == 2
        assert manager._next_step() == 3


class TestMainArgParsing:
    """Test CLI argument parsing."""

    @patch("app.main.load_config")
    @patch("app.main.validate_config")
    @patch("app.main.ReplicationManager")
    def test_default_args_run_initialization(self, mock_manager_cls, mock_validate, mock_load_config):
        """Default args should trigger full initialization."""
        mock_config = make_test_config()
        mock_load_config.return_value = mock_config
        mock_validate.return_value = True

        mock_manager = MagicMock()
        mock_manager.run.return_value = True
        mock_manager_cls.return_value = mock_manager

        with patch("sys.argv", ["app.main"]):
            result = main()

        assert result == 0
        mock_manager.run.assert_called_once()

    @patch("app.main.load_config")
    @patch("app.main.validate_config")
    @patch("app.main.status_command")
    def test_status_flag(self, mock_status, mock_validate, mock_load_config):
        """--status flag should call status_command."""
        mock_config = make_test_config()
        mock_load_config.return_value = mock_config

        with patch("sys.argv", ["app.main", "--status"]):
            result = main()

        assert result == 0
        mock_status.assert_called_once_with(mock_config)

    @patch("app.main.load_config")
    @patch("app.main.validate_config")
    @patch("app.main.metrics_command")
    def test_metrics_flag(self, mock_metrics, mock_validate, mock_load_config):
        """--metrics flag should call metrics_command."""
        mock_config = make_test_config()
        mock_load_config.return_value = mock_config

        with patch("sys.argv", ["app.main", "--metrics"]):
            result = main()

        assert result == 0
        mock_metrics.assert_called_once_with(mock_config)

    @patch("app.main.load_config")
    def test_config_error_returns_1(self, mock_load_config):
        """Configuration error should return exit code 1."""
        mock_load_config.side_effect = ValueError("Missing required env vars")

        with patch("sys.argv", ["app.main"]):
            result = main()

        assert result == 1

    @patch("app.main.load_config")
    @patch("app.main.validate_config")
    @patch("app.main.ReplicationManager")
    def test_skip_flags(self, mock_manager_cls, mock_validate, mock_load_config):
        """Skip flags should be passed through to manager.run()."""
        mock_config = make_test_config()
        mock_load_config.return_value = mock_config

        mock_manager = MagicMock()
        mock_manager.run.return_value = True
        mock_manager_cls.return_value = mock_manager

        with patch("sys.argv", [
            "app.main",
            "--skip-cleanup",
            "--skip-health-check",
            "--skip-data-validation",
            "--no-retry",
        ]):
            result = main()

        mock_manager.run.assert_called_once_with(
            skip_cleanup=True,
            skip_health_check=True,
            skip_data_validation=True,
            retry_on_failure=False,
        )

    @patch("app.main.load_config")
    @patch("app.main.validate_config")
    @patch("app.main.ReplicationManager")
    def test_failed_initialization_returns_1(self, mock_manager_cls, mock_validate, mock_load_config):
        """Failed initialization should return exit code 1."""
        mock_config = make_test_config()
        mock_load_config.return_value = mock_config

        mock_manager = MagicMock()
        mock_manager.run.return_value = False
        mock_manager_cls.return_value = mock_manager

        with patch("sys.argv", ["app.main"]):
            result = main()

        assert result == 1


class TestDockerComposeValidity:
    """Test that the replica docker-compose file is valid and has no inline commands."""

    def test_no_pg_basebackup_in_compose(self):
        """Verify the replica docker-compose doesn't contain pg_basebackup."""
        import os
        compose_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
            "replica-postgre", "docker-compose.yml"
        )

        if os.path.exists(compose_path):
            with open(compose_path, "r") as f:
                content = f.read()

            assert "pg_basebackup" not in content, \
                "docker-compose.yml should not contain inline pg_basebackup command"
            assert "gosu" not in content, \
                "docker-compose.yml should not contain gosu command"
            assert "user: root" not in content, \
                "docker-compose.yml should not run as root"

    def test_compose_has_correct_port_mapping(self):
        """Verify the compose has the correct 7777:5432 port mapping."""
        import os
        compose_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
            "replica-postgre", "docker-compose.yml"
        )

        if os.path.exists(compose_path):
            with open(compose_path, "r") as f:
                content = f.read()

            assert "7777:5432" in content
            assert "postgres:17" in content
            assert "pg_replica_data" in content
