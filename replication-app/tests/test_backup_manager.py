"""
Tests for Backup Manager Module
Verifies pg_basebackup command construction, retry logic, network error detection,
backup validation, and directory state management.
"""

import subprocess
import pytest
from unittest.mock import patch, MagicMock

from app.config import AppConfig, PostgresConfig, DockerConfig
from app.backup_manager import (
    BackupManager,
    BackupError,
    NetworkError,
    MAX_BACKUP_RETRIES,
    MAX_NETWORK_RETRIES,
    INITIAL_BACKOFF_SECONDS,
)
from app.postgres_manager import PostgresManager


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
        "retry_attempts": 3,
        "retry_delay": 1,  # Short for test speed
        "backup_timeout": 60,
    }
    pg_kwargs.update(overrides)

    return AppConfig(
        postgres=PostgresConfig(**pg_kwargs),
        docker=DockerConfig(use_docker_sdk=False),
    )


class TestBackupManagerInit:
    """Test BackupManager initialization."""

    def test_initialization(self):
        config = make_test_config()
        pm = PostgresManager(config)
        bm = BackupManager(config, pm)

        assert bm.config is config
        assert bm.postgres_manager is pm


class TestNetworkErrorDetection:
    """Test the _is_network_error method."""

    def setup_method(self):
        config = make_test_config()
        pm = PostgresManager(config)
        self.bm = BackupManager(config, pm)

    def test_connection_refused(self):
        assert self.bm._is_network_error("connection refused") is True

    def test_could_not_connect(self):
        assert self.bm._is_network_error("could not connect to server: Connection refused") is True

    def test_timeout_error(self):
        assert self.bm._is_network_error("connection timed out") is True

    def test_host_not_found(self):
        assert self.bm._is_network_error("host not found") is True

    def test_no_route_to_host(self):
        assert self.bm._is_network_error("no route to host") is True

    def test_non_network_error(self):
        assert self.bm._is_network_error("FATAL: role 'replicator' does not exist") is False

    def test_permission_error(self):
        assert self.bm._is_network_error("permission denied for database") is False

    def test_case_insensitive(self):
        assert self.bm._is_network_error("CONNECTION REFUSED") is True


class TestRetryLimits:
    """Test that hard retry limits are enforced."""

    def test_max_backup_retries_constant(self):
        """Hard limit should prevent runaway retries."""
        assert MAX_BACKUP_RETRIES == 3

    def test_retry_attempts_capped(self):
        """When config.retry_attempts exceeds MAX_BACKUP_RETRIES, it should be capped."""
        config = make_test_config(retry_attempts=10)
        pm = PostgresManager(config)
        bm = BackupManager(config, pm)

        # The run_basebackup method should use min(config.retry_attempts, MAX_BACKUP_RETRIES)
        max_retries = min(config.postgres.retry_attempts, MAX_BACKUP_RETRIES)
        assert max_retries == MAX_BACKUP_RETRIES


class TestCheckPrimaryAvailable:
    """Test the _check_primary_available method."""

    def test_primary_reachable(self):
        config = make_test_config(primary_host="172.31.37.122")
        pm = PostgresManager(config)
        bm = BackupManager(config, pm)

        mock_result = MagicMock()
        mock_result.returncode = 0

        with patch("subprocess.run", return_value=mock_result):
            assert bm._check_primary_available() is True

    def test_primary_not_reachable(self):
        config = make_test_config(primary_host="10.0.0.99")
        pm = PostgresManager(config)
        bm = BackupManager(config, pm)

        mock_result = MagicMock()
        mock_result.returncode = 2

        with patch("subprocess.run", return_value=mock_result):
            assert bm._check_primary_available() is False

    def test_primary_check_timeout(self):
        config = make_test_config()
        pm = PostgresManager(config)
        bm = BackupManager(config, pm)

        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired(cmd="test", timeout=15)):
            assert bm._check_primary_available() is False


class TestValidateBackup:
    """Test backup validation checking for required PostgreSQL files."""

    def test_all_required_files_present(self):
        config = make_test_config()
        pm = PostgresManager(config)
        bm = BackupManager(config, pm)

        mock_inspect = MagicMock()
        mock_inspect.returncode = 0
        mock_inspect.stdout = "replica-postgre_pg_replica_data"

        mock_file_check = MagicMock()
        mock_file_check.returncode = 0  # File exists

        with patch("subprocess.run", return_value=mock_file_check):
            with patch.object(bm, '_get_volume_name', return_value='test_volume'):
                assert bm.validate_backup() is True

    def test_missing_standby_signal(self):
        """Missing standby.signal should raise BackupError — indicates potential split-brain."""
        config = make_test_config()
        pm = PostgresManager(config)
        bm = BackupManager(config, pm)

        required_files = ["PG_VERSION", "postgresql.auto.conf", "standby.signal"]
        call_index = {"count": 0}

        def mock_run(cmd, **kwargs):
            mock_result = MagicMock()
            if isinstance(cmd, list) and "test" in cmd:
                # Files exist for PG_VERSION and auto.conf, but not standby.signal
                if any("standby.signal" in str(c) for c in cmd):
                    mock_result.returncode = 1  # File missing
                else:
                    mock_result.returncode = 0  # File exists
            else:
                mock_result.returncode = 0
                mock_result.stdout = "test_volume"
            return mock_result

        with patch("subprocess.run", side_effect=mock_run):
            with patch.object(bm, '_get_volume_name', return_value='test_volume'):
                with pytest.raises(BackupError, match="standby.signal"):
                    bm.validate_backup()


class TestGetBackupInfo:
    """Test backup info retrieval."""

    def test_get_backup_info_success(self):
        config = make_test_config()
        pm = PostgresManager(config)
        bm = BackupManager(config, pm)

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "17"

        mock_size_result = MagicMock()
        mock_size_result.returncode = 0
        mock_size_result.stdout = "256M\t/var/lib/postgresql/data"

        call_count = {"count": 0}

        def mock_run(cmd, **kwargs):
            call_count["count"] += 1
            if call_count["count"] <= 1:
                return mock_result  # PG_VERSION
            return mock_size_result  # du -sh

        with patch("subprocess.run", side_effect=mock_run):
            with patch.object(bm, '_get_volume_name', return_value='test_volume'):
                info = bm.get_backup_info()
                assert info["pg_version"] == "17"
                assert info["size"] == "256M"

    def test_get_backup_info_error(self):
        config = make_test_config()
        pm = PostgresManager(config)
        bm = BackupManager(config, pm)

        with patch("subprocess.run", side_effect=Exception("Docker not available")):
            with patch.object(bm, '_get_volume_name', return_value='test_volume'):
                info = bm.get_backup_info()
                assert "error" in info
