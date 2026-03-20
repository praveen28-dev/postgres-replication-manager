"""
Tests for Precondition Checks Module
Verifies precondition checks, network fallback for cross-server deployments,
replication lock mechanism, and directory state detection.
"""

import os
import time
import pytest
from unittest.mock import patch, MagicMock, PropertyMock

from app.config import AppConfig, PostgresConfig, DockerConfig
from app.precondition_checks import (
    PreconditionChecker,
    PreconditionError,
    ReplicationLock,
    ReplicationLockError,
    ContainerState,
    DirectoryState,
    PreconditionResult,
)


def make_test_config(**overrides):
    """Create a test config with sensible defaults."""
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
        docker=DockerConfig(use_docker_sdk=False),  # Subprocess mode for tests
    )


class TestContainerState:
    """Test ContainerState enum values."""

    def test_all_states_defined(self):
        assert ContainerState.RUNNING.value == "running"
        assert ContainerState.STOPPED.value == "stopped"
        assert ContainerState.NOT_FOUND.value == "not_found"
        assert ContainerState.ERROR.value == "error"
        assert ContainerState.EXITED.value == "exited"
        assert ContainerState.DEAD.value == "dead"


class TestDirectoryState:
    """Test DirectoryState enum values."""

    def test_all_states_defined(self):
        assert DirectoryState.EMPTY.value == "empty"
        assert DirectoryState.VALID.value == "valid"
        assert DirectoryState.PARTIAL.value == "partial"
        assert DirectoryState.CORRUPTED.value == "corrupted"
        assert DirectoryState.NOT_FOUND.value == "not_found"


class TestPreconditionResult:
    """Test PreconditionResult dataclass."""

    def test_basic_result(self):
        result = PreconditionResult(passed=True, message="Check passed")
        assert result.passed is True
        assert result.message == "Check passed"
        assert result.details is None

    def test_result_with_details(self):
        details = {"container": "primary", "state": "running"}
        result = PreconditionResult(passed=True, message="OK", details=details)
        assert result.details == details
        assert result.details["state"] == "running"


class TestCheckPrimaryContainer:
    """Test primary container checking with network fallback for cross-server deployments."""

    def test_primary_running_locally(self):
        """Primary found and running via local Docker — should pass without network fallback."""
        config = make_test_config()
        checker = PreconditionChecker(config)

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "running\n"
        mock_result.stderr = ""

        with patch("subprocess.run", return_value=mock_result):
            result = checker.check_primary_container()
            assert result.passed is True
            assert "running" in result.message.lower()

    def test_primary_stopped_locally(self):
        """Primary found but not running — should fail (no fallback for stopped containers)."""
        config = make_test_config()
        checker = PreconditionChecker(config)

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "exited\n"
        mock_result.stderr = ""

        with patch("subprocess.run", return_value=mock_result):
            result = checker.check_primary_container()
            assert result.passed is False
            assert "not running" in result.message.lower()

    def test_primary_not_found_locally_but_reachable_via_network(self):
        """
        Cross-server deployment: primary not found locally, but reachable via network.
        This is the key test for the new fallback mechanism.
        """
        config = make_test_config(primary_host="172.31.37.122", primary_port=5432)
        checker = PreconditionChecker(config)

        # Docker inspect returns "not found"
        docker_inspect_result = MagicMock()
        docker_inspect_result.returncode = 1
        docker_inspect_result.stdout = ""
        docker_inspect_result.stderr = "Error: No such container: postgres-primary"

        # pg_isready returns success (primary is reachable)
        pg_isready_result = MagicMock()
        pg_isready_result.returncode = 0
        pg_isready_result.stdout = "172.31.37.122:5432 - accepting connections"
        pg_isready_result.stderr = ""

        def mock_subprocess_run(cmd, **kwargs):
            if cmd[0] == "docker" and "inspect" in cmd:
                return docker_inspect_result
            elif cmd[0] == "pg_isready":
                return pg_isready_result
            return docker_inspect_result

        with patch("subprocess.run", side_effect=mock_subprocess_run):
            result = checker.check_primary_container()
            assert result.passed is True
            assert "network" in result.message.lower() or "remote" in result.message.lower()
            assert result.details["state"] == "remote_running"
            assert result.details["host"] == "172.31.37.122"
            assert result.details["port"] == 5432

    def test_primary_not_found_locally_and_not_reachable(self):
        """Primary not found locally AND not reachable via network — should fail."""
        config = make_test_config(primary_host="10.0.0.99", primary_port=5432)
        checker = PreconditionChecker(config)

        # Docker inspect returns "not found"
        docker_inspect_result = MagicMock()
        docker_inspect_result.returncode = 1
        docker_inspect_result.stdout = ""
        docker_inspect_result.stderr = "Error: No such container: postgres-primary"

        # pg_isready returns failure (primary not reachable)
        pg_isready_result = MagicMock()
        pg_isready_result.returncode = 2
        pg_isready_result.stdout = ""
        pg_isready_result.stderr = "could not connect to server"

        def mock_subprocess_run(cmd, **kwargs):
            if cmd[0] == "docker" and "inspect" in cmd:
                return docker_inspect_result
            elif cmd[0] == "pg_isready":
                return pg_isready_result
            return docker_inspect_result

        with patch("subprocess.run", side_effect=mock_subprocess_run):
            result = checker.check_primary_container()
            assert result.passed is False
            assert "not reachable" in result.message.lower() or "unreachable" in result.details.get("state", "")

    def test_network_fallback_uses_pg_isready_via_docker_when_not_installed(self):
        """
        When pg_isready is not installed locally, the fallback should try
        running it via a temporary Docker container.
        """
        config = make_test_config(primary_host="172.31.37.122")
        checker = PreconditionChecker(config)

        call_count = {"docker_inspect": 0, "docker_pg_isready": 0}

        # Docker inspect returns "not found"
        docker_inspect_result = MagicMock()
        docker_inspect_result.returncode = 1
        docker_inspect_result.stdout = ""
        docker_inspect_result.stderr = "Error: No such container"

        # Docker-based pg_isready returns success
        docker_pg_isready_result = MagicMock()
        docker_pg_isready_result.returncode = 0
        docker_pg_isready_result.stdout = "accepting connections"
        docker_pg_isready_result.stderr = ""

        def mock_subprocess_run(cmd, **kwargs):
            if isinstance(cmd, list):
                cmd_str = " ".join(cmd)
                if "inspect" in cmd_str and "--format" in cmd_str:
                    call_count["docker_inspect"] += 1
                    return docker_inspect_result
                elif "pg_isready" in cmd_str and "docker" in cmd_str:
                    call_count["docker_pg_isready"] += 1
                    return docker_pg_isready_result
                elif cmd[0] == "pg_isready":
                    raise FileNotFoundError("pg_isready not found")
            return docker_inspect_result

        with patch("subprocess.run", side_effect=mock_subprocess_run):
            result = checker.check_primary_container()
            assert result.passed is True
            assert result.details["state"] == "remote_running"


class TestCheckPrimaryViaNetwork:
    """Test the _check_primary_via_network() method directly."""

    def test_pg_isready_success(self):
        config = make_test_config(primary_host="172.31.37.122", primary_port=5432)
        checker = PreconditionChecker(config)

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "accepting connections"
        mock_result.stderr = ""

        with patch("subprocess.run", return_value=mock_result):
            assert checker._check_primary_via_network() is True

    def test_pg_isready_failure(self):
        config = make_test_config(primary_host="10.0.0.99")
        checker = PreconditionChecker(config)

        mock_result = MagicMock()
        mock_result.returncode = 2
        mock_result.stdout = ""
        mock_result.stderr = "could not connect"

        with patch("subprocess.run", return_value=mock_result):
            assert checker._check_primary_via_network() is False

    def test_pg_isready_not_installed_fallback_to_docker(self):
        config = make_test_config(primary_host="172.31.37.122")
        checker = PreconditionChecker(config)

        docker_result = MagicMock()
        docker_result.returncode = 0

        def mock_run(cmd, **kwargs):
            if cmd[0] == "pg_isready":
                raise FileNotFoundError("pg_isready not installed")
            return docker_result

        with patch("subprocess.run", side_effect=mock_run):
            assert checker._check_primary_via_network() is True

    def test_timeout_returns_false(self):
        config = make_test_config()
        checker = PreconditionChecker(config)

        import subprocess
        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired(cmd="pg_isready", timeout=15)):
            assert checker._check_primary_via_network() is False


class TestReplicationLock:
    """Test the replication lock mechanism."""

    def setup_method(self):
        """Set up a temporary lock file path for tests."""
        self.lock_file = os.path.join(os.environ.get('TEMP', '/tmp'), 'test_replication.lock')
        # Clean up any existing lock file
        if os.path.exists(self.lock_file):
            os.remove(self.lock_file)

    def teardown_method(self):
        """Clean up test lock files."""
        if os.path.exists(self.lock_file):
            os.remove(self.lock_file)

    def test_acquire_and_release(self):
        """Test basic lock acquire and release cycle."""
        lock = ReplicationLock(self.lock_file)
        assert lock.acquire() is True
        assert lock._lock_held is True
        assert os.path.exists(self.lock_file)

        assert lock.release() is True
        assert lock._lock_held is False
        assert not os.path.exists(self.lock_file)

    def test_lock_file_contains_pid(self):
        """Verify lock file contains process PID."""
        lock = ReplicationLock(self.lock_file)
        lock.acquire()

        with open(self.lock_file, "r") as f:
            content = f.read()

        assert f"pid={os.getpid()}" in content
        lock.release()

    def test_lock_prevents_double_acquire(self):
        """Test that a second acquire fails when lock is held."""
        lock1 = ReplicationLock(self.lock_file)
        lock2 = ReplicationLock(self.lock_file)

        lock1.acquire()
        with pytest.raises(ReplicationLockError):
            lock2.acquire()

        lock1.release()

    def test_is_locked(self):
        """Test is_locked returns correct status."""
        lock = ReplicationLock(self.lock_file)

        is_locked, info = lock.is_locked()
        assert is_locked is False
        assert info is None

        lock.acquire()
        is_locked, info = lock.is_locked()
        # The current process's lock should be detected
        assert is_locked is True

        lock.release()

    def test_context_manager(self):
        """Test lock as a context manager."""
        lock = ReplicationLock(self.lock_file)

        with lock:
            assert lock._lock_held is True
            assert os.path.exists(self.lock_file)

        assert lock._lock_held is False
        assert not os.path.exists(self.lock_file)

    def test_stale_lock_detection(self):
        """Test that stale locks older than threshold are removed."""
        lock = ReplicationLock(self.lock_file)

        # Create a fake lock file with old timestamp
        with open(self.lock_file, "w") as f:
            f.write("pid=99999\nstarted=2020-01-01 00:00:00\n")

        # Set file modification time to be older than threshold
        old_time = time.time() - (ReplicationLock.STALE_LOCK_THRESHOLD_SECONDS + 100)
        os.utime(self.lock_file, (old_time, old_time))

        is_locked, info = lock.is_locked()
        assert is_locked is False  # Stale lock should be removed

    def test_release_without_acquire(self):
        """Releasing without acquiring should return False."""
        lock = ReplicationLock(self.lock_file)
        assert lock.release() is False


class TestPreconditionCheckerReplicaContainer:
    """Test replica container checking."""

    def test_replica_running(self):
        config = make_test_config()
        checker = PreconditionChecker(config)

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "running\n"
        mock_result.stderr = ""

        with patch("subprocess.run", return_value=mock_result):
            result = checker.check_replica_container()
            assert result.passed is True
            assert result.details["needs_stop"] is True

    def test_replica_not_found(self):
        config = make_test_config()
        checker = PreconditionChecker(config)

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "Error: No such container"

        with patch("subprocess.run", return_value=mock_result):
            result = checker.check_replica_container()
            assert result.passed is True  # Not found is OK — will be initialized
            assert result.details["initialized"] is False


class TestRunAllPreconditions:
    """Test the full precondition check workflow."""

    def test_all_checks_pass_cross_server(self):
        """Simulate cross-server deployment where primary is remote but reachable."""
        config = make_test_config(primary_host="172.31.37.122")
        checker = PreconditionChecker(config)

        call_tracker = []

        def mock_run(cmd, **kwargs):
            mock_result = MagicMock()
            cmd_str = " ".join(str(c) for c in cmd) if isinstance(cmd, list) else str(cmd)
            call_tracker.append(cmd_str)

            if "inspect" in cmd_str and "primary" in cmd_str.lower():
                # Primary not found locally
                mock_result.returncode = 1
                mock_result.stdout = ""
                mock_result.stderr = "No such container"
            elif "pg_isready" in cmd_str:
                # Primary is reachable via network
                mock_result.returncode = 0
                mock_result.stdout = "accepting connections"
                mock_result.stderr = ""
            elif "inspect" in cmd_str and "replica" in cmd_str.lower():
                # Replica exists and is running
                mock_result.returncode = 0
                mock_result.stdout = "running"
                mock_result.stderr = ""
            else:
                # Default for other docker commands
                mock_result.returncode = 0
                mock_result.stdout = ""
                mock_result.stderr = ""

            return mock_result

        with patch("subprocess.run", side_effect=mock_run):
            # We need to also mock the lock to prevent actual file creation in test
            with patch.object(checker, 'check_replication_lock', return_value=PreconditionResult(
                passed=True, message="No replication in progress"
            )):
                with patch.object(checker, 'check_replica_data_directory', return_value=PreconditionResult(
                    passed=True, message="Empty directory", details={"state": "empty"}
                )):
                    results = checker.run_all_preconditions()

                    assert results["primary_container"].passed is True
                    assert results["replication_lock"].passed is True
                    assert results["replica_container"].passed is True

    def test_primary_down_raises_error(self):
        """If the primary is not reachable at all, preconditions should raise PreconditionError."""
        config = make_test_config(primary_host="10.0.0.99")
        checker = PreconditionChecker(config)

        def mock_run(cmd, **kwargs):
            mock_result = MagicMock()
            cmd_str = " ".join(str(c) for c in cmd)

            if "inspect" in cmd_str:
                mock_result.returncode = 1
                mock_result.stdout = ""
                mock_result.stderr = "No such container"
            elif "pg_isready" in cmd_str:
                mock_result.returncode = 2
                mock_result.stdout = ""
                mock_result.stderr = "no response"
            else:
                mock_result.returncode = 1
                mock_result.stdout = ""
                mock_result.stderr = ""
            return mock_result

        with patch("subprocess.run", side_effect=mock_run):
            with pytest.raises(PreconditionError, match="Primary"):
                checker.run_all_preconditions()
