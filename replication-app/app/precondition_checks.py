"""
Precondition Checks Module

Before we touch a single file in the replica data directory, we need to prove
that the environment is safe to operate in.  This module encapsulates all of
those safety checks.

Why preconditions matter
------------------------
pg_basebackup and the container stop/start lifecycle are destructive operations.
Running them against a downed primary, a corrupt network path, or while another
initialization is already in progress will leave the replica in an unrecoverable
state.  Catching these problems early — before any data is modified — makes the
system safe to retry without manual cleanup.

Checks performed
----------------
1. Primary container running   – If the primary is down, there is nothing to
                                  stream from.  Failing fast here is much safer
                                  than discovering mid-backup that the source
                                  went away.

2. Replication lock free        – An atomic file lock prevents two parallel runs
                                  from racing to initialize the same replica,
                                  which would corrupt the data directory.

3. Replica container state      – Understanding whether the replica exists and
                                  whether it is running lets the orchestrator
                                  decide if a stop is needed before cleanup.

4. Replica data directory state – Distinguishing between empty, valid, partial,
                                  and corrupted directories drives the cleanup
                                  decision and surfaces interrupted replications.

Lock mechanism
--------------
The ReplicationLock class uses os.open with O_CREAT|O_EXCL (atomic file
creation) to prevent race conditions.  Stale locks (process no longer running,
or file older than STALE_LOCK_THRESHOLD_SECONDS) are detected and removed
automatically so a crashed run does not block future initializations.
"""

import os
import subprocess
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, Optional, Tuple

try:
    import docker
    from docker.errors import APIError, NotFound
    DOCKER_SDK_AVAILABLE = True
except ImportError:
    DOCKER_SDK_AVAILABLE = False

from app.config import AppConfig
from app.logger import get_logger


class PreconditionError(Exception):
    """Custom exception for precondition check failures."""
    pass


class ReplicationLockError(Exception):
    """Exception for replication lock issues."""
    pass


class ContainerState(Enum):
    """Possible states of a container."""
    RUNNING = "running"
    STOPPED = "stopped"
    NOT_FOUND = "not_found"
    ERROR = "error"
    RESTARTING = "restarting"
    PAUSED = "paused"
    EXITED = "exited"
    DEAD = "dead"


class DirectoryState(Enum):
    """Possible states of the replica data directory."""
    EMPTY = "empty"
    VALID = "valid"
    PARTIAL = "partial"
    CORRUPTED = "corrupted"
    NOT_FOUND = "not_found"


@dataclass
class PreconditionResult:
    """Result of a precondition check."""
    passed: bool
    message: str
    details: Optional[Dict] = None


class ReplicationLock:
    """
    Manages replication lock to prevent concurrent executions.
    Uses atomic file-based lock mechanism with stale lock detection.
    """
    
    DEFAULT_LOCK_FILE = "/tmp/replication.lock"
    WINDOWS_LOCK_FILE = os.path.join(os.environ.get('TEMP', 'C:\\Temp'), 'replication.lock')
    
    # Lock is considered stale after 1 hour (3600 seconds)
    STALE_LOCK_THRESHOLD_SECONDS = 3600
    
    def __init__(self, lock_file: Optional[str] = None):
        """
        Initialize the replication lock.
        
        Args:
            lock_file: Optional custom path to lock file
        """
        self.logger = get_logger()
        
        # Use provided lock file or determine based on OS
        if lock_file:
            self.lock_file = lock_file
        elif os.name == 'nt':
            self.lock_file = self.WINDOWS_LOCK_FILE
        else:
            self.lock_file = self.DEFAULT_LOCK_FILE
        
        self._lock_held = False
        self._lock_fd = None  # File descriptor for atomic locking
    
    def is_locked(self) -> Tuple[bool, Optional[Dict]]:
        """
        Check if a replication lock exists.
        Handles stale locks older than STALE_LOCK_THRESHOLD_SECONDS.
        
        Returns:
            Tuple of (is_locked, lock_info)
        """
        if os.path.exists(self.lock_file):
            try:
                # Check lock file age first
                file_age_seconds = time.time() - os.path.getmtime(self.lock_file)
                
                with open(self.lock_file, 'r') as f:
                    content = f.read().strip()
                
                # Parse lock info
                lock_info = {}
                for line in content.split('\n'):
                    if '=' in line:
                        key, value = line.split('=', 1)
                        lock_info[key.strip()] = value.strip()
                
                lock_info['age_seconds'] = int(file_age_seconds)
                
                # Check if lock is stale (older than 1 hour)
                if file_age_seconds > self.STALE_LOCK_THRESHOLD_SECONDS:
                    self.logger.warning(
                        f"[PRECHECK] Stale lock detected (age: {file_age_seconds:.0f}s > "
                        f"{self.STALE_LOCK_THRESHOLD_SECONDS}s threshold). Removing."
                    )
                    self._remove_lock_file()
                    return False, None
                
                # Check if the process is still running
                pid = lock_info.get('pid')
                if pid:
                    try:
                        pid_int = int(pid)
                        if self._is_process_running(pid_int):
                            return True, lock_info
                        else:
                            # Stale lock - process no longer running
                            self.logger.warning(f"[PRECHECK] Stale lock detected (PID {pid} not running). Removing.")
                            self._remove_lock_file()
                            return False, None
                    except ValueError:
                        self.logger.warning(f"[PRECHECK] Invalid PID in lock file: {pid}")
                
                return True, lock_info
                
            except Exception as e:
                self.logger.warning(f"[ERROR] Error reading lock file: {e}")
                return True, {"error": str(e)}
        
        return False, None
    
    def _is_process_running(self, pid: int) -> bool:
        """Check if a process with given PID is running."""
        try:
            if os.name == 'nt':
                # Windows
                import ctypes
                kernel32 = ctypes.windll.kernel32
                handle = kernel32.OpenProcess(0x1000, False, pid)
                if handle:
                    kernel32.CloseHandle(handle)
                    return True
                return False
            else:
                # Unix/Linux
                os.kill(pid, 0)
                return True
        except (OSError, ProcessLookupError):
            return False
        except Exception:
            # If we can't determine, assume it's running for safety
            return True
    
    def acquire(self, timeout: int = 0) -> bool:
        """
        Acquire the replication lock using atomic file creation.
        
        Uses os.open with O_CREAT | O_EXCL flags to ensure atomic creation,
        preventing race conditions when multiple processes try to acquire.
        
        Args:
            timeout: Time to wait for lock (0 = no wait)
            
        Returns:
            True if lock was acquired
            
        Raises:
            ReplicationLockError: If lock cannot be acquired
        """
        import errno
        
        start_time = time.time()
        
        while True:
            # First check for stale locks
            is_locked, lock_info = self.is_locked()
            
            if not is_locked:
                try:
                    # Ensure directory exists
                    lock_dir = os.path.dirname(self.lock_file)
                    if lock_dir:
                        os.makedirs(lock_dir, exist_ok=True)
                    
                    # Atomic lock file creation using O_CREAT | O_EXCL
                    # This prevents race conditions - only one process can create the file
                    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
                    
                    try:
                        self._lock_fd = os.open(self.lock_file, flags, 0o644)
                    except OSError as e:
                        if e.errno == errno.EEXIST:
                            # Another process created the lock between our check and create
                            self.logger.warning("[PRECHECK] Lock file created by another process")
                            if timeout == 0:
                                raise ReplicationLockError(
                                    "Race condition: another process acquired the lock"
                                )
                            time.sleep(1)
                            continue
                        raise
                    
                    # Write lock content
                    lock_content = (
                        f"pid={os.getpid()}\n"
                        f"started={time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                        f"host={os.environ.get('HOSTNAME', 'unknown')}\n"
                    )
                    os.write(self._lock_fd, lock_content.encode())
                    os.close(self._lock_fd)
                    self._lock_fd = None
                    
                    self._lock_held = True
                    self.logger.info(f"[PRECHECK] Replication lock acquired atomically: {self.lock_file}")
                    return True
                    
                except ReplicationLockError:
                    raise
                except Exception as e:
                    if self._lock_fd is not None:
                        try:
                            os.close(self._lock_fd)
                        except Exception:
                            pass
                        self._lock_fd = None
                    raise ReplicationLockError(f"[ERROR] Failed to create lock file: {e}")
            
            # Check timeout
            if timeout == 0:
                raise ReplicationLockError(
                    f"[ERROR] Replication already in progress. Lock info: {lock_info}"
                )
            
            if time.time() - start_time >= timeout:
                raise ReplicationLockError(
                    f"[ERROR] Timeout waiting for replication lock. Lock info: {lock_info}"
                )
            
            self.logger.info("[PRECHECK] Waiting for replication lock...")
            time.sleep(5)
    
    def release(self) -> bool:
        """
        Release the replication lock.
        
        Returns:
            True if lock was released
        """
        if self._lock_held:
            self._remove_lock_file()
            self._lock_held = False
            self.logger.info("[PRECHECK] Replication lock released")
            return True
        return False
    
    def _remove_lock_file(self) -> None:
        """Remove the lock file."""
        try:
            if os.path.exists(self.lock_file):
                os.remove(self.lock_file)
        except Exception as e:
            self.logger.warning(f"Failed to remove lock file: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        self.acquire()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.release()
        return False


class PreconditionChecker:
    """
    Performs precondition checks before replication.
    """
    
    def __init__(self, config: AppConfig):
        """
        Initialize the precondition checker.
        
        Args:
            config: Application configuration
        """
        self.config = config
        self.logger = get_logger()
        self._docker_client: Optional[docker.DockerClient] = None
        self._lock = ReplicationLock()
        
        # Initialize Docker client if SDK is available
        if config.docker.use_docker_sdk and DOCKER_SDK_AVAILABLE:
            try:
                self._docker_client = docker.from_env()
            except Exception as e:
                self.logger.warning(f"Failed to initialize Docker SDK: {e}")
    
    def _run_docker_command(self, *args: str, timeout: int = 60) -> Tuple[bool, str, str]:
        """
        Run a docker command using subprocess.
        
        Args:
            *args: Command arguments
            timeout: Command timeout in seconds
            
        Returns:
            Tuple of (success, stdout, stderr)
        """
        cmd = ["docker"] + list(args)
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            return (
                result.returncode == 0,
                result.stdout.strip(),
                result.stderr.strip(),
            )
        except subprocess.TimeoutExpired:
            return False, "", f"Command timed out after {timeout} seconds"
        except Exception as e:
            return False, "", str(e)
    
    def check_primary_container(self) -> PreconditionResult:
        """
        Verify that the primary PostgreSQL container is running.
        
        Returns:
            PreconditionResult with check status
        """
        container_name = self.config.postgres.primary_container
        self.logger.info(f"[PRECHECK] Checking primary container: {container_name}")
        
        try:
            if self._docker_client:
                try:
                    container = self._docker_client.containers.get(container_name)
                    state = ContainerState(container.status)
                    
                    if state == ContainerState.RUNNING:
                        self.logger.info(f"[PRECHECK] Primary container running ✓")
                        return PreconditionResult(
                            passed=True,
                            message="Primary container is running",
                            details={"container": container_name, "state": state.value}
                        )
                    else:
                        return PreconditionResult(
                            passed=False,
                            message=f"Primary container is not running (state: {state.value})",
                            details={"container": container_name, "state": state.value}
                        )
                        
                except NotFound:
                    return PreconditionResult(
                        passed=False,
                        message=f"Primary container '{container_name}' not found",
                        details={"container": container_name, "state": "not_found"}
                    )
            else:
                # Use subprocess
                success, stdout, stderr = self._run_docker_command(
                    "inspect", "--format", "{{.State.Status}}", container_name
                )
                
                if success:
                    state = stdout.strip()
                    if state == "running":
                        self.logger.info(f"[PRECHECK] Primary container running ✓")
                        return PreconditionResult(
                            passed=True,
                            message="Primary container is running",
                            details={"container": container_name, "state": state}
                        )
                    else:
                        return PreconditionResult(
                            passed=False,
                            message=f"Primary container is not running (state: {state})",
                            details={"container": container_name, "state": state}
                        )
                else:
                    return PreconditionResult(
                        passed=False,
                        message=f"Primary container '{container_name}' not found",
                        details={"container": container_name, "error": stderr}
                    )
                    
        except Exception as e:
            return PreconditionResult(
                passed=False,
                message=f"Error checking primary container: {e}",
                details={"error": str(e)}
            )
    
    def check_replica_container(self) -> PreconditionResult:
        """
        Check replica container status and handle different states.
        
        Returns:
            PreconditionResult with check status and container state
        """
        container_name = self.config.postgres.replica_container
        self.logger.info(f"[PRECHECK] Checking replica container: {container_name}")
        
        try:
            if self._docker_client:
                try:
                    container = self._docker_client.containers.get(container_name)
                    state = container.status
                    
                    self.logger.info(f"[PRECHECK] Replica container state: {state}")
                    
                    return PreconditionResult(
                        passed=True,
                        message=f"Replica container exists (state: {state})",
                        details={
                            "container": container_name,
                            "state": state,
                            "needs_stop": state == "running",
                            "initialized": True
                        }
                    )
                    
                except NotFound:
                    self.logger.info(f"[PRECHECK] Replica container not initialized")
                    return PreconditionResult(
                        passed=True,
                        message="Replica container not found (will be initialized)",
                        details={
                            "container": container_name,
                            "state": "not_found",
                            "needs_stop": False,
                            "initialized": False
                        }
                    )
            else:
                # Use subprocess
                success, stdout, stderr = self._run_docker_command(
                    "inspect", "--format", "{{.State.Status}}", container_name
                )
                
                if success:
                    state = stdout.strip()
                    self.logger.info(f"[PRECHECK] Replica container state: {state}")
                    
                    return PreconditionResult(
                        passed=True,
                        message=f"Replica container exists (state: {state})",
                        details={
                            "container": container_name,
                            "state": state,
                            "needs_stop": state == "running",
                            "initialized": True
                        }
                    )
                else:
                    self.logger.info(f"[PRECHECK] Replica container not initialized")
                    return PreconditionResult(
                        passed=True,
                        message="Replica container not found (will be initialized)",
                        details={
                            "container": container_name,
                            "state": "not_found",
                            "needs_stop": False,
                            "initialized": False
                        }
                    )
                    
        except Exception as e:
            return PreconditionResult(
                passed=False,
                message=f"Error checking replica container: {e}",
                details={"error": str(e)}
            )
    
    def check_replication_lock(self) -> PreconditionResult:
        """
        Check if another replication is already in progress.
        
        Returns:
            PreconditionResult indicating if replication can proceed
        """
        self.logger.info("[PRECHECK] Checking for existing replication lock")
        
        is_locked, lock_info = self._lock.is_locked()
        
        if is_locked:
            self.logger.warning(f"[PRECHECK] Replication already in progress: {lock_info}")
            return PreconditionResult(
                passed=False,
                message="Another replication is already in progress",
                details=lock_info
            )
        
        self.logger.info("[PRECHECK] No replication lock detected ✓")
        return PreconditionResult(
            passed=True,
            message="No replication in progress",
            details=None
        )
    
    def acquire_replication_lock(self) -> bool:
        """
        Acquire the replication lock.
        
        Returns:
            True if lock was acquired
            
        Raises:
            ReplicationLockError: If lock cannot be acquired
        """
        return self._lock.acquire()
    
    def release_replication_lock(self) -> bool:
        """
        Release the replication lock.
        
        Returns:
            True if lock was released
        """
        return self._lock.release()
    
    def check_replica_data_directory(self) -> PreconditionResult:
        """
        Validate the replica data directory state.
        
        Returns:
            PreconditionResult with directory state
        """
        data_dir = self.config.postgres.data_directory
        self.logger.info(f"[PRECHECK] Checking replica data directory: {data_dir}")
        
        try:
            # Get volume name for the replica
            volume_name = self._get_volume_name()
            
            # Check directory contents using a temporary container
            check_cmd = [
                "docker", "run", "--rm",
                "-v", f"{volume_name}:{data_dir}:ro",
                "alpine",
                "sh", "-c",
                f"ls -la {data_dir} 2>/dev/null && "
                f"test -f {data_dir}/PG_VERSION && echo 'HAS_PG_VERSION' || echo 'NO_PG_VERSION' && "
                f"test -f {data_dir}/postgresql.conf && echo 'HAS_CONF' || echo 'NO_CONF' && "
                f"test -d {data_dir}/base && echo 'HAS_BASE' || echo 'NO_BASE'"
            ]
            
            result = subprocess.run(check_cmd, capture_output=True, text=True, timeout=60)
            output = result.stdout
            
            if result.returncode != 0:
                # Directory might not exist or be empty
                if "No such file" in result.stderr or "not found" in result.stderr.lower():
                    self.logger.info("[PRECHECK] Replica data directory is empty/new")
                    return PreconditionResult(
                        passed=True,
                        message="Replica data directory is empty (ready for initialization)",
                        details={"state": DirectoryState.EMPTY.value, "needs_cleanup": False}
                    )
            
            # Analyze output
            has_pg_version = "HAS_PG_VERSION" in output
            has_conf = "HAS_CONF" in output
            has_base = "HAS_BASE" in output
            
            if not has_pg_version and not has_conf and not has_base:
                self.logger.info("[PRECHECK] Replica data directory is empty")
                return PreconditionResult(
                    passed=True,
                    message="Replica data directory is empty",
                    details={"state": DirectoryState.EMPTY.value, "needs_cleanup": False}
                )
            
            if has_pg_version and has_conf and has_base:
                # Check for standby.signal to see if it's a valid replica
                standby_check = subprocess.run(
                    ["docker", "run", "--rm",
                     "-v", f"{volume_name}:{data_dir}:ro",
                     "alpine", "test", "-f", f"{data_dir}/standby.signal"],
                    capture_output=True, timeout=30
                )
                
                if standby_check.returncode == 0:
                    self.logger.info("[PRECHECK] Replica data directory has valid replica data")
                    return PreconditionResult(
                        passed=True,
                        message="Replica data directory contains valid replica configuration",
                        details={"state": DirectoryState.VALID.value, "needs_cleanup": True}
                    )
                else:
                    self.logger.warning("[PRECHECK] Replica data directory may have primary data (no standby.signal)")
                    return PreconditionResult(
                        passed=True,
                        message="Replica data directory exists but may need reinitialization",
                        details={"state": DirectoryState.VALID.value, "needs_cleanup": True}
                    )
            
            # Partial data - some files missing
            self.logger.warning("[PRECHECK] Replica data directory has partial/corrupted data")
            return PreconditionResult(
                passed=True,
                message="Replica data directory has partial data (will be cleaned)",
                details={
                    "state": DirectoryState.PARTIAL.value,
                    "needs_cleanup": True,
                    "has_pg_version": has_pg_version,
                    "has_conf": has_conf,
                    "has_base": has_base
                }
            )
            
        except subprocess.TimeoutExpired:
            self.logger.warning("[PRECHECK] Timeout checking replica data directory")
            return PreconditionResult(
                passed=True,
                message="Could not verify directory state (timeout) - will proceed with cleanup",
                details={"state": DirectoryState.CORRUPTED.value, "needs_cleanup": True}
            )
        except Exception as e:
            self.logger.warning(f"[PRECHECK] Error checking replica data directory: {e}")
            return PreconditionResult(
                passed=True,
                message=f"Could not verify directory state: {e} - will proceed with cleanup",
                details={"state": "unknown", "needs_cleanup": True, "error": str(e)}
            )
    
    def _get_volume_name(self) -> str:
        """Get the Docker volume name for replica data."""
        container_name = self.config.postgres.replica_container
        
        try:
            result = subprocess.run(
                [
                    "docker", "inspect",
                    "--format", "{{range .Mounts}}{{if eq .Destination \"/var/lib/postgresql/data\"}}{{.Name}}{{end}}{{end}}",
                    container_name,
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip()
        except Exception:
            pass
        
        # Fallback to common naming convention
        return "replica-postgre_pg_replica_data"
    
    def run_all_preconditions(self) -> Dict[str, PreconditionResult]:
        """
        Run all precondition checks in priority order.

        Checks are ordered from most to least critical:
        1. Primary container — without a running primary, every subsequent step
           will fail, so we fail fast here before any destructive action.
        2. Replication lock  — prevents concurrent initializations from racing.
        3. Replica container — informational; determines the stop strategy.
        4. Replica directory — determines whether cleanup is needed.

        Raises PreconditionError on critical failures (checks 1–2) and returns
        informational results for checks 3–4 so the orchestrator can make
        context-aware decisions.

        Returns:
            Dictionary with all check results

        Raises:
            PreconditionError: If any critical check fails
        """
        self.logger.info("=" * 60)
        self.logger.info("[PRECHECK] Running environment safety checks before initializing replica...")
        self.logger.info("=" * 60)
        
        results = {}
        
        # Check 1: Primary container
        # This is the most critical check — if the primary is not running,
        # pg_basebackup will fail immediately and there is nothing to replicate from.
        results["primary_container"] = self.check_primary_container()
        if not results["primary_container"].passed:
            raise PreconditionError(
                f"Primary container check failed: {results['primary_container'].message}"
            )
        
        # Check 2: Replication lock
        # An existing lock means another process is initializing the replica.
        # Proceeding in parallel would cause both processes to write to the same
        # data directory simultaneously, producing an unusable result.
        results["replication_lock"] = self.check_replication_lock()
        if not results["replication_lock"].passed:
            raise PreconditionError(
                f"Replication lock check failed: {results['replication_lock'].message}"
            )
        
        # Check 3: Replica container (informational)
        # Knowing whether the container exists and is running determines
        # whether we need to stop it before clearing the data directory.
        results["replica_container"] = self.check_replica_container()
        
        # Check 4: Replica data directory (informational)
        # Detecting partial or corrupted data surfaces interrupted replications
        # so the orchestrator can log them and trigger a clean retry.
        results["replica_data_directory"] = self.check_replica_data_directory()
        
        self.logger.info("=" * 60)
        self.logger.info("[PRECHECK] All environment safety checks passed — safe to proceed with replica initialization ✓")
        self.logger.info("=" * 60)
        
        return results
    
    def handle_container_crash(self, container_type: str = "replica") -> bool:
        """
        Handle container crash scenario.
        
        Args:
            container_type: Type of container ("primary" or "replica")
            
        Returns:
            True if recovery action was taken
            
        Raises:
            PreconditionError: If primary container crashed
        """
        if container_type == "primary":
            result = self.check_primary_container()
            if not result.passed:
                self.logger.error("[ERROR] Primary container crashed - cannot proceed with replication")
                raise PreconditionError(
                    "Primary container is not running. Replication cannot proceed."
                )
        
        elif container_type == "replica":
            result = self.check_replica_container()
            details = result.details or {}
            
            if details.get("state") in ["dead", "exited"]:
                self.logger.warning("[WARNING] Replica container crashed - will restart replication")
                return True
        
        return False
    
    def detect_interrupted_replication(self) -> Tuple[bool, Optional[str]]:
        """
        Detect if a previous replication was interrupted.
        
        Returns:
            Tuple of (was_interrupted, reason)
        """
        dir_result = self.check_replica_data_directory()
        details = dir_result.details or {}
        
        state = details.get("state")
        
        if state == DirectoryState.PARTIAL.value:
            return True, "Partial data found - previous replication may have been interrupted"
        
        if state == DirectoryState.CORRUPTED.value:
            return True, "Corrupted data found - previous replication likely failed"
        
        return False, None
    
    def cleanup_for_retry(self) -> bool:
        """
        Clean up after a failed replication to prepare for retry.
        
        Returns:
            True if cleanup was successful
        """
        self.logger.info("[RECOVERY] Cleaning up for replication retry...")
        
        try:
            # Release any stale locks
            is_locked, lock_info = self._lock.is_locked()
            if is_locked:
                pid = lock_info.get('pid')
                if pid and not self._lock._is_process_running(int(pid)):
                    self._lock._remove_lock_file()
                    self.logger.info("[RECOVERY] Removed stale lock file")
            
            return True
            
        except Exception as e:
            self.logger.error(f"[RECOVERY] Cleanup failed: {e}")
            return False
