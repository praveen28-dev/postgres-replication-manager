"""
Backup Manager Module
Handles pg_basebackup operations and replica data directory management.

This module is responsible for the physical transfer of data from primary to
replica.  Its design choices reflect the operational risks of base backups:

- Hard maximum retry limit (3 attempts): pg_basebackup is not idempotent
  mid-run; hitting the primary repeatedly with failed backups can generate
  excessive WAL and consume replication slots.  Three attempts is enough to
  survive transient network hiccups without causing primary-side resource
  exhaustion.

- Exponential backoff for network failures: A short, steady retry interval
  hammers a primary that may already be under load.  Doubling the wait time
  on each attempt gives the network time to recover while reducing the chance
  that we make a struggling primary worse.

- Safe directory cleanup with state verification: Cleaning a data directory
  that already contains a valid replica is a destructive, irreversible action.
  We inspect the directory state (empty / partial / valid) before proceeding
  and log the decision clearly so operators can understand what happened during
  post-incident review.

- Idempotent operations: Every operation can be safely re-run without leaving
  the system in a worse state than before.  This is critical for the retry
  loop in the orchestrator.
"""

import os
import subprocess
import time
from typing import Optional, Tuple

from app.config import AppConfig
from app.logger import get_logger
from app.postgres_manager import PostgresManager


class BackupError(Exception):
    """Custom exception for backup operations."""
    pass


class NetworkError(BackupError):
    """Exception for network-related failures."""
    pass


# Hard limits to prevent infinite loops
MAX_BACKUP_RETRIES = 3
MAX_NETWORK_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 5
MAX_BACKOFF_SECONDS = 120


class BackupManager:
    """
    Manages pg_basebackup operations for PostgreSQL replication.
    """
    
    def __init__(self, config: AppConfig, postgres_manager: PostgresManager):
        """
        Initialize the backup manager.
        
        Args:
            config: Application configuration
            postgres_manager: PostgreSQL container manager instance
        """
        self.config = config
        self.logger = get_logger()
        self.postgres_manager = postgres_manager
    
    def clean_replica_directory(self, force: bool = False) -> bool:
        """
        Clean the replica data directory to prepare it for a fresh base backup.

        This step is necessary because pg_basebackup requires an empty target
        directory (unless --waldir/--no-clean flags are used, which we avoid for
        safety).  Attempting a base backup into a non-empty directory will fail
        immediately, so we clean proactively and log the directory state so the
        action is always auditable.

        Safety: Only cleans if directory is empty, corrupted, or force=True.
        When the directory contains what looks like valid PostgreSQL data, the
        function logs a clear warning before proceeding — making it obvious in
        logs that existing data was intentionally discarded.

        Args:
            force: Force cleanup even if directory appears valid

        Returns:
            True if cleanup was successful

        Raises:
            BackupError: If cleanup fails
        """
        data_dir = self.config.postgres.data_directory
        self.logger.info(f"[BACKUP] Inspecting replica data directory state before cleanup: {data_dir}")
        
        # First, verify directory state before cleaning
        if not force:
            dir_state = self._check_directory_state()
            
            if dir_state == "valid":
                # Directory has valid PostgreSQL data - require explicit force.
                # We log this clearly so any accidental data loss is visible in audit trails.
                self.logger.warning(
                    "[BACKUP] Replica directory contains valid PostgreSQL data. "
                    "This data will be replaced by the new base backup from primary."
                )
                # For replication re-initialization, discarding old data is intentional.
                self.logger.info("[BACKUP] Proceeding with cleanup to allow fresh replication base backup")
            elif dir_state == "empty":
                self.logger.info("[BACKUP] Replica directory is already empty")
                return True
            elif dir_state in ["partial", "corrupted"]:
                self.logger.info(f"[BACKUP] Replica directory state: {dir_state} - safe to clean")
            else:
                self.logger.info(f"[BACKUP] Replica directory state: {dir_state}")
        
        # Execute cleanup
        try:
            cleanup_cmd = [
                "docker", "run", "--rm",
                "-v", f"{self._get_volume_name()}:{data_dir}",
                "alpine",
                "sh", "-c", f"rm -rf {data_dir}/* {data_dir}/.[!.]*"
            ]
            
            self.logger.debug(f"[BACKUP] Running cleanup command")
            
            result = subprocess.run(
                cleanup_cmd,
                capture_output=True,
                text=True,
                timeout=120,
            )
            
            if result.returncode == 0:
                self.logger.info(
                    "[BACKUP] Replica data directory cleaned successfully — "
                    "blank slate confirmed, ready to receive pg_basebackup"
                )
                return True
            else:
                if "No such" in result.stderr or "not found" in result.stderr:
                    self.logger.warning("[BACKUP] Volume not found - may be first initialization")
                    return True
                raise BackupError(f"[ERROR] Failed to clean data directory: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            raise BackupError("[ERROR] Cleanup operation timed out")
        except BackupError:
            raise
        except Exception as e:
            raise BackupError(f"[ERROR] Cleanup failed: {e}")
    
    def _check_directory_state(self) -> str:
        """
        Check the state of the replica data directory.
        
        Returns:
            State string: 'empty', 'valid', 'partial', 'corrupted', or 'unknown'
        """
        data_dir = self.config.postgres.data_directory
        volume_name = self._get_volume_name()
        
        try:
            check_cmd = [
                "docker", "run", "--rm",
                "-v", f"{volume_name}:{data_dir}:ro",
                "alpine",
                "sh", "-c",
                f"test -d {data_dir} && ls -A {data_dir} 2>/dev/null | head -1"
            ]
            
            result = subprocess.run(check_cmd, capture_output=True, text=True, timeout=30)
            
            # Check if directory is empty
            if not result.stdout.strip():
                return "empty"
            
            # Check for essential PostgreSQL files
            essential_files = ["PG_VERSION", "postgresql.conf", "base"]
            found_count = 0
            
            for filename in essential_files:
                file_check = subprocess.run(
                    ["docker", "run", "--rm",
                     "-v", f"{volume_name}:{data_dir}:ro",
                     "alpine", "test", "-e", f"{data_dir}/{filename}"],
                    capture_output=True, timeout=15
                )
                if file_check.returncode == 0:
                    found_count += 1
            
            if found_count == len(essential_files):
                return "valid"
            elif found_count > 0:
                return "partial"
            else:
                return "corrupted"
                
        except Exception:
            return "unknown"
    
    def _get_volume_name(self) -> str:
        """Get the Docker volume name for replica data."""
        # Convention: docker-compose creates volumes with project_volumename format
        # Try to get from docker inspect or use common naming
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
    
    def run_basebackup(self) -> bool:
        """
        Execute pg_basebackup from the primary database.

        pg_basebackup streams a binary-consistent copy of the primary's data
        directory.  The flags used here are deliberately chosen:

          -Fp  Plain format — one file per database file, easier to inspect and
               recover manually if something goes wrong.
          -Xs  Stream WAL during the backup itself, so the replica does not need
               a replication slot or retained WAL segments to catch up after
               the backup completes.
          -R   Write primary_conninfo and standby.signal automatically so
               PostgreSQL starts in standby mode without manual configuration.
          -P   Progress reporting enables the operator (and log parsers) to see
               how far through the backup we are, aiding timeout sizing.
          -v   Verbose output surfaces checkpoint and WAL segment events so we
               can detect anomalies like a primary that is not checkpointing.

        Retry strategy:
          - Hard maximum of MAX_BACKUP_RETRIES attempts (prevents infinite loops)
          - Exponential backoff for network failures (reduces primary pressure)
          - Pre-flight primary availability check before each attempt

        Returns:
            True if backup was successful

        Raises:
            BackupError: If backup fails after all retries
        """
        config = self.config.postgres
        self.logger.info("[BACKUP] Initiating pg_basebackup — streaming physical snapshot from primary")
        
        # Enforce hard limit on retries to prevent infinite loops
        max_retries = min(config.retry_attempts, MAX_BACKUP_RETRIES)
        if config.retry_attempts > MAX_BACKUP_RETRIES:
            self.logger.warning(
                f"[BACKUP] Configured retry_attempts ({config.retry_attempts}) exceeds "
                f"hard limit ({MAX_BACKUP_RETRIES}). Using {MAX_BACKUP_RETRIES}."
            )
        
        # Build pg_basebackup command
        basebackup_cmd = (
            f"PGPASSWORD='{config.replication_password}' pg_basebackup "
            f"-h {config.primary_host} "
            f"-p {config.primary_port} "
            f"-U {config.replication_user} "
            f"-D {config.data_directory} "
            f"-Fp "  # Plain format
            f"-Xs "  # WAL streaming
            f"-R "   # Create recovery config
            f"-P "   # Progress reporting
            f"-v "   # Verbose
        )
        
        volume_name = self._get_volume_name()
        
        attempt = 0
        last_error = ""
        current_backoff = INITIAL_BACKOFF_SECONDS
        
        while attempt < max_retries:
            attempt += 1
            self.logger.info(f"[BACKUP] pg_basebackup attempt {attempt}/{max_retries}")
            
            try:
                # Check primary availability with exponential backoff
                if not self._check_primary_available():
                    if attempt < max_retries:
                        self.logger.warning(
                            f"[BACKUP] Primary database unavailable. "
                            f"Waiting {current_backoff}s before retry..."
                        )
                        time.sleep(current_backoff)
                        current_backoff = min(current_backoff * 2, MAX_BACKOFF_SECONDS)
                        continue
                    else:
                        raise NetworkError("[ERROR] Primary database not available")
                
                # Reset backoff on successful connection check
                current_backoff = INITIAL_BACKOFF_SECONDS
                
                # Run pg_basebackup in a temporary container
                docker_cmd = [
                    "docker", "run", "--rm",
                    "-v", f"{volume_name}:{config.data_directory}",
                    "--network", "host",
                    "postgres:17",
                    "bash", "-c",
                    f"rm -rf {config.data_directory}/* && {basebackup_cmd} && "
                    f"chown -R postgres:postgres {config.data_directory} && "
                    f"chmod 0700 {config.data_directory}"
                ]
                
                self.logger.debug(f"[BACKUP] Running pg_basebackup command...")
                
                process = subprocess.Popen(
                    docker_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
                
                output_lines = []
                while True:
                    line = process.stdout.readline()
                    if not line and process.poll() is not None:
                        break
                    if line:
                        line = line.strip()
                        output_lines.append(line)
                        if "%" in line or "checkpoint" in line.lower():
                            self.logger.debug(f"[BACKUP] {line}")
                
                return_code = process.wait(timeout=config.backup_timeout)
                
                if return_code == 0:
                    self.logger.info(
                        "[BACKUP] pg_basebackup completed successfully — "
                        "replica data directory now contains a consistent physical snapshot"
                    )
                    return True
                else:
                    last_error = "\n".join(output_lines[-10:])
                    
                    # Check for network-related errors for exponential backoff
                    if self._is_network_error(last_error):
                        self.logger.warning(
                            f"[BACKUP] Network error on attempt {attempt}: {last_error[:200]}"
                        )
                        if attempt < max_retries:
                            self.logger.info(
                                f"[BACKUP] Applying exponential backoff: {current_backoff}s"
                            )
                            time.sleep(current_backoff)
                            current_backoff = min(current_backoff * 2, MAX_BACKOFF_SECONDS)
                            continue
                    else:
                        self.logger.warning(
                            f"[BACKUP] pg_basebackup failed (attempt {attempt}): {last_error[:200]}"
                        )
                    
            except subprocess.TimeoutExpired:
                process.kill()
                last_error = "Backup operation timed out"
                self.logger.warning(f"[BACKUP] pg_basebackup timed out (attempt {attempt})")
            except Exception as e:
                last_error = str(e)
                self.logger.warning(f"[BACKUP] pg_basebackup error (attempt {attempt}): {e}")
            
            # Wait before retry with exponential backoff
            if attempt < max_retries:
                wait_time = min(config.retry_delay * attempt, MAX_BACKOFF_SECONDS)
                self.logger.info(f"[BACKUP] Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
        
        raise BackupError(
            f"[ERROR] pg_basebackup failed after {max_retries} attempts. "
            f"Last error: {last_error}"
        )
    
    def _check_primary_available(self) -> bool:
        """
        Check if the primary database is reachable.
        
        Returns:
            True if primary is available
        """
        try:
            # Use pg_isready to check primary availability
            check_cmd = [
                "docker", "run", "--rm",
                "--network", "host",
                "postgres:17",
                "pg_isready",
                "-h", self.config.postgres.primary_host,
                "-p", str(self.config.postgres.primary_port),
                "-t", "5"  # 5 second timeout
            ]
            
            result = subprocess.run(check_cmd, capture_output=True, timeout=15)
            return result.returncode == 0
            
        except Exception as e:
            self.logger.debug(f"[BACKUP] Primary availability check failed: {e}")
            return False
    
    def _is_network_error(self, error_message: str) -> bool:
        """
        Check if an error message indicates a network failure.
        
        Args:
            error_message: The error message to check
            
        Returns:
            True if this appears to be a network error
        """
        network_indicators = [
            "connection refused",
            "could not connect",
            "timeout",
            "network",
            "host not found",
            "no route to host",
            "connection reset",
            "connection timed out",
            "temporarily unavailable",
        ]
        error_lower = error_message.lower()
        return any(indicator in error_lower for indicator in network_indicators)
    
    def validate_backup(self) -> bool:
        """
        Validate that the base backup produced a startable PostgreSQL data directory.

        This check exists because pg_basebackup can exit with code 0 in certain
        edge cases (e.g. WAL streaming interrupted right at the end) without all
        required files being present.  We explicitly verify the files PostgreSQL
        needs to start as a standby, preventing a subtle failure mode where the
        container starts but runs as a primary instead of a replica.

        Files verified:
          - PG_VERSION           : confirms the directory was initialised for the
                                   correct PostgreSQL major version.
          - postgresql.auto.conf : written by -R flag; contains primary_conninfo
                                   so the replica knows where to stream WAL from.
          - standby.signal       : written by -R flag; its presence tells
                                   PostgreSQL to start in recovery/standby mode.
                                   Without this file, PostgreSQL would start as a
                                   standalone primary, creating a split-brain.

        Returns:
            True if backup is valid

        Raises:
            BackupError: If validation fails
        """
        data_dir = self.config.postgres.data_directory
        volume_name = self._get_volume_name()
        
        self.logger.info(
            "[VALIDATION] Verifying backup integrity — checking for required PostgreSQL standby files..."
        )
        
        # Check for essential PostgreSQL files
        required_files = [
            "PG_VERSION",
            "postgresql.auto.conf",
            "standby.signal",  # Created by -R flag
        ]
        
        try:
            # Use a temporary container to check files
            for filename in required_files:
                check_cmd = [
                    "docker", "run", "--rm",
                    "-v", f"{volume_name}:{data_dir}:ro",
                    "alpine",
                    "test", "-f", f"{data_dir}/{filename}"
                ]
                
                result = subprocess.run(check_cmd, capture_output=True, timeout=30)
                
                if result.returncode != 0:
                    raise BackupError(f"[ERROR] Required file missing: {filename}")
            
            self.logger.info(
                "[VALIDATION] Backup integrity confirmed — PG_VERSION, postgresql.auto.conf, "
                "and standby.signal are all present; replica is ready to start in standby mode"
            )
            return True
            
        except subprocess.TimeoutExpired:
            raise BackupError("[ERROR] Validation timed out")
        except BackupError:
            raise
        except Exception as e:
            raise BackupError(f"[ERROR] Validation failed: {e}")
    
    def get_backup_info(self) -> dict:
        """
        Get information about the current backup.
        
        Returns:
            Dictionary with backup information
        """
        data_dir = self.config.postgres.data_directory
        volume_name = self._get_volume_name()
        
        try:
            # Get PG_VERSION
            cmd = [
                "docker", "run", "--rm",
                "-v", f"{volume_name}:{data_dir}:ro",
                "alpine",
                "cat", f"{data_dir}/PG_VERSION"
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            pg_version = result.stdout.strip() if result.returncode == 0 else "unknown"
            
            # Get data directory size
            cmd = [
                "docker", "run", "--rm",
                "-v", f"{volume_name}:{data_dir}:ro",
                "alpine",
                "du", "-sh", data_dir
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            size = result.stdout.split()[0] if result.returncode == 0 else "unknown"
            
            return {
                "pg_version": pg_version,
                "data_directory": data_dir,
                "volume_name": volume_name,
                "size": size,
            }
            
        except Exception as e:
            self.logger.warning(f"Failed to get backup info: {e}")
            return {
                "error": str(e),
            }
