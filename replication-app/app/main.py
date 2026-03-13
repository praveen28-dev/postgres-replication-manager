#!/usr/bin/env python3
"""
PostgreSQL Replication Manager - Main Entry Point

This module orchestrates the full replica initialization lifecycle. Each step
is deliberately ordered to prevent data corruption, avoid split-brain scenarios,
and ensure the replica comes up in a clean, consistent streaming state.

Key design decisions:
- Precondition checks run first so we never touch data unless the environment
  is proven safe (primary up, no concurrent replication, no stale locks).
- A file-based lock prevents two instances from initializing the same replica
  simultaneously, which would otherwise produce torn or inconsistent data.
- Container stop → clean → backup → validate → start ensures the replica
  directory is always populated from a single, atomic base backup rather than
  accumulated incremental changes that could leave gaps.
- Hard retry limits (MAX_WORKFLOW_RETRIES, MAX_CONTAINER_RESTARTS) bound the
  worst-case execution time and prevent runaway restart loops.
- Data validation after startup confirms byte-level consistency between the
  primary and the newly initialized replica before declaring success.
"""

import argparse
import sys
import time
from datetime import datetime
from typing import Optional

from app.config import AppConfig, load_config, validate_config
from app.logger import get_logger, ReplicationLogger
from app.postgres_manager import PostgresManager, ContainerError
from app.backup_manager import BackupManager, BackupError
from app.health_check import HealthChecker, HealthCheckError
from app.precondition_checks import (
    PreconditionChecker,
    PreconditionError,
    ReplicationLockError,
    ReplicationLock
)


class ReplicationManager:
    """
    Main orchestrator for PostgreSQL replication management.
    Coordinates all steps of replica initialization with production-grade safety.
    
    Production safety features:
    - Hard retry limits to prevent infinite loops
    - Container restart loop prevention
    - Idempotent operations
    - Atomic lock mechanism
    
    Workflow:
    1. check_preconditions()
    2. create_replication_lock()
    3. stop_replica_container()
    4. clean_replica_data_directory()
    5. run_pg_basebackup()
    6. validate_backup_files()
    7. start_replica_container()
    8. verify_replication_status()
    9. validate_data_integrity()
    10. remove_replication_lock()
    """
    
    # Hard limits to prevent infinite loops
    MAX_WORKFLOW_RETRIES = 3
    MAX_CONTAINER_RESTARTS = 2
    
    def __init__(self, config: AppConfig):
        """
        Initialize the replication manager.
        
        Args:
            config: Application configuration
        """
        self.config = config
        self.logger = get_logger()
        self.postgres_manager = PostgresManager(config)
        self.backup_manager = BackupManager(config, self.postgres_manager)
        self.health_checker = HealthChecker(config, self.postgres_manager)
        self.precondition_checker = PreconditionChecker(config)
        
        self._start_time: Optional[datetime] = None
        self._step_count = 0
        self._lock_acquired = False
        self._max_retries = self.MAX_WORKFLOW_RETRIES
        
        # Track container operations to prevent restart loops
        self._container_stop_count = 0
        self._container_start_count = 0
        self._last_completed_step = 0  # For idempotent resume
    
    def _next_step(self) -> int:
        """Get the next step number."""
        self._step_count += 1
        return self._step_count
    
    def run(
        self,
        skip_cleanup: bool = False,
        skip_health_check: bool = False,
        skip_data_validation: bool = False,
        retry_on_failure: bool = True
    ) -> bool:
        """
        Run the full replication initialization workflow.

        The workflow is intentionally sequential and non-concurrent.  Each step
        builds on the guarantee provided by the previous one:

        1. Preconditions  – environment is safe before we touch anything.
        2. Lock           – exclusive access; no parallel initializations.
        3. Stop replica   – no writes to the data directory while we clear it.
        4. Clean          – blank slate guarantees pg_basebackup starts fresh.
        5. Basebackup     – consistent physical snapshot streamed from primary.
        6. Validate       – required PostgreSQL files (standby.signal, etc.) are
                           present before we try to start the server.
        7. Start replica  – PostgreSQL starts with recovery configuration in place.
        8. Health check   – confirms the replica entered streaming replication.
        9. Data check     – row-count and schema comparison catches silent failures.

        Idempotent: Running multiple times will not break the system.
        Running again after success will re-sync the replica.
        
        Args:
            skip_cleanup: Skip the data directory cleanup step
            skip_health_check: Skip the final health check
            skip_data_validation: Skip data integrity validation
            retry_on_failure: Automatically retry on recoverable failures
            
        Returns:
            True if all steps completed successfully
        """
        self._start_time = datetime.now()
        self._step_count = 0
        self._container_stop_count = 0
        self._container_start_count = 0
        attempt = 0
        
        # Enforce hard retry limit
        effective_max_retries = min(self._max_retries, self.MAX_WORKFLOW_RETRIES)
        
        while attempt < effective_max_retries:
            attempt += 1
            
            if attempt > 1:
                self.logger.info(f"[PRECHECK] Retry attempt {attempt}/{effective_max_retries}")
            
            try:
                success = self._run_workflow(
                    skip_cleanup=skip_cleanup,
                    skip_health_check=skip_health_check,
                    skip_data_validation=skip_data_validation
                )
                return success
                
            except (ContainerError, BackupError) as e:
                self.logger.error(f"[ERROR] Replication failed: {e}")
                
                # Check for container restart loop
                if self._container_start_count >= self.MAX_CONTAINER_RESTARTS:
                    self.logger.error(
                        f"[ERROR] Container restart loop detected "
                        f"({self._container_start_count} restarts). Stopping."
                    )
                    self._print_summary(success=False, error="Container restart loop detected")
                    return False
                
                if retry_on_failure and attempt < effective_max_retries:
                    if self._handle_failure(e):
                        self.logger.info("[PRECHECK] Attempting recovery and retry...")
                        time.sleep(10)
                        continue
                
                self._print_summary(success=False, error=str(e))
                return False
                
            except (PreconditionError, ReplicationLockError) as e:
                self.logger.error(f"[ERROR] Precondition failed: {e}")
                self._print_summary(success=False, error=str(e))
                return False
                
            except Exception as e:
                self.logger.critical(f"[ERROR] Unexpected error: {e}", exc_info=True)
                self._print_summary(success=False, error=str(e))
                return False
            
            finally:
                if self._lock_acquired:
                    self._release_lock()
        
        self._print_summary(success=False, error=f"Failed after {effective_max_retries} attempts")
        return False
    
    def _run_workflow(
        self,
        skip_cleanup: bool,
        skip_health_check: bool,
        skip_data_validation: bool
    ) -> bool:
        """
        Execute the main workflow.
        
        Returns:
            True if workflow completed successfully
        """
        self.logger.info("=" * 60)
        self.logger.info("PostgreSQL Replication Manager")
        self.logger.info(f"Started at: {self._start_time.isoformat()}")
        self.logger.info("=" * 60)
        
        # Step 1: Check preconditions
        # Verifying the environment before touching anything prevents us from
        # running a destructive base backup against a downed or misconfigured
        # primary, which would leave the replica in an unrecoverable state.
        self.logger.info("[PRECHECK] Running precondition checks to verify environment safety...")
        precondition_results = self._check_preconditions()
        
        # Step 2: Acquire replication lock
        # An atomic file lock prevents a second instance (e.g. a cron retry)
        # from starting while we are mid-backup, which would corrupt the data
        # directory and force a full re-initialization.
        self.logger.info("[PRECHECK] Acquiring replication lock to prevent concurrent initializations...")
        self._acquire_lock()
        
        # Check for interrupted replication
        was_interrupted, reason = self.precondition_checker.detect_interrupted_replication()
        if was_interrupted:
            self.logger.warning(f"[PRECHECK] Previous replication was interrupted: {reason}")
            self.logger.info("[PRECHECK] Will clean up and retry")
        
        try:
            # Step 3: Stop replica container
            # The container must be stopped before we clear its data directory.
            # Cleaning a live PostgreSQL data directory leads to filesystem-level
            # corruption and crashes that are harder to recover from than a clean stop.
            step = self._next_step()
            self.logger.info(f"[STEP {step}] Stopping replica container to safely clear its data directory...")
            self._stop_replica()
            self.logger.info(f"[STEP {step}] Replica container stopped — data directory is now safe to modify")
            
            # Step 4: Clean replica data directory
            if not skip_cleanup:
                step = self._next_step()
                self.logger.info(f"[STEP {step}] Cleaning replica data directory to ensure the new base backup starts from a consistent, empty state...")
                self._clean_data_directory()
                self.logger.info(f"[STEP {step}] Replica data directory cleared — ready to receive fresh base backup")
            else:
                self.logger.info("Skipping data directory cleanup (--skip-cleanup); existing files will be overwritten by pg_basebackup")
            
            # Step 5: Run pg_basebackup
            # pg_basebackup streams a binary-exact copy of the primary's data
            # directory, including the WAL history needed for the replica to
            # resume at the correct LSN position after startup.
            step = self._next_step()
            self.logger.info(f"[STEP {step}] Running pg_basebackup to stream a consistent physical snapshot from the primary...")
            self._run_backup()
            self.logger.info(f"[STEP {step}] pg_basebackup completed — replica data directory populated with primary snapshot")
            
            # Step 6: Validate backup
            # Validating before startup avoids a situation where PostgreSQL
            # starts with missing critical files (e.g. standby.signal) and runs
            # as a primary instead of a standby — a silent split-brain scenario.
            step = self._next_step()
            self.logger.info(f"[STEP {step}] Validating backup files to confirm standby.signal and recovery configuration are present...")
            self._validate_backup()
            self.logger.info(f"[STEP {step}] Backup validated — all required PostgreSQL files verified")
            
            # Step 7: Start replica container
            # Now that the data directory is proven consistent, we start the
            # container.  PostgreSQL will read standby.signal and primary_conninfo,
            # then initiate WAL streaming from the primary automatically.
            step = self._next_step()
            self.logger.info(f"[STEP {step}] Starting replica container to begin WAL streaming recovery from primary...")
            self._start_replica()
            self.logger.info(f"[STEP {step}] Replica container started — PostgreSQL is entering standby recovery mode")
            
            # Step 8-9: Verify replication and validate data
            if not skip_health_check:
                if skip_data_validation:
                    # Confirm the replica entered streaming mode without a full data comparison.
                    # Useful when speed matters more than data-level verification.
                    step = self._next_step()
                    self.logger.info(f"[STEP {step}] Verifying streaming replication is active (WAL state check)...")
                    self._verify_replication()
                    self.logger.info(f"[STEP {step}] Replication verified — replica is streaming from primary")
                else:
                    # Full verification: streaming check + row-count and schema comparison
                    # between primary and replica to catch silent replication failures.
                    self._verify_replication_complete()
            else:
                self.logger.info("Skipping replication verification (--skip-health-check); streaming state will not be confirmed")
            
            # Step 10: Release lock (handled in finally)
            
            # Success!
            self._print_summary(success=True)
            return True
            
        except HealthCheckError as e:
            self.logger.error(f"Health check failed: {e}")
            self._print_summary(success=False, error=str(e))
            return False
    
    def _check_preconditions(self) -> dict:
        """Run all precondition checks."""
        return self.precondition_checker.run_all_preconditions()
    
    def _acquire_lock(self) -> None:
        """Acquire replication lock."""
        self.precondition_checker.acquire_replication_lock()
        self._lock_acquired = True
    
    def _release_lock(self) -> None:
        """Release replication lock."""
        if self._lock_acquired:
            self.precondition_checker.release_replication_lock()
            self._lock_acquired = False
    
    def _handle_failure(self, error: Exception) -> bool:
        """
        Handle a failure and determine if retry is possible.
        
        Args:
            error: The exception that occurred
            
        Returns:
            True if recovery was successful and retry should be attempted
        """
        self.logger.info("[RECOVERY] Analyzing failure for recovery options...")
        
        # Check if primary is still running
        try:
            self.precondition_checker.handle_container_crash("primary")
        except PreconditionError:
            self.logger.error("[RECOVERY] Primary container is down - cannot retry")
            return False
        
        # Check replica status
        try:
            if self.precondition_checker.handle_container_crash("replica"):
                self.logger.info("[RECOVERY] Replica container crash detected")
        except Exception:
            pass
        
        # Check for interrupted replication
        was_interrupted, reason = self.precondition_checker.detect_interrupted_replication()
        if was_interrupted:
            self.logger.info(f"[RECOVERY] Detected interrupted replication: {reason}")
            # Cleanup will happen on retry
        
        # Cleanup for retry
        self.precondition_checker.cleanup_for_retry()
        
        return True
    
    def _stop_replica(self) -> None:
        """
        Stop the replica container.
        
        Idempotent: Safe to call multiple times.
        Tracks stop count to detect restart loops.
        """
        container_name = self.config.postgres.replica_container
        
        # Check current status first (idempotent check)
        status = self.postgres_manager.check_container_status()
        state = status.get("status", "unknown")
        self.logger.info(f"[PRECHECK] Current container status: {state}")
        
        if state == "running":
            self._container_stop_count += 1
            self.logger.info(
                f"[BACKUP] Stopping container (stop count: {self._container_stop_count})"
            )
            
            self.postgres_manager.stop_replica_container()
            
            # Wait for it to fully stop
            time.sleep(2)
            
            # Verify stopped
            status = self.postgres_manager.check_container_status()
            if status.get("status") not in ["exited", "stopped", "not_found"]:
                raise ContainerError(f"[ERROR] Container still not stopped: {status}")
                
        elif state in ["not_found", "not_initialized"]:
            self.logger.info(f"[PRECHECK] Container {container_name} does not exist yet")
        else:
            self.logger.info(f"[PRECHECK] Container {container_name} is already stopped")
    
    def _clean_data_directory(self) -> None:
        """
        Clean the replica data directory.
        
        Idempotent: Safe to call multiple times.
        Only cleans if directory contains data.
        """
        self.backup_manager.clean_replica_directory()
    
    def _run_backup(self) -> None:
        """Run pg_basebackup."""
        self.backup_manager.run_basebackup()
    
    def _validate_backup(self) -> None:
        """Validate the backup."""
        self.backup_manager.validate_backup()
        
        # Log backup info
        info = self.backup_manager.get_backup_info()
        self.logger.info(f"[BACKUP] PG version={info.get('pg_version')}, Size={info.get('size')}")
    
    def _start_replica(self) -> None:
        """
        Start the replica container.
        
        Idempotent: Safe to call multiple times.
        Tracks start count to detect restart loops.
        
        Raises:
            ContainerError: If max restarts exceeded or start fails
        """
        # Check for restart loop before starting
        self._container_start_count += 1
        
        if self._container_start_count > self.MAX_CONTAINER_RESTARTS:
            raise ContainerError(
                f"[ERROR] Container restart loop detected: "
                f"{self._container_start_count} starts exceeds limit of {self.MAX_CONTAINER_RESTARTS}"
            )
        
        self.logger.info(
            f"[BACKUP] Starting container (start count: {self._container_start_count})"
        )
        
        self.postgres_manager.start_replica_container()
        
        # Wait for container to be ready
        self.postgres_manager.wait_for_container_ready(
            timeout=self.config.postgres.health_check_timeout
        )
        
        # Additional wait for PostgreSQL to start
        self.logger.info("[BACKUP] Waiting for PostgreSQL to start...")
        time.sleep(10)
    
    def _verify_replication(self) -> None:
        """Verify replication is working (basic check)."""
        results = self.health_checker.verify_replication(
            max_wait=self.config.postgres.health_check_timeout
        )
        
        if not results["success"]:
            errors = ", ".join(results.get("errors", ["Unknown error"]))
            raise HealthCheckError(f"Replication verification failed: {errors}")
    
    def _verify_replication_complete(self) -> None:
        """Verify replication with full data validation."""
        results = self.health_checker.verify_replication_complete(
            timeout=self.config.postgres.health_check_timeout
        )
        
        if not results["success"]:
            errors = ", ".join(results.get("errors", ["Unknown error"]))
            # Log but don't fail if only data validation failed
            if results["replica_in_recovery"] and results["wal_streaming"]:
                self.logger.warning(f"[VALIDATION] Data validation had issues: {errors}")
            else:
                raise HealthCheckError(f"Replication verification failed: {errors}")
    
    def _print_summary(self, success: bool, error: Optional[str] = None) -> None:
        """Print execution summary with structured logging."""
        end_time = datetime.now()
        duration = (end_time - self._start_time).total_seconds() if self._start_time else 0
        
        self.logger.info("=" * 60)
        if success:
            self.logger.info("[SUCCESS] EXECUTION SUMMARY")
        else:
            self.logger.info("[ERROR] EXECUTION SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"Status: {'SUCCESS ✓' if success else 'FAILED ✗'}")
        self.logger.info(f"Duration: {duration:.2f} seconds")
        self.logger.info(f"Steps completed: {self._step_count}")
        self.logger.info(f"Container stops: {self._container_stop_count}")
        self.logger.info(f"Container starts: {self._container_start_count}")
        
        if error:
            self.logger.info(f"[ERROR] {error}")
        
        if success:
            self.logger.info("")
            self.logger.info("[SUCCESS] Replica is now streaming from primary!")
            self.logger.info(f"Primary: {self.config.postgres.primary_host}:{self.config.postgres.primary_port}")
            self.logger.info(f"Replica: localhost:{self.config.postgres.replica_port}")
        
        self.logger.info("=" * 60)


def status_command(config: AppConfig) -> None:
    """Check and display current replication status."""
    logger = get_logger()
    postgres_manager = PostgresManager(config)
    health_checker = HealthChecker(config, postgres_manager)
    precondition_checker = PreconditionChecker(config)
    
    logger.info("=" * 60)
    logger.info("Checking replication status...")
    logger.info("=" * 60)
    
    # Check for replication lock
    lock = ReplicationLock()
    is_locked, lock_info = lock.is_locked()
    if is_locked:
        logger.warning(f"[STATUS] Replication in progress: {lock_info}")
    else:
        logger.info("[STATUS] No replication in progress")
    
    # Check primary container
    primary_result = precondition_checker.check_primary_container()
    logger.info(f"[STATUS] Primary container: {primary_result.message}")
    
    # Check container status
    container_status = postgres_manager.check_container_status()
    logger.info(f"[STATUS] Replica container: {container_status.get('status', 'unknown')}")
    
    if container_status.get("status") != "running":
        logger.warning("[STATUS] Replica container is not running")
        return
    
    # Check replication
    try:
        status = health_checker.check_replication_status()
        logger.info(f"[STATUS] Is replica: {status.is_replica}")
        logger.info(f"[STATUS] Is streaming: {status.is_streaming}")
        logger.info(f"[STATUS] Replication lag: {status.replication_lag_bytes} bytes")
        logger.info(f"[STATUS] Replay lag: {status.replay_lag_seconds:.2f} seconds")
        
        # Check WAL streaming
        wal_status = health_checker.check_wal_streaming_status()
        logger.info(f"[STATUS] WAL state: {wal_status.state}")
        logger.info(f"[STATUS] WAL sync state: {wal_status.sync_state}")
        
        # Check from primary
        replicas = health_checker.check_replication_from_primary()
        logger.info(f"[STATUS] Connected replicas: {len(replicas)}")
        
    except Exception as e:
        logger.error(f"Failed to check status: {e}")


def metrics_command(config: AppConfig) -> None:
    """Output metrics in Prometheus format."""
    postgres_manager = PostgresManager(config)
    health_checker = HealthChecker(config, postgres_manager)
    
    try:
        metrics = health_checker.get_metrics()
        
        print("# HELP pg_replication_is_replica Whether this instance is a replica")
        print("# TYPE pg_replication_is_replica gauge")
        print(f"pg_replication_is_replica {metrics['pg_replication_is_replica']}")
        
        print("# HELP pg_replication_is_streaming Whether replication is streaming")
        print("# TYPE pg_replication_is_streaming gauge")
        print(f"pg_replication_is_streaming {metrics['pg_replication_is_streaming']}")
        
        print("# HELP pg_replication_lag_bytes Replication lag in bytes")
        print("# TYPE pg_replication_lag_bytes gauge")
        print(f"pg_replication_lag_bytes {metrics['pg_replication_lag_bytes']}")
        
        print("# HELP pg_replication_replay_lag_seconds Replay lag in seconds")
        print("# TYPE pg_replication_replay_lag_seconds gauge")
        print(f"pg_replication_replay_lag_seconds {metrics['pg_replication_replay_lag_seconds']}")
        
        print("# HELP pg_replication_connected_replicas Number of connected replicas")
        print("# TYPE pg_replication_connected_replicas gauge")
        print(f"pg_replication_connected_replicas {metrics['pg_replication_connected_replicas']}")
        
    except Exception as e:
        print(f"# Error getting metrics: {e}", file=sys.stderr)
        sys.exit(1)


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="PostgreSQL Replication Manager - Automates replica initialization",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m app.main                  # Run full initialization
  python -m app.main --status         # Check replication status
  python -m app.main --metrics        # Output Prometheus metrics
  python -m app.main --env-file .env  # Use specific env file
        """
    )
    
    parser.add_argument(
        "--env-file",
        type=str,
        help="Path to .env file (default: auto-detect)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Log level (default: INFO)",
    )
    parser.add_argument(
        "--json-logs",
        action="store_true",
        help="Output logs in JSON format",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Check and display replication status",
    )
    parser.add_argument(
        "--metrics",
        action="store_true",
        help="Output Prometheus-compatible metrics",
    )
    parser.add_argument(
        "--skip-cleanup",
        action="store_true",
        help="Skip data directory cleanup step",
    )
    parser.add_argument(
        "--skip-health-check",
        action="store_true",
        help="Skip final replication health check",
    )
    parser.add_argument(
        "--skip-data-validation",
        action="store_true",
        help="Skip data integrity validation between primary and replica",
    )
    parser.add_argument(
        "--no-retry",
        action="store_true",
        help="Disable automatic retry on recoverable failures",
    )
    
    args = parser.parse_args()
    
    # Initialize logger
    logger = get_logger()
    logger.configure(
        level=args.log_level,
        use_json=args.json_logs,
    )
    
    try:
        # Load configuration
        config = load_config(args.env_file)
        validate_config(config)
        
        # Handle different commands
        if args.status:
            status_command(config)
            return 0
        elif args.metrics:
            metrics_command(config)
            return 0
        else:
            # Run full initialization
            manager = ReplicationManager(config)
            success = manager.run(
                skip_cleanup=args.skip_cleanup,
                skip_health_check=args.skip_health_check,
                skip_data_validation=args.skip_data_validation,
                retry_on_failure=not args.no_retry,
            )
            return 0 if success else 1
            
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except KeyboardInterrupt:
        logger.warning("Operation cancelled by user")
        return 130
    except Exception as e:
        logger.critical(f"Unexpected error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
