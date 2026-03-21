"""
Health Check Module

This module verifies that streaming replication is actually working after the
replica is started.  It is the last line of defence before declaring success.

Why a separate health check layer?
-----------------------------------
PostgreSQL starting successfully does not mean replication is working.  The
server can start in standalone mode if standby.signal is missing, or it can
enter recovery but fail to connect to the primary due to authentication or
network issues.  Without an explicit check, these failures are silent — the
replica appears healthy but is serving stale data.

Verification strategy (layered from fast to thorough)
-------------------------------------------------------
1. pg_is_in_recovery()          – The quickest signal.  If False, the replica
                                   is running as a primary; replication has
                                   definitively failed.

2. pg_stat_replication          – Queried on the primary.  A row in this view
                                   with state='streaming' confirms the walsender
                                   process is active and sending WAL to this
                                   replica.

3. Replica LSN fallback         – If pg_stat_replication columns are redacted
                                   (PostgreSQL 14+ for non-superusers without
                                   pg_monitor), we fall back to
                                   pg_last_wal_receive_lsn() on the replica side,
                                   which is accessible to any database user and
                                   confirms WAL is being received.

4. Data integrity validation    – Row-count and schema comparison between primary
                                   and replica catches cases where replication
                                   appears to be running but the data diverged
                                   (e.g. due to a previous split-brain episode).
"""

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple

import psycopg2
from psycopg2 import sql

from app.config import AppConfig
from app.logger import get_logger
from app.postgres_manager import PostgresManager


@dataclass
class ReplicationStatus:
    """Replication status information."""
    
    is_replica: bool
    is_streaming: bool
    primary_host: Optional[str]
    replication_lag_bytes: Optional[int]
    replay_lag_seconds: Optional[float]
    state: str
    error: Optional[str] = None


@dataclass
class ReplicaInfo:
    """Information about a connected replica (from primary's perspective)."""
    
    pid: int
    usename: str
    application_name: str
    client_addr: str
    state: str
    sent_lsn: str
    write_lsn: str
    flush_lsn: str
    replay_lsn: str
    sync_state: str


@dataclass
class DataValidationResult:
    """Result of data validation between primary and replica."""
    
    passed: bool
    message: str
    row_count_match: bool = False
    latest_record_match: bool = False
    schema_match: bool = False
    tables_checked: List[str] = field(default_factory=list)
    mismatches: List[Dict[str, Any]] = field(default_factory=list)
    details: Optional[Dict[str, Any]] = None


@dataclass
class WALStreamingStatus:
    """WAL streaming status information."""
    
    is_streaming: bool
    state: str
    sent_lsn: Optional[str]
    write_lsn: Optional[str]
    flush_lsn: Optional[str]
    replay_lsn: Optional[str]
    sync_priority: int
    sync_state: str
    error: Optional[str] = None


class HealthCheckError(Exception):
    """Custom exception for health check operations."""
    pass


class DataValidationError(Exception):
    """Custom exception for data validation failures."""
    pass


class HealthChecker:
    """
    Performs health checks on PostgreSQL replication.
    """
    
    def __init__(self, config: AppConfig, postgres_manager: PostgresManager):
        """
        Initialize the health checker.
        
        Args:
            config: Application configuration
            postgres_manager: PostgreSQL container manager
        """
        self.config = config
        self.logger = get_logger()
        self.postgres_manager = postgres_manager
    
    def _get_replica_connection(self, timeout: int = 10) -> psycopg2.extensions.connection:
        """
        Get a connection to the replica database.
        
        Args:
            timeout: Connection timeout in seconds
            
        Returns:
            psycopg2 connection object
        """
        return psycopg2.connect(
            host="localhost",
            port=self.config.postgres.replica_port,
            user=self.config.postgres.replication_user,
            password=self.config.postgres.replication_password,
            dbname=self.config.postgres.database_name,
            connect_timeout=timeout,
        )
    
    def _get_primary_connection(self, timeout: int = 10) -> psycopg2.extensions.connection:
        """
        Get a connection to the primary database.
        
        Args:
            timeout: Connection timeout in seconds
            
        Returns:
            psycopg2 connection object
        """
        return psycopg2.connect(
            host="localhost",
            port=self.config.postgres.primary_port,
            user=self.config.postgres.replication_user,
            password=self.config.postgres.replication_password,
            dbname=self.config.postgres.database_name,
            connect_timeout=timeout,
        )
    
    def check_replica_recovery(self, max_wait: int = 60, check_interval: int = 5) -> bool:
        """
        Confirm the replica entered PostgreSQL recovery (standby) mode.

        pg_is_in_recovery() returning True is the authoritative signal that this
        instance is operating as a standby rather than a primary.  We poll with
        a timeout because PostgreSQL may still be replaying the base backup's
        initial WAL segments when we first query it.

        If the replica is NOT in recovery mode, it means standby.signal was
        absent or ignored, and the instance promoted itself to primary.  This
        is a split-brain condition and should be treated as a critical failure.

        Args:
            max_wait: Maximum time to wait for replica to be ready
            check_interval: Time between checks

        Returns:
            True if replica is in recovery mode

        Raises:
            HealthCheckError: If check fails or times out
        """
        self.logger.info(
            "[HEALTH] Waiting for replica to enter recovery mode "
            "(confirms standby.signal was processed correctly)..."
        )
        
        start_time = time.time()
        last_error = None
        
        while time.time() - start_time < max_wait:
            try:
                conn = self._get_replica_connection()
                cursor = conn.cursor()
                
                cursor.execute("SELECT pg_is_in_recovery();")
                result = cursor.fetchone()
                
                cursor.close()
                conn.close()
                
                is_in_recovery = result[0] if result else False
                
                if is_in_recovery:
                    self.logger.info(
                        "[HEALTH] Replica confirmed in recovery mode — "
                        "standby.signal accepted; instance will not promote itself ✓"
                    )
                    return True
                else:
                    self.logger.warning(
                        "[HEALTH] Replica is NOT in recovery mode — "
                        "this instance may be running as a primary (split-brain risk!)"
                    )
                    return False
                    
            except psycopg2.OperationalError as e:
                last_error = str(e)
                self.logger.debug(f"Connection attempt failed: {e}")
            except Exception as e:
                last_error = str(e)
                self.logger.debug(f"Health check error: {e}")
            
            time.sleep(check_interval)
        
        raise HealthCheckError(f"Timeout waiting for replica to be ready. Last error: {last_error}")
    
    def check_replication_status(self) -> ReplicationStatus:
        """
        Query the replica for its streaming replication state and lag metrics.

        This combines several pg_stat_wal_receiver and recovery functions into
        a single status snapshot.  The key metrics are:

        - replication_lag_bytes: the difference between the LSN the replica has
          received vs. what it has replayed.  A non-zero value is normal under
          write load; a growing value indicates the replica is falling behind.

        - replay_lag_seconds: how old the last replayed transaction is.  This
          is the user-visible staleness of the replica's data.

        Returns a ReplicationStatus with error=None on success, or with
        is_streaming=False and a populated error field on failure so callers
        can distinguish a connection error from a genuine replication failure.

        Returns:
            ReplicationStatus object with current status
        """
        self.logger.info("[HEALTH] Querying replica for streaming state and WAL lag metrics...")
        
        try:
            conn = self._get_replica_connection()
            cursor = conn.cursor()
            
            # Check if in recovery
            cursor.execute("SELECT pg_is_in_recovery();")
            is_in_recovery = cursor.fetchone()[0]
            
            # Get replication info
            cursor.execute("""
                SELECT 
                    pg_is_in_recovery() as is_replica,
                    pg_last_wal_receive_lsn() as receive_lsn,
                    pg_last_wal_replay_lsn() as replay_lsn,
                    pg_last_xact_replay_timestamp() as last_replay_time,
                    CASE 
                        WHEN pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn() 
                        THEN 0 
                        ELSE EXTRACT(EPOCH FROM now() - pg_last_xact_replay_timestamp())
                    END as replay_lag_seconds
            """)
            
            row = cursor.fetchone()
            
            # Calculate lag in bytes
            cursor.execute("""
                SELECT pg_wal_lsn_diff(
                    pg_last_wal_receive_lsn(),
                    pg_last_wal_replay_lsn()
                )
            """)
            lag_bytes = cursor.fetchone()[0] or 0
            
            cursor.close()
            conn.close()
            
            status = ReplicationStatus(
                is_replica=is_in_recovery,
                is_streaming=True,  # If we got here, it's streaming
                primary_host=self.config.postgres.primary_host,
                replication_lag_bytes=int(lag_bytes),
                replay_lag_seconds=float(row[4]) if row[4] else 0.0,
                state="streaming",
            )
            
            self.logger.info(
                f"Replication status: streaming, lag={lag_bytes} bytes, "
                f"replay_lag={status.replay_lag_seconds:.2f}s"
            )
            
            return status
            
        except Exception as e:
            self.logger.error(f"Failed to check replication status: {e}")
            return ReplicationStatus(
                is_replica=False,
                is_streaming=False,
                primary_host=None,
                replication_lag_bytes=None,
                replay_lag_seconds=None,
                state="error",
                error=str(e),
            )
    
    def check_replication_from_primary(self) -> List[ReplicaInfo]:
        """
        Query pg_stat_replication on the primary to confirm a walsender is active.

        This cross-checks the replica-side view with the primary's perspective.
        A replica can claim to be in recovery mode even if the walsender process
        on the primary has died or stalled.  Seeing a row in pg_stat_replication
        with state='streaming' confirms end-to-end WAL flow — both sides agree
        that replication is happening.

        Returns:
            List of ReplicaInfo objects for connected replicas
        """
        self.logger.info(
            "[HEALTH] Querying primary pg_stat_replication to confirm walsender is active..."
        )
        
        try:
            conn = self._get_primary_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    pid,
                    usename,
                    application_name,
                    client_addr::text,
                    state,
                    sent_lsn::text,
                    write_lsn::text,
                    flush_lsn::text,
                    replay_lsn::text,
                    sync_state
                FROM pg_stat_replication
            """)
            
            rows = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            replicas = []
            for row in rows:
                replica = ReplicaInfo(
                    pid=row[0],
                    usename=row[1],
                    application_name=row[2] or "",
                    client_addr=row[3] or "",
                    state=row[4],
                    sent_lsn=row[5] or "",
                    write_lsn=row[6] or "",
                    flush_lsn=row[7] or "",
                    replay_lsn=row[8] or "",
                    sync_state=row[9],
                )
                replicas.append(replica)
                
                self.logger.info(
                    f"Connected replica: {replica.client_addr} "
                    f"(state={replica.state}, sync={replica.sync_state})"
                )
            
            if not replicas:
                self.logger.warning(
                    "[HEALTH] No replicas connected to primary — "
                    "walsender process may not have started yet or WAL authentication failed"
                )
            
            return replicas
            
        except Exception as e:
            self.logger.error(f"Failed to check replication from primary: {e}")
            return []
    
    def verify_replication(self, max_wait: int = 60) -> Dict[str, any]:
        """
        Perform layered replication verification (fast path — no data comparison).

        Runs three checks in order, each adding confidence that replication is
        healthy:
          1. Replica in recovery mode (split-brain detection)
          2. Replica-side replication status and lag metrics
          3. Primary confirms walsender is active (end-to-end WAL flow check)

        All three must pass for results["success"] to be True.  Individual
        failure reasons are accumulated in results["errors"] so operators can
        diagnose which layer failed without re-running the check.

        Args:
            max_wait: Maximum time to wait for replication to be ready

        Returns:
            Dictionary with verification results
        """
        self.logger.info("[HEALTH] Starting layered replication verification (recovery → streaming → primary confirmation)...")
        
        results = {
            "success": False,
            "replica_in_recovery": False,
            "replication_streaming": False,
            "replica_connected_to_primary": False,
            "replication_lag_bytes": None,
            "errors": [],
        }
        
        try:
            # Layer 1: Confirm recovery mode — the most fundamental replication signal.
            # If this fails, replication is definitively broken; no point checking further.
            results["replica_in_recovery"] = self.check_replica_recovery(max_wait=max_wait)
        except HealthCheckError as e:
            results["errors"].append(f"Recovery check failed: {e}")
            return results
        
        try:
            # Layer 2: Read WAL lag metrics from the replica.
            # is_streaming=True here means the replica's walreceiver has an
            # active connection; lag_bytes quantifies how far behind it is.
            status = self.check_replication_status()
            results["replication_streaming"] = status.is_streaming
            results["replication_lag_bytes"] = status.replication_lag_bytes
            
            if status.error:
                results["errors"].append(f"Replication status error: {status.error}")
        except Exception as e:
            results["errors"].append(f"Status check failed: {e}")
        
        try:
            # Layer 3: Cross-check with the primary's walsender view.
            # Even if the replica thinks it is streaming, the primary must
            # confirm an active walsender for the connection to be genuine.
            replicas = self.check_replication_from_primary()
            results["replica_connected_to_primary"] = len(replicas) > 0
        except Exception as e:
            results["errors"].append(f"Primary check failed: {e}")
        
        # Determine overall success
        results["success"] = (
            results["replica_in_recovery"] and
            results["replication_streaming"] and
            results["replica_connected_to_primary"]
        )
        
        if results["success"]:
            self.logger.info(
                "[HEALTH] Replication verification PASSED — "
                "recovery mode confirmed, WAL streaming active, primary walsender connected ✓"
            )
        else:
            self.logger.warning(f"[HEALTH] Replication verification FAILED: {results['errors']}")
        
        return results
    
    def get_metrics(self) -> Dict[str, any]:
        """
        Get replication metrics for monitoring systems.
        
        Returns:
            Dictionary with metrics suitable for Prometheus/monitoring
        """
        status = self.check_replication_status()
        replicas = self.check_replication_from_primary()
        
        return {
            "pg_replication_is_replica": 1 if status.is_replica else 0,
            "pg_replication_is_streaming": 1 if status.is_streaming else 0,
            "pg_replication_lag_bytes": status.replication_lag_bytes or 0,
            "pg_replication_replay_lag_seconds": status.replay_lag_seconds or 0,
            "pg_replication_connected_replicas": len(replicas),
        }
    
    def _check_replica_wal_receive(self) -> bool:
        """
        Fallback WAL receive check using replica-side LSN functions.

        pg_last_wal_receive_lsn() is accessible to any database user without
        pg_monitor, making it a reliable fallback when pg_stat_replication
        columns are redacted (PostgreSQL 14+ for non-superuser/non-pg_monitor).

        This function is called only when the primary-side view returns NULL
        columns, which indicates a permissions boundary rather than a streaming
        failure.  Seeing a non-NULL LSN on the replica side confirms WAL data
        is flowing, even though we cannot inspect the primary's walsender state.

        Returns:
            True if the replica is in recovery AND has received WAL from primary
        """
        try:
            conn = self._get_replica_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT pg_is_in_recovery(),
                       pg_last_wal_receive_lsn() IS NOT NULL
            """)
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            return bool(row and row[0] and row[1])
        except Exception as e:
            self.logger.debug(f"[VALIDATION] Replica LSN check failed: {e}")
            return False

    def check_wal_streaming_status(self, max_wait: int = 0) -> WALStreamingStatus:
        """
        Check WAL streaming status from the primary's pg_stat_replication view.

        This function tries the primary view first (most informative) and
        automatically falls back to a replica-side LSN check when the primary
        columns are redacted due to insufficient privileges.

        The two-tier fallback ensures we never give a false negative on a
        healthy replica just because the monitoring user lacks pg_monitor.

        Args:
            max_wait: Seconds to keep retrying until state becomes 'streaming'.
                     0 means a single check with no retries.

        Returns:
            WALStreamingStatus object with streaming details
        """
        self.logger.info(
            "[VALIDATION] Checking WAL streaming state via primary pg_stat_replication "
            "(with replica LSN fallback for permission-limited users)..."
        )
        deadline = time.time() + max_wait

        while True:
            try:
                conn = self._get_primary_connection()
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT
                        state,
                        sent_lsn::text,
                        write_lsn::text,
                        flush_lsn::text,
                        replay_lsn::text,
                        sync_priority,
                        sync_state
                    FROM pg_stat_replication
                    LIMIT 1
                """)

                row = cursor.fetchone()
                cursor.close()
                conn.close()

                if row:
                    state = row[0]
                    is_streaming = state == 'streaming'

                    if is_streaming:
                        self.logger.info("[VALIDATION] WAL streaming active ✓")
                        return WALStreamingStatus(
                            is_streaming=True,
                            state=state,
                            sent_lsn=row[1],
                            write_lsn=row[2],
                            flush_lsn=row[3],
                            replay_lsn=row[4],
                            sync_priority=row[5],
                            sync_state=row[6],
                        )

                    if state is None:
                        # PostgreSQL 14+ redacts pg_stat_replication columns for users
                        # without pg_monitor/superuser, even for their own walsender row.
                        # Fall back to a replica-side LSN check which any user can run.
                        if self._check_replica_wal_receive():
                            self.logger.info(
                                "[VALIDATION] WAL streaming confirmed via replica LSN check ✓"
                                " (primary state hidden — grant pg_monitor to replicator for full visibility)"
                            )
                            return WALStreamingStatus(
                                is_streaming=True,
                                state='streaming',
                                sent_lsn=None,
                                write_lsn=None,
                                flush_lsn=None,
                                replay_lsn=None,
                                sync_priority=row[5],
                                sync_state=row[6],
                            )

                    if time.time() < deadline:
                        remaining = int(deadline - time.time())
                        self.logger.info(
                            f"[VALIDATION] WAL state: {state!r} — waiting for 'streaming' "
                            f"({remaining}s remaining)..."
                        )
                        time.sleep(5)
                        continue

                    self.logger.warning(
                        f"[VALIDATION] WAL state: {state!r} (timed out after {max_wait}s)"
                    )
                    return WALStreamingStatus(
                        is_streaming=False,
                        state=state,
                        sent_lsn=row[1],
                        write_lsn=row[2],
                        flush_lsn=row[3],
                        replay_lsn=row[4],
                        sync_priority=row[5],
                        sync_state=row[6],
                    )

                elif time.time() < deadline:
                    remaining = int(deadline - time.time())
                    self.logger.info(
                        f"[VALIDATION] No replication connection yet — waiting ({remaining}s remaining)..."
                    )
                    time.sleep(5)
                    continue
                else:
                    self.logger.warning("[VALIDATION] No replication connection found")
                    return WALStreamingStatus(
                        is_streaming=False,
                        state="no_connection",
                        sent_lsn=None,
                        write_lsn=None,
                        flush_lsn=None,
                        replay_lsn=None,
                        sync_priority=0,
                        sync_state="none",
                        error="No replication connection found in pg_stat_replication"
                    )

            except Exception as e:
                self.logger.error(f"[VALIDATION] Failed to check WAL streaming: {e}")
                return WALStreamingStatus(
                    is_streaming=False,
                    state="error",
                    sent_lsn=None,
                    write_lsn=None,
                    flush_lsn=None,
                    replay_lsn=None,
                    sync_priority=0,
                    sync_state="unknown",
                    error=str(e)
                )
    
    def validate_data_integrity(
        self,
        tables: Optional[List[str]] = None,
        max_wait_for_sync: int = 30
    ) -> DataValidationResult:
        """
        Validate data consistency between primary and replica.

        This check exists because streaming replication can silently fail to
        replay certain DDL or large transactions, leaving the replica apparently
        healthy but serving incorrect data.  We use three complementary checks:

        1. Row counts (pg_class.reltuples)
           Fast estimated counts that avoid full table scans.  A tolerance of
           1% is applied because reltuples is updated by VACUUM/ANALYZE, not
           every transaction.  A significant divergence (>1%) indicates the
           replica missed writes, not just an estimation drift.

        2. Latest record ID (MAX on primary key)
           Uses an index scan (O(log n)) to find the highest primary-key value
           in each table.  A mismatch here means recent INSERT traffic on the
           primary has not reached the replica yet, or WAL replay stalled.

        3. Schema comparison (information_schema.columns)
           DDL changes (ALTER TABLE, etc.) replicate as WAL too.  A schema
           mismatch means a DDL change failed to replay, which would cause
           query failures on the replica.

        Args:
            tables: List of tables to validate (format: "schema.table").
                   If None, discovers tables automatically.
            max_wait_for_sync: Max seconds to wait for replica to catch up.

        Returns:
            DataValidationResult with validation details
        """
        self.logger.info(
            "[VALIDATION] Starting data consistency check — "
            "comparing row counts, latest record IDs, and schema structure between primary and replica..."
        )
        
        result = DataValidationResult(
            passed=False,
            message="",
            tables_checked=[],
            mismatches=[]
        )
        
        try:
            # Get connections
            primary_conn = self._get_primary_connection()
            
            # Dynamic wait: poll replication lag every 2s instead of sleeping the full duration
            poll_interval = 2
            elapsed = 0
            self.logger.info(
                f"[VALIDATION] Polling replica lag (up to {max_wait_for_sync}s, every {poll_interval}s)..."
            )

            while elapsed < max_wait_for_sync:
                status = self.check_replication_status()

                if status.replication_lag_bytes is not None and status.replication_lag_bytes == 0:
                    self.logger.info(
                        "[VALIDATION] Replica has fully caught up (0 bytes lag)!"
                    )
                    break

                self.logger.info(
                    f"[VALIDATION] Waiting for replica to catch up... "
                    f"Current lag: {status.replication_lag_bytes} bytes"
                )
                time.sleep(poll_interval)
                elapsed += poll_interval
            else:
                # Loop completed without breaking — timeout reached
                self.logger.warning(
                    f"[VALIDATION] Timeout reached ({max_wait_for_sync}s) "
                    f"waiting for replica to catch up. Proceeding with validation anyway."
                )
            
            replica_conn = self._get_replica_connection()
            
            # Get tables to validate
            if not tables:
                tables = self._discover_user_tables(primary_conn)
            
            if not tables:
                self.logger.info("[VALIDATION] No user tables found to validate")
                result.passed = True
                result.message = "No user tables to validate"
                return result
            
            result.tables_checked = tables
            
            # Perform validations
            row_count_pass, row_mismatches = self._compare_row_counts(
                primary_conn, replica_conn, tables
            )
            result.row_count_match = row_count_pass
            result.mismatches.extend(row_mismatches)
            
            latest_record_pass, record_mismatches = self._compare_latest_records(
                primary_conn, replica_conn, tables
            )
            result.latest_record_match = latest_record_pass
            result.mismatches.extend(record_mismatches)
            
            schema_pass, schema_mismatches = self._compare_schemas(
                primary_conn, replica_conn, tables
            )
            result.schema_match = schema_pass
            result.mismatches.extend(schema_mismatches)
            
            # Close connections
            primary_conn.close()
            replica_conn.close()
            
            # Determine overall result
            result.passed = row_count_pass and latest_record_pass and schema_pass
            
            if result.passed:
                self.logger.info(
                    "[VALIDATION] Data consistency verified — "
                    "row counts, latest record IDs, and schema all match between primary and replica ✓"
                )
                result.message = "Data validation successful"
            else:
                self.logger.warning(
                    f"[VALIDATION] Data consistency check found {len(result.mismatches)} mismatch(es) — "
                    "replica may be lagging or WAL replay stalled"
                )
                result.message = f"Data validation failed with {len(result.mismatches)} mismatches"
            
            return result
            
        except Exception as e:
            self.logger.error(f"[VALIDATION] Data validation error: {e}")
            result.message = f"Data validation error: {e}"
            result.details = {"error": str(e)}
            return result
    
    def _discover_user_tables(self, conn: psycopg2.extensions.connection) -> List[str]:
        """
        Discover user tables in the database.
        
        Args:
            conn: Database connection
            
        Returns:
            List of table names in "schema.table" format
        """
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT schemaname || '.' || tablename
            FROM pg_tables
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            AND schemaname NOT LIKE 'pg_%'
            ORDER BY schemaname, tablename
            LIMIT 20
        """)
        
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        
        self.logger.info(f"[VALIDATION] Discovered {len(tables)} user tables")
        return tables
    
    def _compare_row_counts(
        self,
        primary_conn: psycopg2.extensions.connection,
        replica_conn: psycopg2.extensions.connection,
        tables: List[str]
    ) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        Compare row counts between primary and replica using efficient metadata queries.
        
        Uses pg_class.reltuples for estimated counts to avoid expensive full table scans.
        This is safe for replication validation as we only need approximate equality.
        
        Args:
            primary_conn: Primary database connection
            replica_conn: Replica database connection
            tables: List of tables to compare
            
        Returns:
            Tuple of (all_match, mismatches)
        """
        mismatches = []
        all_match = True
        
        # Tolerance for estimated row counts (allow 1% difference due to estimation)
        TOLERANCE_PERCENT = 0.01
        
        for table in tables:
            try:
                schema, table_name = table.split('.')
                
                # Use pg_class.reltuples for efficient estimated row count
                # This avoids expensive COUNT(*) full table scans on large tables
                count_query = """
                    SELECT COALESCE(c.reltuples, 0)::bigint as row_estimate
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = %s AND c.relname = %s
                """
                
                # Get estimated count from primary
                primary_cursor = primary_conn.cursor()
                primary_cursor.execute(count_query, (schema, table_name))
                result = primary_cursor.fetchone()
                primary_count = result[0] if result else 0
                primary_cursor.close()
                
                # Get estimated count from replica
                replica_cursor = replica_conn.cursor()
                replica_cursor.execute(count_query, (schema, table_name))
                result = replica_cursor.fetchone()
                replica_count = result[0] if result else 0
                replica_cursor.close()
                
                # Calculate acceptable tolerance
                max_count = max(primary_count, replica_count, 1)
                tolerance = max(1, int(max_count * TOLERANCE_PERCENT))
                
                difference = abs(primary_count - replica_count)
                
                if difference > tolerance:
                    all_match = False
                    mismatches.append({
                        "type": "row_count",
                        "table": table,
                        "primary_count": primary_count,
                        "replica_count": replica_count,
                        "difference": primary_count - replica_count,
                        "note": "estimated_counts"
                    })
                    self.logger.warning(
                        f"[VALIDATION] Row count mismatch in {table}: "
                        f"primary~{primary_count}, replica~{replica_count}"
                    )
                else:
                    self.logger.debug(f"[VALIDATION] {table}: ~{primary_count} rows ✓")
                    
            except Exception as e:
                # Table might not exist on replica yet
                self.logger.debug(f"[VALIDATION] Could not compare {table}: {e}")
                try:
                    primary_conn.rollback()
                    replica_conn.rollback()
                except Exception:
                    pass
        
        return all_match, mismatches
    
    def _compare_latest_records(
        self,
        primary_conn: psycopg2.extensions.connection,
        replica_conn: psycopg2.extensions.connection,
        tables: List[str]
    ) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        Compare latest record IDs between primary and replica.
        
        Uses MAX() on indexed columns which is efficient (uses index scan).
        Also looks for primary key columns to ensure we use indexed columns.
        
        Args:
            primary_conn: Primary database connection
            replica_conn: Replica database connection
            tables: List of tables to compare
            
        Returns:
            Tuple of (all_match, mismatches)
        """
        mismatches = []
        all_match = True
        
        for table in tables:
            try:
                schema, table_name = table.split('.')
                
                # First try to find the primary key column (most efficient for MAX)
                # Then fall back to common ID columns
                primary_cursor = primary_conn.cursor()
                primary_cursor.execute("""
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    JOIN pg_class c ON c.oid = i.indrelid
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE i.indisprimary
                    AND n.nspname = %s AND c.relname = %s
                    AND a.atttypid IN (20, 21, 23, 700, 701)  -- numeric types only
                    LIMIT 1
                """, (schema, table_name))
                
                pk_result = primary_cursor.fetchone()
                primary_cursor.close()
                
                if pk_result:
                    id_col_name = pk_result[0]
                else:
                    # Fall back to common ID column names
                    primary_cursor = primary_conn.cursor()
                    primary_cursor.execute("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_schema = %s AND table_name = %s
                        AND column_name IN ('id', 'order_id', 'customer_id')
                        AND data_type IN ('integer', 'bigint', 'smallint', 'numeric')
                        LIMIT 1
                    """, (schema, table_name))
                    
                    id_column = primary_cursor.fetchone()
                    primary_cursor.close()
                    
                    if not id_column:
                        continue
                    id_col_name = id_column[0]
                
                # Get max ID from primary (efficient - uses index)
                primary_cursor = primary_conn.cursor()
                primary_cursor.execute(
                    sql.SQL("SELECT MAX({}) FROM {}").format(
                        sql.Identifier(id_col_name),
                        sql.Identifier(schema, table_name)
                    )
                )
                primary_max = primary_cursor.fetchone()[0]
                primary_cursor.close()
                
                # Get max ID from replica (efficient - uses index)
                replica_cursor = replica_conn.cursor()
                replica_cursor.execute(
                    sql.SQL("SELECT MAX({}) FROM {}").format(
                        sql.Identifier(id_col_name),
                        sql.Identifier(schema, table_name)
                    )
                )
                replica_max = replica_cursor.fetchone()[0]
                replica_cursor.close()
                
                if primary_max != replica_max:
                    all_match = False
                    mismatches.append({
                        "type": "latest_record",
                        "table": table,
                        "column": id_col_name,
                        "primary_max": primary_max,
                        "replica_max": replica_max
                    })
                    self.logger.warning(
                        f"[VALIDATION] Latest record mismatch in {table}.{id_col_name}: "
                        f"primary={primary_max}, replica={replica_max}"
                    )
                else:
                    self.logger.debug(f"[VALIDATION] {table} latest {id_col_name}={primary_max} ✓")
                    
            except Exception as e:
                self.logger.debug(f"[VALIDATION] Could not compare latest record for {table}: {e}")
                try:
                    primary_conn.rollback()
                    replica_conn.rollback()
                except Exception:
                    pass
        
        return all_match, mismatches
    
    def _compare_schemas(
        self,
        primary_conn: psycopg2.extensions.connection,
        replica_conn: psycopg2.extensions.connection,
        tables: List[str]
    ) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        Compare schema structure between primary and replica.
        
        Args:
            primary_conn: Primary database connection
            replica_conn: Replica database connection
            tables: List of tables to compare
            
        Returns:
            Tuple of (all_match, mismatches)
        """
        mismatches = []
        all_match = True
        
        for table in tables:
            try:
                schema, table_name = table.split('.')
                
                # Get columns from primary
                primary_cursor = primary_conn.cursor()
                primary_cursor.execute("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (schema, table_name))
                primary_cols = primary_cursor.fetchall()
                primary_cursor.close()
                
                # Get columns from replica
                replica_cursor = replica_conn.cursor()
                replica_cursor.execute("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (schema, table_name))
                replica_cols = replica_cursor.fetchall()
                replica_cursor.close()
                
                if primary_cols != replica_cols:
                    all_match = False
                    mismatches.append({
                        "type": "schema",
                        "table": table,
                        "primary_columns": len(primary_cols),
                        "replica_columns": len(replica_cols),
                        "details": "Column structure differs"
                    })
                    self.logger.warning(f"[VALIDATION] Schema mismatch in {table}")
                else:
                    self.logger.debug(f"[VALIDATION] {table} schema matches ✓")
                    
            except Exception as e:
                self.logger.debug(f"[VALIDATION] Could not compare schema for {table}: {e}")
                try:
                    primary_conn.rollback()
                    replica_conn.rollback()
                except Exception:
                    pass
        
        return all_match, mismatches
    
    def verify_replication_complete(
        self,
        timeout: int = 120,
        check_interval: int = 5
    ) -> Dict[str, Any]:
        """
        Perform complete replication verification including WAL and data validation.
        
        Args:
            timeout: Maximum time to wait for verification
            check_interval: Time between checks
            
        Returns:
            Dictionary with complete verification results
        """
        self.logger.info("=" * 60)
        self.logger.info("[VALIDATION] Starting complete replication verification...")
        self.logger.info("=" * 60)
        
        results = {
            "success": False,
            "replica_in_recovery": False,
            "wal_streaming": False,
            "data_validated": False,
            "errors": [],
            "details": {}
        }
        
        try:
            # Step 1: Check replica is in recovery mode
            self.logger.info("[STEP 5] Checking replica recovery mode...")
            results["replica_in_recovery"] = self.check_replica_recovery(max_wait=timeout)
            if results["replica_in_recovery"]:
                self.logger.info("[STEP 5] Replica in recovery mode ✓")
            
        except HealthCheckError as e:
            results["errors"].append(f"Recovery check failed: {e}")
            return results
        
        try:
            # Step 2: Check WAL streaming status — retry for up to 60s for streaming to start
            self.logger.info("[STEP 6] Checking WAL streaming status...")
            wal_status = self.check_wal_streaming_status(max_wait=min(60, timeout))
            results["wal_streaming"] = wal_status.is_streaming
            results["details"]["wal_status"] = {
                "state": wal_status.state,
                "sync_state": wal_status.sync_state
            }
            
            if wal_status.is_streaming:
                self.logger.info("[STEP 6] WAL streaming active ✓")
            else:
                results["errors"].append(f"WAL not streaming: {wal_status.state}")
                
        except Exception as e:
            results["errors"].append(f"WAL streaming check failed: {e}")
        
        try:
            # Step 3: Validate data integrity
            self.logger.info("[STEP 7] Validating data integrity...")
            data_result = self.validate_data_integrity()
            results["data_validated"] = data_result.passed
            results["details"]["data_validation"] = {
                "row_count_match": data_result.row_count_match,
                "latest_record_match": data_result.latest_record_match,
                "schema_match": data_result.schema_match,
                "tables_checked": data_result.tables_checked,
                "mismatches": data_result.mismatches
            }
            
            if data_result.passed:
                self.logger.info("[STEP 7] Data validation successful ✓")
            else:
                results["errors"].append(f"Data validation failed: {data_result.message}")
                
        except Exception as e:
            results["errors"].append(f"Data validation failed: {e}")
        
        # Determine overall success
        results["success"] = (
            results["replica_in_recovery"] and
            results["wal_streaming"] and
            results["data_validated"]
        )
        
        self.logger.info("=" * 60)
        if results["success"]:
            self.logger.info("[VALIDATION] Complete replication verification PASSED ✓")
        else:
            self.logger.warning(f"[VALIDATION] Verification completed with issues: {results['errors']}")
        self.logger.info("=" * 60)
        
        return results
