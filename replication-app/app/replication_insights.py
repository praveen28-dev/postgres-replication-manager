"""
Replication Insight Logger

This module is a passive, read-only observer of the replication workflow.
It does NOT participate in the execution flow — it only reads log output and
classifies what it sees into human-readable replication health states.

Purpose
-------
Raw log lines are precise but verbose.  During an incident or post-mortem, an
engineer needs to quickly answer questions like:
  - Did the replica ever reach streaming replication?
  - Was there a retry triggered by a network error?
  - Did a container restart cause a backup to abort?
  - Is the current lag growing or shrinking?

This module answers those questions by scanning log text for known patterns
and emitting a concise insight summary.  It is safe to run against live log
files, historical log archives, or captured stdout from a previous run.

Usage
-----
    from app.replication_insights import ReplicationInsightLogger

    # Analyse a log file
    insight_logger = ReplicationInsightLogger()
    insights = insight_logger.analyze_file("/var/log/replication-manager.log")
    insight_logger.print_summary(insights)

    # Analyse a list of log lines already in memory
    insights = insight_logger.analyze_lines(log_lines)

The module has no side effects and does not import any other app modules,
making it safe to use independently for offline log analysis.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


# ---------------------------------------------------------------------------
# State taxonomy
# ---------------------------------------------------------------------------

class ReplicationState(Enum):
    """High-level states inferred from log patterns."""

    HEALTHY_REPLICATION = "healthy_replication"
    """Replica is confirmed streaming from primary with acceptable lag."""

    REPLICA_LAG = "replica_lag"
    """Replica is streaming but lag exceeds the normal threshold."""

    RETRY_TRIGGERED = "retry_triggered"
    """A backup or workflow retry was initiated due to a transient error."""

    CONTAINER_RESTART = "container_restart"
    """A container stop/start event was detected during the workflow."""

    PRECONDITION_FAILED = "precondition_failed"
    """A pre-flight check blocked the replication from starting."""

    BACKUP_IN_PROGRESS = "backup_in_progress"
    """pg_basebackup is actively streaming data from the primary."""

    BACKUP_COMPLETE = "backup_complete"
    """pg_basebackup finished successfully."""

    VALIDATION_PASSED = "validation_passed"
    """Backup file validation or data consistency check passed."""

    VALIDATION_FAILED = "validation_failed"
    """Backup file validation or data consistency check found issues."""

    INTERRUPTED = "interrupted"
    """A previous replication run was interrupted before completion."""

    LOCK_ACQUIRED = "lock_acquired"
    """Replication lock was successfully acquired (exclusive run confirmed)."""

    LOCK_CONTENTION = "lock_contention"
    """Another replication process holds the lock; this run cannot proceed."""

    UNKNOWN = "unknown"
    """No recognisable pattern was found in the log input."""


@dataclass
class ReplicationInsight:
    """A single classified observation extracted from a log line."""

    state: ReplicationState
    line_number: int
    raw_line: str
    detail: Optional[str] = None

    def __str__(self) -> str:
        label = self.state.value.replace("_", " ").title()
        loc = f"line {self.line_number}"
        detail_str = f" — {self.detail}" if self.detail else ""
        return f"[{label}] ({loc}){detail_str}"


@dataclass
class InsightSummary:
    """Aggregated view of all insights extracted from a log session."""

    insights: List[ReplicationInsight] = field(default_factory=list)
    states_seen: List[ReplicationState] = field(default_factory=list)
    retry_count: int = 0
    container_restart_count: int = 0
    lag_warnings: int = 0
    final_state: ReplicationState = ReplicationState.UNKNOWN

    def add(self, insight: ReplicationInsight) -> None:
        self.insights.append(insight)
        if insight.state not in self.states_seen:
            self.states_seen.append(insight.state)


# ---------------------------------------------------------------------------
# Pattern registry
# ---------------------------------------------------------------------------

# Each entry: (compiled regex, ReplicationState, detail_extractor_fn_or_None)
# Patterns are evaluated in order; the first match wins for a given line.
_PATTERNS: List[tuple] = [
    # ---- Success / healthy ----
    (
        re.compile(r"Replica is now streaming from primary", re.IGNORECASE),
        ReplicationState.HEALTHY_REPLICATION,
        "End-to-end streaming replication confirmed by orchestrator",
    ),
    (
        re.compile(r"Replication verification PASSED", re.IGNORECASE),
        ReplicationState.HEALTHY_REPLICATION,
        "Layered health check passed: recovery mode, WAL streaming, and primary walsender all verified",
    ),
    (
        re.compile(r"WAL streaming (active|confirmed)", re.IGNORECASE),
        ReplicationState.HEALTHY_REPLICATION,
        "WAL streaming state confirmed active",
    ),
    (
        re.compile(r"Replica confirmed in recovery mode", re.IGNORECASE),
        ReplicationState.HEALTHY_REPLICATION,
        "pg_is_in_recovery() returned True — replica will not self-promote",
    ),

    # ---- Lag ----
    (
        re.compile(r"replay_lag[=: ]+(\d+(?:\.\d+)?)\s*s", re.IGNORECASE),
        ReplicationState.REPLICA_LAG,
        None,  # detail extracted dynamically below
    ),
    (
        re.compile(r"Row count mismatch", re.IGNORECASE),
        ReplicationState.REPLICA_LAG,
        "Row count divergence detected — replica may be behind or WAL replay stalled",
    ),
    (
        re.compile(r"Latest record mismatch", re.IGNORECASE),
        ReplicationState.REPLICA_LAG,
        "MAX primary-key divergence — recent inserts on primary have not yet replayed on replica",
    ),

    # ---- Retries ----
    (
        re.compile(r"Retry attempt \d+/\d+", re.IGNORECASE),
        ReplicationState.RETRY_TRIGGERED,
        "Workflow-level retry initiated — previous attempt encountered a recoverable error",
    ),
    (
        re.compile(r"pg_basebackup attempt \d+/\d+", re.IGNORECASE),
        ReplicationState.RETRY_TRIGGERED,
        "pg_basebackup retry — primary may have been temporarily unreachable",
    ),
    (
        re.compile(r"(Retrying in|exponential backoff)", re.IGNORECASE),
        ReplicationState.RETRY_TRIGGERED,
        "Exponential backoff applied before next attempt to avoid overwhelming a struggling primary",
    ),

    # ---- Container events ----
    (
        re.compile(r"Stopping (container|replica container)", re.IGNORECASE),
        ReplicationState.CONTAINER_RESTART,
        "Replica container stopped to allow safe data directory modification",
    ),
    (
        re.compile(r"Starting (container|replica container)", re.IGNORECASE),
        ReplicationState.CONTAINER_RESTART,
        "Replica container started — PostgreSQL entering standby recovery mode",
    ),
    (
        re.compile(r"container restart loop detected", re.IGNORECASE),
        ReplicationState.CONTAINER_RESTART,
        "CRITICAL: Container restart limit exceeded — manual investigation required",
    ),

    # ---- Precondition failures ----
    (
        re.compile(r"Precondition (failed|check failed)", re.IGNORECASE),
        ReplicationState.PRECONDITION_FAILED,
        "Environment safety check blocked replication — see preceding log for root cause",
    ),
    (
        re.compile(r"Primary container is not running", re.IGNORECASE),
        ReplicationState.PRECONDITION_FAILED,
        "Primary PostgreSQL container is down — replication cannot proceed without a source",
    ),

    # ---- Backup progress ----
    (
        re.compile(r"Initiating pg_basebackup", re.IGNORECASE),
        ReplicationState.BACKUP_IN_PROGRESS,
        "Physical snapshot streaming started from primary",
    ),
    (
        re.compile(r"pg_basebackup completed successfully", re.IGNORECASE),
        ReplicationState.BACKUP_COMPLETE,
        "Binary-consistent base backup received; replica directory ready for startup",
    ),

    # ---- Validation ----
    (
        re.compile(r"(Backup integrity confirmed|Data consistency verified)", re.IGNORECASE),
        ReplicationState.VALIDATION_PASSED,
        "All required files and data consistency checks passed",
    ),
    (
        re.compile(r"(Data validation failed|Required file missing)", re.IGNORECASE),
        ReplicationState.VALIDATION_FAILED,
        "Validation found issues — check mismatch details in preceding log lines",
    ),

    # ---- Interrupted replication ----
    (
        re.compile(r"Previous replication was interrupted", re.IGNORECASE),
        ReplicationState.INTERRUPTED,
        "Partial data detected from a prior run — will clean up and retry",
    ),
    (
        re.compile(r"(Partial data|Corrupted data) found", re.IGNORECASE),
        ReplicationState.INTERRUPTED,
        "Data directory left in incomplete state by a previously failed backup",
    ),

    # ---- Lock ----
    (
        re.compile(r"Replication lock acquired", re.IGNORECASE),
        ReplicationState.LOCK_ACQUIRED,
        "Exclusive replication lock held — no concurrent initialization possible",
    ),
    (
        re.compile(r"(Replication already in progress|Another replication)", re.IGNORECASE),
        ReplicationState.LOCK_CONTENTION,
        "Lock contention detected — another process is running; this run will not proceed",
    ),
]

# Dedicated lag extractor
_LAG_PATTERN = re.compile(r"replay_lag[=: ]+(\d+(?:\.\d+)?)\s*s", re.IGNORECASE)

# Threshold above which lag is considered noteworthy in the summary
LAG_WARNING_THRESHOLD_SECONDS = 5.0


# ---------------------------------------------------------------------------
# Core analyser
# ---------------------------------------------------------------------------

class ReplicationInsightLogger:
    """
    Reads execution log lines and classifies them into replication health states.

    This class is intentionally stateless between analyze() calls so it can be
    reused across multiple log files or streaming log inputs without leaking
    state from one session into the next.
    """

    def analyze_lines(self, lines: List[str]) -> InsightSummary:
        """
        Classify a list of log lines and return an aggregated insight summary.

        Args:
            lines: Raw log lines (may include timestamps, log levels, etc.)

        Returns:
            InsightSummary with all extracted insights and aggregate counters
        """
        summary = InsightSummary()

        for line_no, raw_line in enumerate(lines, start=1):
            line = raw_line.strip()
            if not line:
                continue

            for pattern, state, fixed_detail in _PATTERNS:
                if not pattern.search(line):
                    continue

                # Dynamically extract lag value when present
                detail = fixed_detail
                if state == ReplicationState.REPLICA_LAG and detail is None:
                    m = _LAG_PATTERN.search(line)
                    if m:
                        lag_val = float(m.group(1))
                        detail = f"Replay lag = {lag_val:.2f}s"
                        if lag_val > LAG_WARNING_THRESHOLD_SECONDS:
                            summary.lag_warnings += 1
                    else:
                        detail = "Lag indicator detected"

                insight = ReplicationInsight(
                    state=state,
                    line_number=line_no,
                    raw_line=raw_line.rstrip(),
                    detail=detail,
                )
                summary.add(insight)

                # Update aggregate counters
                if state == ReplicationState.RETRY_TRIGGERED:
                    summary.retry_count += 1
                elif state == ReplicationState.CONTAINER_RESTART:
                    summary.container_restart_count += 1

                break  # first matching pattern wins per line

        # Determine the most meaningful final state
        # Priority: errors > healthy > in-progress
        priority_order = [
            ReplicationState.PRECONDITION_FAILED,
            ReplicationState.LOCK_CONTENTION,
            ReplicationState.VALIDATION_FAILED,
            ReplicationState.INTERRUPTED,
            ReplicationState.HEALTHY_REPLICATION,
            ReplicationState.VALIDATION_PASSED,
            ReplicationState.BACKUP_COMPLETE,
            ReplicationState.BACKUP_IN_PROGRESS,
            ReplicationState.RETRY_TRIGGERED,
            ReplicationState.REPLICA_LAG,
            ReplicationState.CONTAINER_RESTART,
            ReplicationState.LOCK_ACQUIRED,
        ]

        for candidate in priority_order:
            if candidate in summary.states_seen:
                summary.final_state = candidate
                break

        return summary

    def analyze_file(self, log_file_path: str) -> InsightSummary:
        """
        Read a log file from disk and analyze its contents.

        Args:
            log_file_path: Absolute or relative path to a replication log file

        Returns:
            InsightSummary with all extracted insights

        Raises:
            FileNotFoundError: If the log file does not exist
            IOError: If the file cannot be read
        """
        with open(log_file_path, "r", encoding="utf-8", errors="replace") as fh:
            lines = fh.readlines()
        return self.analyze_lines(lines)

    def print_summary(self, summary: InsightSummary) -> None:
        """
        Print a human-readable insight report to stdout.

        Args:
            summary: InsightSummary returned by analyze_lines() or analyze_file()
        """
        sep = "=" * 64
        print(sep)
        print("  Replication Insight Report")
        print(sep)

        if not summary.insights:
            print("  No recognisable replication patterns found in the log input.")
            print(sep)
            return

        # Per-insight lines
        for insight in summary.insights:
            print(f"  {insight}")

        print()
        print(sep)
        print("  SUMMARY")
        print(sep)

        final_label = summary.final_state.value.replace("_", " ").title()
        print(f"  Final inferred state : {final_label}")
        print(f"  Retry events         : {summary.retry_count}")
        print(f"  Container events     : {summary.container_restart_count}")
        print(f"  Lag warnings         : {summary.lag_warnings}")

        states_str = ", ".join(s.value.replace("_", " ") for s in summary.states_seen)
        print(f"  States observed      : {states_str}")
        print(sep)


# ---------------------------------------------------------------------------
# CLI convenience — allows: python -m app.replication_insights <log_file>
# ---------------------------------------------------------------------------

def main() -> None:
    """Entry point for standalone CLI usage."""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m app.replication_insights <log_file_path>")
        print("       Reads the log file and prints a replication health summary.")
        sys.exit(1)

    log_path = sys.argv[1]
    insight_logger = ReplicationInsightLogger()

    try:
        summary = insight_logger.analyze_file(log_path)
    except FileNotFoundError:
        print(f"Error: log file not found: {log_path}", file=sys.stderr)
        sys.exit(1)
    except IOError as exc:
        print(f"Error reading log file: {exc}", file=sys.stderr)
        sys.exit(1)

    insight_logger.print_summary(summary)


if __name__ == "__main__":
    main()
