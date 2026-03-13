# PostgreSQL Replication Manager

Automates PostgreSQL replica initialization using `pg_basebackup`.

## Overview

This application manages the complete lifecycle of PostgreSQL replica initialization:

1. **Stop Replica Container** - Safely stops the replica PostgreSQL container
2. **Clean Data Directory** - Removes existing data to prepare for fresh backup
3. **Run pg_basebackup** - Creates a base backup from the primary database
4. **Validate Backup** - Verifies all required files are present
5. **Start Replica Container** - Restarts the replica with new data
6. **Verify Replication** - Confirms streaming replication is working

## Project Structure

```
replication-app/
├── app/
│   ├── __init__.py               # Package initialization
│   ├── config.py                  # Configuration management
│   ├── logger.py                  # Structured logging
│   ├── postgres_manager.py        # Docker container management
│   ├── backup_manager.py          # pg_basebackup operations
│   ├── health_check.py            # Replication health checks
│   ├── precondition_checks.py     # Pre-flight safety checks
│   ├── replication_insights.py    # Passive log analyser (no execution side-effects)
│   └── main.py                    # Main orchestration
├── requirements.txt               # Python dependencies
├── Dockerfile                     # Container image definition
├── .env.example                   # Example environment configuration
└── README.md                      # This file
```

## Prerequisites

- Python 3.10+
- Docker installed and running
- PostgreSQL primary and replica containers configured
- Replication user with appropriate permissions

## Installation

### Local Installation

```bash
# Clone or navigate to the project
cd replication-app

# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Copy and configure environment
cp .env.example .env
# Edit .env with your configuration
```

### Docker Installation

```bash
# Build the image
docker build -t replication-manager:latest .

# Run with Docker socket access
docker run --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v $(pwd)/.env:/app/.env \
  replication-manager:latest
```

## Configuration

All configuration is managed through environment variables. Copy `.env.example` to `.env` and configure:

### Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `PRIMARY_HOST` | Primary PostgreSQL host | `host.docker.internal` |
| `PRIMARY_PORT` | Primary PostgreSQL port | `5432` |
| `REPLICATION_USER` | Replication username | `replicator` |
| `REPLICATION_PASSWORD` | Replication password | `secure_password` |

### Optional Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `REPLICA_CONTAINER` | Replica container name | `postgres-replica` |
| `DATA_DIRECTORY` | PostgreSQL data directory | `/var/lib/postgresql/data` |
| `REPLICA_PORT` | Replica host port | `7777` |
| `BACKUP_TIMEOUT` | Backup timeout (seconds) | `3600` |
| `RETRY_ATTEMPTS` | pg_basebackup retry count | `5` |
| `RETRY_DELAY` | Delay between retries | `10` |
| `LOG_LEVEL` | Logging level | `INFO` |

## Usage

### Run Full Initialization

```bash
# Using Python directly
python -m app.main

# Or with explicit env file
python -m app.main --env-file /path/to/.env
```

### Check Replication Status

```bash
python -m app.main --status
```

### Export Prometheus Metrics

```bash
python -m app.main --metrics
```

### Command Line Options

```
usage: main.py [-h] [--env-file ENV_FILE] [--log-level {DEBUG,INFO,WARNING,ERROR}]
               [--json-logs] [--status] [--metrics] [--skip-cleanup] [--skip-health-check]

Options:
  --env-file          Path to .env file
  --log-level         Log level (DEBUG, INFO, WARNING, ERROR)
  --json-logs         Output logs in JSON format
  --status            Check and display replication status
  --metrics           Output Prometheus-compatible metrics
  --skip-cleanup      Skip data directory cleanup step
  --skip-health-check Skip final replication verification
```

## Docker Compose Integration

Add the replication manager to your docker-compose.yml:

```yaml
services:
  replication-manager:
    build: ./replication-app
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ./replication-app/.env:/app/.env
    depends_on:
      - postgres-primary
    profiles:
      - tools
```

Run on-demand:

```bash
docker-compose --profile tools run --rm replication-manager
```

## Scheduling

### Using Cron

```bash
# Add to crontab for weekly re-initialization
0 2 * * 0 /path/to/venv/bin/python -m app.main >> /var/log/replication-manager.log 2>&1
```

### Using Kubernetes CronJob

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: pg-replication-manager
spec:
  schedule: "0 2 * * 0"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: replication-manager
            image: replication-manager:latest
            envFrom:
            - secretRef:
                name: pg-replication-secrets
          restartPolicy: OnFailure
```

### Using Airflow

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

dag = DAG(
    'pg_replication_init',
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=5)},
    schedule_interval='@weekly',
    start_date=datetime(2024, 1, 1),
)

init_replica = BashOperator(
    task_id='initialize_replica',
    bash_command='python -m app.main',
    dag=dag,
)
```

## Intelligent Observability

The system does not simply run commands and check exit codes.  At each stage
it actively analyses the signals available to it and makes decisions based on
what those signals mean for replication correctness.

### Replication State Detection

After the replica container starts, the system queries `pg_is_in_recovery()`
on the replica.  This is the most fundamental replication signal in PostgreSQL:
a `True` result confirms the instance read `standby.signal` at startup and
entered recovery mode, meaning it will not accept writes and will not promote
itself to a primary.  A `False` result here is treated as a critical failure —
it means the instance is running as a standalone primary (a split-brain
scenario) and replication has definitively not been established.

### WAL Streaming Verification

Once recovery mode is confirmed, the system checks `pg_stat_replication` on
the primary to verify that a walsender process is actively sending WAL to the
replica.  A row in this view with `state = 'streaming'` confirms end-to-end
WAL flow: the primary is generating WAL, the walsender is sending it, and the
replica's walreceiver is consuming it.  The system records LSN positions
(`sent_lsn`, `write_lsn`, `flush_lsn`, `replay_lsn`) for lag calculation.

### Fallback Validation via Replica LSN

PostgreSQL 14+ restricts `pg_stat_replication` column visibility for users
without `pg_monitor` or superuser.  In that case the walsender row is present
but all LSN columns appear as `NULL`.  The system detects this condition and
automatically falls back to querying `pg_last_wal_receive_lsn()` on the
replica, which is accessible to any database user.  A non-`NULL` value here
confirms WAL data is flowing even when the primary view is redacted.  The
system logs the fallback path clearly so operators know their monitoring user
needs `GRANT pg_monitor TO replicator` for full visibility.

### Data Consistency Checks

Streaming replication can silently fail to replay large transactions or
specific DDL statements, leaving the replica apparently healthy but serving
incorrect data.  After streaming is confirmed, the system performs three
targeted consistency checks:

- **Row count comparison** using `pg_class.reltuples` (estimated, avoids
  full table scans).  A tolerance of 1 % is applied to account for estimation
  drift from delayed VACUUM/ANALYZE.  A larger divergence indicates missed
  writes.
- **Latest record ID check** using `MAX()` on the primary-key column of each
  table.  This is an index-only operation and directly reflects whether the
  most recent inserts have replicated.
- **Schema comparison** via `information_schema.columns`.  DDL changes
  (ALTER TABLE) replicate as WAL too; a column count or type mismatch
  indicates a DDL replay failure.

### Replication Insight Logger

The `replication_insights.py` module is a passive, read-only log analyser that
classifies log output into named states without modifying any execution flow:

| State | Meaning |
|-------|---------|
| `healthy_replication` | Recovery mode confirmed, WAL streaming active |
| `replica_lag` | Replay lag above threshold or row-count divergence |
| `retry_triggered` | pg_basebackup or workflow retry initiated |
| `container_restart` | Container stop/start event detected |
| `precondition_failed` | Pre-flight check blocked the initialization |
| `backup_in_progress` | pg_basebackup is actively streaming |
| `backup_complete` | Base backup finished successfully |
| `validation_passed` | File or data consistency check passed |
| `validation_failed` | File or data consistency check found issues |
| `interrupted` | Partial data from a previously failed run |

```bash
# Analyse a captured log file
python -m app.replication_insights /var/log/replication-manager.log
```

---

## System Intelligence Design

The system is built around a set of explicit design principles that make it
safe to run in automated pipelines without human supervision.

### Safety Preconditions

Before any destructive operation (stopping the container, clearing the data
directory), the system verifies that the environment is in a known-safe state:
primary running, no concurrent replication lock, data directory state assessed.
If any critical precondition fails, the system exits without touching data.
This prevents the common failure mode of running `rm -rf` on a data directory
while the source database is unavailable, which would leave the replica
unrecoverable.

### Retry Strategies

Two independent retry layers handle transient failures:

1. **pg_basebackup retries** (up to `MAX_BACKUP_RETRIES = 3`) with exponential
   backoff (5 s → 10 s → 20 s, capped at 120 s).  Network errors are
   distinguished from configuration errors so backoff is only applied when
   it can actually help.

2. **Workflow-level retries** (up to `MAX_WORKFLOW_RETRIES = 3`) cover broader
   failures such as Docker API errors.  Each retry runs the full precondition
   check cycle to verify the environment recovered before attempting again.

Both limits are hard-coded maximums that the configuration cannot override, so
a misconfigured `RETRY_ATTEMPTS=999` cannot create an infinite loop.

### Fallback Replication Validation

When the primary's `pg_stat_replication` view is redacted due to insufficient
privileges, the system falls back to replica-side LSN functions that are
accessible to any database user.  This means replication validation degrades
gracefully instead of falsely reporting failure in permission-restricted
environments.

### Defensive Cleanup

The data directory cleanup step inspects the current directory state before
acting.  It classifies the directory as `empty`, `valid`, `partial`, or
`corrupted`, logs that classification, and proceeds appropriately.  A
`partial` or `corrupted` state surfaces an interrupted prior run as a warning
in the logs so operators are aware of it.  Cleanup is performed using an
ephemeral Alpine container to avoid requiring root on the host.

### Health Verification Layers

The post-startup health check is deliberately layered rather than a single
binary pass/fail:

1. `pg_is_in_recovery()` — confirms standby mode (split-brain guard)
2. `pg_stat_replication` on primary — confirms active walsender (end-to-end WAL)
3. Replica LSN fallback — covers permission-restricted environments
4. Data integrity validation — catches silent replication failures

All four layers must pass for the system to declare success.  If the first two
layers pass but data validation finds minor discrepancies, the system logs a
warning but does not fail the run — replication lag during a high-write period
is expected and self-correcting.

---

## Monitoring

### Prometheus Metrics

The application exports the following metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `pg_replication_is_replica` | Gauge | 1 if instance is a replica |
| `pg_replication_is_streaming` | Gauge | 1 if replication is streaming |
| `pg_replication_lag_bytes` | Gauge | Replication lag in bytes |
| `pg_replication_replay_lag_seconds` | Gauge | WAL replay lag in seconds |
| `pg_replication_connected_replicas` | Gauge | Number of connected replicas |

### Integration with Monitoring Systems

```bash
# Export metrics to file
python -m app.main --metrics > /var/lib/node_exporter/pg_replication.prom
```

## Error Handling

The application handles the following error scenarios:

- **Connection Failures**: Automatic retry with configurable attempts
- **Backup Failures**: Retry logic with exponential backoff
- **Docker Errors**: Graceful fallback to subprocess commands
- **Permission Issues**: Clear error messages with suggested fixes
- **Timeout Errors**: Configurable timeouts for all operations

## Logging

Logs are structured and include:

- Timestamps
- Log levels (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- Step progress indicators
- Operation durations

Enable JSON logging for integration with log aggregators:

```bash
python -m app.main --json-logs
```

Example output:

```json
{"timestamp": "2024-01-15T10:30:00.000Z", "level": "INFO", "message": "[Step 1] Starting: Stop Replica Container", "logger": "replication-manager"}
```

## Troubleshooting

### Common Issues

1. **Docker socket permission denied**
   ```bash
   # Add user to docker group
   sudo usermod -aG docker $USER
   ```

2. **Primary not accepting connections**
   - Verify `pg_hba.conf` allows replication connections
   - Check firewall rules between containers

3. **Backup fails with "could not connect"**
   - Ensure `PRIMARY_HOST` is reachable from the backup container
   - Verify replication user credentials

4. **Replica doesn't start streaming**
   - Check `standby.signal` file exists
   - Verify `postgresql.auto.conf` has correct primary_conninfo

### Debug Mode

Enable debug logging for detailed output:

```bash
python -m app.main --log-level DEBUG
```

