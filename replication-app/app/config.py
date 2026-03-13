"""
Configuration Module
Handles all configuration values and environment variable loading.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
@dataclass
class PostgresConfig:
    """PostgreSQL connection and replication configuration."""
    
    primary_host: str
    primary_port: int
    replication_user: str
    replication_password: str
    replica_container: str
    data_directory: str
    primary_container: str
    replica_port: int
    database_name: str = "postgres"  # database for health checks and table validation

    # Optional settings
    backup_timeout: int = 3600  # 1 hour default
    retry_attempts: int = 5
    retry_delay: int = 10  # seconds
    health_check_timeout: int = 60
    

@dataclass
class DockerConfig:
    """Docker-related configuration."""
    
    docker_host: Optional[str] = None
    use_docker_sdk: bool = True


@dataclass
class AppConfig:
    """Main application configuration."""
    
    postgres: PostgresConfig
    docker: DockerConfig
    log_level: str = "INFO"
    metrics_enabled: bool = False
    metrics_port: int = 8080


def load_config(env_file: Optional[str] = None) -> AppConfig:
    """
    Load configuration from environment variables.
    
    Args:
        env_file: Optional path to .env file
        
    Returns:
        AppConfig instance with all configuration values
        
    Raises:
        ValueError: If required environment variables are missing
    """
    # Load .env file if provided or look for default locations
    if env_file:
        load_dotenv(env_file)
    else:
        # Try multiple locations
        possible_paths = [
            Path(".env"),
            Path("../.env"),
            Path("/app/.env"),
        ]
        for path in possible_paths:
            if path.exists():
                load_dotenv(path)
                break
    
    # Required environment variables
    required_vars = [
        "PRIMARY_HOST",
        "PRIMARY_PORT",
        "REPLICATION_USER",
        "REPLICATION_PASSWORD",
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    # Build PostgreSQL configuration
    postgres_config = PostgresConfig(
        primary_host=os.getenv("PRIMARY_HOST", "localhost"),
        primary_port=int(os.getenv("PRIMARY_PORT", "5432")),
        replication_user=os.getenv("REPLICATION_USER", "replicator"),
        replication_password=os.getenv("REPLICATION_PASSWORD", ""),
        replica_container=os.getenv("REPLICA_CONTAINER", "postgres-replica"),
        data_directory=os.getenv("DATA_DIRECTORY", "/var/lib/postgresql/data"),
        primary_container=os.getenv("PRIMARY_CONTAINER", "postgres-primary"),
        replica_port=int(os.getenv("REPLICA_PORT", "7777")),
        database_name=os.getenv("DATABASE_NAME", "postgres"),
        backup_timeout=int(os.getenv("BACKUP_TIMEOUT", "3600")),
        retry_attempts=int(os.getenv("RETRY_ATTEMPTS", "5")),
        retry_delay=int(os.getenv("RETRY_DELAY", "10")),
        health_check_timeout=int(os.getenv("HEALTH_CHECK_TIMEOUT", "60")),
    )
    
    # Build Docker configuration
    docker_config = DockerConfig(
        docker_host=os.getenv("DOCKER_HOST"),
        use_docker_sdk=os.getenv("USE_DOCKER_SDK", "true").lower() == "true",
    )
    
    # Build main app configuration
    app_config = AppConfig(
        postgres=postgres_config,
        docker=docker_config,
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        metrics_enabled=os.getenv("METRICS_ENABLED", "false").lower() == "true",
        metrics_port=int(os.getenv("METRICS_PORT", "8080")),
    )
    
    return app_config


def validate_config(config: AppConfig) -> bool:
    """
    Validate configuration values.
    
    Args:
        config: AppConfig instance to validate
        
    Returns:
        True if configuration is valid
        
    Raises:
        ValueError: If configuration is invalid
    """
    if config.postgres.primary_port < 1 or config.postgres.primary_port > 65535:
        raise ValueError(f"Invalid primary port: {config.postgres.primary_port}")
    
    if config.postgres.retry_attempts < 1:
        raise ValueError("Retry attempts must be at least 1")
    
    if config.postgres.retry_delay < 1:
        raise ValueError("Retry delay must be at least 1 second")
    
    if not config.postgres.data_directory:
        raise ValueError("Data directory cannot be empty")
    
    return True
