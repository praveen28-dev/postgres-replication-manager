import subprocess
import time
from typing import Dict, Optional, Tuple

try:
    import docker
    from docker.errors import APIError, NotFound
    DOCKER_SDK_AVAILABLE = True
except ImportError:
    DOCKER_SDK_AVAILABLE = False

from app.config import AppConfig
from app.logger import get_logger


class ContainerError(Exception):
    """Custom exception for container operations."""
    pass


class PostgresManager:
    """
    Manages PostgreSQL Docker containers.
    Supports both Docker SDK and subprocess-based operations.
    """
    
    def __init__(self, config: AppConfig):
        """
        Initialize the PostgreSQL manager.
        
        Args:
            config: Application configuration
        """
        self.config = config
        self.logger = get_logger()
        self._docker_client: Optional[docker.DockerClient] = None
        
        # Initialize Docker client if SDK is available and enabled
        if config.docker.use_docker_sdk and DOCKER_SDK_AVAILABLE:
            try:
                self._docker_client = docker.from_env()
                self.logger.debug("Docker SDK client initialized")
            except Exception as e:
                self.logger.warning(f"Failed to initialize Docker SDK: {e}. Falling back to subprocess.")
    
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
        self.logger.debug(f"Running command: {' '.join(cmd)}")
        
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
    
    def stop_replica_container(self) -> bool:
        """
        Stop the replica PostgreSQL container.
        
        Returns:
            True if container was stopped successfully
            
        Raises:
            ContainerError: If stop operation fails
        """
        container_name = self.config.postgres.replica_container
        self.logger.info(f"Stopping replica container: {container_name}")
        
        if self._docker_client:
            try:
                container = self._docker_client.containers.get(container_name)
                if container.status == "running":
                    container.stop(timeout=30)
                    self.logger.info(f"Container {container_name} stopped successfully")
                else:
                    self.logger.info(f"Container {container_name} is already stopped (status: {container.status})")
                return True
            except NotFound:
                self.logger.warning(f"Container {container_name} not found")
                return True  # Container doesn't exist, consider it stopped
            except APIError as e:
                raise ContainerError(f"Failed to stop container: {e}")
        else:
            # Use subprocess
            success, stdout, stderr = self._run_docker_command("stop", container_name, timeout=60)
            if success:
                self.logger.info(f"Container {container_name} stopped successfully")
                return True
            elif "No such container" in stderr:
                self.logger.warning(f"Container {container_name} not found")
                return True
            else:
                raise ContainerError(f"Failed to stop container: {stderr}")
    
    def start_replica_container(self) -> bool:
        """
        Start the replica PostgreSQL container.
        
        Returns:
            True if container was started successfully
            
        Raises:
            ContainerError: If start operation fails
        """
        container_name = self.config.postgres.replica_container
        self.logger.info(f"Starting replica container: {container_name}")
        
        if self._docker_client:
            try:
                container = self._docker_client.containers.get(container_name)
                if container.status != "running":
                    container.start()
                    self.logger.info(f"Container {container_name} started successfully")
                else:
                    self.logger.info(f"Container {container_name} is already running")
                return True
            except NotFound:
                raise ContainerError(f"Container {container_name} not found. Please create it first.")
            except APIError as e:
                raise ContainerError(f"Failed to start container: {e}")
        else:
            # Use subprocess
            success, stdout, stderr = self._run_docker_command("start", container_name, timeout=60)
            if success:
                self.logger.info(f"Container {container_name} started successfully")
                return True
            else:
                raise ContainerError(f"Failed to start container: {stderr}")
    
    def check_container_status(self, container_name: Optional[str] = None) -> Dict[str, str]:
        """
        Check the status of a container.
        
        Args:
            container_name: Name of container to check (defaults to replica)
            
        Returns:
            Dictionary with container status information
        """
        if container_name is None:
            container_name = self.config.postgres.replica_container
        
        if self._docker_client:
            try:
                container = self._docker_client.containers.get(container_name)
                return {
                    "name": container.name,
                    "status": container.status,
                    "id": container.short_id,
                    "image": container.image.tags[0] if container.image.tags else "unknown",
                }
            except NotFound:
                return {
                    "name": container_name,
                    "status": "not_found",
                    "id": "",
                    "image": "",
                }
            except APIError as e:
                return {
                    "name": container_name,
                    "status": "error",
                    "error": str(e),
                }
        else:
            # Use subprocess with inspect
            success, stdout, stderr = self._run_docker_command(
                "inspect",
                "--format",
                "{{.State.Status}}",
                container_name,
            )
            if success:
                return {
                    "name": container_name,
                    "status": stdout,
                }
            else:
                return {
                    "name": container_name,
                    "status": "not_found" if "No such" in stderr else "error",
                    "error": stderr,
                }
    
    def wait_for_container_ready(
        self,
        container_name: Optional[str] = None,
        timeout: int = 60,
        check_interval: int = 5,
    ) -> bool:
        """
        Wait for a container to be in running state.
        
        Args:
            container_name: Name of container to wait for
            timeout: Maximum time to wait in seconds
            check_interval: Time between status checks
            
        Returns:
            True if container is running
            
        Raises:
            ContainerError: If timeout is reached
        """
        if container_name is None:
            container_name = self.config.postgres.replica_container
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            status = self.check_container_status(container_name)
            if status.get("status") == "running":
                self.logger.info(f"Container {container_name} is running")
                return True
            
            self.logger.debug(f"Container {container_name} status: {status.get('status')}. Waiting...")
            time.sleep(check_interval)
        
        raise ContainerError(f"Timeout waiting for container {container_name} to be ready")
    
    def exec_in_container(
        self,
        command: str,
        container_name: Optional[str] = None,
        user: str = "postgres",
        timeout: int = 300,
    ) -> Tuple[bool, str, str]:
        """
        Execute a command inside a container.
        
        Args:
            command: Command to execute
            container_name: Container to execute in (defaults to replica)
            user: User to run command as
            timeout: Command timeout
            
        Returns:
            Tuple of (success, stdout, stderr)
        """
        if container_name is None:
            container_name = self.config.postgres.replica_container
        
        if self._docker_client:
            try:
                container = self._docker_client.containers.get(container_name)
                result = container.exec_run(command, user=user)
                output = result.output.decode("utf-8") if result.output else ""
                return result.exit_code == 0, output, ""
            except Exception as e:
                return False, "", str(e)
        else:
            return self._run_docker_command(
                "exec",
                "-u", user,
                container_name,
                "bash", "-c", command,
                timeout=timeout,
            )
    
    def get_container_logs(
        self,
        container_name: Optional[str] = None,
        tail: int = 100,
    ) -> str:
        """
        Get container logs.
        
        Args:
            container_name: Container to get logs from
            tail: Number of lines to retrieve
            
        Returns:
            Container logs as string
        """
        if container_name is None:
            container_name = self.config.postgres.replica_container
        
        if self._docker_client:
            try:
                container = self._docker_client.containers.get(container_name)
                logs = container.logs(tail=tail)
                return logs.decode("utf-8") if logs else ""
            except Exception as e:
                return f"Error getting logs: {e}"
        else:
            success, stdout, stderr = self._run_docker_command(
                "logs", "--tail", str(tail), container_name
            )
            return stdout if success else stderr
