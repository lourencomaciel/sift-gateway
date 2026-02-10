"""Auto-provision a Postgres container via Docker for the gateway.

When ``mcp-gateway init`` detects no explicit Postgres DSN, it calls
:func:`provision_postgres` which starts (or reuses) a named Docker
container running ``postgres:16-alpine``.

The generated DSN is written to the gateway config so subsequent
``mcp-gateway`` invocations connect automatically.
"""

from __future__ import annotations

import json
import secrets
import socket
import subprocess
import time
from dataclasses import dataclass
from typing import Any


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CONTAINER_NAME = "mcp-gateway-postgres"
VOLUME_NAME = "mcp-gateway-pgdata"
IMAGE = "postgres:16-alpine"
DEFAULT_DB = "mcp_gateway"
DEFAULT_USER = "mcp_gateway"
DEFAULT_PORT = 5432
PORT_SCAN_RANGE = 11  # try preferred .. preferred+10
HEALTH_CHECK_INTERVAL = 0.5  # seconds between polls
HEALTH_CHECK_TIMEOUT = 30.0  # seconds before giving up


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------
class DockerNotFoundError(RuntimeError):
    """Docker CLI is not installed or the daemon is not running."""


class DockerCommandError(RuntimeError):
    """A ``docker`` command exited with a non-zero status."""


class DockerHealthCheckError(RuntimeError):
    """The Postgres container did not become healthy within the timeout."""


class PortConflictError(RuntimeError):
    """No available port found in the scanned range."""


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class DockerPostgresResult:
    """Outcome of :func:`provision_postgres`."""

    dsn: str
    container_name: str
    port: int
    password: str
    already_running: bool


# ---------------------------------------------------------------------------
# Subprocess gateway — single point of Docker CLI interaction
# ---------------------------------------------------------------------------
def _run_docker(
    args: list[str],
    *,
    check: bool = True,
    capture: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run a Docker CLI command.

    All subprocess calls in this module go through this function so that
    tests can mock it in one place.
    """
    try:
        return subprocess.run(
            args,
            check=check,
            capture_output=capture,
            text=True,
        )
    except FileNotFoundError:
        raise DockerNotFoundError(
            "Docker CLI not found. Install Docker Desktop or set --postgres-dsn."
        ) from None
    except subprocess.CalledProcessError as exc:
        if not check:
            raise  # pragma: no cover
        raise DockerCommandError(
            f"docker command failed: {' '.join(args)}\n{exc.stderr or ''}"
        ) from exc


# ---------------------------------------------------------------------------
# Docker availability
# ---------------------------------------------------------------------------
def check_docker_available() -> bool:
    """Return ``True`` if ``docker info`` succeeds."""
    try:
        _run_docker(["docker", "info"], check=True)
        return True
    except (DockerNotFoundError, DockerCommandError):
        return False


# ---------------------------------------------------------------------------
# Container inspection
# ---------------------------------------------------------------------------
def _inspect_container(container_name: str) -> list[dict[str, Any]] | None:
    """Return parsed ``docker inspect`` output, or ``None`` if not found."""
    try:
        result = _run_docker(
            ["docker", "inspect", container_name],
            check=True,
        )
        return json.loads(result.stdout)
    except DockerCommandError:
        return None


def _container_is_running(container_name: str) -> bool:
    """Check if the named container exists and is running."""
    info = _inspect_container(container_name)
    if info is None or not info:
        return False
    return bool(info[0].get("State", {}).get("Running", False))


def _container_exists(container_name: str) -> bool:
    """Check if the named container exists (running or stopped)."""
    return _inspect_container(container_name) is not None


# ---------------------------------------------------------------------------
# Credential/port extraction from running containers
# ---------------------------------------------------------------------------
def _get_container_env(info: list[dict[str, Any]], key: str) -> str | None:
    """Extract an environment variable value from inspect output."""
    env_list = info[0].get("Config", {}).get("Env", [])
    prefix = f"{key}="
    for entry in env_list:
        if entry.startswith(prefix):
            return entry[len(prefix) :]
    return None


def _get_container_password(container_name: str) -> str:
    """Extract ``POSTGRES_PASSWORD`` from a container's environment."""
    info = _inspect_container(container_name)
    if info is None or not info:
        msg = f"container '{container_name}' not found"
        raise DockerCommandError(msg)
    password = _get_container_env(info, "POSTGRES_PASSWORD")
    if password is None:
        msg = f"POSTGRES_PASSWORD not found in container '{container_name}'"
        raise DockerCommandError(msg)
    return password


def _get_container_host_port(container_name: str) -> int:
    """Extract the host port mapped to ``5432/tcp``."""
    info = _inspect_container(container_name)
    if info is None or not info:
        msg = f"container '{container_name}' not found"
        raise DockerCommandError(msg)
    bindings = info[0].get("HostConfig", {}).get("PortBindings", {}).get("5432/tcp", [])
    if not bindings:
        msg = f"no port binding for 5432/tcp on container '{container_name}'"
        raise DockerCommandError(msg)
    return int(bindings[0]["HostPort"])


# ---------------------------------------------------------------------------
# Port scanning
# ---------------------------------------------------------------------------
def _find_available_port(preferred: int = DEFAULT_PORT) -> int:
    """Find an available TCP port starting from *preferred*."""
    for port in range(preferred, preferred + PORT_SCAN_RANGE):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    msg = f"no available port in range {preferred}-{preferred + PORT_SCAN_RANGE - 1}"
    raise PortConflictError(msg)


# ---------------------------------------------------------------------------
# Password generation
# ---------------------------------------------------------------------------
def _generate_password() -> str:
    """Generate a cryptographically random URL-safe password."""
    return secrets.token_urlsafe(24)


# ---------------------------------------------------------------------------
# Container lifecycle
# ---------------------------------------------------------------------------
def _start_existing_container(container_name: str) -> None:
    """Start a stopped container."""
    _run_docker(["docker", "start", container_name])


def _create_and_start_container(
    *,
    container_name: str,
    volume_name: str,
    image: str,
    port: int,
    user: str,
    password: str,
    db: str,
) -> None:
    """Create and start a new Postgres container."""
    _run_docker(
        [
            "docker",
            "run",
            "-d",
            "--name",
            container_name,
            "-e",
            f"POSTGRES_USER={user}",
            "-e",
            f"POSTGRES_PASSWORD={password}",
            "-e",
            f"POSTGRES_DB={db}",
            "-p",
            f"{port}:5432",
            "-v",
            f"{volume_name}:/var/lib/postgresql/data",
            "--health-cmd",
            f"pg_isready -U {user}",
            "--health-interval",
            "5s",
            "--health-timeout",
            "5s",
            "--health-retries",
            "5",
            image,
        ]
    )


def _wait_for_healthy(
    container_name: str,
    *,
    timeout: float = HEALTH_CHECK_TIMEOUT,
    interval: float = HEALTH_CHECK_INTERVAL,
) -> None:
    """Poll container health status until healthy or timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        info = _inspect_container(container_name)
        if info and info[0].get("State", {}).get("Health", {}).get("Status") == "healthy":
            return
        time.sleep(interval)
    msg = f"container '{container_name}' did not become healthy within {timeout}s"
    raise DockerHealthCheckError(msg)


# ---------------------------------------------------------------------------
# DSN builder
# ---------------------------------------------------------------------------
def _build_dsn(
    *,
    user: str,
    password: str,
    host: str,
    port: int,
    db: str,
) -> str:
    """Build a ``postgresql://`` connection string."""
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------
def provision_postgres(
    *,
    container_name: str = CONTAINER_NAME,
    volume_name: str = VOLUME_NAME,
    image: str = IMAGE,
    preferred_port: int = DEFAULT_PORT,
    dry_run: bool = False,
) -> DockerPostgresResult:
    """Provision or reuse a Postgres Docker container.

    Returns a :class:`DockerPostgresResult` with the connection DSN.
    Raises :class:`DockerNotFoundError` if Docker is not available.
    """
    if not check_docker_available():
        raise DockerNotFoundError(
            "Docker not found. Install Docker Desktop or provide --postgres-dsn."
        )

    if dry_run:
        return DockerPostgresResult(
            dsn=_build_dsn(
                user=DEFAULT_USER,
                password="<generated>",
                host="localhost",
                port=preferred_port,
                db=DEFAULT_DB,
            ),
            container_name=container_name,
            port=preferred_port,
            password="<generated>",
            already_running=False,
        )

    # Reuse running container
    if _container_is_running(container_name):
        password = _get_container_password(container_name)
        port = _get_container_host_port(container_name)
        return DockerPostgresResult(
            dsn=_build_dsn(
                user=DEFAULT_USER,
                password=password,
                host="localhost",
                port=port,
                db=DEFAULT_DB,
            ),
            container_name=container_name,
            port=port,
            password=password,
            already_running=True,
        )

    # Restart stopped container
    if _container_exists(container_name):
        _start_existing_container(container_name)
        _wait_for_healthy(container_name)
        password = _get_container_password(container_name)
        port = _get_container_host_port(container_name)
        return DockerPostgresResult(
            dsn=_build_dsn(
                user=DEFAULT_USER,
                password=password,
                host="localhost",
                port=port,
                db=DEFAULT_DB,
            ),
            container_name=container_name,
            port=port,
            password=password,
            already_running=False,
        )

    # Create new container
    port = _find_available_port(preferred_port)
    password = _generate_password()
    _create_and_start_container(
        container_name=container_name,
        volume_name=volume_name,
        image=image,
        port=port,
        user=DEFAULT_USER,
        password=password,
        db=DEFAULT_DB,
    )
    _wait_for_healthy(container_name)
    return DockerPostgresResult(
        dsn=_build_dsn(
            user=DEFAULT_USER,
            password=password,
            host="localhost",
            port=port,
            db=DEFAULT_DB,
        ),
        container_name=container_name,
        port=port,
        password=password,
        already_running=False,
    )
