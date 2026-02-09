"""Tests for Docker Postgres auto-provisioning."""

from __future__ import annotations

import json
import subprocess
from unittest.mock import patch

import pytest

from mcp_artifact_gateway.config.docker_postgres import (
    CONTAINER_NAME,
    DEFAULT_DB,
    DEFAULT_PORT,
    DEFAULT_USER,
    IMAGE,
    VOLUME_NAME,
    DockerCommandError,
    DockerHealthCheckError,
    DockerNotFoundError,
    DockerPostgresResult,
    PortConflictError,
    _build_dsn,
    _container_exists,
    _container_is_running,
    _create_and_start_container,
    _find_available_port,
    _generate_password,
    _get_container_host_port,
    _get_container_password,
    _inspect_container,
    _run_docker,
    _start_existing_container,
    _wait_for_healthy,
    check_docker_available,
    provision_postgres,
)


# ---------------------------------------------------------------------------
# Helpers for building mock docker inspect output
# ---------------------------------------------------------------------------

def _make_inspect_output(
    *,
    running: bool = True,
    password: str = "secret123",
    host_port: int = 5432,
    healthy: bool = True,
) -> list[dict]:
    return [{
        "State": {
            "Running": running,
            "Health": {"Status": "healthy" if healthy else "starting"},
        },
        "Config": {
            "Env": [
                f"POSTGRES_USER={DEFAULT_USER}",
                f"POSTGRES_PASSWORD={password}",
                f"POSTGRES_DB={DEFAULT_DB}",
            ],
        },
        "HostConfig": {
            "PortBindings": {
                "5432/tcp": [{"HostPort": str(host_port)}],
            },
        },
    }]


def _mock_run_docker(stdout: str = "", returncode: int = 0):
    """Return a CompletedProcess for mocking _run_docker."""
    return subprocess.CompletedProcess(
        args=[], returncode=returncode, stdout=stdout, stderr="",
    )


# ---------------------------------------------------------------------------
# _run_docker
# ---------------------------------------------------------------------------

class TestRunDocker:
    def test_raises_docker_not_found_on_file_not_found(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres.subprocess.run",
            lambda *a, **kw: (_ for _ in ()).throw(FileNotFoundError()),
        )
        with pytest.raises(DockerNotFoundError):
            _run_docker(["docker", "info"])

    def test_raises_docker_command_error_on_failure(self, monkeypatch) -> None:
        def _fail(*args, **kwargs):
            raise subprocess.CalledProcessError(1, "docker", stderr="fail")
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres.subprocess.run", _fail,
        )
        with pytest.raises(DockerCommandError):
            _run_docker(["docker", "fail"])


# ---------------------------------------------------------------------------
# check_docker_available
# ---------------------------------------------------------------------------

class TestCheckDockerAvailable:
    def test_returns_true_when_docker_info_succeeds(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._run_docker",
            lambda *a, **kw: _mock_run_docker(),
        )
        assert check_docker_available() is True

    def test_returns_false_when_docker_not_found(self, monkeypatch) -> None:
        def _fail(*a, **kw):
            raise DockerNotFoundError("not found")
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._run_docker", _fail,
        )
        assert check_docker_available() is False

    def test_returns_false_when_daemon_not_running(self, monkeypatch) -> None:
        def _fail(*a, **kw):
            raise DockerCommandError("daemon not running")
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._run_docker", _fail,
        )
        assert check_docker_available() is False


# ---------------------------------------------------------------------------
# _generate_password
# ---------------------------------------------------------------------------

class TestGeneratePassword:
    def test_returns_nonempty_string(self) -> None:
        pw = _generate_password()
        assert isinstance(pw, str)
        assert len(pw) > 16

    def test_generates_unique_passwords(self) -> None:
        assert _generate_password() != _generate_password()


# ---------------------------------------------------------------------------
# _find_available_port
# ---------------------------------------------------------------------------

class TestFindAvailablePort:
    def test_returns_preferred_when_available(self) -> None:
        # Use a high port unlikely to be in use
        port = _find_available_port(59000)
        assert port == 59000

    def test_skips_to_next_when_preferred_in_use(self, monkeypatch) -> None:
        original_bind = __import__("socket").socket.bind
        call_count = [0]

        def _bind(self, addr):
            call_count[0] += 1
            if call_count[0] == 1:
                raise OSError("in use")
            return original_bind(self, addr)

        monkeypatch.setattr("socket.socket.bind", _bind)
        port = _find_available_port(59100)
        assert port == 59101

    def test_raises_when_all_ports_taken(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "socket.socket.bind",
            lambda self, addr: (_ for _ in ()).throw(OSError("in use")),
        )
        with pytest.raises(PortConflictError):
            _find_available_port(59200)


# ---------------------------------------------------------------------------
# Container inspection
# ---------------------------------------------------------------------------

class TestContainerInspection:
    def test_inspect_returns_parsed_json(self, monkeypatch) -> None:
        output = _make_inspect_output()
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._run_docker",
            lambda *a, **kw: _mock_run_docker(stdout=json.dumps(output)),
        )
        result = _inspect_container("test")
        assert result is not None
        assert result[0]["State"]["Running"] is True

    def test_inspect_returns_none_for_missing(self, monkeypatch) -> None:
        def _fail(*a, **kw):
            raise DockerCommandError("no such container")
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._run_docker", _fail,
        )
        assert _inspect_container("missing") is None

    def test_container_is_running_true(self, monkeypatch) -> None:
        output = _make_inspect_output(running=True)
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._run_docker",
            lambda *a, **kw: _mock_run_docker(stdout=json.dumps(output)),
        )
        assert _container_is_running("test") is True

    def test_container_is_running_false_when_stopped(self, monkeypatch) -> None:
        output = _make_inspect_output(running=False)
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._run_docker",
            lambda *a, **kw: _mock_run_docker(stdout=json.dumps(output)),
        )
        assert _container_is_running("test") is False

    def test_container_exists_true(self, monkeypatch) -> None:
        output = _make_inspect_output()
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._run_docker",
            lambda *a, **kw: _mock_run_docker(stdout=json.dumps(output)),
        )
        assert _container_exists("test") is True

    def test_container_exists_false(self, monkeypatch) -> None:
        def _fail(*a, **kw):
            raise DockerCommandError("no such container")
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._run_docker", _fail,
        )
        assert _container_exists("missing") is False


# ---------------------------------------------------------------------------
# Credential/port extraction
# ---------------------------------------------------------------------------

class TestContainerCredentials:
    def test_get_password(self, monkeypatch) -> None:
        output = _make_inspect_output(password="mypass")
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._run_docker",
            lambda *a, **kw: _mock_run_docker(stdout=json.dumps(output)),
        )
        assert _get_container_password("test") == "mypass"

    def test_get_password_missing_raises(self, monkeypatch) -> None:
        output = [{"Config": {"Env": ["OTHER_VAR=x"]},
                    "State": {"Running": True},
                    "HostConfig": {"PortBindings": {}}}]
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._run_docker",
            lambda *a, **kw: _mock_run_docker(stdout=json.dumps(output)),
        )
        with pytest.raises(DockerCommandError, match="POSTGRES_PASSWORD not found"):
            _get_container_password("test")

    def test_get_password_container_not_found(self, monkeypatch) -> None:
        def _fail(*a, **kw):
            raise DockerCommandError("no such container")
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._run_docker", _fail,
        )
        with pytest.raises(DockerCommandError, match="not found"):
            _get_container_password("missing")

    def test_get_host_port(self, monkeypatch) -> None:
        output = _make_inspect_output(host_port=5433)
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._run_docker",
            lambda *a, **kw: _mock_run_docker(stdout=json.dumps(output)),
        )
        assert _get_container_host_port("test") == 5433

    def test_get_host_port_no_bindings(self, monkeypatch) -> None:
        output = [{"HostConfig": {"PortBindings": {}},
                    "State": {"Running": True},
                    "Config": {"Env": []}}]
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._run_docker",
            lambda *a, **kw: _mock_run_docker(stdout=json.dumps(output)),
        )
        with pytest.raises(DockerCommandError, match="no port binding"):
            _get_container_host_port("test")


# ---------------------------------------------------------------------------
# Container lifecycle
# ---------------------------------------------------------------------------

class TestContainerLifecycle:
    def test_start_existing_calls_docker_start(self, monkeypatch) -> None:
        calls: list[list[str]] = []
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._run_docker",
            lambda args, **kw: (calls.append(args), _mock_run_docker())[1],
        )
        _start_existing_container("mycontainer")
        assert calls == [["docker", "start", "mycontainer"]]

    def test_create_container_passes_correct_args(self, monkeypatch) -> None:
        calls: list[list[str]] = []
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._run_docker",
            lambda args, **kw: (calls.append(args), _mock_run_docker())[1],
        )
        _create_and_start_container(
            container_name="test-pg",
            volume_name="test-vol",
            image="postgres:16-alpine",
            port=5433,
            user="testuser",
            password="testpass",
            db="testdb",
        )
        assert len(calls) == 1
        cmd = calls[0]
        assert "docker" in cmd
        assert "--name" in cmd
        assert "test-pg" in cmd
        assert "-p" in cmd
        assert "5433:5432" in cmd
        assert "POSTGRES_PASSWORD=testpass" in cmd


# ---------------------------------------------------------------------------
# _wait_for_healthy
# ---------------------------------------------------------------------------

class TestWaitForHealthy:
    def test_returns_when_healthy_immediately(self, monkeypatch) -> None:
        output = _make_inspect_output(healthy=True)
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._run_docker",
            lambda *a, **kw: _mock_run_docker(stdout=json.dumps(output)),
        )
        _wait_for_healthy("test", timeout=5.0)  # should not raise

    def test_polls_then_healthy(self, monkeypatch) -> None:
        poll_count = [0]
        unhealthy = _make_inspect_output(healthy=False)
        healthy = _make_inspect_output(healthy=True)

        def _mock(*a, **kw):
            poll_count[0] += 1
            data = healthy if poll_count[0] >= 3 else unhealthy
            return _mock_run_docker(stdout=json.dumps(data))

        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._run_docker", _mock,
        )
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres.time.sleep",
            lambda _: None,
        )
        _wait_for_healthy("test", timeout=10.0, interval=0.01)
        assert poll_count[0] >= 3

    def test_raises_on_timeout(self, monkeypatch) -> None:
        output = _make_inspect_output(healthy=False)
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._run_docker",
            lambda *a, **kw: _mock_run_docker(stdout=json.dumps(output)),
        )
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres.time.sleep",
            lambda _: None,
        )
        with pytest.raises(DockerHealthCheckError, match="did not become healthy"):
            _wait_for_healthy("test", timeout=0.0)


# ---------------------------------------------------------------------------
# _build_dsn
# ---------------------------------------------------------------------------

class TestBuildDsn:
    def test_builds_correct_dsn(self) -> None:
        dsn = _build_dsn(
            user="u", password="p", host="localhost", port=5432, db="mydb",
        )
        assert dsn == "postgresql://u:p@localhost:5432/mydb"


# ---------------------------------------------------------------------------
# provision_postgres (full orchestrator)
# ---------------------------------------------------------------------------

class TestProvisionPostgres:
    def test_raises_when_docker_not_available(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres.check_docker_available",
            lambda: False,
        )
        with pytest.raises(DockerNotFoundError):
            provision_postgres()

    def test_dry_run_returns_placeholder(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres.check_docker_available",
            lambda: True,
        )
        result = provision_postgres(dry_run=True)
        assert result.password == "<generated>"
        assert result.already_running is False
        assert "postgresql://" in result.dsn

    def test_reuses_running_container(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres.check_docker_available",
            lambda: True,
        )
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._container_is_running",
            lambda name: True,
        )
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._get_container_password",
            lambda name: "existingpass",
        )
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._get_container_host_port",
            lambda name: 5432,
        )
        result = provision_postgres()
        assert result.already_running is True
        assert result.password == "existingpass"
        assert result.port == 5432

    def test_starts_stopped_container(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres.check_docker_available",
            lambda: True,
        )
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._container_is_running",
            lambda name: False,
        )
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._container_exists",
            lambda name: True,
        )
        started = [False]
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._start_existing_container",
            lambda name: (started.__setitem__(0, True)),
        )
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._wait_for_healthy",
            lambda name, **kw: None,
        )
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._get_container_password",
            lambda name: "stoppedpass",
        )
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._get_container_host_port",
            lambda name: 5433,
        )
        result = provision_postgres()
        assert started[0] is True
        assert result.already_running is False
        assert result.password == "stoppedpass"
        assert result.port == 5433

    def test_creates_new_container(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres.check_docker_available",
            lambda: True,
        )
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._container_is_running",
            lambda name: False,
        )
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._container_exists",
            lambda name: False,
        )
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._find_available_port",
            lambda preferred: preferred,
        )
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._generate_password",
            lambda: "newpass123",
        )
        created = [False]
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._create_and_start_container",
            lambda **kw: created.__setitem__(0, True),
        )
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._wait_for_healthy",
            lambda name, **kw: None,
        )
        result = provision_postgres()
        assert created[0] is True
        assert result.already_running is False
        assert result.password == "newpass123"
        assert "newpass123" in result.dsn

    def test_finds_alternative_port(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres.check_docker_available",
            lambda: True,
        )
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._container_is_running",
            lambda name: False,
        )
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._container_exists",
            lambda name: False,
        )
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._find_available_port",
            lambda preferred: 5435,
        )
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._generate_password",
            lambda: "pw",
        )
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._create_and_start_container",
            lambda **kw: None,
        )
        monkeypatch.setattr(
            "mcp_artifact_gateway.config.docker_postgres._wait_for_healthy",
            lambda name, **kw: None,
        )
        result = provision_postgres()
        assert result.port == 5435
        assert ":5435/" in result.dsn
