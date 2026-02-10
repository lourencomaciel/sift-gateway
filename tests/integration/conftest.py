"""Integration test configuration.

Provides a default ``SIDEPOUCH_MCP_TEST_POSTGRES_DSN`` that matches the
docker-compose.yml setup, so ``docker compose up -d`` + ``pytest`` works
without any manual env-var configuration.

Override by setting the env var explicitly (e.g. in CI with a different host).
"""

from __future__ import annotations

import os

# Default DSN matches docker-compose.yml (sidepouch user) + the mcp_test
# database created by scripts/init-test-db.sql.
_DEFAULT_TEST_DSN = (
    "postgresql://sidepouch:sidepouch@localhost:5432/sidepouch_test"
)
_ENV_KEY = "SIDEPOUCH_MCP_TEST_POSTGRES_DSN"


def pytest_configure(config):  # noqa: ARG001
    """Set the default test DSN if the user hasn't provided one."""
    if _ENV_KEY not in os.environ:
        os.environ[_ENV_KEY] = _DEFAULT_TEST_DSN
