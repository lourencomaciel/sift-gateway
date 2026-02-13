"""DB connection helpers."""

from __future__ import annotations

from dataclasses import dataclass

import psycopg
from psycopg_pool import ConnectionPool

from sift_mcp.config.settings import GatewayConfig


@dataclass(frozen=True)
class DbConnInfo:
    """Postgres connection parameters extracted from config.

    Attributes:
        dsn: PostgreSQL connection string.
        statement_timeout_ms: Per-statement timeout in milliseconds.
        pool_min: Minimum pool size.
        pool_max: Maximum pool size.
    """

    dsn: str
    statement_timeout_ms: int
    pool_min: int
    pool_max: int


def db_conn_info(config: GatewayConfig) -> DbConnInfo:
    """Extract Postgres connection parameters from config.

    Args:
        config: Gateway configuration instance.

    Returns:
        A DbConnInfo with DSN, timeout, and pool size settings.
    """
    return DbConnInfo(
        dsn=config.postgres_dsn,
        statement_timeout_ms=config.postgres_statement_timeout_ms,
        pool_min=config.postgres_pool_min,
        pool_max=config.postgres_pool_max,
    )


def connect(config: GatewayConfig) -> psycopg.Connection:
    """Open a single psycopg connection with statement timeout.

    Args:
        config: Gateway configuration instance.

    Returns:
        An open psycopg connection.
    """
    info = db_conn_info(config)
    return psycopg.connect(
        info.dsn,
        options=f"-c statement_timeout={info.statement_timeout_ms}",
    )


def create_pool(config: GatewayConfig) -> ConnectionPool:
    """Create a psycopg3 connection pool from gateway settings.

    Args:
        config: Gateway configuration instance.

    Returns:
        A ConnectionPool sized per config pool_min/pool_max.
    """
    info = db_conn_info(config)
    return ConnectionPool(
        conninfo=info.dsn,
        min_size=info.pool_min,
        max_size=info.pool_max,
        open=True,
        kwargs={"options": f"-c statement_timeout={info.statement_timeout_ms}"},
    )
