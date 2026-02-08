"""DB connection helpers."""

from __future__ import annotations

from dataclasses import dataclass

import psycopg
from psycopg_pool import ConnectionPool

from mcp_artifact_gateway.config.settings import GatewayConfig


@dataclass(frozen=True)
class DbConnInfo:
    dsn: str
    statement_timeout_ms: int
    pool_min: int
    pool_max: int


def db_conn_info(config: GatewayConfig) -> DbConnInfo:
    return DbConnInfo(
        dsn=config.postgres_dsn,
        statement_timeout_ms=config.postgres_statement_timeout_ms,
        pool_min=config.postgres_pool_min,
        pool_max=config.postgres_pool_max,
    )


def connect(config: GatewayConfig) -> psycopg.Connection:
    info = db_conn_info(config)
    return psycopg.connect(
        info.dsn,
        options=f"-c statement_timeout={info.statement_timeout_ms}",
    )


def create_pool(config: GatewayConfig) -> ConnectionPool:
    """Create a psycopg3 connection pool from gateway settings."""
    info = db_conn_info(config)
    return ConnectionPool(
        conninfo=info.dsn,
        min_size=info.pool_min,
        max_size=info.pool_max,
        kwargs={"options": f"-c statement_timeout={info.statement_timeout_ms}"},
    )
