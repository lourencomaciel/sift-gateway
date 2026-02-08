"""DB connection helpers."""

from __future__ import annotations

from dataclasses import dataclass

import psycopg

from mcp_artifact_gateway.config.settings import GatewayConfig


@dataclass(frozen=True)
class DbConnInfo:
    dsn: str
    statement_timeout_ms: int


def db_conn_info(config: GatewayConfig) -> DbConnInfo:
    return DbConnInfo(
        dsn=config.postgres_dsn,
        statement_timeout_ms=config.postgres_statement_timeout_ms,
    )


def connect(config: GatewayConfig) -> psycopg.Connection:
    info = db_conn_info(config)
    return psycopg.connect(
        info.dsn,
        options=f"-c statement_timeout={info.statement_timeout_ms}",
    )

