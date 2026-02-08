# MCP Artifact Gateway

Local single-tenant MCP gateway (Python) intended to proxy upstream MCP tools, persist every result as a durable artifact envelope, and support bounded deterministic retrieval.

## Status

This repository is an early scaffold.

- Implemented:
  - package skeleton and module layout
  - gateway configuration models (`pydantic-settings`)
  - constants and CLI entrypoint shape
  - development tooling (`ruff`, `mypy`, `pytest`) and Docker Postgres service
- Not yet implemented:
  - serving MCP tools
  - upstream mirroring/proxy behavior
  - artifact persistence, mapping, retrieval, and cursor flow
  - real `--check` health validation (currently a placeholder)

Current CLI behavior:

- `--check` prints `mcp-gateway --check: not yet implemented`
- default run prints `mcp-gateway serve: not yet implemented`

## Requirements

- Python `>=3.11` (repo currently uses Python `3.13`)
- [`uv`](https://docs.astral.sh/uv/)
- Docker (for local Postgres via `docker compose`)

## Quick Start

1. Sync dependencies:

```bash
UV_CACHE_DIR=.uv-cache uv sync --all-extras
```

2. Start Postgres:

```bash
docker compose up -d postgres
```

3. Create local env file:

```bash
cp .env.example .env
```

4. Run the current CLI stub from source:

```bash
PYTHONPATH=src UV_CACHE_DIR=.uv-cache uv run python -c 'from mcp_artifact_gateway.main import cli; cli()' --check
```

## Configuration

Environment variables use the `MCP_GATEWAY_` prefix.

Common values:

- `MCP_GATEWAY_POSTGRES_DSN` (default in code: `postgresql://localhost:5432/mcp_gateway`)
- `MCP_GATEWAY_DATA_DIR` (default: `.mcp_gateway`)
- `MCP_GATEWAY_ENVELOPE_JSONB_MODE` (`full | minimal_for_large | none`)
- `MCP_GATEWAY_ENVELOPE_CANONICAL_ENCODING` (`zstd | gzip | none`)
- `MCP_GATEWAY_MAX_JSON_PART_PARSE_BYTES` (default: `50000000`)
- `MCP_GATEWAY_CURSOR_TTL_MINUTES` (default: `60`)
- `MCP_GATEWAY_WHERE_CANONICALIZATION_MODE` (`raw_string | canonical_ast`)

See `.env.example` and `src/mcp_artifact_gateway/config/settings.py` for the full set.

## Development

Run lint and type-check:

```bash
UV_CACHE_DIR=.uv-cache uv run ruff check src
UV_CACHE_DIR=.uv-cache uv run mypy src
```

Run tests:

```bash
UV_CACHE_DIR=.uv-cache uv run pytest
```

Note: this scaffold currently contains no concrete test cases, so `pytest` reports `no tests ran`.

## Project Layout

```text
src/mcp_artifact_gateway/
  main.py                  # CLI entrypoint (stub)
  constants.py             # version/identity/constants
  config/settings.py       # typed gateway settings
  ...                      # planned modules (artifacts, db, retrieval, etc.)
tests/
  unit/
  integration/
docker-compose.yml         # local Postgres
```

## Design References

- `MCP Artifact Gateway (Python) Full Implementation  3015bf0c026f80cd9f19f3b71f098cb4.md`
- `findings.md`
- `progress.md`
