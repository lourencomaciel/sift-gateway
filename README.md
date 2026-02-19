# Sift

**Artifact gateway for MCP**. Keep large tool responses out of model context, store them durably, and retrieve only what you need.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![PyPI](https://img.shields.io/pypi/v/sift-gateway.svg)](https://pypi.org/project/sift-gateway/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Sift sits between your MCP client and your upstream MCP servers.

- Mirrors upstream tools with original schemas.
- Persists mirrored responses as artifact envelopes in SQLite + blob storage.
- Returns raw payloads for small responses and artifact handles for larger ones.
- Provides retrieval/compute workflows over artifacts without loading full payloads into chat context.

## Quick Start

### 1. Install

```bash
pipx install sift-gateway
```

### 2. Import your MCP config

```bash
sift-gateway init --from claude
```

`--from` accepts shortcuts (`claude`, `claude-code`, `cursor`, `vscode`, `windsurf`, `zed`, `auto`) or an explicit config path.

### 3. Verify setup

```bash
sift-gateway --check
```

### 4. Restart your MCP client

After restart, your client routes tool calls through Sift.

## CLI Surface

### Gateway CLI (`sift-gateway`)

```bash
sift-gateway --help
```

Main commands:
- `init` to import/rewrite MCP config with gateway sync metadata
- `upstream` to add/manage upstream servers
- `install` / `uninstall` for code-query packages
- `--check` to validate FS/DB/upstream readiness

### Artifact CLI (`sift`)

```bash
sift --help
```

Main commands:
- `list`, `schema`, `get`
- `query`, `code`
- `run`, `diff`

## Artifact Workflow

When mirrored tools return large payloads, Sift returns a lightweight handle with metadata. You then retrieve/select/compute as needed.

### Query kinds

| query_kind | Purpose |
|---|---|
| `describe` | Inspect schema and metadata for an artifact |
| `get` | Retrieve full stored payload |
| `select` | Project/filter rows from a root path |
| `search` | List session artifacts available in the current workspace |
| `code` | Execute constrained Python over artifact data |

Example `query_kind="search"` request:

```python
artifact(
    action="query",
    query_kind="search",
    query="github issues",
    limit=25,
)
```

Example `query_kind="select"` request:

```python
artifact(
    action="query",
    query_kind="select",
    artifact_id="art_123",
    root_path="$.items",
    select_paths=["id", "name", "status"],
    limit=50,
)
```

## Configuration Highlights

| Setting | Default | Description |
|---|---|---|
| `SIFT_GATEWAY_DATA_DIR` | `.sift-gateway` | Local state root |
| `SIFT_GATEWAY_PASSTHROUGH_MAX_BYTES` | `8192` | Max raw upstream response size before handle mode |
| `SIFT_GATEWAY_CODE_QUERY_ENABLED` | `true` | Enable `query_kind="code"` |
| `SIFT_GATEWAY_SECRET_REDACTION_ENABLED` | `true` | Redact likely secrets in outbound responses |
| `SIFT_GATEWAY_AUTH_TOKEN` | unset | Token for non-local HTTP binds |

See the full matrix in [`docs/config.md`](docs/config.md).

## Security Notes

- Code queries run with AST/import/time/memory guardrails, but this is not full OS isolation.
- Outbound secret redaction is enabled by default.
- For strict environments, disable code queries:

```bash
export SIFT_GATEWAY_CODE_QUERY_ENABLED=false
```

See [`SECURITY.md`](SECURITY.md) for reporting and policies.

## Documentation

- [`docs/quickstart.md`](docs/quickstart.md) - full setup walkthrough
- [`docs/config.md`](docs/config.md) - complete configuration reference
- [`docs/api_contracts.md`](docs/api_contracts.md) - contract and response shapes
- [`docs/recipes.md`](docs/recipes.md) - practical retrieval patterns
- [`docs/deployment.md`](docs/deployment.md) - transport and deployment guidance
- [`docs/observability.md`](docs/observability.md) - logs and metrics
- [`docs/errors.md`](docs/errors.md) - error taxonomy
- [`docs/architecture.md`](docs/architecture.md) - architecture and invariants
- [`docs/openclaw/README.md`](docs/openclaw/README.md) - OpenClaw integration pack

## Development

```bash
git clone https://github.com/lourencomaciel/sift-gateway.git
cd sift-gateway
uv sync --extra dev

UV_CACHE_DIR=/tmp/uv-cache uv run python -m ruff check src tests
UV_CACHE_DIR=/tmp/uv-cache uv run python -m mypy src
UV_CACHE_DIR=/tmp/uv-cache uv run python -m pytest tests/unit/ -q
```

## License

MIT - see [`LICENSE`](LICENSE).
