# Sift

Sift is the gateway layer for AI-agent work with external tools.
Use it whether an agent operates through MCP servers or through CLI-driven
tooling. Sift captures outputs as artifacts so you can fix context bloat:
the model sees handles and focused retrieval, not massive raw payloads.

Sift has two operating modes:

- **MCP gateway mode** via `sift-gateway`: run Sift between MCP clients and upstream MCP servers.
- **Artifact CLI mode** via `sift`: inspect, query, and compute over captured artifacts directly.

It keeps large tool outputs out of model context while preserving full payloads
for later retrieval.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![PyPI](https://img.shields.io/pypi/v/sift-gateway.svg)](https://pypi.org/project/sift-gateway/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## MCP Context

Sift is a local, single-tenant MCP proxy/gateway:

- Sits between an MCP client and upstream MCP servers
- Preserves upstream tool schemas while mirroring calls
- Stores tool outputs as durable artifacts for retrieval workflows

Typical clients: Claude Desktop, Claude Code, Cursor, VS Code, Windsurf, Zed.  
Typical upstreams: any MCP server reachable via stdio or HTTP transports.

## Quick Start (CLI)

### Install

```bash
pipx install sift-gateway
```

### Import your MCP config into Sift

```bash
sift-gateway init --from claude
```

Supported shortcuts for `--from`:
`claude`, `claude-code`, `cursor`, `vscode`, `windsurf`, `zed`, `auto`.

### Validate runtime

```bash
sift-gateway --check
```

### Explore stored artifacts

```bash
sift list --limit 10
```

## CLI Overview

### `sift-gateway` (operations)

```bash
sift-gateway --help
```

Primary commands:

- `sift-gateway init --from <source>`
- `sift-gateway upstream add '<json>' --from <source>`
- `sift-gateway install <package...>`
- `sift-gateway uninstall <package...>`
- `sift-gateway --check`
- `sift-gateway --transport sse --host 127.0.0.1 --port 8080`

### `sift` (artifact retrieval CLI)

```bash
sift --help
```

Primary commands:

- `sift list --limit 25`
- `sift schema <artifact_id>`
- `sift get <artifact_id>`
- `sift query <artifact_id> --root '$.items' --select id,name`
- `sift code <artifact_id> --expr 'len(data)'`
- `sift run -- <command ...>`
- `sift diff <left_artifact_id> <right_artifact_id>`

## How Sift Works

1. Sift mirrors upstream MCP tools with original schemas.
2. Mirrored responses are stored as envelopes in SQLite + blob storage.
3. Small responses can pass through directly.
4. Large responses return an artifact handle, then you retrieve selectively.

## Artifact Query Model

Sift's main retrieval primitive is the `artifact` tool with `action="query"`.

Supported `query_kind` values:

| query_kind | Purpose |
|---|---|
| `describe` | Schema and metadata |
| `get` | Full payload retrieval |
| `select` | Field projection and filtering |
| `search` | Search and list session artifacts in the current workspace |
| `code` | Execute constrained Python over artifact data |

Example `query_kind="search"`:

```python
artifact(
    action="query",
    query_kind="search",
    query="github issues",
    limit=25,
)
```

Example `query_kind="select"`:

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

| Env var | Default | Description |
|---|---|---|
| `SIFT_GATEWAY_DATA_DIR` | `.sift-gateway` | Data directory |
| `SIFT_GATEWAY_PASSTHROUGH_MAX_BYTES` | `8192` | Raw passthrough threshold |
| `SIFT_GATEWAY_CODE_QUERY_ENABLED` | `true` | Enable code queries |
| `SIFT_GATEWAY_SECRET_REDACTION_ENABLED` | `true` | Redact likely outbound secrets |
| `SIFT_GATEWAY_AUTH_TOKEN` | unset | Required for non-local HTTP binds |

See full configuration in [`docs/config.md`](docs/config.md).

## Security Notes

- Code queries use AST/import/time/memory guardrails, but this is not full OS sandboxing.
- Outbound secret redaction is enabled by default.

Disable code queries if required:

```bash
export SIFT_GATEWAY_CODE_QUERY_ENABLED=false
```

More details: [`SECURITY.md`](SECURITY.md)

## Documentation

- [`docs/quickstart.md`](docs/quickstart.md)
- [`docs/config.md`](docs/config.md)
- [`docs/api_contracts.md`](docs/api_contracts.md)
- [`docs/recipes.md`](docs/recipes.md)
- [`docs/deployment.md`](docs/deployment.md)
- [`docs/errors.md`](docs/errors.md)
- [`docs/observability.md`](docs/observability.md)
- [`docs/architecture.md`](docs/architecture.md)
- [`docs/openclaw/README.md`](docs/openclaw/README.md)

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
