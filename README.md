# Sift

Sift is the gateway layer for AI agents working with external tools.
It fixes context bloat by turning large tool outputs into durable artifacts,
then letting agents retrieve only the needed slices.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![PyPI](https://img.shields.io/pypi/v/sift-gateway.svg)](https://pypi.org/project/sift-gateway/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## Why Sift

Without a gateway, agents ingest raw tool payloads directly into prompt context.
That causes avoidable token burn, weaker reasoning over long runs, and brittle
multi-step flows.

Sift changes that default:

- Captures tool outputs as artifacts.
- Returns small outputs inline and large outputs as handles.
- Provides retrieval/query/compute paths over stored artifacts.

## Two Operating Modes

| Mode | CLI | Use it when |
|---|---|---|
| MCP gateway mode | `sift-gateway` | The agent uses MCP clients + upstream MCP servers |
| Artifact CLI mode | `sift` | You want direct artifact operations in terminal workflows |

These modes are complementary: run Sift as an MCP gateway, then inspect/query
artifacts with the CLI.

## MCP Gateway Mode (`sift-gateway`)

### Install

```bash
pipx install sift-gateway
```

### Import MCP client config

```bash
sift-gateway init --from claude
```

`--from` supports shortcuts:
`claude`, `claude-code`, `cursor`, `vscode`, `windsurf`, `zed`, `auto`
or an explicit file path.

### Check gateway health

```bash
sift-gateway --check
```

### Restart your MCP client

After restart, tool calls route through Sift.

### Common gateway commands

```bash
sift-gateway --help
sift-gateway upstream add '<json>' --from claude
sift-gateway install pandas
sift-gateway uninstall pandas
sift-gateway --transport sse --host 127.0.0.1 --port 8080
```

## Artifact CLI Mode (`sift`)

### Inspect recent artifacts

```bash
sift list --limit 10
```

### Inspect an artifact schema

```bash
sift schema art_123
```

### Select fields from a root path

```bash
sift query art_123 '$.items' --select id,name,status --limit 50
```

### Run code against stored data

```bash
sift code art_123 '$.items' --expr 'len(df)'
```

### Capture command output as artifact

```bash
sift run -- git status --porcelain
```

### Common artifact commands

```bash
sift --help
sift get art_123
sift diff art_left art_right
```

## Artifact Query Model (MCP)

For MCP agents, retrieval is done via the `artifact` tool with
`action="query"` and a `query_kind`.

Supported `query_kind` values:

| query_kind | Purpose |
|---|---|
| `describe` | Schema and metadata |
| `get` | Full payload retrieval |
| `select` | Field projection/filtering from a root path |
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

## Context-Bloat Controls

- `SIFT_GATEWAY_PASSTHROUGH_MAX_BYTES` controls inline vs handle behavior.
- Default is `8192` bytes.
- Set `SIFT_GATEWAY_PASSTHROUGH_MAX_BYTES=0` to force handle-first behavior.

## Configuration Highlights

| Env var | Default | Description |
|---|---|---|
| `SIFT_GATEWAY_DATA_DIR` | `.sift-gateway` | Gateway state directory |
| `SIFT_GATEWAY_PASSTHROUGH_MAX_BYTES` | `8192` | Inline response threshold |
| `SIFT_GATEWAY_CODE_QUERY_ENABLED` | `true` | Enable code query execution |
| `SIFT_GATEWAY_SECRET_REDACTION_ENABLED` | `true` | Redact likely outbound secrets |
| `SIFT_GATEWAY_AUTH_TOKEN` | unset | Required for non-local HTTP binds |

Full reference: [`docs/config.md`](docs/config.md)

## Security Notes

- Code queries use AST/import/time/memory guardrails, but not full OS sandboxing.
- Outbound secret redaction is enabled by default.

Disable code queries if needed:

```bash
export SIFT_GATEWAY_CODE_QUERY_ENABLED=false
```

More: [`SECURITY.md`](SECURITY.md)

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
