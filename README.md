# Sift

**MCP Artifact Gateway** — Keep large MCP responses out of your context window. Query them.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![PyPI](https://img.shields.io/pypi/v/sift-mcp.svg)](https://pypi.org/project/sift-mcp/)

## What is Sift?

Standard MCP tool calls return responses directly in your context window. When responses are large, this quickly exhausts your token budget and makes follow-up queries expensive.

**Sift intercepts MCP responses, stores them durably, and gives you retrieval tools to query just what you need.**

### Key Benefits

- 🔄 **Keep large responses out of context** — Automatic threshold-based storage (default 8 KB)
- 🔍 **Query artifacts with bounded responses** — Deterministic pagination and JSONPath selection
- 💾 **Durable storage** — SQLite (default) or PostgreSQL backend
- 🔗 **Chain tools using artifact references** — Pass `artifact_id` to downstream tools
- 🐍 **Python code queries** — Run pandas/jmespath/numpy against stored data
- ✅ **Zero upstream changes** — Transparent proxy for existing MCP servers

## Quick Start

**1. Install**

```bash
pipx install sift-mcp
```

**2. Import your MCP config**

```bash
sift-mcp init --from claude
```

This rewrites your MCP client config to route through Sift.

You can also use:

- `--from claude-code`
- `--from cursor`
- `--from vscode`
- `--from windsurf`
- `--from zed`
- `--from auto`
- `--from /absolute/path/to/config.json` (explicit path)

**3. Restart your MCP client**

Sift is now proxying your upstream MCP servers. Responses > 8 KB are automatically stored as artifacts.

> **Note:** Your MCP client (Claude Desktop, Cursor, Claude Code) launches Sift automatically via the config. Use `sift-mcp --check` to verify health without starting the server.

## How It Works

1. **Sift connects** to configured upstream MCP servers (stdio or HTTP)
2. **Each upstream tool** is mirrored as `{prefix}.{tool}`
3. **Small responses** (< 8 KB default) pass through transparently
4. **Large responses** return an artifact handle with metadata for retrieval
5. **Query tools** (`artifact` with `query_kind`) provide bounded, paginated access

## Core Concepts

### Artifact Handles

When a response exceeds the passthrough threshold, you get:

- `artifact_id` — Unique identifier for retrieval
- `schemas` — Inferred data structure (schema-first approach)
- `usage_hint` — How to query the artifact
- `pagination` — Whether more data is available upstream

### Query Kinds

Use `artifact(action="query", query_kind=...)` to retrieve data:

- **`describe`** — Get artifact metadata and schema
- **`get`** — Fetch the full original response
- **`select`** — Extract specific JSONPath with pagination
- **`code`** — Run Python code against root-scoped data
- **`search`** — List session artifacts available to the current session

### Tool Chaining

Pass artifact IDs directly to other tools — Sift resolves them server-side:

```python
# tool_a returns artifact_id
# tool_b receives the actual data, not the ID
tool_b(input="art_7f3a...")

# JSONPath references
tool_b(email="art_7f3a...:$.users[0].email")

# Wildcards
tool_b(ids="art_7f3a...:$.items[*].id")
```

Only top-level string arguments are resolved. Nested values inside dicts or lists are never expanded.

## Documentation

- **[Quick Start Guide](docs/quickstart.md)** — Detailed setup walkthrough
- **[Configuration Reference](docs/config.md)** — All settings and environment variables
- **[Recipes & Examples](docs/recipes.md)** — Common usage patterns
- **[Architecture & Spec](docs/spec_v1_9.md)** — Technical specification (v1.9)
- **[Deployment Guide](docs/deployment.md)** — PostgreSQL, URL mode, production setup
- **[API Contracts](docs/api_contracts.md)** — Pagination, handles, response formats
- **[Error Reference](docs/errors.md)** — Error codes and troubleshooting
- **[Observability](docs/observability.md)** — Logging and metrics

## Development

```bash
# Setup
uv sync

# Tests
uv run pytest tests/unit/ -q          # ~1026 unit tests
uv run pytest tests/integration/ -q   # requires PostgreSQL

# Lint & type check
uv run ruff check src tests
uv run mypy src
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for full development guide.

## Requirements

- Python >= 3.11
- [pipx](https://pipx.pypa.io/) or [uv](https://docs.astral.sh/uv/)
- Docker (optional, for PostgreSQL backend)

## License

MIT — See [LICENSE](LICENSE)
