# Sift

**MCP Artifact Gateway** — Keep large MCP responses out of your context window. Query them.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![PyPI](https://img.shields.io/pypi/v/sift-mcp.svg)](https://pypi.org/project/sift-mcp/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

AI agents connect to real-world tools through MCP. When those tools return large responses, the data floods the context window, degrading model performance, inflating cost, and blocking multi-step workflows. Sift sits invisibly between your MCP client and upstream servers: mirrored tool responses are always stored durably, and small responses can still pass through directly to the model. You get retrieval tools to query, page, chain, and compute over the stored data without loading it into context.

Nothing about your existing MCP servers needs to change.

```
┌──────────────┐         ┌──────────────┐         ┌──────────────────────┐
│  MCP Client  │────────▶│     Sift     │────────▶│  Upstream MCP Servers│
│  (Claude,    │◀────────│              │◀────────│  (GitHub, Slack,     │
│   Cursor,    │ handle  │   ┌──────┐   │  tool   │   Meta Ads, ...)     │
│   VS Code)   │ or raw  │   │Store │   │  calls  │                      │
└──────────────┘         │   └──────┘   │         └──────────────────────┘
                         └──────────────┘
                          <= 8 KB default ─▶ return raw response
                          > 8 KB or pagination continuation ─▶ return handle
                          all mirrored responses are still persisted
```

## Why This Matters

A typical MCP tool call returning GitHub issues, database query results, or Slack threads can easily be 30-100 KB. Without Sift, that's 8,000-25,000 tokens consumed every time the model references the data. With Sift, the handle is roughly 200-400 tokens, and selective retrieval pulls only what's needed.

Research confirms this problem doesn't go away with larger context windows. Models show measurable performance degradation well before hitting their advertised limits, effective capacity is 60-70% of the stated maximum, and cost scales linearly with tokens regardless.

Sift is the first tool in the MCP ecosystem to treat tool responses as first-class, persistent, queryable, computable artifacts. It's not a proxy (it stores and indexes), not a cache (it infers schemas and supports compute), and not a database connector (it intercepts outputs from any tool automatically). It's a new category: the **artifact gateway**.

## Quick Start

**1. Install**

```bash
pipx install sift-mcp
```

**2. Import your MCP config**

```bash
sift-mcp init --from claude
```

This reads your existing MCP client config, moves your servers behind Sift, and writes everything back. One command. Shortcuts: `claude`, `claude-code`, `cursor`, `vscode`, `windsurf`, `zed`, `auto`, or pass an explicit file path.

**3. Restart your MCP client**

That's it. Sift is now proxying your upstream servers. Mirrored responses are persisted as queryable artifacts; small responses return raw, larger/continuation responses return artifact handles.

> **Note:** Your MCP client (Claude Desktop, Claude Code, Cursor, VS Code, Windsurf, Zed) launches Sift automatically via the config. Use `sift-mcp --check` to verify health without starting the server.

## How It Works

1. Sift connects to your configured upstream MCP servers (stdio or HTTP).
2. Each upstream tool is mirrored as `{prefix}.{tool_name}` with the original schema preserved. No injected fields.
3. Every mirrored tool response is persisted durably in SQLite + blob storage as an artifact envelope.
4. If the serialized response is small (default <= 8 KB), Sift returns the raw upstream result directly; otherwise it returns a lightweight **artifact handle** with `artifact_id`, schemas, and usage hints.
5. You query persisted artifacts using `artifact(action="query", query_kind=...)` with bounded responses and explicit pagination metadata.
6. Raw passthrough responses omit `artifact_id`; set `SIFT_MCP_PASSTHROUGH_MAX_BYTES=0` when you need deterministic handle returns.

## What You Can Do With Artifacts

### Selective retrieval

Pull exactly the fields you need from a stored response without loading the full payload:

```python
artifact(
    action="query",
    query_kind="select",
    artifact_id="art_7f3a...",
    root_path="$.items",
    select_paths=["name", "status", "assignee"],
    limit=50,
)
```

Results come back paginated. Continue with the returned cursor until `pagination.retrieval_status == "COMPLETE"`.

### Code queries

Run Python against stored data without loading it into context. Install libraries with `sift-mcp install pandas` (or `pipx install "sift-mcp[data-science]"` for the bundle):

```python
artifact(
    action="query",
    query_kind="code",
    artifact_id="art_7f3a...",
    root_path="$.issues",
    code="""
def run(data, schema, params):
    import pandas as pd
    df = pd.DataFrame(data)
    return df.groupby("assignee")["story_points"].sum().to_dict()
""",
)
```

Multi-artifact code queries let you join data across tools:

```python
artifact(
    action="query",
    query_kind="code",
    artifact_ids=["art_users...", "art_orders..."],
    root_paths={"art_users...": "$.users", "art_orders...": "$.orders"},
    code="""
def run(artifacts, schemas, params):
    users = {u["id"]: u["name"] for u in artifacts["art_users..."]}
    return [
        {"user": users.get(o["user_id"]), "amount": o["amount"]}
        for o in artifacts["art_orders..."]
    ]
""",
)
```

### Tool chaining with artifact references

Pass data between tools without the model ever loading the intermediate payload:

```python
# If tool_a returned a handle, pass artifact_id (or a JSONPath into it) to another tool
# Small mirrored responses may return raw payloads; set passthrough_max_bytes=0 for deterministic handles
tool_b(input="art_7f3a...")                     # full payload resolved server-side
tool_b(email="art_7f3a...:$.users[0].email")    # specific field
tool_b(ids="art_7f3a...:$.items[*].id")         # wildcard expansion
```

Sift resolves references server-side before forwarding to the upstream tool. The model sends a reference string, not the data. Two tool calls, zero intermediate tokens.

### Other query kinds

| Query kind | Purpose |
|---|---|
| `describe` | Schema and metadata for a stored artifact |
| `get` | Full original response (paginated if large) |
| `select` | JSONPath extraction with pagination |
| `code` | Python execution against stored data |
| `search` | List session artifacts visible in the current workspace |

## Security Note: Code Queries

Code queries execute model-generated Python in a subprocess with AST-level validation, an import allowlist, timeout enforcement, and memory limits. **This is not a full OS-level sandbox.** The guardrails prevent common abuse patterns but do not provide the isolation guarantees of a container or VM.

For production environments where untrusted models generate code, consider running Sift inside a container or disabling code queries entirely:

```bash
export SIFT_MCP_CODE_QUERY_ENABLED=false
```

See [SECURITY.md](SECURITY.md) for reporting vulnerabilities.

## Documentation

| Doc | What it covers |
|---|---|
| **[Quick Start Guide](docs/quickstart.md)** | Detailed setup: install, init, first artifact |
| **[Configuration Reference](docs/config.md)** | Every setting, env var, and default |
| **[Recipes & Examples](docs/recipes.md)** | Pagination loops, tool chaining, code queries, search |
| **[API Contracts](docs/api_contracts.md)** | Response shapes, pagination layers, handle format |
| **[Deployment Guide](docs/deployment.md)** | Transport modes, multi-process, monitoring |
| **[Error Reference](docs/errors.md)** | Error codes and troubleshooting |
| **[Observability](docs/observability.md)** | Structured logging events and metrics |
| **[Architecture](docs/architecture.md)** | Design specification and invariants |

## Development

```bash
git clone https://github.com/lourencomaciel/sift-mcp.git
cd sift-mcp
uv sync --extra dev

# Tests
uv run pytest tests/unit/ -q

# Lint and type check
uv run ruff check src tests
uv run mypy src
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full development guide, release workflow, and coding conventions.

## Requirements

- Python >= 3.11
- [pipx](https://pipx.pypa.io/) or [uv](https://docs.astral.sh/uv/)
- SQLite (bundled with Python, no external setup needed)

## License

MIT — See [LICENSE](LICENSE)
