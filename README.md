# Sift

MCP Artifact Gateway

Keep big MCP responses out of your context window. Query them.

## Why this exists

Standard MCP tool calls are great for immediate answers, but long responses can quickly clog model context windows, push out useful conversation state, and make MCPs unusable for complex workflows.

- large tool outputs consume prompt tokens repeatedly on follow-up turns;
- tool outputs are often ephemeral and hard to revisit across sessions;
- large payloads are expensive to resend and reprocess;
- follow-up queries need deterministic pagination and bounded traversal;
- upstream errors are usually not captured as first-class artifacts.

Sift moves bulky MCP output out of context, keeps it durable, and makes retrieval deterministic without changing upstream tool schemas.

## What Sift does

1. Connects to configured upstream MCP servers (stdio or HTTP).
2. Mirrors each upstream tool as `{prefix}.{tool}`.
3. Intercepts each call, forwards arguments upstream, and stores the normalized response envelope.
4. Returns the result: small payloads (< `passthrough_max_bytes`, default 8 KB) are returned raw (gateway is transparent); larger payloads return an artifact handle for retrieval via query tools.
5. Exposes retrieval tools over stored artifacts with bounded response budgets and signed cursors.

Design invariants (from v1.9 spec):

- single workspace (`WORKSPACE_ID = "local"`);
- deterministic traversal/mapping/cursor behavior;
- bounded responses (items, bytes, compute, wildcard caps);
- crash-safe storage writes;
- always-store semantics (including upstream errors).

## Built-in gateway tools

- `gateway_status` — health and configuration snapshot
- `artifact` — consolidated retrieval tool with an `action` parameter:
  - `describe` — inspect artifact structure and mapping roots
  - `get` — retrieve raw envelope or mapped data
  - `select` — project specific fields from a mapped root array
  - `search` — find artifacts visible to this session
  - `next_page` — fetch the next page of a paginated upstream response

## Pagination Contract v1

Sift exposes layer-explicit pagination metadata:

- mirrored upstream tool responses use `pagination.layer = "upstream"`;
- retrieval tool responses (`artifact` with action search/get/select)
  use `pagination.layer = "artifact_retrieval"`.

Key fields:

- `pagination.retrieval_status`: `PARTIAL` or `COMPLETE`
- `pagination.partial_reason`: machine-readable partial reason or `null`
- `pagination.has_more`: whether more data is available

For upstream pagination, compatibility fields remain:

- `pagination.has_next_page`
- `pagination.hint`

Completion rule:

- do not claim full completeness until
  `pagination.retrieval_status == "COMPLETE"`.

## Gateway Context Controls

Mirrored tool calls may include `_gateway_context.cache_mode`:

- `normal` (default): reuse by `request_key` when possible;
- `bypass`: skip reuse and force a fresh upstream call;
- `refresh`: same as bypass, with explicit refresh intent.

Backward-compatible aliases are still accepted:

- `allow` -> `normal`
- `fresh` -> `bypass`

Handle responses include consistent cache metadata:

- `reused`
- `request_key`
- `reason`
- `artifact_id_origin` (`cache` or `fresh`)
- `cache_mode` (normalized mode)

### Session visibility on cache reuse

When a mirrored call returns a reused `artifact_id`, Sift first
attaches that artifact to the caller's session (`artifact_refs`) before
returning the handle. This guarantees the returned handle is immediately
retrievable by `artifact(action="get")`, `artifact(action="describe")`,
and `artifact(action="select")` in the same session.

## Artifact-first Recipes

### Large mirrored result -> retrieve deterministically

1. Call mirrored tool (for example `meta_ads_get_campaigns`).
2. If response includes `artifact_id`, use inline `describe` data to choose
   `root_path`.
3. Call `artifact(action="select")` with pagination.
4. Continue paging until `pagination.retrieval_status == "COMPLETE"`.

### Upstream pagination chain

1. Call mirrored tool and inspect `pagination.layer == "upstream"`.
2. If `pagination.has_next_page` is true, call
   `artifact(action="next_page")` with the returned `artifact_id`.
3. Repeat until `pagination.retrieval_status == "COMPLETE"`.

### Tool chaining with artifact query references

Pass an `artifact_id` (or `artifact_id:$.jsonpath`) directly as an
argument to another mirrored tool. Sift resolves the reference
server-side before forwarding — the LLM never loads the intermediate
data.

```
# Bare reference — resolves to the full JSON payload
tool_b(input="art_7f3a...")

# Query reference — resolves to a specific field
tool_b(input="art_7f3a...:$.items[0].name")

# Wildcard — resolves to a list of values
tool_b(emails="art_7f3a...:$.users[*].email")
```

Only top-level string arguments are inspected. Nested values inside
dicts or lists are never resolved.

## Requirements

- Python `>=3.11`
- [`uv`](https://docs.astral.sh/uv/)
- Docker (only needed for PostgreSQL backend)

## Quick start

1. Install:

```bash
uv sync
```

2. Import your existing MCP config (e.g. from Claude Desktop):

```bash
uv run sift-mcp init \
  --from ~/Library/Application\ Support/Claude/claude_desktop_config.json
```

3. Restart your MCP client. Sift is now running as a proxy in
   front of your upstream servers.

The default SQLite backend stores data at
`.sift-mcp/state/gateway.db` and requires no setup.

> Sift is an MCP server — your MCP client (Claude Desktop,
> Cursor, Claude Code, etc.) launches it automatically via the
> config that `init` writes. You don't need to run it directly.
> Use `sift-mcp --check` to verify config and health without
> starting the server.

### Using PostgreSQL instead

For production deployments or when you need concurrent multi-process
access, pass `--db-backend postgres` during init:

```bash
uv sync --extra postgres
docker compose up -d

uv run sift-mcp init \
  --from ~/Library/Application\ Support/Claude/claude_desktop_config.json \
  --db-backend postgres
```

`docker compose` provisions:

- `sift` (app runtime DB)
- `sift_test` (integration test DB; created by
  `scripts/init-test-db.sql` on first container init)

If the container already existed before the init script mount,
recreate it:

```bash
docker compose down -v && docker compose up -d
```

Optionally provide a DSN to skip Docker auto-provisioning:

```bash
uv run sift-mcp init \
  --from ~/Library/Application\ Support/Claude/claude_desktop_config.json \
  --db-backend postgres \
  --postgres-dsn postgresql://user:pass@host:5432/sift
```

Verify health at any time:

```bash
uv run sift-mcp --check
```

## Configure upstream MCP servers

Sift accepts the standard `mcpServers` format used by Claude
Desktop, Cursor, and Claude Code.

### What `init` does

1. Copies source `mcpServers` into `.sift-mcp/state/config.json`.
2. Sets `db_backend` to `sqlite` by default.
3. Externalizes inline `env` and `headers` into per-upstream secret
   files under `.sift-mcp/state/upstream_secrets/`.
4. Writes a backup to `<source>.backup`.
5. Rewrites the source config to point to Sift only.
6. Stores `_gateway_sync` metadata so future restarts can auto-sync.

Preview:

```bash
uv run sift-mcp init \
  --from ~/Library/Application\ Support/Claude/claude_desktop_config.json \
  --dry-run
```

Revert:

```bash
uv run sift-mcp init \
  --from ~/Library/Application\ Support/Claude/claude_desktop_config.json \
  --revert
```

If the client should connect to Sift over HTTP instead of stdio,
pass `--gateway-url`:

```bash
uv run sift-mcp init \
  --from claude_desktop_config.json \
  --gateway-url http://localhost:8080/mcp
```

This writes a `{"url": "..."}` entry in the source file instead of
a `command` entry.

### Adding MCPs after initial setup

After `init`, the source config file (e.g. `claude_desktop_config.json`)
contains only the Sift gateway entry. To add a new upstream MCP:

1. Edit the source config and add the new server entry alongside the
   gateway entry.
2. Restart Sift.

On startup, Sift reads the `_gateway_sync` metadata, detects
new non-gateway entries in the source file, imports them (including
secret externalization), and rewrites the source file back to
gateway-only. No manual re-init is needed.

### Manual config (`mcpServers`)

```json
{
  "mcpServers": {
    "github": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-github"],
      "_gateway": {
        "secret_ref": "github",
        "semantic_salt_env_keys": ["GITHUB_ORG"],
        "strict_schema_reuse": true,
        "passthrough_allowed": true,
        "dedupe_exclusions": ["$.timestamp"]
      }
    },
    "remote": {
      "url": "https://example.com/mcp",
      "_gateway": {
        "secret_ref": "remote"
      }
    }
  }
}
```

Secrets are stored externally in
`.sift-mcp/state/upstream_secrets/<prefix>.json` (0600 perms).
Use `_gateway.secret_ref` to reference them instead of placing
credentials inline. See `docs/config.md` for details.

Transport inference:

- `command` present -> `stdio`
- `url` present -> `http`

### Environment isolation

By default, stdio upstream processes receive only a minimal set of
environment variables: `PATH`, `HOME`, `LANG`, `LC_ALL`, `TMPDIR`,
`TMP`, `TEMP`, `USER`, `LOGNAME`, `SHELL`. Explicit `env` from
the config or secret file is merged on top.

Set `_gateway.inherit_parent_env: true` to pass the full parent
process environment to a specific upstream instead.

## Running in URL mode

Sift defaults to stdio transport. To expose the gateway over
HTTP (SSE or Streamable HTTP):

```bash
uv run sift-mcp --transport sse --host 127.0.0.1 --port 8080
```

```bash
uv run sift-mcp \
  --transport streamable-http \
  --host 0.0.0.0 --port 9090 --path /mcp \
  --auth-token "$SIFT_MCP_AUTH_TOKEN"
```

Security defaults:

- Localhost binds (`127.0.0.1`, `localhost`, `::1`) require no token.
- Non-local binds (e.g. `0.0.0.0`) require `--auth-token` or the
  `SIFT_MCP_AUTH_TOKEN` environment variable. The process exits
  with a security error if neither is provided.

## Configuration model

Precedence:

1. `SIFT_MCP_*` environment variables
2. `DATA_DIR/state/config.json`
3. compiled defaults

Key defaults:

- `SIFT_MCP_DB_BACKEND=sqlite`
- `SIFT_MCP_POSTGRES_DSN=postgresql://localhost:5432/sift`
- `SIFT_MCP_DATA_DIR=.sift-mcp`
- `SIFT_MCP_MAPPING_MODE=hybrid`
- `SIFT_MCP_ENVELOPE_JSONB_MODE=full`
- `SIFT_MCP_ENVELOPE_CANONICAL_ENCODING=zstd`
- `SIFT_MCP_MAX_FULL_MAP_BYTES=10000000`
- `SIFT_MCP_MAX_ITEMS=1000`
- `SIFT_MCP_MAX_BYTES_OUT=5000000`
- `SIFT_MCP_CURSOR_TTL_MINUTES=60`
- `SIFT_MCP_WHERE_CANONICALIZATION_MODE=raw_string`

For the full key/type/default reference, see:

- `docs/config.md`
- `src/sift_mcp/config/settings.py`

## Docs map

- Architecture and invariants: `docs/spec_v1_9.md`
- Configuration reference: `docs/config.md`
- Traversal ordering contract: `docs/traversal_contract.md`
- Cursor format and staleness rules: `docs/cursor_contract.md`

## Development

### Unit tests

```bash
uv run pytest tests/unit -q
```

### Integration tests

```bash
docker compose up -d
uv run pytest tests/integration -v
```

Default test DSN is provided by `tests/integration/conftest.py`:
`postgresql://sift:sift@localhost:5432/sift_test`

Override:

```bash
SIFT_MCP_TEST_POSTGRES_DSN="postgresql://user:pass@host:5432/db" uv run pytest tests/integration -v
```

### Runtime validation

```bash
PYTHONPATH=src uv run python scripts/validate.py
```

### Lint and type-check

```bash
uv run ruff check src
uv run mypy src
```

## Project layout

```text
src/sift_mcp/
  main.py                  # CLI entrypoint
  app.py                   # app composition root
  config/                  # settings, mcpServers parser, init, sync, secrets
  db/                      # pool, migrations, repositories
  fs/                      # content-addressed blob storage
  mcp/                     # upstream connections, mirroring, server wiring
  artifacts/               # envelope and artifact creation pipeline
  mapping/                 # full + partial mapping
  retrieval/               # bounded deterministic traversal responses
  cursor/                  # signed cursor payload + HMAC verification
  query/                   # JSONPath subset, select paths, where DSL
  tools/                   # gateway and artifact retrieval tool handlers
  jobs/                    # soft delete, hard delete, reconcile tasks
  obs/                     # structured logging + metrics
tests/
  unit/
  integration/
docs/
  spec_v1_9.md
  config.md
  traversal_contract.md
  cursor_contract.md
```
