# SidePouch

MCP Artifact Gateway

Keep big MCP responses out of your context window. Query them.

## Why this exists

Standard MCP tool calls are great for immediate answers, but long responses can quickly clog model context windows, push out useful conversation state, and make MCPs unusable for complex workflows.

- large tool outputs consume prompt tokens repeatedly on follow-up turns;
- tool outputs are often ephemeral and hard to revisit across sessions;
- large payloads are expensive to resend and reprocess;
- follow-up queries need deterministic pagination and bounded traversal;
- upstream errors are usually not captured as first-class artifacts.

SidePouch moves bulky MCP output out of context, keeps it durable, and makes retrieval deterministic without changing upstream tool schemas.

## What SidePouch does

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

- `gateway.status`
- `artifact.search`
- `artifact.get`
- `artifact.select`
- `artifact.describe`
- `artifact.find`
- `artifact.chain_pages`

## Requirements

- Python `>=3.11`
- [`uv`](https://docs.astral.sh/uv/)
- Docker (only needed for PostgreSQL backend)

## Quick start

1. Install dependencies:

```bash
uv sync
```

2. Run SidePouch (uses SQLite by default — no external dependencies):

```bash
uv run sidepouch-mcp
```

That's it. The default SQLite backend stores data at `.sidepouch-mcp/state/gateway.db` and requires no setup.

### Using PostgreSQL instead

For production deployments or when you need concurrent multi-process access:

1. Install with the Postgres extra:

```bash
uv sync --extra postgres
```

2. Copy environment template and set the backend:

```bash
cp .env.example .env
# Edit .env: set SIDEPOUCH_MCP_DB_BACKEND=postgres
```

3. Start Postgres:

```bash
docker compose up -d
```

`docker compose` provisions:

- `sidepouch` (app runtime DB)
- `sidepouch_test` (integration test DB; created by `scripts/init-test-db.sql` on first container init)

If the container already existed before the init script mount, recreate it:

```bash
docker compose down -v && docker compose up -d
```

4. Run startup checks:

```bash
uv run sidepouch-mcp --check
```

5. Run SidePouch:

```bash
uv run sidepouch-mcp
```

## Configure upstream MCP servers

SidePouch accepts the standard `mcpServers` format used by Claude
Desktop, Cursor, and Claude Code.

### Migrate an existing config

```bash
uv run sidepouch-mcp init \
  --from ~/Library/Application\ Support/Claude/claude_desktop_config.json
```

This command:

1. copies source `mcpServers` into `.sidepouch-mcp/state/config.json`;
2. externalizes inline `env` and `headers` into per-upstream secret
   files under `.sidepouch-mcp/state/upstream_secrets/`;
3. writes a backup to `<source>.backup`;
4. rewrites the source config to point to SidePouch only;
5. stores `_gateway_sync` metadata so future restarts can auto-sync.

Preview:

```bash
uv run sidepouch-mcp init \
  --from ~/Library/Application\ Support/Claude/claude_desktop_config.json \
  --dry-run
```

Revert:

```bash
uv run sidepouch-mcp init \
  --from ~/Library/Application\ Support/Claude/claude_desktop_config.json \
  --revert
```

If the client should connect to SidePouch over HTTP instead of stdio,
pass `--gateway-url`:

```bash
uv run sidepouch-mcp init \
  --from claude_desktop_config.json \
  --gateway-url http://localhost:8080/mcp
```

This writes a `{"url": "..."}` entry in the source file instead of
a `command` entry.

### Adding MCPs after initial setup

After `init`, the source config file (e.g. `claude_desktop_config.json`)
contains only the SidePouch gateway entry. To add a new upstream MCP:

1. Edit the source config and add the new server entry alongside the
   gateway entry.
2. Restart SidePouch.

On startup, SidePouch reads the `_gateway_sync` metadata, detects
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
`.sidepouch-mcp/state/upstream_secrets/<prefix>.json` (0600 perms).
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

SidePouch defaults to stdio transport. To expose the gateway over
HTTP (SSE or Streamable HTTP):

```bash
uv run sidepouch-mcp --transport sse --host 127.0.0.1 --port 8080
```

```bash
uv run sidepouch-mcp \
  --transport streamable-http \
  --host 0.0.0.0 --port 9090 --path /mcp \
  --auth-token "$SIDEPOUCH_MCP_AUTH_TOKEN"
```

Security defaults:

- Localhost binds (`127.0.0.1`, `localhost`, `::1`) require no token.
- Non-local binds (e.g. `0.0.0.0`) require `--auth-token` or the
  `SIDEPOUCH_MCP_AUTH_TOKEN` environment variable. The process exits
  with a security error if neither is provided.

## Configuration model

Precedence:

1. `SIDEPOUCH_MCP_*` environment variables
2. `DATA_DIR/state/config.json`
3. compiled defaults

Key defaults:

- `SIDEPOUCH_MCP_DB_BACKEND=sqlite`
- `SIDEPOUCH_MCP_POSTGRES_DSN=postgresql://localhost:5432/sidepouch`
- `SIDEPOUCH_MCP_DATA_DIR=.sidepouch-mcp`
- `SIDEPOUCH_MCP_MAPPING_MODE=hybrid`
- `SIDEPOUCH_MCP_ENVELOPE_JSONB_MODE=full`
- `SIDEPOUCH_MCP_ENVELOPE_CANONICAL_ENCODING=zstd`
- `SIDEPOUCH_MCP_MAX_FULL_MAP_BYTES=10000000`
- `SIDEPOUCH_MCP_MAX_ITEMS=1000`
- `SIDEPOUCH_MCP_MAX_BYTES_OUT=5000000`
- `SIDEPOUCH_MCP_CURSOR_TTL_MINUTES=60`
- `SIDEPOUCH_MCP_WHERE_CANONICALIZATION_MODE=raw_string`

For the full key/type/default reference, see:

- `docs/config.md`
- `src/sidepouch_mcp/config/settings.py`

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
`postgresql://sidepouch:sidepouch@localhost:5432/sidepouch_test`

Override:

```bash
SIDEPOUCH_MCP_TEST_POSTGRES_DSN="postgresql://user:pass@host:5432/db" uv run pytest tests/integration -v
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
src/sidepouch_mcp/
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
