# Deployment Guide

Running Sift Gateway in production environments.

## Transport Modes

Sift supports multiple transport protocols for connecting to MCP clients.

### stdio (Default)

The default mode where Sift communicates via standard input/output. This is the standard for MCP servers launched by desktop clients like Claude Desktop, Cursor, and Claude Code.

**No configuration needed** — this is the default when you run `sift-gateway` without transport flags.

### SSE (Server-Sent Events)

Expose Sift over HTTP using Server-Sent Events for real-time updates.

```bash
sift-gateway --transport sse --host 127.0.0.1 --port 8080
```

**Use cases:**
- Web-based MCP clients
- Remote connections over HTTP
- Development and testing

### Streamable HTTP

Full HTTP transport with streaming support.

```bash
sift-gateway \
  --transport streamable-http \
  --host 0.0.0.0 --port 9090 --path /mcp \
  --auth-token "$SIFT_GATEWAY_AUTH_TOKEN"
```

**Use cases:**
- Production deployments
- Load-balanced environments
- Remote access with authentication

## Security Configuration

### Authentication Tokens

**Security defaults:**

- **Localhost binds** (`127.0.0.1`, `localhost`, `::1`) — No token required
- **Non-local binds** (e.g., `0.0.0.0`) — Token required

**Setting the auth token:**

```bash
# Option 1: Command-line flag
sift-gateway --transport sse --host 0.0.0.0 --auth-token "your-secret-token"

# Option 2: Environment variable
export SIFT_GATEWAY_AUTH_TOKEN="your-secret-token"
sift-gateway --transport sse --host 0.0.0.0
```

**Important:** The process exits with a security error if binding to a non-local address without an auth token.

### Outbound secret redaction

Outbound tool responses are redacted by default before they are returned to the
MCP client. This helps prevent accidental leakage of API keys/tokens from
upstream payloads.

```bash
# Enabled by default
export SIFT_GATEWAY_SECRET_REDACTION_ENABLED=true

# Optional: fail requests with INTERNAL if redaction cannot run
export SIFT_GATEWAY_SECRET_REDACTION_FAIL_CLOSED=false

# Optional tuning
export SIFT_GATEWAY_SECRET_REDACTION_MAX_SCAN_BYTES=32768
export SIFT_GATEWAY_SECRET_REDACTION_PLACEHOLDER='[REDACTED_SECRET]'
```

Set `SIFT_GATEWAY_SECRET_REDACTION_ENABLED=false` only in trusted environments that
explicitly need raw upstream payloads.

### Client Configuration

When using URL mode, configure your MCP client with the gateway URL:

```bash
sift-gateway init \
  --from claude \
  --gateway-url http://localhost:8080/mcp
```

This writes a `{"url": "..."}` entry in the source file instead of a `command` entry.
`--from` accepts either a shortcut (for example `claude`) or an explicit file path.

### Migrations

Sift automatically runs SQLite migrations on startup. No manual intervention needed.

To check migration status:

```bash
sift-gateway --check
```

## Monitoring and Observability

### Health Checks

Use `sift-gateway --check` as a CLI health check:

```bash
#!/bin/bash
# health_check.sh
sift-gateway --check
if [ $? -eq 0 ]; then
  echo "HEALTHY"
  exit 0
else
  echo "UNHEALTHY"
  exit 1
fi
```

This validates:
- Configuration file syntax
- Database connectivity
- Filesystem permissions
- Upstream server availability

### Logging

Sift emits structured logs to stderr (JSON format) only when `--logs` is passed.
Without `--logs`, structured logs are suppressed by default. The current
release does not expose a dedicated `SIFT_GATEWAY_LOG_LEVEL` setting.

See [Observability](observability.md) for full event catalog.

### Metrics

Sift records Prometheus-style counters and histograms internally for gateway operations (cache, upstream calls, mapping, cursor outcomes, pruning, outbound redaction, and code queries). There is currently no configurable standalone `/metrics` HTTP endpoint.

## Performance Tuning

### Response Mode Handling

Mirrored upstream responses are always persisted as artifacts. Return shape is
controlled by full-vs-schema mode selection.

```bash
export SIFT_GATEWAY_PASSTHROUGH_MAX_BYTES=8192  # Legacy name; inline-mode cap
```

- If upstream pagination is present, Sift returns `response_mode="schema_ref"`.
- If non-paginated full payload exceeds the cap, Sift returns `schema_ref`.
- Under the cap, Sift returns `schema_ref` only when compact schema is at
  least 50% smaller than full payload; otherwise it returns `full`.

To limit disk growth, tune storage quota settings instead:

```bash
export SIFT_GATEWAY_MAX_TOTAL_STORAGE_BYTES=10000000000
export SIFT_GATEWAY_QUOTA_ENFORCEMENT_ENABLED=true
```

### Response Budgets

Limit retrieval response sizes to prevent memory exhaustion:

```bash
export SIFT_GATEWAY_MAX_ITEMS=1000        # Max items per query response
export SIFT_GATEWAY_MAX_BYTES_OUT=5000000 # Max bytes per query response (5 MB)
```

### Cursor TTL

Control how long pagination cursors remain valid:

```bash
export SIFT_GATEWAY_CURSOR_TTL_MINUTES=60  # Default: 60 minutes
```

Shorter TTLs reduce storage overhead but may break long-running pagination workflows.

### Code Query Settings

Configure Python code query execution:

```bash
export SIFT_GATEWAY_CODE_QUERY_ENABLED=true
export SIFT_GATEWAY_CODE_QUERY_TIMEOUT_SECONDS=30
export SIFT_GATEWAY_CODE_QUERY_MAX_INPUT_RECORDS=10000
export SIFT_GATEWAY_CODE_QUERY_ALLOWED_IMPORT_ROOTS='["math","json","jmespath","numpy","pandas"]'
```

See [Configuration Reference](config.md) for all available settings.

## Backup and Recovery

### Database Backups

```bash
# Backup
cp .sift-gateway/state/gateway.db .sift-gateway/state/gateway.db.backup

# Restore
cp .sift-gateway/state/gateway.db.backup .sift-gateway/state/gateway.db
```

### Blob Storage Backups

Artifact blobs are stored in `.sift-gateway/blobs/` (content-addressed):

```bash
# Backup
tar -czf blobs_backup.tar.gz .sift-gateway/blobs/

# Restore
tar -xzf blobs_backup.tar.gz
```

**Important:** Back up both database and blob storage together to maintain referential integrity.

## Troubleshooting

### Connection refused errors

- Check that Sift is running: `ps aux | grep sift-gateway`
- Verify port is open: `netstat -an | grep 8080`
- Check firewall rules if accessing remotely

### Performance degradation

- Check database size: `du -sh .sift-gateway/state/gateway.db`
- Monitor query performance through structured stderr events (see [Observability](observability.md))
- Review blob storage usage: `du -sh .sift-gateway/blobs/`
- Tune retrieval budgets (`max_items`, `max_bytes_out`) and mapping budgets in `state/config.json`

### Authentication failures

- Verify auth token matches between server and client
- Check token isn't expired or rotated
- Ensure environment variable is set: `echo $SIFT_GATEWAY_AUTH_TOKEN`

## Next Steps

- **[Configuration Reference](config.md)** — Full list of environment variables and settings
- **[Observability](observability.md)** — Structured logging and metrics catalog
- **[Architecture](architecture.md)** — Understanding Sift's design for production planning
- **[Quick Start Guide](quickstart.md)** — Return to basic setup if needed
