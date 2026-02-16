# Deployment Guide

Running Sift MCP in production environments.

## Transport Modes

Sift supports multiple transport protocols for connecting to MCP clients.

### stdio (Default)

The default mode where Sift communicates via standard input/output. This is the standard for MCP servers launched by desktop clients like Claude Desktop, Cursor, and Claude Code.

**No configuration needed** — this is the default when you run `sift-mcp` without transport flags.

### SSE (Server-Sent Events)

Expose Sift over HTTP using Server-Sent Events for real-time updates.

```bash
sift-mcp --transport sse --host 127.0.0.1 --port 8080
```

**Use cases:**
- Web-based MCP clients
- Remote connections over HTTP
- Development and testing

### Streamable HTTP

Full HTTP transport with streaming support.

```bash
sift-mcp \
  --transport streamable-http \
  --host 0.0.0.0 --port 9090 --path /mcp \
  --auth-token "$SIFT_MCP_AUTH_TOKEN"
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
sift-mcp --transport sse --host 0.0.0.0 --auth-token "your-secret-token"

# Option 2: Environment variable
export SIFT_MCP_AUTH_TOKEN="your-secret-token"
sift-mcp --transport sse --host 0.0.0.0
```

**Important:** The process exits with a security error if binding to a non-local address without an auth token.

### Client Configuration

When using URL mode, configure your MCP client with the gateway URL:

```bash
sift-mcp init \
  --from claude \
  --gateway-url http://localhost:8080/mcp
```

This writes a `{"url": "..."}` entry in the source file instead of a `command` entry.
`--from` accepts either a shortcut (for example `claude`) or an explicit file path.

## PostgreSQL Production Setup

For production deployments, use PostgreSQL instead of SQLite for:

- **Concurrent access** — Multiple processes/clients
- **Durability** — Better crash recovery
- **Performance** — Optimized for high throughput

### Option 1: Docker Compose (Development/Staging)

The project includes a `docker-compose.yml` with production-ready configuration:

```bash
docker compose up -d
```

This provisions:
- `sift` database (application runtime)
- `sift_test` database (integration tests)

### Option 2: Managed PostgreSQL (Production)

For production, use a managed PostgreSQL service (AWS RDS, Google Cloud SQL, Azure Database, etc.):

```bash
sift-mcp init \
  --from claude \
  --db-backend postgres \
  --postgres-dsn "postgresql://user:password@prod-db.example.com:5432/sift"
```

**Connection pooling:**

Sift uses `psycopg_pool.ConnectionPool`. Configure pool bounds via environment variables:

```bash
export SIFT_MCP_POSTGRES_POOL_MIN=5
export SIFT_MCP_POSTGRES_POOL_MAX=20
```

Keep `SIFT_MCP_POSTGRES_POOL_MAX >= SIFT_MCP_POSTGRES_POOL_MIN`.

**SSL/TLS:**

For secure connections, include SSL parameters in the DSN:

```bash
export SIFT_MCP_POSTGRES_DSN="postgresql://user:pass@host:5432/sift?sslmode=require"
```

### Migrations

Sift automatically runs migrations on startup. No manual intervention needed.

To check migration status:

```bash
sift-mcp --check
```

## Multi-Process Deployment

When running multiple Sift instances (e.g., behind a load balancer):

1. **Use PostgreSQL backend** — SQLite doesn't support concurrent access
2. **Shared filesystem** — Ensure all instances use the same `DATA_DIR` for blob storage
3. **Session affinity** — Not required (each request is stateless)

**Example Docker Compose with multiple instances:**

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: sift
      POSTGRES_PASSWORD: sift
      POSTGRES_DB: sift
    volumes:
      - postgres_data:/var/lib/postgresql/data

  sift-1:
    image: sift-mcp:latest
    command: --transport sse --host 0.0.0.0 --port 8080
    environment:
      SIFT_MCP_DB_BACKEND: postgres
      SIFT_MCP_POSTGRES_DSN: postgresql://sift:sift@postgres:5432/sift
      SIFT_MCP_AUTH_TOKEN: ${SIFT_AUTH_TOKEN}
    volumes:
      - shared_data:/app/.sift-mcp
    ports:
      - "8080:8080"

  sift-2:
    image: sift-mcp:latest
    command: --transport sse --host 0.0.0.0 --port 8080
    environment:
      SIFT_MCP_DB_BACKEND: postgres
      SIFT_MCP_POSTGRES_DSN: postgresql://sift:sift@postgres:5432/sift
      SIFT_MCP_AUTH_TOKEN: ${SIFT_AUTH_TOKEN}
    volumes:
      - shared_data:/app/.sift-mcp
    ports:
      - "8081:8080"

  nginx:
    image: nginx:alpine
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf
    ports:
      - "80:80"
    depends_on:
      - sift-1
      - sift-2

volumes:
  postgres_data:
  shared_data:
```

## Monitoring and Observability

### Health Checks

Use `sift-mcp --check` for health check endpoints:

```bash
#!/bin/bash
# health_check.sh
sift-mcp --check
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

Sift emits structured logs to stderr (JSON format, INFO level by default). The current release does not expose a dedicated `SIFT_MCP_LOG_LEVEL` setting.

See [Observability](observability.md) for full event catalog.

### Metrics

Sift records Prometheus-style counters and histograms internally for gateway operations (cache, upstream calls, mapping, cursor outcomes, pruning, and code queries). There is currently no configurable standalone `/metrics` HTTP endpoint.

## Performance Tuning

### Passthrough Threshold

Control when responses are stored as artifacts:

```bash
export SIFT_MCP_PASSTHROUGH_MAX_BYTES=8192  # Default: 8 KB
```

**Guidance:**
- Lower threshold (2-4 KB) → More artifacts, lower context usage
- Higher threshold (16-32 KB) → Fewer artifacts, higher context usage

### Response Budgets

Limit retrieval response sizes to prevent memory exhaustion:

```bash
export SIFT_MCP_MAX_ITEMS=1000        # Max items per query response
export SIFT_MCP_MAX_BYTES_OUT=5000000 # Max bytes per query response (5 MB)
```

### Cursor TTL

Control how long pagination cursors remain valid:

```bash
export SIFT_MCP_CURSOR_TTL_MINUTES=60  # Default: 60 minutes
```

Shorter TTLs reduce storage overhead but may break long-running pagination workflows.

### Code Query Settings

Configure Python code query execution:

```bash
export SIFT_MCP_CODE_QUERY_ENABLED=true
export SIFT_MCP_CODE_QUERY_TIMEOUT_SECONDS=30
export SIFT_MCP_CODE_QUERY_MAX_INPUT_RECORDS=10000
export SIFT_MCP_CODE_QUERY_ALLOWED_IMPORT_ROOTS='["math","json","jmespath","numpy","pandas"]'
```

See [Configuration Reference](config.md) for all available settings.

## Backup and Recovery

### Database Backups

**PostgreSQL:**

```bash
# Backup
pg_dump -h localhost -U sift sift > sift_backup.sql

# Restore
psql -h localhost -U sift sift < sift_backup.sql
```

**SQLite:**

```bash
# Backup
cp .sift-mcp/state/gateway.db .sift-mcp/state/gateway.db.backup

# Restore
cp .sift-mcp/state/gateway.db.backup .sift-mcp/state/gateway.db
```

### Blob Storage Backups

Artifact blobs are stored in `.sift-mcp/blobs/` (content-addressed):

```bash
# Backup
tar -czf blobs_backup.tar.gz .sift-mcp/blobs/

# Restore
tar -xzf blobs_backup.tar.gz
```

**Important:** Back up both database and blob storage together to maintain referential integrity.

## Troubleshooting

### Connection refused errors

- Check that Sift is running: `ps aux | grep sift-mcp`
- Verify port is open: `netstat -an | grep 8080`
- Check firewall rules if accessing remotely

### Database connection errors

- Verify PostgreSQL is running: `docker ps` or `systemctl status postgresql`
- Test connection: `psql $SIFT_MCP_POSTGRES_DSN`
- Check DSN format: `postgresql://user:pass@host:port/database`

### Performance degradation

- Check database size: `SELECT pg_database_size('sift');` (PostgreSQL)
- Monitor query performance through structured stderr events (see [Observability](observability.md))
- Review blob storage usage: `du -sh .sift-mcp/blobs/`
- Consider adjusting `passthrough_max_bytes` threshold

### Authentication failures

- Verify auth token matches between server and client
- Check token isn't expired or rotated
- Ensure environment variable is set: `echo $SIFT_MCP_AUTH_TOKEN`

## Next Steps

- **[Configuration Reference](config.md)** — Full list of environment variables and settings
- **[Observability](observability.md)** — Structured logging and metrics catalog
- **[Architecture & Spec](spec_v1_9.md)** — Understanding Sift's design for production planning
- **[Quick Start Guide](quickstart.md)** — Return to basic setup if needed
