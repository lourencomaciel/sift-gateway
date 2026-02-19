# Security Hardening

Phase 8 security pass focuses on two high-risk surfaces: capture execution and
code execution.

## Capture Surface (`sift run`)

Current controls:

1. Command execution uses `subprocess.run(command_argv, shell=False)` semantics.
2. `--stdin` cannot be combined with command execution in the same invocation.
3. Command failures are persisted as structured error metadata (`status=error`).
4. Dedup keys are hash-based identity keys, not raw shell strings.

Operational guidance:

1. Treat `sift run` as trusted-local execution; it runs real commands.
2. Restrict who can invoke CLI capture in shared environments.
3. Prefer `--stdin` ingestion for externally fetched data when practical.

## Code Surface (`sift code` / `query_kind=code`)

Current controls:

1. AST guard enforces import/root restrictions.
2. Runtime executes code in a subprocess with timeout/memory constraints.
3. Import allowlist is explicit and configurable.
4. Code runtime can be disabled globally:

```bash
export SIFT_MCP_CODE_QUERY_ENABLED=false
```

Operational guidance:

1. Keep code query disabled in high-trust boundaries unless required.
2. If enabled for untrusted model code, run Sift inside a container boundary.
3. Keep optional code dependencies minimal and controlled (`[code]` extra only).

## Validation Checklist

Run before release:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run pytest \
  tests/unit/test_cli_main.py \
  tests/unit/test_core_artifact_code.py \
  tests/unit/test_cleanup_lifecycle.py -q
```

Also run static checks:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run ruff check src tests
UV_CACHE_DIR=/tmp/uv-cache uv run mypy src
```

## Residual Risk

Code-query isolation is process-level, not VM-level. This is sufficient for many
developer workflows but not equivalent to full sandbox isolation. Treat it as a
guarded execution path, not a hardened multi-tenant sandbox.

