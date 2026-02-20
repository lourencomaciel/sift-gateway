# Release Checklist

Use this checklist for contract-v1 releases.

## Quality gates

1. One-command preflight (recommended):
   - `UV_CACHE_DIR=/tmp/uv-cache uv run python scripts/run_rc_preflight.py`
2. Or run individual checks:
   - `UV_CACHE_DIR=/tmp/uv-cache uv run pytest tests/unit -q`
   - `UV_CACHE_DIR=/tmp/uv-cache uv run ruff check src tests`
   - `UV_CACHE_DIR=/tmp/uv-cache uv run mypy src`
   - `UV_CACHE_DIR=/tmp/uv-cache uv run python scripts/check_docs_consistency.py`

## Hardening gates

1. Cleanup lifecycle coverage present:
   - `tests/unit/test_cleanup_lifecycle.py`
2. Security pass checklist reviewed:
   - `docs/security-hardening.md`
3. Benchmark evidence captured:
   - `docs/performance-benchmarks.md`
   - `docs/benchmarks/2026-02-19-local-baseline.json`
   - `docs/benchmarks/2026-02-19-local-baseline.md`

## Release notes and migration

1. `CHANGELOG.md` updated under `Unreleased`.
2. `docs/migration-guide.md` current and linked from README/docs index.
3. Packaging transition notes aligned with:
   - `docs/packaging-transition.md`

## Final verify

1. Build artifacts:
   - `UV_CACHE_DIR=/tmp/uv-cache uv build`
2. Smoke CLI:
   - `UV_CACHE_DIR=/tmp/uv-cache uv run python scripts/smoke_cli_behavior.py --fail-fast`
   - `sift-gateway --version`
   - `sift-gateway run --json -- echo '{"ok":true}'`
   - `sift-gateway code <artifact_id-from-previous-step> '$' --expr 'len(df)' --json`
3. Smoke MCP status path:
   - `sift-gateway --check`
