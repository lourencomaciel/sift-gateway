# Performance Benchmarks

This guide defines the Phase 8 benchmark path for CLI-agnostic Sift.

## Scope

Measure core operations on large synthetic payloads:

1. Capture (`execute_artifact_capture`)
2. Select query (`execute_artifact_select`)
3. Get query (`execute_artifact_get`)

Both MCP and CLI surfaces call these same core services, so this benchmark is
a shared performance signal.

## Runner

Use:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run python scripts/benchmark_large_payloads.py \
  --rows 1000,5000,20000 \
  --repeats 3 \
  --query-limit 50
```

Machine output:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run python scripts/benchmark_large_payloads.py \
  --rows 1000,5000,20000 \
  --repeats 3 \
  --query-limit 50 \
  --json
```

Notes:

1. The script uses a temporary data dir by default.
2. Use `--data-dir <path>` if you want persistent benchmark artifacts.
3. Log lines may be emitted to stderr; `--json` controls stdout payload shape.

## Suggested Baseline Matrix

1. Small-large mix: `--rows 1000,5000,20000`
2. Stress pass: `--rows 50000`
3. Repeat stability: `--repeats 5`

## Output Interpretation

For each row-count case, track:

1. `payload_total_bytes_p50`
2. `capture.p50_ms` and `capture.p95_ms`
3. `select.p50_ms` and `select.p95_ms`
4. `get.p50_ms` and `get.p95_ms`

Flag for investigation when:

1. `p95` regresses by >20% against prior baseline at similar payload size.
2. Capture latency scales super-linearly between adjacent row tiers.
3. Select/get p95 grows disproportionately vs payload byte growth.

## CI vs Local

This benchmark is intentionally manual and environment-sensitive; it is not a
strict CI gate. Use it as release evidence and trend tracking.

## Published Baselines

Committed benchmark artifacts live under:

1. `docs/benchmarks/README.md`
2. `docs/benchmarks/2026-02-19-local-baseline.json`
3. `docs/benchmarks/2026-02-19-local-baseline.md`
