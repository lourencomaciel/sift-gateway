# Benchmark Baselines

This directory stores reproducible benchmark outputs for release and regression
tracking.

## Files

1. `2026-02-19-local-baseline.json`:
   - Raw machine output from `scripts/benchmark_large_payloads.py`
2. `2026-02-19-local-baseline.md`:
   - Human summary table and run context for that JSON

## Regenerating

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run python scripts/benchmark_large_payloads.py \
  --rows 1000,5000,20000 \
  --repeats 3 \
  --query-limit 50 \
  --json > docs/benchmarks/YYYY-MM-DD-local-baseline.json
```

Then add a matching markdown summary file with key p50/p95 metrics.

