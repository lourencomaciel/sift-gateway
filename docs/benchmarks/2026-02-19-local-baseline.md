# 2026-02-19 Local Baseline

Command:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run python scripts/benchmark_large_payloads.py \
  --rows 1000,5000,20000 \
  --repeats 3 \
  --query-limit 50 \
  --json > docs/benchmarks/2026-02-19-local-baseline.json
```

Environment:

1. Date: February 19, 2026
2. Host: local development machine
3. Python runtime via `uv run`

Results:

| Rows | Payload Bytes (p50) | Capture p50/p95 (ms) | Select p50/p95 (ms) | Get p50/p95 (ms) |
|---|---:|---:|---:|---:|
| 1,000 | 179,747 | 58.55 / 60.09 | 7.71 / 12.50 | 1.04 / 24.92 |
| 5,000 | 907,639 | 291.63 / 301.56 | 15.28 / 15.94 | 4.00 / 4.04 |
| 20,000 | 3,657,225 | 1181.72 / 1188.79 | 48.45 / 78.98 | 18.03 / 20.39 |

Notes:

1. Capture and retrieval-kernel latency scale predictably with payload size.
2. The 1,000-row `get` p95 outlier reflects a warm-up/variance spike in this local run.
3. `select`/`get` here are legacy internal kernel probes, not public contract-v1 APIs.
4. Use this baseline as a trend reference, not a strict pass/fail gate.
