# Benchmarks

Evaluation suite measuring Sift Gateway's accuracy and token efficiency
against traditional context-stuffing approaches.

## Tier 1 — Factual Q&A on Structured Data

Compares two conditions across 12 real-world JSON datasets and 103 questions:

| Condition | How it works |
|-----------|-------------|
| **Baseline** | Full JSON payload stuffed into the LLM prompt (truncated to fit context) |
| **Sift** | LLM receives only the schema, generates Python code, Sift executes it against the artifact |

### Quick start

```bash
# 1. Download datasets (one-time)
uv run python benchmarks/tier1/fetch_data.py

# 2. Run the benchmark (requires ANTHROPIC_API_KEY or OPENAI_API_KEY)
uv run python benchmarks/tier1/harness.py
```

### CLI options

| Flag | Default | Description |
|------|---------|-------------|
| `--model` | `claude-sonnet-4-6` | Model to evaluate |
| `--api-key` | env var | Anthropic or OpenAI API key |
| `--datasets` | all | Restrict to specific datasets |
| `--questions` | all | Restrict to specific question IDs |
| `--data-dir` | `benchmarks/tier1/data` | Path to dataset JSON files |
| `--results-dir` | `benchmarks/tier1/results` | Path for JSON reports |
| `--max-baseline-payload-bytes` | `400000` | Byte cap for baseline payloads |
| `--max-baseline-tokens` | `180000` | Token cap for baseline payloads |
| `--temperature` | `0.0` | Sampling temperature |
| `--max-retries` | `2` | Code-generation retries on failure |
| `--skip-baseline` | — | Run Sift condition only |
| `--skip-sift` | — | Run baseline condition only |
| `--continue-on-error` | — | Don't abort on API errors |
| `--json` | — | Emit full JSON report to stdout |

### Datasets

| Dataset | Source | Focus |
|---------|--------|-------|
| earthquakes | USGS GeoJSON | magnitude, location, depth |
| products | dummyjson | price, rating, brand, stock |
| users | dummyjson | age, email, address, demographics |
| comments | JSONPlaceholder | email, body, postId |
| photos | JSONPlaceholder | albumId, title |
| countries | REST Countries | population, capital, region, area |
| laureates | Nobel Prize API | gender, birth, prizes, categories |
| weather | Open-Meteo | hourly temperature, wind, precipitation |
| github_repos | GitHub API | stars, forks, language, license |
| pokemon | Pokemon JSON | type, base stats, hp, attack, speed |
| openlibrary | OpenLibrary API | edition count, authors, cover |
| airports | Airports JSON | elevation, latitude, country |

Each dataset has 5–15 questions spanning count, aggregation, lookup, filter,
cross-field, cross-root, datetime, string operation, median, percentage,
and multi-condition question types.

### File layout

```
benchmarks/tier1/
├── harness.py        # Orchestration: runs both conditions, generates reports
├── datasets.py       # Dataset definitions (URLs, extraction paths, filenames)
├── questions.py      # 103 questions with gold-answer functions and tolerances
├── sift_runtime.py   # Wrapper around Sift artifact capture/describe/execute
├── llm_client.py     # Anthropic + OpenAI API client (zero third-party deps)
├── evaluate.py       # Answer matching (number/string/list) and reporting
├── fetch_data.py     # One-time dataset downloader
├── data/             # Downloaded JSON datasets (gitignored)
└── results/          # Timestamped JSON benchmark reports
```

### Latest results (2026-02-22)

| Model | Baseline | Sift | Token reduction |
|-------|----------|------|-----------------|
| claude-sonnet-4-6 | 31/103 (30.1%) | 99/103 (96.1%) | 95.3% |
| claude-opus-4-6 | 34/103 (33.0%) | 98/103 (95.1%) | 95.3% |

**Per-dataset accuracy (Sift condition):**

| Dataset | Sonnet 4.6 | Opus 4.6 |
|---------|-----------|---------|
| airports | 6/6 | 6/6 |
| comments | 5/5 | 5/5 |
| countries | 13/13 | 13/13 |
| earthquakes | 13/13 | 13/13 |
| github_repos | 6/6 | 6/6 |
| laureates | 10/11 | 11/11 |
| openlibrary | 6/6 | 6/6 |
| photos | 5/5 | 5/5 |
| pokemon | 6/6 | 6/6 |
| products | 15/15 | 15/15 |
| users | 7/7 | 7/7 |
| weather | 7/10 | 5/10 |

**Latency (Sift condition):**

| Model | p50 | p90 | mean |
|-------|-----|-----|------|
| claude-sonnet-4-6 | 2,846 ms | 4,687 ms | 3,321 ms |
| claude-opus-4-6 | 4,574 ms | 6,653 ms | 5,424 ms |

### Report format

Reports are saved as `tier1_<model>_<timestamp>.json` in the results
directory. Each report includes:

- Overall accuracy for baseline and Sift conditions
- Input/output token counts and token reduction percentage
- Latency percentiles (p50, p90) and mean for both conditions
- Per-dataset and per-question-type breakdowns
- Per-difficulty breakdowns (easy, medium, hard) with retry counts
- Detailed per-question results with gold answers, LLM answers, and
  correctness flags
