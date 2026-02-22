# Benchmarks

Evaluation suite measuring Sift Gateway's accuracy and token efficiency
against traditional context-stuffing approaches.

## Tier 1 — Factual Q&A on Structured Data

Compares two conditions across 8 real-world JSON datasets and 44 questions:

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

Each dataset has 5–6 questions spanning count, aggregation, lookup, filter,
and cross-field question types.

### File layout

```
benchmarks/tier1/
├── harness.py        # Orchestration: runs both conditions, generates reports
├── datasets.py       # Dataset definitions (URLs, extraction paths, filenames)
├── questions.py      # 44 questions with gold-answer functions and tolerances
├── sift_runtime.py   # Wrapper around Sift artifact capture/describe/execute
├── llm_client.py     # Anthropic + OpenAI API client (zero third-party deps)
├── evaluate.py       # Answer matching (number/string/list) and reporting
├── fetch_data.py     # One-time dataset downloader
├── data/             # Downloaded JSON datasets (gitignored)
└── results/          # Timestamped JSON benchmark reports
```

### Results

Reports are saved as `tier1_<model>_<timestamp>.json` in the results
directory. Each report includes:

- Overall accuracy for baseline and Sift conditions
- Input/output token counts and token reduction percentage
- Per-dataset and per-question-type breakdowns
- Detailed per-question results with gold answers, LLM answers, and
  correctness flags
