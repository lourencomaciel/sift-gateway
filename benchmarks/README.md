# Benchmarks

Evaluation suite measuring Sift Gateway's accuracy and token efficiency
against traditional context-stuffing approaches.

## Tier 1 — Factual Q&A on Structured Data

Compares two conditions across 12 real-world JSON datasets and 103 questions:

| Condition | How it works |
|-----------|-------------|
| **Baseline** | Full JSON payload stuffed into the LLM prompt (truncated to fit context) |
| **Sift** | Dataset served via MCP, gateway captures artifact + computes schema, LLM generates Python code, gateway executes it |

### Architecture

```
Benchmark Harness (sync)
  └─ FastMCP Client (in-process)
       └─ Gateway FastMCP App (in-process)
            ├─ "bench_get_earthquakes" → mock upstream (subprocess via stdio)
            ├─ "bench_get_products"    → mock upstream (subprocess via stdio)
            ├─ ... (12 mirrored dataset tools)
            └─ "artifact" (action=query, query_kind=code)
```

**Tested through real MCP protocol (stdio subprocess):**
- Gateway ↔ Mock upstream: tool discovery, tool calls, response serialization

**Tested through gateway handlers (in-process):**
- Mirrored tool routing and argument validation
- `_gateway_context` handling and reserved key stripping
- Envelope normalization and artifact persistence
- Schema mapping and response mode selection (`schema_ref` forced)
- Code query execution through `artifact` tool handler
- Secret redaction pipeline

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
| `--sift-data-dir` | temp dir | Sift state directory (DB, blobs) |
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
| github_repos | GitHub API | stars, language, license |
| pokemon | JSON file | stats, types, abilities |
| openlibrary | OpenLibrary API | author, publish_date, editions |
| airports | JSON file | iata, country, city |

Each dataset has 5–12 questions spanning count, aggregation, lookup, filter,
and cross-field question types.

### File layout

```
benchmarks/tier1/
├── harness.py          # Orchestration: runs both conditions, generates reports
├── mock_upstream.py    # FastMCP server serving datasets (launched as subprocess)
├── sift_runtime.py     # MCP client wrapper (gateway + mock upstream integration)
├── datasets.py         # Dataset definitions (URLs, extraction paths, filenames)
├── questions.py        # 103 questions with gold-answer functions and tolerances
├── llm_client.py       # Anthropic + OpenAI API client (zero third-party deps)
├── evaluate.py         # Answer matching (number/string/boolean/list) and reporting
├── fetch_data.py       # One-time dataset downloader
├── code_extract.py     # Extract Python code from LLM responses
├── code_result.py      # Unwrap code-query execution responses
├── schema_prompt.py    # Format schema into LLM-ready prompt text
├── data/               # Downloaded JSON datasets (gitignored)
└── results/            # Timestamped JSON benchmark reports (gitignored)
```

### How it works

**Baseline condition:**
1. Load dataset JSON from disk
2. Truncate to fit model context window (binary search on array prefix)
3. Send full JSON + question to LLM → extract answer
4. Evaluate against gold answer

**Sift condition:**
1. Call mirrored tool (`bench_get_{dataset}`) through the gateway
   - Gateway proxies to mock upstream via MCP stdio transport
   - Mock upstream returns dataset JSON
   - Gateway persists artifact, computes schema, returns `schema_ref`
2. Extract schema and root paths from gateway response
3. Send schema + question to LLM → generates `def run(data, schema, params):`
4. Call `artifact(action=query, query_kind=code)` to execute code against artifact
5. Send code result + question to LLM → extract final answer
6. Evaluate against gold answer

### Results

Reports are saved as `tier1_<model>_<timestamp>.json` in the results
directory. Each report includes:

- Overall accuracy for baseline and Sift conditions
- Input/output token counts and token reduction percentage
- Per-dataset and per-question-type breakdowns
- Detailed per-question results with gold answers, LLM answers, and
  correctness flags
