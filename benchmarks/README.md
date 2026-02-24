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

#### Sonnet 4.6 — Sift condition (2025-02-24)

| Metric | Value |
|--------|-------|
| Accuracy | **99/103 (96.1%)** |
| Errors | 3 (weather dataset — columnar cross-root datetime queries) |
| Input tokens | 482,599 |
| Output tokens | 12,895 |
| Latency p50 | 5,661 ms |
| Latency p90 | 11,456 ms |

Per-dataset breakdown:

| Dataset | Accuracy |
|---------|----------|
| airports | 6/6 |
| comments | 5/5 |
| countries | 13/13 |
| earthquakes | 13/13 |
| github_repos | 6/6 |
| laureates | 10/11 |
| openlibrary | 6/6 |
| photos | 5/5 |
| pokemon | 6/6 |
| products | 15/15 |
| users | 7/7 |
| weather | 7/10 |

By difficulty:

| Difficulty | Accuracy | Retries |
|-----------|----------|---------|
| Easy | 38/38 | 0 |
| Medium | 39/40 | 0 |
| Hard | 22/25 | 8 |

## Tier 2 — LLM-Driven Autonomous Agent Loop

Tier 1 uses scripted orchestration — the harness decides when to call tools,
when to generate code, when to retry. Tier 2 makes the LLM the autonomous
decision maker: it receives a question + tool list and decides which tools to
call, when to paginate, when to write code, and how to recover from errors.

This tests whether the gateway's response format (`schema_ref`, pagination
metadata, error messages) is genuinely useful to LLMs — what users actually
experience.

### Architecture

```
Agent Loop (sync)
  └─ LLM (tool-use API)
       ↕ tool_use / tool_result messages
  └─ FastMCP Client (in-process)
       └─ Gateway FastMCP App (in-process)
            ├─ "bench_get_earthquakes" → mock upstream (subprocess via stdio)
            ├─ "bench_get_products"    → mock upstream (subprocess via stdio)
            ├─ ... (12 mirrored dataset tools)
            └─ "artifact" (action=query/next_page/describe)
```

**Key difference from Tier 1:** The LLM sees the raw tool schemas and
responses, and autonomously decides the workflow:
- Which dataset tool to call
- How to interpret the schema in the response
- What Python code to write for `artifact(action=query, query_kind=code)`
- Whether to paginate (`next_page`) when results are partial
- How to recover from code execution errors

### Quick start

```bash
# 1. Download datasets (one-time, shared with Tier 1)
uv run python benchmarks/tier1/fetch_data.py

# 2. Run the benchmark (requires ANTHROPIC_API_KEY)
uv run python benchmarks/tier2/harness.py

# 3. Run a single question
uv run python benchmarks/tier2/harness.py \
  --datasets earthquakes --questions eq_count_total
```

### CLI options

| Flag | Default | Description |
|------|---------|-------------|
| `--model` | `claude-sonnet-4-6` | Model to evaluate |
| `--api-key` | env var | Anthropic API key |
| `--datasets` | all | Restrict to specific datasets |
| `--questions` | all | Restrict to specific question IDs |
| `--data-dir` | `benchmarks/tier1/data` | Path to dataset JSON files |
| `--results-dir` | `benchmarks/tier2/results` | Path for JSON reports |
| `--sift-data-dir` | temp dir | Sift state directory (DB, blobs) |
| `--max-turns` | `15` | Max agent turns per question |
| `--max-pages` | `10` | Max pagination calls per question |
| `--max-input-tokens` | `200000` | Token budget safety valve |
| `--max-baseline-payload-bytes` | `400000` | Byte cap for baseline payloads |
| `--max-baseline-tokens` | `180000` | Token cap for baseline payloads |
| `--temperature` | `0.0` | Sampling temperature |
| `--skip-baseline` | — | Run Sift condition only |
| `--skip-sift` | — | Run baseline condition only |
| `--continue-on-error` | — | Don't abort on API errors |
| `--save-conversations` | — | Include full conversations in results |
| `--json` | — | Emit full JSON report to stdout |

### File layout

```
benchmarks/tier2/
├── harness.py          # CLI entrypoint + orchestration
├── agent_loop.py       # Core loop: LLM ↔ tool execution cycle
├── tool_bridge.py      # MCP tools → LLM tool format, _gateway_context injection
├── llm_tool_client.py  # Tool-use LLM client (urllib only, Anthropic)
├── system_prompt.py    # System prompt text
├── metrics.py          # Per-question + aggregate metric tracking & reporting
└── results/            # Timestamped JSON reports (gitignored)
```

### Reused from Tier 1

| Module | What |
|--------|------|
| `sift_runtime.py` | `create_runtime`, `_MCPRuntime` (+ new `list_tools()`) |
| `mock_upstream.py` | Entire module (unchanged) |
| `datasets.py` | `DATASETS`, `ALL_DATASET_NAMES` |
| `questions.py` | `Question`, `get_questions_for_dataset`, `question_set_hash` |
| `evaluate.py` | `evaluate_answer` |
| `llm_client.py` | `_detect_provider`, `_resolve_api_key`, `LLMAPIError` |

### How it works

1. Boot gateway via `create_runtime()` (same as Tier 1)
2. Discover tools via `runtime.list_tools()`
3. Convert to LLM tool definitions (stripping `_gateway_context`)
4. For each question:
   - Send question to LLM with tool definitions
   - LLM autonomously calls tools, writes code, paginates, retries
   - Loop until LLM produces a text-only response (final answer)
   - Evaluate against gold answer
5. Build aggregate report with metrics

### Results

Reports are saved as `tier2_<model>_<timestamp>.json` in the results
directory. Each report includes:

- Overall accuracy and per-dataset/type/difficulty breakdowns
- Average turns and tool calls per question
- Code retry rate and pagination usage
- Token totals and latency percentiles
- Per-question detailed metrics including tool call sequences

#### Sonnet 4.6 — Sift condition (2025-02-24)

| Metric | Value |
|--------|-------|
| Accuracy | **102/103 (99.0%)** |
| Errors | 1 (countries — aggregation, medium) |
| Input tokens | 2,886,381 |
| Output tokens | 35,260 |
| Avg turns | 3.16 |
| Avg tool calls | 2.16 |
| Code retry rate | 14.2% |
| Latency p50 | 6,288 ms |
| Latency p90 | 9,815 ms |

Per-dataset breakdown:

| Dataset | Accuracy |
|---------|----------|
| airports | 6/6 |
| comments | 5/5 |
| countries | 12/13 |
| earthquakes | 13/13 |
| github_repos | 6/6 |
| laureates | 11/11 |
| openlibrary | 6/6 |
| photos | 5/5 |
| pokemon | 6/6 |
| products | 15/15 |
| users | 7/7 |
| weather | 10/10 |

By difficulty:

| Difficulty | Accuracy |
|-----------|----------|
| Easy | 38/38 |
| Medium | 39/40 |
| Hard | 25/25 |
