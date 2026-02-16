# Recipes & Examples

Common usage patterns and real-world examples for working with Sift artifacts.

## Basic Patterns

### Pattern 1: Large Mirrored Result → Retrieve Deterministically

When an upstream tool returns a large response, Sift automatically stores it as an artifact and returns a handle. Use this pattern to retrieve the data with pagination.

**Steps:**

1. Call a mirrored tool (e.g., `github.search_repositories`)
2. If response includes `artifact_id`, use inline `schemas` data to choose `root_path`
   - Typically `schemas[0].root_path` when a unique primary schema exists
3. Call `artifact(action="query", query_kind="select")` with pagination
4. Continue paging until `pagination.retrieval_status == "COMPLETE"`

**Example:**

```python
# Step 1: Call upstream tool
response = github.search_repositories(query="MCP server", limit=100)

# Step 2: Response is an artifact handle
{
  "artifact_id": "art_7f3a...",
  "schemas": [
    {"root_path": "$.items", "coverage": {"observed_records": 100}}
  ],
  "usage_hint": "Use artifact(action='query', query_kind='select', ...) to retrieve data"
}

# Step 3: Retrieve data with pagination
page1 = artifact(
    action="query",
    query_kind="select",
    artifact_id="art_7f3a...",
    root_path="$.items",
    limit=50
)

# Step 4: Continue paging if needed
if page1["pagination"]["has_more"]:
    page2 = artifact(
        action="query",
        query_kind="select",
        artifact_id="art_7f3a...",
        root_path="$.items",
        cursor=page1["pagination"]["cursor"],
        limit=50
    )
```

### Pattern 2: Upstream Pagination Chain

Some upstream tools return paginated results. Sift captures each page as a separate artifact and links them via lineage.

**Steps:**

1. Call mirrored tool and inspect `pagination.layer == "upstream"`
2. If `pagination.has_next_page` is true, call `artifact(action="next_page")`
3. Repeat until `pagination.retrieval_status == "COMPLETE"`

**Example:**

```python
# Step 1: First page
page1 = github.list_issues(repo="owner/repo", per_page=100)

# Check if more pages exist
if page1["pagination"]["has_next_page"]:
    # Step 2: Fetch next page
    page2 = artifact(
        action="next_page",
        artifact_id=page1["artifact_id"]
    )

    # Step 3: Continue until complete
    while page2["pagination"]["has_next_page"]:
        page2 = artifact(
            action="next_page",
            artifact_id=page2["artifact_id"]
        )
```

### Pattern 3: Tool Chaining with Artifact References

Pass an `artifact_id` (or `artifact_id:$.jsonpath`) directly as an argument to another mirrored tool. Sift resolves the reference server-side before forwarding — **the LLM never loads the intermediate data.**

**Syntax:**

```python
# Bare reference — resolves to the full JSON payload
tool_b(input="art_7f3a...")

# Query reference — resolves to a specific field
tool_b(input="art_7f3a...:$.items[0].name")

# Wildcard — resolves to a list of values
tool_b(emails="art_7f3a...:$.users[*].email")
```

**Important:** Only top-level string arguments are inspected. Nested values inside dicts or lists are never resolved.

**Example:**

```python
# Get user list from database
users_artifact = database.query("SELECT * FROM users")
# Returns: {"artifact_id": "art_abc123..."}

# Send email to all users (email addresses resolved server-side)
email.send_bulk(
    recipients="art_abc123...:$.users[*].email",
    subject="Newsletter",
    body="..."
)
```

## Python Code Queries

`query_kind="code"` executes model-generated Python against lineage-merged root records in a deterministic subprocess.

### Basic Code Query

**Required arguments:**

- `artifact_id` — The artifact to query
- `root_path` — JSONPath to the data root
- `code` — Python code defining `run(data, schema, params)`

**Optional arguments:**

- `params` — JSON object passed to `run()`
- `limit` — Maximum number of records to process
- `cursor` — Pagination cursor for large datasets

**Example:**

```json
{
  "action": "query",
  "query_kind": "code",
  "artifact_id": "art_123...",
  "root_path": "$.result.data",
  "code": "def run(data, schema, params):\n    floor = float(params.get('min_spend', 0))\n    out = []\n    for row in data:\n        spend = float(row.get('spend', 0) or 0)\n        if spend >= floor:\n            out.append({'ad_id': row.get('ad_id'), 'spend': spend})\n    return out",
  "params": {"min_spend": 10}
}
```

**Response format:**

The response is select-like and includes:

- `items` — The result of your `run()` function
- `truncated` — Whether results were truncated
- `pagination` — Pagination metadata
- `total_matched` — Total records processed
- `scope` — Lineage scope (`all_related`)
- `determinism` — Hash values (`code_hash`, `params_hash`, `schema_hash`)

### Constraints and Security

- **Scope:** `scope=all_related` only (operates on root-scoped data + lineage)
- **Imports:** Allowlisted modules only
  - `math`, `statistics`, `decimal`, `datetime`, `re`
  - `itertools`, `collections`, `functools`, `operator`, `heapq`
  - `json`, `jmespath`
  - `pandas`, `numpy` (when installed and included in the active import-root allowlist)
- **Customization:** Set `code_query_allowed_import_roots` or `SIFT_MCP_CODE_QUERY_ALLOWED_IMPORT_ROOTS` to define the exact import-root allowlist
- **Disable:** Set `code_query_enabled=false` to disable code queries entirely

**Security note:** `query_kind=code` runs model-authored Python. It is guardrailed (AST/import policy, timeout, memory/input budgets) but is **not a full OS-level sandbox**.

### Common Code Query Use Cases

#### 1. Filtering Records

```python
def run(data, schema, params):
    min_age = params.get('min_age', 18)
    return [
        user for user in data
        if user.get('age', 0) >= min_age
    ]
```

#### 2. Aggregations with Pandas

```python
def run(data, schema, params):
    import pandas as pd
    df = pd.DataFrame(data)
    return df.groupby('category')['amount'].sum().to_dict()
```

#### 3. Complex JMESPath Queries

```python
def run(data, schema, params):
    import jmespath
    return jmespath.search('[?price > `100`].{name: name, price: price}', data)
```

#### 4. Statistical Analysis

```python
def run(data, schema, params):
    import statistics
    values = [row['value'] for row in data if 'value' in row]
    return {
        'mean': statistics.mean(values),
        'median': statistics.median(values),
        'stdev': statistics.stdev(values)
    }
```

## Real-World Workflows

### Workflow 1: Analyzing Large API Responses

**Scenario:** You've called a marketing API that returns thousands of ad campaigns. You want to find campaigns with specific performance characteristics.

```python
# Step 1: Call upstream tool
campaigns = meta_ads.get_campaigns(account_id="123")
# Returns artifact handle

# Step 2: Run code query to filter
high_performers = artifact(
    action="query",
    query_kind="code",
    artifact_id=campaigns["artifact_id"],
    root_path="$.data",
    code="""
def run(data, schema, params):
    threshold = params['roas_threshold']
    return [
        c for c in data
        if c.get('roas', 0) > threshold
    ]
""",
    params={"roas_threshold": 2.5}
)

# Step 3: Extract specific fields for further processing
campaign_ids = artifact(
    action="query",
    query_kind="select",
    artifact_id=campaigns["artifact_id"],
    root_path="$.data[*].id"
)
```

### Workflow 2: Multi-Step Data Pipeline

**Scenario:** Extract data, transform it, and pass it to downstream tools.

```python
# Step 1: Extract data from database
raw_data = database.query("SELECT * FROM transactions WHERE date > '2024-01-01'")

# Step 2: Transform with code query (summarize by category)
summary = artifact(
    action="query",
    query_kind="code",
    artifact_id=raw_data["artifact_id"],
    root_path="$.results",
    code="""
def run(data, schema, params):
    import pandas as pd
    df = pd.DataFrame(data)
    return df.groupby('category').agg({
        'amount': ['sum', 'count', 'mean']
    }).to_dict()
"""
)

# Step 3: Pass summary to reporting tool (using artifact reference)
report.generate(
    data_source=f"{summary['artifact_id']}:$.items[0]",
    template="financial_summary"
)
```

### Workflow 3: Iterative Analysis

**Scenario:** Run multiple queries on the same dataset without reloading it.

```python
# Step 1: Load large dataset once
dataset = data_warehouse.export(table="sales", year=2024)

# Step 2: Multiple analyses without re-fetching
# Analysis 1: Top products
top_products = artifact(
    action="query",
    query_kind="code",
    artifact_id=dataset["artifact_id"],
    root_path="$.records",
    code="def run(data, schema, params): ..."
)

# Analysis 2: Regional breakdown
by_region = artifact(
    action="query",
    query_kind="code",
    artifact_id=dataset["artifact_id"],  # Same artifact!
    root_path="$.records",
    code="def run(data, schema, params): ..."
)

# Analysis 3: Time series trends
trends = artifact(
    action="query",
    query_kind="code",
    artifact_id=dataset["artifact_id"],  # Same artifact again!
    root_path="$.records",
    code="def run(data, schema, params): ..."
)
```

## Advanced Techniques

### Using Cursors for Pagination

When processing large datasets with code queries, use pagination to avoid memory limits:

```python
def process_in_chunks(artifact_id, root_path, code, chunk_size=1000):
    results = []
    cursor = None

    while True:
        response = artifact(
            action="query",
            query_kind="code",
            artifact_id=artifact_id,
            root_path=root_path,
            code=code,
            limit=chunk_size,
            cursor=cursor
        )

        results.extend(response["items"])

        if response["pagination"]["retrieval_status"] == "COMPLETE":
            break

        cursor = response["pagination"]["cursor"]

    return results
```

### Determinism Metadata for Reproducibility

Code-query responses include `determinism` metadata so repeated runs can be compared and audited:

```json
{
  "determinism": {
    "code_hash": "sha256:abc123...",
    "params_hash": "sha256:def456...",
    "schema_hash": "sha256:ghi789..."
  }
}
```

Use these hashes to verify two runs used the same code, params, and schema inputs.

### Handling Missing Fields

Always use `.get()` with defaults when accessing fields:

```python
def run(data, schema, params):
    # BAD: Raises KeyError if 'optional_field' missing
    # value = row['optional_field']

    # GOOD: Returns None if missing
    value = row.get('optional_field')

    # BETTER: Returns default value if missing
    value = row.get('optional_field', 0)
```

## Next Steps

- **[API Contracts](api_contracts.md)** — Understand pagination and response formats
- **[Configuration Reference](config.md)** — Customize passthrough thresholds and code query settings
- **[Architecture & Spec](spec_v1_9.md)** — Deep dive into Sift's design
- **[Deployment Guide](deployment.md)** — Run Sift in production
