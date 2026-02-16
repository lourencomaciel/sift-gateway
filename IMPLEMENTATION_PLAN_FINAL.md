# Sift MCP - Code Query Improvements - Final Implementation Plan

**Date:** 2026-02-15
**Status:** Approved and ready for implementation
**Based on:** User feedback from Meta Ads analysis (CHF 2.3M spend audit)

---

## Executive Summary

**Core finding:** Code queries validated their value proposition but revealed friction points. All issues are non-blocking but address real usability gaps.

**Approved changes:**
1. **Remove code query pagination entirely** (not just raise default)
2. **Add error tracebacks** with line numbers for debugging
3. **Remove scope parameter** (not just make optional)
4. **Add usage hints** for return value auto-wrapping
5. **Multi-artifact queries** with positional API (Phase 2)
6. **Sample-based schema enums** with 10-value cap (Phase 2)

---

## Phase 1: Quick Wins (1-2 weeks)

### 1. Remove Code Query Pagination ✅ **APPROVED**

**Decision:** Remove pagination entirely (not just raise default to 200)

**Rationale:**
- Code executes server-side, produces all results
- Claude needs complete dataset for analytics
- Pagination forces multi-round trips without benefit
- User controls output size via their `run()` function

**Implementation:**

```python
# In src/sift_mcp/mcp/handlers/artifact_code.py
# REMOVE pagination logic (lines ~626-647):

# OLD CODE (delete this):
# max_items = ctx._bounded_limit(arguments.get("limit"))
# selected, truncated, omitted, used_bytes = apply_output_budgets(
#     normalized_items[offset:],
#     max_items=max_items,
#     max_bytes_out=ctx.config.max_bytes_out,
# )
# next_cursor = ctx._issue_cursor(...) if truncated else None

# NEW CODE:
# Enforce only byte budget, return ALL items
total_bytes = sum(len(encode_json_bytes(item)) for item in normalized_items)
if total_bytes > ctx.config.max_bytes_out:
    return gateway_error(
        "RESPONSE_TOO_LARGE",
        f"Code query results ({total_bytes} bytes) exceed "
        f"max_bytes_out ({ctx.config.max_bytes_out} bytes). "
        f"Aggregate data in your run() function to reduce output size."
    )

response = build_select_result(
    items=normalized_items,  # ALL items, not sliced
    truncated=False,         # Never truncated by row count
    cursor=None,             # No cursor
    total_matched=len(normalized_items),
    sampled_only=bool(sampled_artifacts),
    omitted=None,            # No row-based omission
    stats={
        "bytes_out": total_bytes,
        "input_records": input_count,
        "input_bytes": input_bytes,
        "output_records": len(normalized_items),
    },
    # ... rest unchanged
)
```

**Files to modify:**
- `src/sift_mcp/mcp/handlers/artifact_code.py` - Remove pagination, enforce byte budget only
- `docs/spec_v1_9.md` - Document unpaginated behavior, byte budget enforcement
- `tests/unit/test_artifact_code_handler.py` - Update tests (no cursor, no truncated flag)
- `src/sift_mcp/tools/usage_hint.py` - Add hint about aggregating large results

**Testing:**
- [ ] Remove tests for code query pagination/cursors
- [ ] Add test: results under max_bytes_out → all returned
- [ ] Add test: results over max_bytes_out → error with guidance
- [ ] Integration: verify large code query results work correctly

**Usage hint to add:**
```
Code queries return ALL results (no pagination). Large result sets are
limited only by max_bytes_out transport budget. For large datasets,
aggregate within your run() function to reduce output size.
```

**Backwards compatibility:** **BREAKING** - Code that expects cursor/pagination will change behavior. Document as improvement in CHANGELOG.

---

### 2. Error Tracebacks ✅ **APPROVED** (unchanged from original plan)

**Objective:** Capture and return Python traceback for runtime exceptions

**Files to modify:**
- `src/sift_mcp/codegen/worker_main.py` - Capture traceback
- `src/sift_mcp/codegen/runtime.py` - Pass traceback to parent
- `src/sift_mcp/mcp/handlers/artifact_code.py` - Include in error response
- `docs/spec_v1_9.md` - Document error.traceback field
- `tests/unit/test_codegen_runtime.py` - Test traceback capture

**Implementation:**
```python
# src/sift_mcp/codegen/worker_main.py
import traceback

def _worker_error(
    code: str,
    message: str,
    tb: str | None = None,  # NEW
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ok": False,
        "error": {
            "code": code,
            "message": message,
        },
    }
    if tb:
        payload["error"]["traceback"] = tb[:2000]  # Truncate to 2KB
    return payload

# In _execute() exception handlers:
except Exception as exc:
    tb = traceback.format_exc()
    return _worker_error("CODE_RUNTIME_EXCEPTION", str(exc), tb=tb)
```

```python
# src/sift_mcp/codegen/runtime.py - Update CodeRuntimeError class:
class CodeRuntimeError(RuntimeError):
    code: str
    message: str
    traceback: str | None = None  # NEW

    def __init__(self, *, code: str, message: str, traceback: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.traceback = traceback
```

```python
# src/sift_mcp/codegen/runtime.py - In execute_code_in_subprocess():
if isinstance(parsed, dict) and parsed.get("ok") is False:
    err = parsed.get("error")
    if isinstance(err, dict):
        code_val = err.get("code")
        msg_val = err.get("message")
        tb_val = err.get("traceback")  # NEW
        if isinstance(code_val, str) and isinstance(msg_val, str):
            if code_val == "CODE_RUNTIME_MEMORY_LIMIT":
                raise CodeRuntimeMemoryLimit(
                    code=code_val,
                    message=msg_val,
                    traceback=tb_val if isinstance(tb_val, str) else None
                )
            raise CodeRuntimeError(
                code=code_val,
                message=msg_val,
                traceback=tb_val if isinstance(tb_val, str) else None
            )
```

```python
# src/sift_mcp/mcp/handlers/artifact_code.py
except CodeRuntimeError as exc:
    ctx._increment_metric("codegen_failure")
    # ... existing logging ...
    error_details = {"code": exc.code}
    if exc.traceback:  # NEW
        error_details["traceback"] = exc.traceback
    return _code_error(str(exc), details_code=exc.code, details=error_details)
```

**Testing:**
- [ ] Add unit test: KeyError includes traceback with line number
- [ ] Add unit test: Traceback truncated at 2000 chars
- [ ] Add unit test: Validation errors have no traceback
- [ ] Add integration test: End-to-end with runtime error

---

### 3. Remove Scope Parameter ✅ **APPROVED**

**Decision:** Remove from schema entirely (not just make optional)

**Implementation:**

```python
# Remove scope from tool schema definition entirely
# (Find and update wherever artifact tool schema is defined)

# In src/sift_mcp/mcp/handlers/artifact_code.py
# REMOVE validation for scope parameter (lines ~104-111)

# OLD CODE (delete):
# raw_scope = arguments.get("scope")
# if raw_scope is not None and raw_scope != "all_related":
#     return _code_error(...)

# No new code needed - just always use all_related behavior internally
```

**Files to modify:**
- Tool schema definition (remove scope field)
- `src/sift_mcp/mcp/handlers/artifact_code.py` - Remove validation
- `docs/spec_v1_9.md` - Remove scope from documentation
- `tests/unit/test_artifact_code_handler.py` - Remove scope tests

**Testing:**
- [ ] Remove all scope-related tests
- [ ] Verify code queries work without scope parameter

**Backwards compatibility:** FULL - Old clients that send `scope` will have it silently ignored (just don't validate it)

---

### 4. Return Value Auto-Wrapping - Usage Hint ✅ **APPROVED**

**Decision:** Add usage hint (not just spec documentation)

**Implementation:**

Add to `src/sift_mcp/tools/usage_hint.py` for artifact code query tool:

```python
CODE_QUERY_USAGE_HINT = """
Code queries (query_kind="code"): Write a run(data, schema, params)
function that processes artifact data and returns results.

- Return any JSON-serializable value (scalar, dict, list, etc.)
- Scalar/dict returns are auto-wrapped in a list for consistency
- Both `return 42` and `return [42]` produce identical output
- No pagination: all results returned (limited only by max_bytes_out)
- For large datasets, aggregate within your run() to reduce output size

Error tracebacks include line numbers for debugging runtime failures.
"""
```

**Files to modify:**
- `src/sift_mcp/tools/usage_hint.py` - Add comprehensive code query hint
- `docs/spec_v1_9.md` - Also document for human readers

**Testing:**
- [ ] Verify usage hint appears in tool description
- [ ] Manual test: Claude sees hint when using code queries

---

## Phase 2: High-Value Features (4-8 weeks)

### 5. Multi-Artifact Queries ✅ **APPROVED - Positional API**

**Decision:** Use positional API (`artifact_ids: [str]`)

**API Design:**

```python
# Tool call:
artifact(
    action="query",
    query_kind="code",
    artifact_ids=["art_123", "art_456", "art_789"],  # List of IDs
    root_path="$",
    code="def run(artifacts, schemas, params): ...",
    params={}
)

# Worker receives:
def run(artifacts, schemas, params):
    # artifacts is dict[artifact_id, list[records]]
    campaigns = artifacts["art_123"]
    placements = artifacts["art_456"]
    ages = artifacts["art_789"]

    # User can comment for clarity:
    # campaigns = artifacts["art_123"]  # Campaign-level data

    # Perform joins, aggregations, etc.
    return merged_results
```

**Rationale for positional:**
- ✅ Simpler (fewer places to make mistakes)
- ✅ IDs are canonical (no naming conflicts)
- ✅ Fewer round-trips (list vs dict construction)
- User can add comments for semantic clarity

**Implementation areas:**

1. **Handler signature changes:**
```python
# In artifact_code.py _validate_code_args():
artifact_ids = arguments.get("artifact_ids")
if artifact_ids:
    if not isinstance(artifact_ids, list):
        return gateway_error("INVALID_ARGUMENT", "artifact_ids must be a list")
    if not artifact_ids:
        return gateway_error("INVALID_ARGUMENT", "artifact_ids cannot be empty")
    for aid in artifact_ids:
        if not isinstance(aid, str):
            return gateway_error("INVALID_ARGUMENT", "artifact_ids items must be strings")
else:
    # Single artifact mode (backwards compat)
    artifact_id = arguments.get("artifact_id")
    if not artifact_id:
        return gateway_error("INVALID_ARGUMENT", "missing artifact_id or artifact_ids")
    artifact_ids = [artifact_id]
```

2. **Data loading:**
```python
# Load and validate visibility for all artifacts
artifacts_data: dict[str, list[Any]] = {}
for artifact_id in artifact_ids:
    # Existing visibility check
    if not ctx._artifact_visible(connection, artifact_id, session_id):
        return gateway_error("NOT_FOUND", f"artifact not found: {artifact_id}")

    # Load data for this artifact (reuse existing traversal logic)
    # ... existing code to load samples/data ...
    artifacts_data[artifact_id] = records
```

3. **Worker contract update:**
```python
# In execute_code_in_subprocess():
payload = encode_json_bytes({
    "code": code,
    "artifacts": artifacts_data,  # dict[str, list] instead of single "data"
    "schemas": schemas_dict,      # dict[str, schema] keyed by artifact_id
    "params": params,
    "allowed_import_roots": ...
})

# Worker signature becomes:
def run(artifacts, schemas, params):
    # artifacts: dict[artifact_id, list[records]]
    # schemas: dict[artifact_id, schema_object]
    # params: user-provided params dict
    ...
```

4. **Cursor encoding:**
```python
# Cursor must track all artifact IDs + their lineage hashes
cursor_extra = {
    "artifact_ids": artifact_ids,
    "related_set_hashes": {aid: hash for aid, hash in ...},
    "root_path": root_path,
    "code_hash": code_hash,
    "params_hash": params_hash,
}
```

5. **Security:**
```python
# Enforce visibility for ALL artifacts before loading any data
for artifact_id in artifact_ids:
    if not ctx._artifact_visible(connection, artifact_id, session_id):
        return gateway_error("NOT_FOUND", f"artifact {artifact_id} not found")
```

**Files to modify:**
- `src/sift_mcp/mcp/handlers/artifact_code.py` - Multi-artifact loading, validation
- `src/sift_mcp/codegen/runtime.py` - Update worker contract (artifacts dict)
- `src/sift_mcp/codegen/worker_main.py` - Accept artifacts dict instead of data
- `docs/spec_v1_9.md` - Add §16.5 "Multi-Artifact Code Queries"
- `tests/unit/test_artifact_code_handler.py` - Multi-artifact test cases
- Tool schema - Add `artifact_ids` parameter (list of strings)

**Testing:**
- [ ] Unit: artifact_ids validation (must be list, non-empty, strings)
- [ ] Unit: Single artifact backwards compat (artifact_id still works)
- [ ] Unit: Visibility enforced for all artifacts
- [ ] Unit: Cursor encodes all artifact IDs
- [ ] Integration: Load 2+ artifacts, join in code query
- [ ] Integration: Security - reject if any artifact not visible

**Backwards compatibility:**
- Keep `artifact_id` (singular) for single-artifact queries
- New `artifact_ids` (plural) for multi-artifact queries
- Mutually exclusive (provide one or the other, not both)

---

### 6. Schema Enum Discovery ✅ **APPROVED - Sample-Based, 10-Value Cap**

**Decision:** Compute distinct values from 50 sampled items only, cap at 10 values

**Context bloat mitigation:** "Do not repeat repeated values" - sets automatically deduplicate

**Implementation:**

```python
# In src/sift_mcp/mapping/schema.py

@dataclass
class _PathStats:
    types: set[str]
    observed_count: int
    example_value: str | None
    distinct_values: set[Any] = field(default_factory=set)  # NEW

def _walk_value(
    value: Any,
    *,
    path: str,
    stats: dict[str, _PathStats],
    seen_paths: set[str],
) -> None:
    """Collect types for a value and recurse through nested structures."""
    existing = stats.get(path)
    if existing is None:
        existing = _PathStats(
            types=set(),
            observed_count=0,
            example_value=_format_example_value(value),
            distinct_values=set(),  # NEW
        )
        stats[path] = existing
    elif existing.example_value is None:
        existing.example_value = _format_example_value(value)

    existing.types.add(_json_type_name(value))
    seen_paths.add(path)

    # NEW: Track distinct values for leaf nodes (non-dict, non-list)
    if not isinstance(value, (dict, list)):
        # Cap at 10 to prevent context bloat
        if len(existing.distinct_values) < 10:
            # Hashable types only (skip unhashable like nested dicts)
            try:
                existing.distinct_values.add(value)
            except TypeError:
                pass  # Skip unhashable types

    # ... rest of function unchanged (recurse into dict/list)

# In _build_fields():
def _build_fields(records: Sequence[Any]) -> tuple[list[SchemaFieldInventory], int]:
    # ... existing code to walk records ...

    fields: list[SchemaFieldInventory] = []
    for path in sorted(path for path in stats if path != "$"):
        path_stats = stats[path]
        types = sorted(path_stats.types, key=_type_sort_key)

        # NEW: Format distinct values for output
        distinct_list = None
        if path_stats.distinct_values:
            # Sort for determinism (strings sort alphabetically, numbers numerically)
            try:
                distinct_sorted = sorted(list(path_stats.distinct_values))
            except TypeError:
                # Mixed types can't sort - just convert to list
                distinct_sorted = list(path_stats.distinct_values)
            distinct_list = distinct_sorted[:10]  # Cap at 10

        fields.append(
            SchemaFieldInventory(
                path=path,
                types=types,
                nullable="null" in path_stats.types,
                required=(
                    observed_records > 0
                    and path_stats.observed_count == observed_records
                ),
                observed_count=path_stats.observed_count,
                example_value=path_stats.example_value,
                distinct_values=distinct_list,  # NEW
                cardinality=len(path_stats.distinct_values) if distinct_list else None,  # NEW
            )
        )
    return fields, observed_records
```

```python
# Update SchemaFieldInventory dataclass:
@dataclass(frozen=True)
class SchemaFieldInventory:
    path: str
    types: list[str]
    nullable: bool
    required: bool
    observed_count: int
    example_value: str | None = None
    distinct_values: list[Any] | None = None  # NEW
    cardinality: int | None = None             # NEW
```

**Schema output example:**
```json
{
  "path": "$[*].action_values[*].action_type",
  "types": ["string"],
  "nullable": false,
  "required": true,
  "observed_count": 50,
  "example_value": "omni_purchase",
  "distinct_values": [
    "offsite_conversion.fb_pixel_purchase",
    "omni_purchase",
    "onsite_web_app_purchase",
    "web_in_store_purchase"
  ],
  "cardinality": 4,
  "cardinality_note": "Based on sample of 50 records"
}
```

**Files to modify:**
- `src/sift_mcp/mapping/schema.py` - Add distinct_values tracking
- `docs/spec_v1_9.md` - Document distinct_values and cardinality fields
- `tests/unit/test_mapping_schema.py` - Test distinct value collection

**Testing:**
- [ ] Unit: Distinct values collected (max 10)
- [ ] Unit: Cardinality matches set size
- [ ] Unit: Unhashable types skipped gracefully
- [ ] Unit: Sorting works for homogeneous types
- [ ] Integration: Meta action_type use case (8 values)

**Performance:** Zero additional cost (piggybacks on existing 50-item sampling)

**Context impact:** Max 10 values per field = bounded context

---

## Phase 3: Deferred

- **Intermediate state** (stateful queries) - Deferred to future phase

---

## Testing Strategy

### Phase 1 Tests

**Code query pagination removal:**
- Remove: All pagination/cursor tests for code queries
- Add: Byte budget enforcement tests
- Add: Large result set tests

**Error tracebacks:**
- Add: Traceback capture for common errors (KeyError, AttributeError, TypeError)
- Add: Truncation at 2000 chars
- Add: Validation errors have no traceback

**Scope parameter removal:**
- Remove: All scope parameter tests
- Verify: Code queries work without scope

**Usage hints:**
- Manual: Verify hint appears in tool description

### Phase 2 Tests

**Multi-artifact queries:**
- Single artifact backwards compat
- Multiple artifacts load correctly
- Visibility enforced for all
- Cross-artifact joins work
- Cursor encoding/decoding

**Schema enums:**
- Distinct values collected (max 10)
- Cardinality correct
- Unhashable types handled

---

## Documentation Updates

### spec_v1_9.md

1. **§5.2.4.5 Pagination (UPDATE):**
   - Remove: Code query pagination section
   - Add: "Code queries return all results (no pagination). Limited only by max_bytes_out transport budget."

2. **§16.X Error Tracebacks (NEW):**
   - Document error.traceback field (optional, max 2000 chars)
   - Show example error response with traceback

3. **§16.X Return Value Normalization (NEW):**
   - Document auto-wrapping behavior
   - Show scalar vs list examples

4. **§16.5 Multi-Artifact Code Queries (NEW - Phase 2):**
   - Document artifact_ids parameter
   - Show worker signature: run(artifacts, schemas, params)
   - Security: all artifacts must be visible
   - Example: cross-artifact join

5. **§16.X Schema Distinct Values (NEW - Phase 2):**
   - Document distinct_values and cardinality fields
   - Note: sample-based, not exhaustive
   - Max 10 values per field

### config.md

1. Remove `default_page_limit` section (no longer needed)

### CHANGELOG.md

```markdown
## [Unreleased]

### Changed
- **BREAKING:** Code queries now return all results without pagination.
  Limited only by max_bytes_out transport budget. Aggregate large
  result sets within your run() function.
- Removed `scope` parameter from code queries (always uses all_related
  behavior)

### Added
- Error tracebacks now included in code query runtime failures with
  line numbers for debugging
- Usage hints added to code query tool descriptions
- Multi-artifact code queries: query multiple artifacts in single
  execution (artifact_ids parameter)
- Schema distinct_values: Sample-based enumeration of field values
  (max 10 per field, based on 50-item sample)

### Fixed
- Return value normalization now documented (scalars auto-wrap to lists)
```

---

## Backwards Compatibility

### Breaking Changes
1. **Code query pagination removed** - No cursor, no truncated flag, all results returned
   - Mitigation: Unlikely to affect anyone (pagination was friction, not feature)
   - Old code expecting cursor will just not receive it

### Non-Breaking Changes
1. **Error tracebacks** - Adds optional field, no schema change
2. **Scope removal** - Old clients sending scope will have it silently ignored
3. **Multi-artifact queries** - New parameter, keeps artifact_id for single mode
4. **Schema enums** - Adds optional fields to schema response

---

## Implementation Order

1. **Phase 1 (weeks 1-2):**
   - Day 1-2: Remove code query pagination
   - Day 3-4: Add error tracebacks
   - Day 5: Remove scope parameter
   - Day 6: Add usage hints
   - Day 7-10: Testing, docs, review

2. **Phase 2 (weeks 3-8):**
   - Week 3-5: Multi-artifact queries (complex, needs thorough testing)
   - Week 6-7: Schema enum discovery (simpler, piggybacks on sampling)
   - Week 8: Integration testing, docs

---

## Success Metrics

**Phase 1:**
- ✅ Code queries return complete results without pagination surprise
- ✅ Runtime errors include line numbers and tracebacks
- ✅ Reduced boilerplate (no scope parameter)
- ✅ Clear usage guidance via hints

**Phase 2:**
- ✅ Multi-artifact joins work in single query (unblocks Meta Ads use case)
- ✅ Schema enum values visible (solves action_type discovery problem)
- ✅ Context stays bounded (max 10 values per field)

---

## Notes

- All decisions documented here were approved by Lou on 2026-02-15
- Multi-artifact API choice: **Positional** (artifact_ids list)
- Schema enums: **Sample-based**, **10-value cap**, no repeated values
- Code query pagination: **Removed entirely**, not just raised default
- Scope parameter: **Removed from schema**, not just made optional

**End of Implementation Plan**
