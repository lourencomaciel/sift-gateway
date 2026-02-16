# Revised Assessment - Addressing Lou's Questions

## Phase 1 Revisions

### 1. Pagination on Code Query Results - RECONSIDERED ❌

**Original proposal:** Raise default from 50 to 200

**Lou's question:** Why paginate at all? The model needs to parse everything anyway.

**Analysis:**
Code execution flow:
1. Query runs server-side, produces ALL results
2. Results paginated (50 items default)
3. Claude receives page 1 + cursor
4. Claude must parse to understand data
5. For analytics, Claude needs ALL data (not just page 1)

**The paradox:** Pagination forces multi-round trips while Claude needs complete dataset for analysis.

**When pagination helps:**
- Results exceed transport budget (`max_bytes_out`)
- User genuinely wants paginated view ("show first 10")

**When pagination hurts:**
- Analytical queries (compute median, detect outliers)
- Code queries where aggregation should happen server-side

**Revised recommendation:**
- ❌ Don't just raise default to 200
- ✅ **Make pagination purely transport-driven** (not arbitrary row count)
- ✅ Use `max_bytes_out` budget, not "50 rows" arbitrary limit
- ✅ Add to usage hints: "For analytics, aggregate in your code query to avoid pagination"

**Alternative:** Remove pagination entirely for code queries since:
- User can control output size in their `run()` function
- Arbitrary row limits don't align with analytical use cases
- Transport budgets already enforced via `max_bytes_out`

**Question for Lou:** Should code query results be unpaginated (budget-only), or keep pagination but make it more intelligent?

---

### 2. Error Tracebacks - APPROVED ✅

No changes. This is high-value, low-risk.

---

### 3. Scope Parameter - REMOVE ENTIRELY ✅

**Original proposal:** Make optional

**Lou's question:** If it only accepts one value, why require it?

**Revised approach:**
- ❌ Don't make "optional"
- ✅ **Remove from schema entirely**
- ✅ Internally always use "all_related" behavior
- ✅ If old clients send `scope`, silently ignore (backwards compat)

**Implementation:**
```python
# Remove 'scope' from tool schema entirely

# In handler, remove validation:
# raw_scope = arguments.get("scope")  # No longer check
# Just always use all_related behavior
```

---

### 4. Return Shape Documentation - ADD USAGE HINT ✅

**Original proposal:** Update spec_v1_9.md

**Lou's question:** How does updating a file help Claude understand?

**Context clarification:**
- `spec_v1_9.md` → MCP tool descriptions → Claude sees when calling Sift
- BUT better mechanism: **usage hints**

**Revised approach:**
- ✅ Add usage hint to `artifact(action="query", query_kind="code")` tool
- ✅ Include in usage hint: "Return any JSON value; scalars auto-wrap to [value]"
- ⚠️ Also update spec for human readers

**Usage hint text:**
```
Code queries: return any JSON-serializable value from run(). Scalar
returns are automatically wrapped in a list for consistent pagination
(both `return 42` and `return [42]` produce the same output).
For large result sets, aggregate within your code query to avoid
pagination overhead.
```

---

## Phase 2 Revisions

### Schema Enum Discovery - PERFORMANCE & CONTEXT CONCERNS

**Original proposal:** Compute distinct values for array fields

**Lou's concerns:**
1. **Performance:** Schema inference samples (50 items) for speed. Computing distinct values across ALL records breaks this.
2. **Context bloat:** Sending many examples inflates context, defeating Sift's purpose.

**Current implementation:**
- Samples up to 50 items from arrays (`_FIELD_SAMPLE_LIMIT = 50`)
- Captures field KEYS from all 50 items
- Captures only FIRST VALUE encountered per field

**Proposed solution: Sample-based distinct values**

**Approach:**
- Compute distinct values ONLY from the 50 already-sampled items
- No additional traversal cost (piggyback on existing sampling)
- Cap at 10-15 distinct values per field
- Add note: "Based on sample of N records"

**Implementation:**
```python
# In mapping/schema.py _PathStats:
distinct_values: set[Any] = field(default_factory=set)

# In _walk_value(), for leaf values:
if not isinstance(value, (dict, list)):
    if len(existing.distinct_values) < 15:  # Context cap
        existing.distinct_values.add(value)
```

**Trade-offs:**
- ✅ Zero additional cost (same 50-item traversal)
- ✅ Context bounded: max 15 values per field
- ✅ Likely captures Meta's ~8 action_type values
- ⚠️ Not exhaustive: may miss rare values outside sample
- ⚠️ Cardinality is sample-based, not true global cardinality

**Alternative: Skip entirely**
- Just document in usage hint: "Use code queries to explore value distributions"
- Don't try to solve in schema introspection

**Question for Lou:** Is sample-based distinct values (10-15 max) worth adding, or skip this feature?

---

## Phase 3 Priorities

### Multi-Artifact Queries - HIGH ROI ✅

**Lou's input:** "I think there's a lot of roi here"

**Revised priority:** Move to Phase 2 (ahead of schema enum discovery)

**Why high ROI:**
- Unblocks cross-dimensional analysis (the core Meta Ads use case)
- No workaround exists (user did mental joins)
- High user value despite implementation complexity

**Design questions:**
1. **Signature:** `artifact_ids: [str]` or `artifacts: {name: id}` (named)?
2. **Worker contract:** `run(artifacts: dict[str, list], ...)` or `run(data, related_data, ...)`?
3. **Security:** All artifacts must pass visibility checks
4. **Pagination:** How to cursor over multi-artifact results?

**Proposed API:**
```python
# Option A: List of IDs (positional)
artifact(action="query", query_kind="code",
         artifact_ids=["art_123", "art_456", "art_789"],
         code="def run(artifacts, schemas, params): ...")

# Worker receives:
artifacts = {
    "art_123": [...],  # Keyed by artifact_id
    "art_456": [...],
    "art_789": [...]
}

# Option B: Named artifacts (semantic)
artifact(action="query", query_kind="code",
         artifacts={
             "age_gender": "art_123",
             "placements": "art_456",
             "campaigns": "art_789"
         },
         code="def run(artifacts, schemas, params): ...")

# Worker receives:
artifacts = {
    "age_gender": [...],  # Keyed by user-provided name
    "placements": [...],
    "campaigns": [...]
}
```

**Question for Lou:** Prefer positional (artifact_ids list) or named (artifacts dict)?

---

## Phase 3 Other

### Issue #4: Scope Parameter

**Lou:** "If we only accept one, why require it at all?"

**Agreed.** Revised Phase 1 item: Remove entirely (not just make optional).

---

### Issue #6: Return Shape - Use Usage Hints ✅

**Lou:** "Remember that anything that there was confusion, should be solved with usage hints"

**Agreed.** Revised Phase 1 item: Add usage hint explaining auto-wrapping behavior.

---

## Summary of Changes

### Phase 1 (Quick Wins)
1. **Pagination** → RECONSIDER: Budget-driven, not row-count-driven (or remove entirely?)
2. **Error tracebacks** → APPROVED unchanged
3. **Scope parameter** → REVISED: Remove entirely (not just optional)
4. **Return shape** → REVISED: Usage hint (not just docs)

### Phase 2 (Medium Features)
1. **Multi-artifact queries** → PROMOTED from Phase 3 (Lou: high ROI)
2. **Schema enum discovery** → DOWNGRADED: Sample-based only (10-15 values), or skip?

### Phase 3 (Architectural)
1. **Intermediate state** → Keep as-is (deferred)

---

## Questions for Lou

1. **Pagination:** Remove code query pagination entirely (budget-only), or make smarter?
2. **Multi-artifact API:** Positional (`artifact_ids: [str]`) or named (`artifacts: {name: id}`)?
3. **Schema enums:** Worth doing sample-based (15-value cap), or skip feature entirely?
