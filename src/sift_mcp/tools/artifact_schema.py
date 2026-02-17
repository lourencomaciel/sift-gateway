"""Shared SQL helpers for persisted schema rows."""

FETCH_SCHEMA_ROOTS_SQL = """
SELECT root_key, root_path, schema_version, schema_hash,
       mode, completeness, observed_records, dataset_hash,
       traversal_contract_version, map_budget_fingerprint
FROM artifact_schema_roots
WHERE workspace_id = %s AND artifact_id = %s
ORDER BY observed_records DESC, root_path ASC
"""

FETCH_SCHEMA_ROOT_BY_PATH_SQL = """
SELECT root_key, root_path, schema_version, schema_hash,
       mode, completeness, observed_records, dataset_hash,
       traversal_contract_version, map_budget_fingerprint
FROM artifact_schema_roots
WHERE workspace_id = %s AND artifact_id = %s AND root_path = %s
LIMIT 1
"""

FETCH_SCHEMA_FIELDS_SQL = """
SELECT field_path, types, nullable, required, observed_count, example_value,
       distinct_values, cardinality
FROM artifact_schema_fields
WHERE workspace_id = %s AND artifact_id = %s AND root_key = %s
ORDER BY field_path ASC
"""
