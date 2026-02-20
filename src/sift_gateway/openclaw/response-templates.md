# OpenClaw Response Templates

Compact templates for low-token agent replies.

## Capture summary

```text
Captured to <artifact_id> (<records> records, <bytes> bytes).
Next: run `sift-gateway code <artifact_id> '$' --expr "<narrow_expr>"`.
```

## Follow-up capture

```text
Captured page artifact <artifact_id> linked to <parent_artifact_id>.
Next: continue with `sift-gateway run --continue-from <artifact_id> -- <next-command>` if more pages remain.
```

## Code result

```text
Computed <summary> from <artifact_id>.
Returned <count> result rows.
```

## Pagination follow-up

```text
Upstream pagination is PARTIAL for <artifact_id>.
Use: `sift-gateway run --continue-from <artifact_id> -- <next-command-with-next_params-applied>`.
```

## Failure template

```text
Operation failed: <code> - <message>.
Next action: <single command to unblock>.
```
