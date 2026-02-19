# OpenClaw Response Templates

Use these compact templates to keep agent replies small and actionable.

## Capture Summary

```text
Captured to <artifact_id> (<records> records, <bytes> bytes).
Next: `sift-gateway query <artifact_id> '<root_path>' --limit <n>`.
```

## Follow-Up Capture

```text
Captured a fresh artifact <artifact_id> for this run.
Next: compare with `sift-gateway diff <previous_id> <artifact_id>` if needed.
```

## Narrow Query Result

```text
Returned <count> rows from <artifact_id> at <root_path>.
Fields: <field_a>, <field_b>, <field_c>.
```

## Paginated Follow-Up

```text
Returned first <count> rows; more available.
Use cursor: `<cursor>` for next page.
```

## Diff Result

```text
Compared <left_artifact_id> vs <right_artifact_id>: equal=<true|false>.
<if false> Included bounded unified diff (<lines> lines).
```

## Failure Template

```text
Capture/query failed: <code> - <message>.
Action: <single next command to unblock>.
```
