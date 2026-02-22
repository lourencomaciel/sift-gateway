Please look at this file:
`/Users/lourenco/GitHub/sift-gateway/docs/openclaw/testdata/squad_registry_v3.json`

I need one answer: which timezone appears most often among members who are enabled, whose lead competency is java, and whose utilization is at least 70?

If multiple timezones are tied, return the alphabetically first timezone string.

You must use the Sift flow for the data query:
1. capture with `sift-gateway run --json -- cat <path>`
2. query with `sift-gateway code --json` (choose the appropriate root path based on the captured structure)

Do not compute the business answer by directly querying the raw JSON with `jq` or Python. You may use `jq`/Python only to compute audit metrics (printed KB and elapsed seconds).

For auditability, keep a full transcript of your run at:
`/Users/lourenco/GitHub/sift-gateway/tmp/openclaw_logs/run_log_with_sift.txt`

The transcript must include every command plus complete stdout/stderr, with command start timestamps in ISO-8601 UTC.

At the end of that same file, append:

```
FINAL_ANSWER=<timezone>
TOTAL_CLI_OUTPUT_KB=<base2_kib_with_3_decimals>
TOTAL_RUNTIME_SECONDS=<seconds_with_3_decimals>
```
