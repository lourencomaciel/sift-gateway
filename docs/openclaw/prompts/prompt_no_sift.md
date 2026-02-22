Please look at this file:
`/Users/lourenco/GitHub/sift-gateway/docs/openclaw/testdata/employee_roster_v2.json`

I need one answer: which timezone appears most often among people who are active contractors and whose main skill is rust?

If multiple timezones are tied, return the alphabetically first timezone string.

Use only regular command-line processing (for example `jq`, `python`, or `awk`). Do not use `sift-gateway`, OpenClaw skills, or artifact capture tools.

For auditability, keep a full transcript of your run at:
`/Users/lourenco/GitHub/sift-gateway/tmp/openclaw_logs/run_log_no_sift.txt`

The transcript must include every command plus complete stdout/stderr, with command start timestamps in ISO-8601 UTC.

At the end of that same file, append:

```
FINAL_ANSWER=<timezone>
TOTAL_CLI_OUTPUT_KB=<base2_kib_with_3_decimals>
TOTAL_RUNTIME_SECONDS=<seconds_with_3_decimals>
```
