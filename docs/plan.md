# Implementation Plan: Anthropic Usage Stats Collector

**Source spec:** `anthropic-usage-stats-spec.md`
**Date:** 2026-04-04

---

## Module Breakdown

### 1. `src/db.py` — Database layer
- `init_db(db_path)` — create tables if not exist (4 tables from spec)
- `upsert_usage_records(conn, records)` — INSERT OR REPLACE usage data
- `upsert_cost_records(conn, records)` — INSERT OR REPLACE cost data
- `log_sync(conn, status, count, error)` — write to sync_log
- `set_plan_limits(conn, **kwargs)` — insert/update plan_limits
- `get_plan_limits(conn, date)` — fetch active limits for a date
- `get_daily_totals(conn, date)` — aggregate tokens/cost for a day
- `get_weekly_totals(conn, date)` — aggregate tokens/cost for current week

### 2. `src/api.py` — API client
- `fetch_usage(api_key, start, end, bucket_width, group_by)` — paginated fetch from /v1/organizations/usage_report/messages
- `fetch_costs(api_key, start, end, group_by)` — paginated fetch from /v1/organizations/cost_report
- Both handle `has_more` / `next_page` pagination
- Use `httpx` for modern async-ready HTTP (sync mode for v1)

### 3. `src/limits.py` — Limit checking
- `check_limits(conn)` — compare today's totals against plan_limits
- Returns list of warnings with severity (warning at ≥80%, critical at ≥100%)
- Prints warnings to stderr

### 4. `src/reports.py` — Report generation
- `report_weekly(conn)` — weekly token + cost summary
- `report_daily(conn)` — daily usage vs plan limits
- `report_overage(conn)` — days/weeks exceeding limits
- Uses Python `tabulate` or manual formatting

### 5. `src/collect.py` — CLI entry point
- argparse CLI matching the spec's interface
- Orchestrates: config → fetch → upsert → check limits → log → report
- Exit codes: 0 success, 1 error, 2 limits exceeded

### 6. `tests/`
- `test_db.py` — schema creation, upserts, dedup, limit queries
- `test_api.py` — mock HTTP responses, pagination, error handling
- `test_limits.py` — threshold detection at 80%/100%
- `test_reports.py` — output formatting
- `test_collect.py` — CLI integration (dry-run mode)

## Execution order
1. Write tests for db module → implement db.py
2. Write tests for api module → implement api.py
3. Write tests for limits module → implement limits.py
4. Write tests for reports module → implement reports.py
5. Wire up collect.py CLI → integration test
6. Add requirements.txt, README, CLAUDE.md
7. Merge to main
