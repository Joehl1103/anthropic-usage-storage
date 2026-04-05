# CLAUDE.md — Anthropic Usage Stats Collector

## Overview

Python CLI tool that fetches usage/cost data from the Anthropic Admin API and stores it in SQLite. Designed for cron scheduling and local reporting.

## Tech stack

- Python 3.10+ (stdlib sqlite3, argparse)
- httpx for HTTP requests
- pytest for testing

## Architecture

Functional style, dependency injection (all DB functions take `sqlite3.Connection`). No global state.

```
src/db.py       — SQLite layer: init, upsert, query
src/api.py      — HTTP client: paginated fetch from Admin API
src/limits.py   — Compare usage vs plan limits, emit warnings
src/reports.py  — Format weekly/daily/overage reports as text tables
src/collect.py  — CLI entry point, orchestrates all modules
```

## Key decisions

- `INSERT OR REPLACE` for idempotent upserts (safe to re-run)
- WAL mode on SQLite for concurrent read access
- 25-hour default lookback with 1h buckets (overlapping window prevents gaps)
- Exit code 2 when limits exceeded (cron can alert on non-zero exit)

## Testing

```bash
python3 -m pytest tests/ -v
```

80 tests covering all modules. API tests use mocks (no real HTTP calls).

## Projects and Tasks

- No outstanding tasks.
